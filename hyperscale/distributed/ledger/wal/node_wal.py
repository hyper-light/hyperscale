from __future__ import annotations

import asyncio
import os
import struct
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, AsyncIterator, Mapping

import aiofiles

from hyperscale.logging.lsn import HybridLamportClock

from ..events.event_type import JobEventType
from .entry_state import WALEntryState
from .wal_entry import WALEntry, HEADER_SIZE
from .wal_status_snapshot import WALStatusSnapshot

if TYPE_CHECKING:
    pass


class NodeWAL:
    __slots__ = (
        "_path",
        "_clock",
        "_file",
        "_write_lock",
        "_pending_entries_internal",
        "_status_snapshot",
        "_pending_snapshot",
    )

    def __init__(
        self,
        path: Path,
        clock: HybridLamportClock,
    ) -> None:
        self._path = path
        self._clock = clock
        self._file: Any = None
        self._write_lock = asyncio.Lock()
        self._pending_entries_internal: dict[int, WALEntry] = {}
        self._status_snapshot = WALStatusSnapshot.initial()
        self._pending_snapshot: Mapping[int, WALEntry] = MappingProxyType({})

    @classmethod
    async def open(
        cls,
        path: Path,
        clock: HybridLamportClock,
    ) -> NodeWAL:
        wal = cls(path=path, clock=clock)
        await wal._initialize()
        return wal

    async def _initialize(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)

        if self._path.exists():
            await self._recover()
        else:
            self._file = await aiofiles.open(self._path, mode="ab")
            self._publish_snapshot()

    async def _recover(self) -> None:
        recovered_entries: list[WALEntry] = []
        next_lsn = 0
        last_synced_lsn = -1

        async with aiofiles.open(self._path, mode="rb") as file:
            while True:
                header_data = await file.read(HEADER_SIZE)
                if len(header_data) == 0:
                    break

                if len(header_data) < HEADER_SIZE:
                    break

                total_length = struct.unpack(">I", header_data[4:8])[0]
                payload_length = total_length - HEADER_SIZE

                if payload_length < 0:
                    break

                payload_data = await file.read(payload_length)
                if len(payload_data) < payload_length:
                    break

                full_entry = header_data + payload_data

                try:
                    entry = WALEntry.from_bytes(full_entry)
                    recovered_entries.append(entry)

                    if entry.lsn >= next_lsn:
                        next_lsn = entry.lsn + 1

                    await self._clock.witness(entry.hlc)

                except ValueError:
                    break

        for entry in recovered_entries:
            if entry.state < WALEntryState.APPLIED:
                self._pending_entries_internal[entry.lsn] = entry

        if recovered_entries:
            last_synced_lsn = recovered_entries[-1].lsn

        self._file = await aiofiles.open(self._path, mode="ab")

        self._status_snapshot = WALStatusSnapshot(
            next_lsn=next_lsn,
            last_synced_lsn=last_synced_lsn,
            pending_count=len(self._pending_entries_internal),
            closed=False,
        )
        self._pending_snapshot = MappingProxyType(dict(self._pending_entries_internal))

    def _publish_snapshot(self) -> None:
        self._status_snapshot = WALStatusSnapshot(
            next_lsn=self._status_snapshot.next_lsn,
            last_synced_lsn=self._status_snapshot.last_synced_lsn,
            pending_count=len(self._pending_entries_internal),
            closed=self._status_snapshot.closed,
        )
        self._pending_snapshot = MappingProxyType(dict(self._pending_entries_internal))

    async def append(
        self,
        event_type: JobEventType,
        payload: bytes,
        fsync: bool = True,
    ) -> WALEntry:
        async with self._write_lock:
            if self._status_snapshot.closed:
                raise RuntimeError("WAL is closed")

            hlc = await self._clock.generate()
            lsn = self._status_snapshot.next_lsn

            entry = WALEntry(
                lsn=lsn,
                hlc=hlc,
                state=WALEntryState.PENDING,
                event_type=event_type,
                payload=payload,
            )

            entry_bytes = entry.to_bytes()
            await self._file.write(entry_bytes)

            new_last_synced = self._status_snapshot.last_synced_lsn
            if fsync:
                await self._file.flush()
                os.fsync(self._file.fileno())
                new_last_synced = lsn

            self._pending_entries_internal[lsn] = entry

            self._status_snapshot = WALStatusSnapshot(
                next_lsn=lsn + 1,
                last_synced_lsn=new_last_synced,
                pending_count=len(self._pending_entries_internal),
                closed=False,
            )
            self._pending_snapshot = MappingProxyType(
                dict(self._pending_entries_internal)
            )

            return entry

    async def mark_regional(self, lsn: int) -> None:
        async with self._write_lock:
            if lsn in self._pending_entries_internal:
                entry = self._pending_entries_internal[lsn]
                if entry.state == WALEntryState.PENDING:
                    self._pending_entries_internal[lsn] = entry.with_state(
                        WALEntryState.REGIONAL
                    )
                    self._pending_snapshot = MappingProxyType(
                        dict(self._pending_entries_internal)
                    )

    async def mark_global(self, lsn: int) -> None:
        async with self._write_lock:
            if lsn in self._pending_entries_internal:
                entry = self._pending_entries_internal[lsn]
                if entry.state <= WALEntryState.REGIONAL:
                    self._pending_entries_internal[lsn] = entry.with_state(
                        WALEntryState.GLOBAL
                    )
                    self._pending_snapshot = MappingProxyType(
                        dict(self._pending_entries_internal)
                    )

    async def mark_applied(self, lsn: int) -> None:
        async with self._write_lock:
            if lsn in self._pending_entries_internal:
                entry = self._pending_entries_internal[lsn]
                if entry.state <= WALEntryState.GLOBAL:
                    self._pending_entries_internal[lsn] = entry.with_state(
                        WALEntryState.APPLIED
                    )
                    self._pending_snapshot = MappingProxyType(
                        dict(self._pending_entries_internal)
                    )

    async def compact(self, up_to_lsn: int) -> int:
        async with self._write_lock:
            compacted_count = 0
            lsns_to_remove = []

            for lsn, entry in self._pending_entries_internal.items():
                if lsn <= up_to_lsn and entry.state == WALEntryState.APPLIED:
                    lsns_to_remove.append(lsn)
                    compacted_count += 1

            for lsn in lsns_to_remove:
                del self._pending_entries_internal[lsn]

            if compacted_count > 0:
                self._status_snapshot = WALStatusSnapshot(
                    next_lsn=self._status_snapshot.next_lsn,
                    last_synced_lsn=self._status_snapshot.last_synced_lsn,
                    pending_count=len(self._pending_entries_internal),
                    closed=self._status_snapshot.closed,
                )
                self._pending_snapshot = MappingProxyType(
                    dict(self._pending_entries_internal)
                )

            return compacted_count

    def get_pending_entries(self) -> list[WALEntry]:
        return [
            entry
            for entry in self._pending_snapshot.values()
            if entry.state < WALEntryState.APPLIED
        ]

    async def iter_from(self, start_lsn: int) -> AsyncIterator[WALEntry]:
        async with aiofiles.open(self._path, mode="rb") as file:
            while True:
                header_data = await file.read(HEADER_SIZE)
                if len(header_data) == 0:
                    break

                if len(header_data) < HEADER_SIZE:
                    break

                total_length = struct.unpack(">I", header_data[4:8])[0]
                payload_length = total_length - HEADER_SIZE

                if payload_length < 0:
                    break

                payload_data = await file.read(payload_length)
                if len(payload_data) < payload_length:
                    break

                full_entry = header_data + payload_data

                try:
                    entry = WALEntry.from_bytes(full_entry)
                    if entry.lsn >= start_lsn:
                        yield entry
                except ValueError:
                    break

    @property
    def status(self) -> WALStatusSnapshot:
        return self._status_snapshot

    @property
    def next_lsn(self) -> int:
        return self._status_snapshot.next_lsn

    @property
    def last_synced_lsn(self) -> int:
        return self._status_snapshot.last_synced_lsn

    @property
    def pending_count(self) -> int:
        return self._status_snapshot.pending_count

    @property
    def is_closed(self) -> bool:
        return self._status_snapshot.closed

    async def sync(self) -> None:
        async with self._write_lock:
            if self._file and not self._status_snapshot.closed:
                await self._file.flush()
                os.fsync(self._file.fileno())

                self._status_snapshot = WALStatusSnapshot(
                    next_lsn=self._status_snapshot.next_lsn,
                    last_synced_lsn=self._status_snapshot.next_lsn - 1,
                    pending_count=self._status_snapshot.pending_count,
                    closed=False,
                )

    async def close(self) -> None:
        async with self._write_lock:
            if self._file and not self._status_snapshot.closed:
                await self._file.flush()
                os.fsync(self._file.fileno())
                await self._file.close()
                self._file = None

                self._status_snapshot = WALStatusSnapshot(
                    next_lsn=self._status_snapshot.next_lsn,
                    last_synced_lsn=self._status_snapshot.last_synced_lsn,
                    pending_count=self._status_snapshot.pending_count,
                    closed=True,
                )
