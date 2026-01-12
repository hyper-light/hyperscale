from __future__ import annotations

import asyncio
import os
import struct
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator

import aiofiles
import aiofiles.os

from hyperscale.logging.lsn import LSN, HybridLamportClock

from ..events.event_type import JobEventType
from .entry_state import WALEntryState
from .wal_entry import WALEntry, HEADER_SIZE

if TYPE_CHECKING:
    from ..events.job_event import JobEventUnion


class NodeWAL:
    """
    Per-node Write-Ahead Log with fsync durability.

    Provides crash recovery for control plane operations.
    Each entry is CRC-checked and fsync'd before acknowledgment.
    """

    __slots__ = (
        "_path",
        "_clock",
        "_file",
        "_write_lock",
        "_next_lsn",
        "_last_synced_lsn",
        "_pending_entries",
        "_closed",
    )

    def __init__(
        self,
        path: Path,
        clock: HybridLamportClock,
    ) -> None:
        self._path = path
        self._clock = clock
        self._file: aiofiles.threadpool.binary.AsyncBufferedIOBase | None = None
        self._write_lock = asyncio.Lock()
        self._next_lsn: int = 0
        self._last_synced_lsn: int = -1
        self._pending_entries: dict[int, WALEntry] = {}
        self._closed = False

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

    async def _recover(self) -> None:
        recovered_entries: list[WALEntry] = []

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

                    if entry.lsn >= self._next_lsn:
                        self._next_lsn = entry.lsn + 1

                    await self._clock.witness(entry.hlc)

                except ValueError:
                    break

        for entry in recovered_entries:
            if entry.state < WALEntryState.APPLIED:
                self._pending_entries[entry.lsn] = entry

        if recovered_entries:
            self._last_synced_lsn = recovered_entries[-1].lsn

        self._file = await aiofiles.open(self._path, mode="ab")

    async def append(
        self,
        event_type: JobEventType,
        payload: bytes,
        fsync: bool = True,
    ) -> WALEntry:
        async with self._write_lock:
            if self._closed:
                raise RuntimeError("WAL is closed")

            hlc = await self._clock.generate()
            lsn = self._next_lsn
            self._next_lsn += 1

            entry = WALEntry(
                lsn=lsn,
                hlc=hlc,
                state=WALEntryState.PENDING,
                event_type=event_type,
                payload=payload,
            )

            entry_bytes = entry.to_bytes()
            await self._file.write(entry_bytes)

            if fsync:
                await self._file.flush()
                os.fsync(self._file.fileno())
                self._last_synced_lsn = lsn

            self._pending_entries[lsn] = entry
            return entry

    async def mark_regional(self, lsn: int) -> None:
        async with self._write_lock:
            if lsn in self._pending_entries:
                entry = self._pending_entries[lsn]
                if entry.state == WALEntryState.PENDING:
                    self._pending_entries[lsn] = entry.with_state(
                        WALEntryState.REGIONAL
                    )

    async def mark_global(self, lsn: int) -> None:
        async with self._write_lock:
            if lsn in self._pending_entries:
                entry = self._pending_entries[lsn]
                if entry.state <= WALEntryState.REGIONAL:
                    self._pending_entries[lsn] = entry.with_state(WALEntryState.GLOBAL)

    async def mark_applied(self, lsn: int) -> None:
        async with self._write_lock:
            if lsn in self._pending_entries:
                entry = self._pending_entries[lsn]
                if entry.state <= WALEntryState.GLOBAL:
                    self._pending_entries[lsn] = entry.with_state(WALEntryState.APPLIED)

    async def compact(self, up_to_lsn: int) -> int:
        async with self._write_lock:
            compacted_count = 0
            lsns_to_remove = []

            for lsn, entry in self._pending_entries.items():
                if lsn <= up_to_lsn and entry.state == WALEntryState.APPLIED:
                    lsns_to_remove.append(lsn)
                    compacted_count += 1

            for lsn in lsns_to_remove:
                del self._pending_entries[lsn]

            return compacted_count

    async def get_pending_entries(self) -> list[WALEntry]:
        async with self._write_lock:
            return [
                entry
                for entry in self._pending_entries.values()
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
    def next_lsn(self) -> int:
        return self._next_lsn

    @property
    def last_synced_lsn(self) -> int:
        return self._last_synced_lsn

    @property
    def pending_count(self) -> int:
        return len(self._pending_entries)

    async def sync(self) -> None:
        async with self._write_lock:
            if self._file and not self._closed:
                await self._file.flush()
                os.fsync(self._file.fileno())
                self._last_synced_lsn = self._next_lsn - 1

    async def close(self) -> None:
        async with self._write_lock:
            if self._file and not self._closed:
                await self._file.flush()
                os.fsync(self._file.fileno())
                await self._file.close()
                self._closed = True
                self._file = None
