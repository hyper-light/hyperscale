from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, AsyncIterator, Mapping

from hyperscale.logging.lsn import HybridLamportClock
from hyperscale.distributed.reliability.robust_queue import QueuePutResult, QueueState
from hyperscale.distributed.reliability.backpressure import (
    BackpressureLevel,
    BackpressureSignal,
)

from ..events.event_type import JobEventType
from .entry_state import WALEntryState, TransitionResult
from .wal_entry import HEADER_SIZE, WALEntry
from .wal_status_snapshot import WALStatusSnapshot
from hyperscale.distributed.runtime import (
    Filesystem,
    RealFilesystem,
)

from .wal_writer import (
    WALWriter,
    WALWriterConfig,
    WriteRequest,
    WALBackpressureError,
)

# Module-level storage seam (Phase 7): borrowed, never shut down here;
# swap_defaults rebinds it under SIM.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()

if TYPE_CHECKING:
    from hyperscale.logging import Logger


@dataclass(slots=True)
class WALAppendResult:
    entry: WALEntry
    queue_result: QueuePutResult

    @property
    def backpressure(self) -> BackpressureSignal:
        return self.queue_result.backpressure

    @property
    def backpressure_level(self) -> BackpressureLevel:
        return self.queue_result.backpressure.level

    @property
    def queue_state(self) -> QueueState:
        return self.queue_result.queue_state

    @property
    def in_overflow(self) -> bool:
        return self.queue_result.in_overflow


class NodeWAL:
    __slots__ = (
        "_path",
        "_clock",
        "_writer",
        "_loop",
        "_pending_entries_internal",
        "_status_snapshot",
        "_pending_snapshot",
        "_state_lock",
        "_logger",
        "_filesystem",
    )

    def __init__(
        self,
        path: Path,
        clock: HybridLamportClock,
        config: WALWriterConfig | None = None,
        logger: Logger | None = None,
        filesystem: Filesystem | None = None,
    ) -> None:
        self._path = path
        self._clock = clock
        self._logger = logger
        # Borrowed, never shut down here — see _DEFAULT_FILESYSTEM.
        self._filesystem = (
            filesystem if filesystem is not None else _DEFAULT_FILESYSTEM
        )
        self._writer = WALWriter(
            path=path,
            config=config,
            logger=logger,
            filesystem=self._filesystem,
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending_entries_internal: dict[int, WALEntry] = {}
        self._status_snapshot = WALStatusSnapshot.initial()
        self._pending_snapshot: Mapping[int, WALEntry] = MappingProxyType({})
        self._state_lock = asyncio.Lock()

    @classmethod
    async def open(
        cls,
        path: Path,
        clock: HybridLamportClock,
        config: WALWriterConfig | None = None,
        logger: Logger | None = None,
        filesystem: Filesystem | None = None,
    ) -> NodeWAL:
        wal = cls(
            path=path,
            clock=clock,
            config=config,
            logger=logger,
            filesystem=filesystem,
        )
        await wal._initialize()
        return wal

    async def _initialize(self) -> None:
        self._loop = asyncio.get_running_loop()
        await self._filesystem.mkdir(
            self._path.parent, parents=True, exist_ok=True
        )

        if await self._filesystem.exists(self._path):
            await self._recover()

        await self._writer.start()

    async def _recover(self) -> None:
        loop = self._loop
        assert loop is not None

        data = await self._filesystem.read_bytes(self._path)
        recovered_entries, next_lsn, last_synced_lsn = (
            self._parse_recovery_frames(data)
        )

        for entry in recovered_entries:
            await self._clock.witness(entry.hlc)

            if entry.state < WALEntryState.APPLIED:
                self._pending_entries_internal[entry.lsn] = entry

        self._status_snapshot = WALStatusSnapshot(
            next_lsn=next_lsn,
            last_synced_lsn=last_synced_lsn,
            pending_count=len(self._pending_entries_internal),
            closed=False,
        )
        self._pending_snapshot = MappingProxyType(dict(self._pending_entries_internal))

    @staticmethod
    def _parse_recovery_frames(
        data: bytes,
    ) -> tuple[list[WALEntry], int, int]:
        recovered_entries: list[WALEntry] = []
        next_lsn = 0
        last_synced_lsn = -1

        offset = 0
        while offset < len(data):
            if offset + HEADER_SIZE > len(data):
                break

            header_data = data[offset : offset + HEADER_SIZE]
            total_length = struct.unpack(">I", header_data[4:8])[0]
            payload_length = total_length - HEADER_SIZE

            if payload_length < 0:
                break

            if offset + total_length > len(data):
                break

            full_entry = data[offset : offset + total_length]

            try:
                entry = WALEntry.from_bytes(full_entry)
                recovered_entries.append(entry)

                if entry.lsn >= next_lsn:
                    next_lsn = entry.lsn + 1

            except ValueError:
                break

            offset += total_length

        if recovered_entries:
            last_synced_lsn = recovered_entries[-1].lsn

        return recovered_entries, next_lsn, last_synced_lsn

    async def append(
        self,
        event_type: JobEventType,
        payload: bytes,
    ) -> WALAppendResult:
        if self._status_snapshot.closed:
            raise RuntimeError("WAL is closed")

        if self._writer.has_error:
            raise RuntimeError(f"WAL writer failed: {self._writer.error}")

        loop = self._loop
        assert loop is not None

        hlc = await self._clock.generate()

        async with self._state_lock:
            lsn = self._status_snapshot.next_lsn

            entry = WALEntry(
                lsn=lsn,
                hlc=hlc,
                state=WALEntryState.PENDING,
                event_type=event_type,
                payload=payload,
            )

            entry_bytes = entry.to_bytes()
            future: asyncio.Future[None] = loop.create_future()
            request = WriteRequest(data=entry_bytes, future=future)

            queue_result = self._writer.submit(request)

            if not queue_result.accepted:
                raise WALBackpressureError(
                    f"WAL rejected write due to backpressure: {queue_result.queue_state.name}",
                    queue_state=queue_result.queue_state,
                    backpressure=queue_result.backpressure,
                )

            self._pending_entries_internal[lsn] = entry

            self._status_snapshot = WALStatusSnapshot(
                next_lsn=lsn + 1,
                last_synced_lsn=self._status_snapshot.last_synced_lsn,
                pending_count=len(self._pending_entries_internal),
                closed=False,
            )
            self._pending_snapshot = MappingProxyType(
                dict(self._pending_entries_internal)
            )

        await future

        async with self._state_lock:
            self._status_snapshot = WALStatusSnapshot(
                next_lsn=self._status_snapshot.next_lsn,
                last_synced_lsn=lsn,
                pending_count=self._status_snapshot.pending_count,
                closed=False,
            )

        return WALAppendResult(entry=entry, queue_result=queue_result)

    async def mark_regional(self, lsn: int) -> TransitionResult:
        async with self._state_lock:
            entry = self._pending_entries_internal.get(lsn)
            if entry is None:
                return TransitionResult.ENTRY_NOT_FOUND

            if entry.state == WALEntryState.REGIONAL:
                return TransitionResult.ALREADY_AT_STATE

            if entry.state > WALEntryState.REGIONAL:
                return TransitionResult.ALREADY_PAST_STATE

            if entry.state != WALEntryState.PENDING:
                return TransitionResult.INVALID_TRANSITION

            self._pending_entries_internal[lsn] = entry.with_state(
                WALEntryState.REGIONAL
            )
            self._pending_snapshot = MappingProxyType(
                dict(self._pending_entries_internal)
            )
            return TransitionResult.SUCCESS

    async def mark_global(self, lsn: int) -> TransitionResult:
        async with self._state_lock:
            entry = self._pending_entries_internal.get(lsn)
            if entry is None:
                return TransitionResult.ENTRY_NOT_FOUND

            if entry.state == WALEntryState.GLOBAL:
                return TransitionResult.ALREADY_AT_STATE

            if entry.state > WALEntryState.GLOBAL:
                return TransitionResult.ALREADY_PAST_STATE

            if entry.state > WALEntryState.REGIONAL:
                return TransitionResult.INVALID_TRANSITION

            self._pending_entries_internal[lsn] = entry.with_state(WALEntryState.GLOBAL)
            self._pending_snapshot = MappingProxyType(
                dict(self._pending_entries_internal)
            )
            return TransitionResult.SUCCESS

    async def mark_applied(self, lsn: int) -> TransitionResult:
        async with self._state_lock:
            entry = self._pending_entries_internal.get(lsn)
            if entry is None:
                return TransitionResult.ENTRY_NOT_FOUND

            if entry.state == WALEntryState.APPLIED:
                return TransitionResult.ALREADY_AT_STATE

            if entry.state > WALEntryState.APPLIED:
                return TransitionResult.ALREADY_PAST_STATE

            if entry.state > WALEntryState.GLOBAL:
                return TransitionResult.INVALID_TRANSITION

            self._pending_entries_internal[lsn] = entry.with_state(
                WALEntryState.APPLIED
            )
            self._pending_snapshot = MappingProxyType(
                dict(self._pending_entries_internal)
            )
            return TransitionResult.SUCCESS

    async def compact(self, up_to_lsn: int) -> int:
        async with self._state_lock:
            compacted_count = 0
            lsns_to_remove = []

            for lsn, entry in list(self._pending_entries_internal.items()):
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
        loop = self._loop
        assert loop is not None

        if await self._filesystem.exists(self._path):
            data = await self._filesystem.read_bytes(self._path)
        else:
            data = b""
        entries = self._parse_entries_from(data, start_lsn)

        for entry in entries:
            yield entry

    @staticmethod
    def _parse_entries_from(data: bytes, start_lsn: int) -> list[WALEntry]:
        entries: list[WALEntry] = []

        offset = 0
        while offset < len(data):
            if offset + HEADER_SIZE > len(data):
                break

            header_data = data[offset : offset + HEADER_SIZE]
            total_length = struct.unpack(">I", header_data[4:8])[0]
            payload_length = total_length - HEADER_SIZE

            if payload_length < 0:
                break

            if offset + total_length > len(data):
                break

            full_entry = data[offset : offset + total_length]

            try:
                entry = WALEntry.from_bytes(full_entry)
                if entry.lsn >= start_lsn:
                    entries.append(entry)
            except ValueError:
                break

            offset += total_length

        return entries

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

    @property
    def backpressure_level(self) -> BackpressureLevel:
        return self._writer.backpressure_level

    @property
    def queue_state(self) -> QueueState:
        return self._writer.queue_state

    def get_metrics(self) -> dict:
        return self._writer.get_queue_metrics()

    async def close(self) -> None:
        async with self._state_lock:
            if not self._status_snapshot.closed:
                await self._writer.stop()

                self._status_snapshot = WALStatusSnapshot(
                    next_lsn=self._status_snapshot.next_lsn,
                    last_synced_lsn=self._status_snapshot.last_synced_lsn,
                    pending_count=self._status_snapshot.pending_count,
                    closed=True,
                )
