from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, AsyncIterator, Mapping

from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock
from hyperscale.distributed.reliability.robust_queue import QueuePutResult, QueueState
from hyperscale.distributed.reliability.backpressure import (
    BackpressureLevel,
    BackpressureSignal,
)

from hyperscale.distributed.ledger.events.event_type import JobEventType
from .entry_state import WALEntryState, TransitionResult
from hyperscale.distributed.ledger.storage_format import (
    StorageFormat,
    UnrecognizedStorageFormatError,
    free_sibling_path,
    require_stable_read,
    set_aside_unrecognized,
)
from hyperscale.logging.hyperscale_logging_models import WALTailDiscarded
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


# Every WAL file begins with this header (AD-39 HLC entry layout). Files
# without it -- earlier layouts, other programs, corruption -- are never
# read as entries.
WAL_FORMAT = StorageFormat(b"HSWL", 1)

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
        "_last_regional_lsn",
        "_last_global_lsn",
    )

    def __init__(
        self,
        path: Path,
        clock: HybridLogicalClock,
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
        # Highest LSN that actually REACHED each replicated tier. Kept
        # separately from entry state because compaction removes the
        # entries: without these the only surviving record of what was
        # replicated disappears the moment a checkpoint runs.
        self._last_regional_lsn = 0
        self._last_global_lsn = 0

    @classmethod
    async def open(
        cls,
        path: Path,
        clock: HybridLogicalClock,
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
            data = await self._filesystem.read_bytes(self._path)
            if await self._accept_existing_file(data):
                recovered_count, recovered_length = self._recover(data)
                if recovered_length < len(data):
                    await self._discard_unrecoverable_tail(data, recovered_count, recovered_length)

        # A new WAL starts with its format header, before any entry.
        if not await self._filesystem.exists(self._path):
            await self._filesystem.append_fsync(self._path, WAL_FORMAT.header)

        await self._writer.start()

    async def _accept_existing_file(self, data: bytes) -> bool:
        """Whether the file on disk is a WAL in this format to recover.

        A torn header (a crash while creating the file, before any entry
        could follow) is rewritten whole. Anything else unrecognized is
        set aside, loudly, and the WAL starts empty -- or, with no logger
        to report it through, refused outright: never read as if it were
        a WAL this node wrote.
        """
        if WAL_FORMAT.is_torn_header(data):
            await self._filesystem.atomic_write(self._path, WAL_FORMAT.header)
            return False
        try:
            WAL_FORMAT.validate(data)
        except UnrecognizedStorageFormatError as format_error:
            if self._logger is None:
                raise
            await set_aside_unrecognized(
                self._filesystem, self._path, data, format_error.reason, self._logger
            )
            return False
        return True

    async def _discard_unrecoverable_tail(
        self, data: bytes, recovered_count: int, recovered_length: int
    ) -> None:
        """Cut the file back to its last recoverable frame.

        Recovery stops at the first torn or corrupt frame; left in place,
        that frame would hide every entry appended after it from the next
        recovery -- acknowledged writes lost on the following restart. The
        discarded bytes (a crash's torn append, or damage) are preserved
        beside the WAL, never destroyed.
        """
        await require_stable_read(self._filesystem, self._path, data)
        preserved_path = await free_sibling_path(self._filesystem, self._path, "discarded")
        await self._filesystem.atomic_write(preserved_path, data[recovered_length:])
        await self._filesystem.atomic_write(self._path, data[:recovered_length])
        if self._logger is not None:
            await self._logger.log(
                WALTailDiscarded(
                    message=(
                        f"WAL {self._path}: {len(data) - recovered_length} bytes after the last "
                        f"recoverable entry discarded (preserved at {preserved_path})"
                    ),
                    path=str(self._path),
                    preserved_path=str(preserved_path),
                    discarded_bytes=len(data) - recovered_length,
                    recovered_entries=recovered_count,
                )
            )

    def _recover(self, data: bytes) -> tuple[int, int]:
        """Recover the entries in ``data``: how many, and the length of
        its recoverable prefix (format header plus every whole, valid
        frame)."""
        recovered_entries, frames_length = self._parse_frames(WAL_FORMAT.decode(data))
        next_lsn = max((entry.lsn + 1 for entry in recovered_entries), default=0)
        last_synced_lsn = recovered_entries[-1].lsn if recovered_entries else -1

        for entry in recovered_entries:
            self._clock.witness(entry.hlc)

            if entry.state < WALEntryState.APPLIED:
                self._pending_entries_internal[entry.lsn] = entry

        self._status_snapshot = WALStatusSnapshot(
            next_lsn=next_lsn,
            last_synced_lsn=last_synced_lsn,
            pending_count=len(self._pending_entries_internal),
            closed=False,
        )
        self._pending_snapshot = MappingProxyType(dict(self._pending_entries_internal))
        return len(recovered_entries), WAL_FORMAT.header_size + frames_length

    @staticmethod
    def _parse_frames(frames: bytes) -> tuple[list[WALEntry], int]:
        """The entries in the bytes after the format header, up to the
        first torn or corrupt frame (a crash mid-append leaves at most
        one, at the tail), and how many bytes those entries span."""
        entries: list[WALEntry] = []
        offset = 0
        while offset + HEADER_SIZE <= len(frames):
            total_length = struct.unpack(">I", frames[offset + 4 : offset + 8])[0]
            if total_length < HEADER_SIZE or offset + total_length > len(frames):
                break
            try:
                entries.append(WALEntry.from_bytes(frames[offset : offset + total_length]))
            except ValueError:
                break
            offset += total_length
        return entries, offset

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

        hlc = self._clock.now()

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
            self._last_regional_lsn = max(self._last_regional_lsn, lsn)
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
            self._last_global_lsn = max(self._last_global_lsn, lsn)
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

        data = await self._filesystem.read_bytes(self._path)
        for entry in self._parse_frames(WAL_FORMAT.decode(data))[0]:
            if entry.lsn >= start_lsn:
                yield entry

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
    def last_regional_lsn(self) -> int:
        """Highest LSN that actually reached REGIONAL durability.

        Zero on a WAL whose entries never replicated -- which is what a
        deployment with no regional replicator configured must report,
        rather than borrowing the local fsync watermark.
        """
        return self._last_regional_lsn

    @property
    def last_global_lsn(self) -> int:
        """Highest LSN that actually reached GLOBAL durability."""
        return self._last_global_lsn

    def restore_durability_watermarks(
        self, regional_lsn: int, global_lsn: int
    ) -> None:
        """Re-seed the replicated watermarks from a checkpoint at
        recovery.

        These live in memory, so a restart would otherwise reset them
        to zero and the next checkpoint would report LESS replication
        than the previous one had already recorded -- a monotonic
        watermark moving backwards. Takes the max so replay can only
        advance them.
        """
        self._last_regional_lsn = max(self._last_regional_lsn, regional_lsn)
        self._last_global_lsn = max(self._last_global_lsn, global_lsn)

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
