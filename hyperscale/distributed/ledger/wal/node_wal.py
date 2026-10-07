"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, AsyncIterator, Mapping
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp
from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock
from hyperscale.distributed.reliability.robust_queue import QueuePutResult, QueueState
from hyperscale.distributed.reliability.backpressure import BackpressureLevel, BackpressureSignal
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.storage_format import (
    StorageFormat,
    UnrecognizedStorageFormatError,
    free_sibling_path,
    require_stable_read,
    set_aside_unrecognized,
)
from hyperscale.logging.hyperscale_logging_models import WALTailDiscarded
from hyperscale.distributed.runtime import Filesystem, RealFilesystem

from .entry_state import WALEntryState, TransitionResult
from .wal_entry import HEADER_SIZE, WALEntry
from .wal_status_snapshot import WALStatusSnapshot
from .wal_writer import WALWriter, WALWriterConfig, WriteRequest, WALBackpressureError
from .wal_append_result import WALAppendResult

if TYPE_CHECKING:
    from hyperscale.logging import Logger

# Module-level storage seam (Phase 7): borrowed, never shut down here;
# swap_defaults rebinds it under SIM.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()

# Every WAL file begins with this header (AD-39 HLC entry layout). Files
# without it -- earlier layouts, other programs, corruption -- are never
# read as entries.
WAL_FORMAT = StorageFormat(b"HSWL", 1)


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
        storage_health: StorageHealth | None = None,
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
            storage_health=storage_health,
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
        storage_health: StorageHealth | None = None,
    ) -> NodeWAL:
        wal = cls(
            path=path,
            clock=clock,
            config=config,
            logger=logger,
            filesystem=filesystem,
            storage_health=storage_health,
        )
        await wal._initialize()
        return wal

    async def _initialize(self) -> None:
        self._loop = asyncio.get_running_loop()
        await self._filesystem.mkdir(
            self._path.parent, parents=True, exist_ok=True
        )

        if await self._filesystem.exists(self._path):
            await self._recover_existing_file()

        # A new WAL starts with its format header, before any entry.
        if not await self._filesystem.exists(self._path):
            await self._filesystem.append_fsync(self._path, WAL_FORMAT.header)

        await self._writer.start()

    async def _recover_existing_file(self) -> None:
        """Recover the WAL file on disk, cutting back any unrecoverable
        tail (a file not accepted as this format recovers nothing)."""
        data = await self._filesystem.read_bytes(self._path)
        if not await self._accept_existing_file(data):
            return
        recovered_count, recovered_length = self._recover(data)
        if recovered_length < len(data):
            await self._discard_unrecoverable_tail(data, recovered_count, recovered_length)

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
            await self._set_aside_unrecognized_file(data, format_error)
            return False
        return True

    async def _set_aside_unrecognized_file(
        self, data: bytes, format_error: UnrecognizedStorageFormatError
    ) -> None:
        """Set an unrecognized file aside, loudly; with no logger to
        report it through, re-raise the format error being handled."""
        if self._logger is None:
            raise
        await set_aside_unrecognized(
            self._filesystem, self._path, data, format_error.reason, self._logger
        )

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

        self._adopt_recovered_entries(recovered_entries)

        self._status_snapshot = WALStatusSnapshot(
            next_lsn=next_lsn,
            last_synced_lsn=last_synced_lsn,
            pending_count=len(self._pending_entries_internal),
            closed=False,
        )
        self._pending_snapshot = MappingProxyType(dict(self._pending_entries_internal))
        return len(recovered_entries), WAL_FORMAT.header_size + frames_length

    def _adopt_recovered_entries(self, recovered_entries: list[WALEntry]) -> None:
        """Witness every recovered entry's HLC and hold each one not yet
        applied as pending."""
        for entry in recovered_entries:
            self._clock.witness(entry.hlc)

            if entry.state < WALEntryState.APPLIED:
                self._pending_entries_internal[entry.lsn] = entry

    @staticmethod
    def _parse_frames(frames: bytes) -> tuple[list[WALEntry], int]:
        """The entries in the bytes after the format header, up to the
        first torn or corrupt frame (a crash mid-append leaves at most
        one, at the tail), and how many bytes those entries span."""
        entries: list[WALEntry] = []
        offset = 0
        while offset + HEADER_SIZE <= len(frames):
            if (next_offset := NodeWAL._parse_next_frame(frames, offset, entries)) is None:
                break
            offset = next_offset
        return entries, offset

    @staticmethod
    def _parse_next_frame(frames: bytes, offset: int, entries: list[WALEntry]) -> int | None:
        """Append the whole, valid frame at ``offset`` to ``entries`` and
        return the offset past it; None at a torn or corrupt frame."""
        total_length = struct.unpack(">I", frames[offset + 4 : offset + 8])[0]
        if NodeWAL._frame_is_torn(frames, offset, total_length) or not NodeWAL._append_frame(
            entries, frames[offset : offset + total_length]
        ):
            return None
        return offset + total_length

    @staticmethod
    def _frame_is_torn(frames: bytes, offset: int, total_length: int) -> bool:
        """Whether the frame at ``offset`` claims an impossible length or
        runs past the end of ``frames``."""
        return total_length < HEADER_SIZE or offset + total_length > len(frames)

    @staticmethod
    def _append_frame(entries: list[WALEntry], frame: bytes) -> bool:
        """Decode ``frame`` onto ``entries``; whether it decoded."""
        try:
            entries.append(WALEntry.from_bytes(frame))
        except ValueError:
            return False
        return True

    async def append(
        self,
        event_type: JobEventType,
        payload: bytes,
    ) -> WALAppendResult:
        self._require_open_writer()

        loop = self._loop
        assert loop is not None

        hlc = self._clock.now()

        entry, future, queue_result, lsn = await self._enqueue_entry(loop, hlc, event_type, payload)

        try:
            await future
        except BaseException:
            # The write failed (the writer rolled the log back): this
            # entry was never durable, so it must not linger as pending.
            # Its LSN stays consumed -- recovery tolerates the gap.
            await self._forget_unwritten_entry(lsn)
            raise

        async with self._state_lock:
            # Appenders of one batch resume in any order once it syncs; the
            # watermark only ever rises.
            self._status_snapshot = WALStatusSnapshot(
                next_lsn=self._status_snapshot.next_lsn,
                last_synced_lsn=max(self._status_snapshot.last_synced_lsn, lsn),
                pending_count=self._status_snapshot.pending_count,
                closed=False,
            )

        return WALAppendResult(entry=entry, queue_result=queue_result)

    def _require_open_writer(self) -> None:
        """Refuse an append to a closed WAL or one whose writer failed."""
        if self._status_snapshot.closed:
            raise RuntimeError("WAL is closed")

        if self._writer.has_error:
            raise RuntimeError(f"WAL writer failed: {self._writer.error}")

    async def _enqueue_entry(
        self,
        loop: asyncio.AbstractEventLoop,
        hlc: HLCTimestamp,
        event_type: JobEventType,
        payload: bytes,
    ) -> tuple[WALEntry, asyncio.Future[None], QueuePutResult, int]:
        """Under the state lock, assign the next LSN and queue the entry's
        write, holding it as pending: the entry, the write's future, the
        queue's verdict and the LSN."""
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

        return entry, future, queue_result, lsn

    async def _forget_unwritten_entry(self, lsn: int) -> None:
        async with self._state_lock:
            if self._pending_entries_internal.pop(lsn, None) is None:
                return
            self._status_snapshot = WALStatusSnapshot(
                next_lsn=self._status_snapshot.next_lsn,
                last_synced_lsn=self._status_snapshot.last_synced_lsn,
                pending_count=len(self._pending_entries_internal),
                closed=self._status_snapshot.closed,
            )
            self._pending_snapshot = MappingProxyType(
                dict(self._pending_entries_internal)
            )

    @staticmethod
    def _refuse_transition(entry: WALEntry | None, target_state: WALEntryState) -> TransitionResult | None:
        """Why ``entry`` cannot move to ``target_state`` because it is
        missing or already there or past it; None when neither."""
        if entry is None:
            return TransitionResult.ENTRY_NOT_FOUND
        return NodeWAL._refuse_reached_state(entry.state, target_state)

    @staticmethod
    def _refuse_reached_state(state: WALEntryState, target_state: WALEntryState) -> TransitionResult | None:
        """ALREADY_AT_STATE or ALREADY_PAST_STATE when ``state`` has
        reached ``target_state``; None otherwise."""
        if state == target_state:
            return TransitionResult.ALREADY_AT_STATE

        if state > target_state:
            return TransitionResult.ALREADY_PAST_STATE

        return None

    async def mark_regional(self, lsn: int) -> TransitionResult:
        async with self._state_lock:
            entry = self._pending_entries_internal.get(lsn)
            if (refusal := self._refuse_transition(entry, WALEntryState.REGIONAL)) is not None:
                return refusal

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
            if (refusal := self._refuse_transition(entry, WALEntryState.GLOBAL)) is not None:
                return refusal

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
            if (refusal := self._refuse_transition(entry, WALEntryState.APPLIED)) is not None:
                return refusal

            if entry.state > WALEntryState.GLOBAL:
                return TransitionResult.INVALID_TRANSITION

            self._pending_entries_internal[lsn] = entry.with_state(
                WALEntryState.APPLIED
            )
            self._pending_snapshot = MappingProxyType(
                dict(self._pending_entries_internal)
            )
            return TransitionResult.SUCCESS

    async def mark_applied_through(self, up_to_lsn: int) -> int:
        """Mark every entry at or below ``up_to_lsn`` APPLIED in one pass --
        recovery's: the ledger applied each recovered entry (replayed, or
        held by the checkpoint it resumed from), and an entry never marked
        applied could never be compacted. Returns how many moved."""
        async with self._state_lock:
            applied_count = self._apply_pending_through(up_to_lsn)
            if applied_count > 0:
                self._pending_snapshot = MappingProxyType(dict(self._pending_entries_internal))
            return applied_count

    def _apply_pending_through(self, up_to_lsn: int) -> int:
        """Move every unapplied entry at or below ``up_to_lsn`` to APPLIED
        (the caller holds the state lock); how many moved."""
        applied_count = 0
        for lsn, entry in self._pending_entries_internal.items():
            if self._awaits_apply_through(lsn, entry, up_to_lsn):
                self._pending_entries_internal[lsn] = entry.with_state(WALEntryState.APPLIED)
                applied_count += 1
        return applied_count

    @staticmethod
    def _awaits_apply_through(lsn: int, entry: WALEntry, up_to_lsn: int) -> bool:
        """Whether ``entry`` is at or below ``up_to_lsn`` and not yet applied."""
        return lsn <= up_to_lsn and entry.state < WALEntryState.APPLIED

    async def discard_through(self, lsn: int) -> int:
        """Drop every frame at or below ``lsn`` from the log file -- they
        are held by a checkpoint -- keeping its format header and every
        later frame byte for byte. Frames are in LSN order (appends queue
        FIFO), so the cut is a prefix. Returns how many bytes the log
        shrank by."""

        def drop_through(committed: bytes) -> bytes:
            frames = WAL_FORMAT.decode(committed)
            header_length = len(committed) - len(frames)
            offset = NodeWAL._droppable_prefix_length(frames, lsn)
            if offset == 0:
                return committed
            return committed[:header_length] + frames[offset:]

        return await self._writer.rewrite(drop_through)

    @staticmethod
    def _droppable_prefix_length(frames: bytes, lsn: int) -> int:
        """How many leading bytes of ``frames`` are whole frames at or
        below ``lsn``."""
        offset = 0
        while offset + HEADER_SIZE <= len(frames):
            if (frame_length := NodeWAL._droppable_frame_length(frames, offset, lsn)) is None:
                break
            offset += frame_length
        return offset

    @staticmethod
    def _droppable_frame_length(frames: bytes, offset: int, lsn: int) -> int | None:
        """The length of the frame at ``offset`` when it is whole and at
        or below ``lsn``; None where the cut ends."""
        frame_length, frame_lsn = struct.unpack(">IQ", frames[offset + 4 : offset + 16])
        if NodeWAL._frame_ends_cut(frame_length, frame_lsn, offset, len(frames), lsn):
            return None
        return frame_length

    @staticmethod
    def _frame_ends_cut(frame_length: int, frame_lsn: int, offset: int, frames_length: int, lsn: int) -> bool:
        """Whether a frame is past ``lsn``, or torn: the cut stops before it."""
        return frame_lsn > lsn or frame_length < HEADER_SIZE or offset + frame_length > frames_length

    def _compactable_lsns(self, up_to_lsn: int) -> list[int]:
        """The applied entries' LSNs at or below ``up_to_lsn`` (the caller
        holds the state lock)."""
        return [
            lsn
            for lsn, entry in list(self._pending_entries_internal.items())
            if self._is_compactable(lsn, entry, up_to_lsn)
        ]

    @staticmethod
    def _is_compactable(lsn: int, entry: WALEntry, up_to_lsn: int) -> bool:
        """Whether ``entry`` is applied and at or below ``up_to_lsn``."""
        return lsn <= up_to_lsn and entry.state == WALEntryState.APPLIED

    async def compact(self, up_to_lsn: int) -> int:
        async with self._state_lock:
            lsns_to_remove = self._compactable_lsns(up_to_lsn)
            compacted_count = len(lsns_to_remove)

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

    def restore_checkpointed_lsn(self, checkpoint_local_lsn: int) -> None:
        """At recovery from a checkpoint: the next entry's LSN follows every
        LSN the checkpoint holds through. A checkpoint lets the log drop the
        frames it covers -- possibly all of them -- and a log recovered
        with none would number its next entries from 0 again, at or below
        the checkpoint's LSN, where the next recovery (replaying only past
        that LSN) would skip them: acknowledged writes lost."""
        if self._status_snapshot.next_lsn <= checkpoint_local_lsn:
            self._status_snapshot = WALStatusSnapshot(
                next_lsn=checkpoint_local_lsn + 1,
                last_synced_lsn=max(self._status_snapshot.last_synced_lsn, checkpoint_local_lsn),
                pending_count=self._status_snapshot.pending_count,
                closed=self._status_snapshot.closed,
            )

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

_REHOMED = (
    WALAppendResult,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
