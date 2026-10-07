from __future__ import annotations

import asyncio
from pathlib import Path
import struct
import zlib
from typing import Generic, TypeVar

from hyperscale.distributed.ledger.storage_format import StorageFormat, UnrecognizedStorageFormatError
from hyperscale.distributed.ledger.storage_format.set_aside import (
    free_sibling_path,
    require_stable_read,
    set_aside_unrecognized,
)
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.runtime import Runner
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import IdempotencyError

from .idempotency_config import IdempotencyConfig
from .idempotency_key import IdempotencyKey
from .idempotency_status import IdempotencyStatus
from .ledger_entry import IdempotencyLedgerEntry

from hyperscale.distributed.runtime import (
    Clock,
    Filesystem,
    RealClock,
    RealFilesystem,
)


_DEFAULT_CLOCK: Clock = RealClock()

# Module-level storage seam (Phase 7). The ledger BORROWS this (or an
# injected instance) — it never shuts the filesystem down.
# ``swap_defaults`` rebinds it to the SIM filesystem so exactly-once
# persistence becomes deterministic and storage-faultable under replay.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()

T = TypeVar("T")

# The idempotency WAL: a format header, then one frame per entry --
# [4: crc32 of the entry][4: entry length][entry] -- so a damaged entry is
# found, never replayed as a different key or job.
IDEMPOTENCY_WAL_FORMAT = StorageFormat(b"HSIL", 1)
FRAME_HEADER = struct.Struct(">II")


class ManagerIdempotencyLedger(Generic[T]):
    """Manager-level idempotency ledger with WAL persistence."""

    def __init__(
        self,
        config: IdempotencyConfig,
        wal_path: str | Path,
        task_runner: Runner,
        logger: Logger,
        filesystem: Filesystem | None = None,
        storage_health: StorageHealth | None = None,
    ) -> None:
        self._config = config
        self._wal_path = Path(wal_path)
        # The node's storage health this ledger records its appends into
        # (None: the owner does not track storage health).
        self._storage_health = storage_health
        # Bytes of complete entries in the WAL file: a failed append is
        # cut back to here so its torn record cannot hide later entries.
        self._committed_length = 0
        self._task_runner = task_runner
        self._logger = logger
        # Borrowed, never shut down here — see _DEFAULT_FILESYSTEM.
        self._filesystem = (
            filesystem if filesystem is not None else _DEFAULT_FILESYSTEM
        )
        self._index: dict[IdempotencyKey, IdempotencyLedgerEntry] = {}
        self._job_to_key: dict[str, IdempotencyKey] = {}
        self._lock = asyncio.Lock()
        self._cleanup_token: str | None = None
        self._closed = False

    async def start(self) -> None:
        """Start the ledger and replay the WAL."""
        await self._filesystem.mkdir(
            self._wal_path.parent, parents=True, exist_ok=True
        )
        await self._replay_wal()
        # What lapsed while this manager was down goes now, not a cleanup
        # interval from now -- and the WAL it outgrew is compacted.
        await self._cleanup_expired()

        if self._cleanup_token is None:
            run = self._task_runner.run(self._cleanup_loop)
            if run:
                self._cleanup_token = f"{run.task_name}:{run.run_id}"

    async def close(self) -> None:
        """Stop cleanup and close the ledger."""
        self._closed = True
        cleanup_error = await self._cancel_cleanup()

        if cleanup_error:
            raise cleanup_error

    async def _cancel_cleanup(self) -> Exception | None:
        """Cancel the cleanup loop: the error its cancellation raised
        (logged), or None."""
        if not self._cleanup_token:
            return None
        try:
            await self._task_runner.cancel(self._cleanup_token)
        except Exception as exc:
            await self._logger.log(
                IdempotencyError(
                    message=f"Failed to cancel idempotency ledger cleanup: {exc}",
                    component="manager-ledger",
                )
            )
            return exc
        finally:
            self._cleanup_token = None
        return None

    async def check_or_reserve(
        self,
        key: IdempotencyKey,
        job_id: str,
    ) -> tuple[bool, IdempotencyLedgerEntry | None]:
        """Check for an entry, reserving it as PENDING if absent."""
        async with self._lock:
            entry = self._index.get(key)
            if entry:
                return True, entry

            entry = IdempotencyLedgerEntry(
                idempotency_key=key,
                job_id=job_id,
                status=IdempotencyStatus.PENDING,
                result_serialized=None,
                created_at=_DEFAULT_CLOCK.time(),
                committed_at=None,
            )
            await self._persist_entry(entry)
            # At most ``max_entries`` are held, the oldest going first (as
            # from the gate's cache): a key evicted early is decided afresh,
            # as one that lapsed is. The WAL sheds it at the next compaction.
            while len(self._index) >= self._config.max_entries:
                self._evict_oldest()
            self._index[key] = entry
            self._job_to_key[job_id] = key

        return False, None

    async def commit(self, key: IdempotencyKey, result_serialized: bytes) -> None:
        """Commit a PENDING entry with serialized result."""
        async with self._lock:
            entry = self._index.get(key)
            if entry is None or entry.status != IdempotencyStatus.PENDING:
                return

            updated_entry = IdempotencyLedgerEntry(
                idempotency_key=entry.idempotency_key,
                job_id=entry.job_id,
                status=IdempotencyStatus.COMMITTED,
                result_serialized=result_serialized,
                created_at=entry.created_at,
                committed_at=_DEFAULT_CLOCK.time(),
            )
            await self._persist_entry(updated_entry)
            self._index[key] = updated_entry
            self._job_to_key[updated_entry.job_id] = key

    async def reject(self, key: IdempotencyKey, result_serialized: bytes) -> None:
        """Reject a PENDING entry with serialized result."""
        async with self._lock:
            entry = self._index.get(key)
            if entry is None or entry.status != IdempotencyStatus.PENDING:
                return

            updated_entry = IdempotencyLedgerEntry(
                idempotency_key=entry.idempotency_key,
                job_id=entry.job_id,
                status=IdempotencyStatus.REJECTED,
                result_serialized=result_serialized,
                created_at=entry.created_at,
                committed_at=_DEFAULT_CLOCK.time(),
            )
            await self._persist_entry(updated_entry)
            self._index[key] = updated_entry
            self._job_to_key[updated_entry.job_id] = key

    async def release(self, key: IdempotencyKey) -> None:
        """Drop a PENDING reservation whose request ended undecided, so the
        next request carrying the key decides it.

        Not persisted: a reservation replayed after a restart lapses at its
        pending TTL, as an abandoned one does.
        """
        async with self._lock:
            entry = self._index.get(key)
            if entry is None or entry.status != IdempotencyStatus.PENDING:
                return

            self._index.pop(key)
            self._unmap_job_if_current(entry.job_id, key)

    def _evict_oldest(self) -> None:
        """Drop the oldest held entry (and its job mapping, if current)."""
        evicted_key, evicted_entry = next(iter(self._index.items()))
        del self._index[evicted_key]
        self._unmap_job_if_current(evicted_entry.job_id, evicted_key)

    def _unmap_job_if_current(self, job_id: str, key: IdempotencyKey) -> None:
        """Unmap ``job_id`` when it still maps to ``key``: a job id maps to
        its newest key, which an older key leaving must not unmap."""
        if self._job_to_key.get(job_id) == key:
            del self._job_to_key[job_id]

    def get_by_key(self, key: IdempotencyKey) -> IdempotencyLedgerEntry | None:
        """Get a ledger entry by idempotency key."""
        return self._index.get(key)

    def get_by_job_id(self, job_id: str) -> IdempotencyLedgerEntry | None:
        """Get a ledger entry by job ID."""
        key = self._job_to_key.get(job_id)
        if key is None:
            return None
        return self._index.get(key)

    async def _persist_entry(self, entry: IdempotencyLedgerEntry) -> None:
        payload = entry.to_bytes()
        record = FRAME_HEADER.pack(zlib.crc32(payload), len(payload)) + payload
        # One durable unit per entry through the storage seam — the
        # same append+flush+fsync sequence as before, off-loop on the
        # filesystem's own executor.
        try:
            await self._filesystem.append_fsync(self._wal_path, record)
        except OSError as storage_error:
            await self._cut_back_failed_append(storage_error, len(record))
            raise
        self._committed_length += len(record)
        if self._storage_health is not None:
            self._storage_health.record_success(len(record))

    async def _cut_back_failed_append(self, storage_error: OSError, attempted_bytes: int) -> None:
        """Cut a refused append's torn record back and record the failure."""
        # The device refused the append: cut the torn record back
        # (replay stops at the first incomplete frame, so it would
        # hide every later entry) and report the failure.
        if await self._filesystem.exists(self._wal_path):
            await self._filesystem.truncate(self._wal_path, self._committed_length)
        if self._storage_health is not None:
            self._storage_health.record_failure(storage_error, attempted_bytes)

    async def _replay_wal(self) -> None:
        header = IDEMPOTENCY_WAL_FORMAT.header
        if (loaded := await self._load_wal(header)) is None:
            return
        data, frames = loaded
        await self._recover_entries(header, data, frames)

    async def _load_wal(self, header: bytes) -> tuple[bytes, bytes] | None:
        """The WAL's bytes and its frames; None when the WAL starts empty
        (absent, torn at its header, or set aside)."""
        if not await self._filesystem.exists(self._wal_path):
            await self._filesystem.append_fsync(self._wal_path, header)
            self._committed_length = len(header)
            return None

        data = await self._filesystem.read_bytes(self._wal_path)
        if IDEMPOTENCY_WAL_FORMAT.is_torn_header(data):
            # A crash while the file was made, before any entry followed.
            await self._filesystem.atomic_write(self._wal_path, header)
            self._committed_length = len(header)
            return None
        return await self._decode_wal(data, header)

    async def _decode_wal(self, data: bytes, header: bytes) -> tuple[bytes, bytes] | None:
        """The WAL's bytes and frames; an unrecognized WAL is set aside
        and restarted empty (None)."""
        try:
            frames = IDEMPOTENCY_WAL_FORMAT.decode(data)
        except UnrecognizedStorageFormatError as format_error:
            # Never read as if this node wrote it: set aside, loudly.
            await set_aside_unrecognized(
                self._filesystem, self._wal_path, data, format_error.reason, self._logger
            )
            await self._filesystem.append_fsync(self._wal_path, header)
            self._committed_length = len(header)
            return None
        return data, frames

    async def _recover_entries(self, header: bytes, data: bytes, frames: bytes) -> None:
        """Index the WAL's intact entries, within the bound, and cut any
        damaged tail."""
        entries, frames_length = self._parse_wal_entries(frames)
        self._index_entries(entries)
        # The bound holds across a restart: a WAL not yet compacted after
        # evictions replays more entries than are kept.
        while len(self._index) > self._config.max_entries:
            self._evict_oldest()

        recovered_length = len(header) + frames_length
        self._committed_length = recovered_length
        if recovered_length < len(data):
            await self._preserve_damaged_tail(data, recovered_length, len(entries))

    def _index_entries(self, entries: list[IdempotencyLedgerEntry]) -> None:
        """Index replayed entries, the newest per key and per job winning."""
        for entry in entries:
            self._index[entry.idempotency_key] = entry
            self._job_to_key[entry.job_id] = entry.idempotency_key

    async def _preserve_damaged_tail(self, data: bytes, recovered_length: int, entry_count: int) -> None:
        """Cut the WAL back to its last good entry, preserving the rest."""
        # Cut what follows the last good entry, so entries appended
        # from now on are not stranded behind it at the next replay --
        # its bytes preserved beside the WAL, never destroyed, and
        # only when a second read agrees (a flipped read must not cut
        # good entries).
        await require_stable_read(self._filesystem, self._wal_path, data)
        preserved_path = await free_sibling_path(self._filesystem, self._wal_path, "discarded")
        await self._filesystem.atomic_write(preserved_path, data[recovered_length:])
        await self._filesystem.atomic_write(self._wal_path, data[:recovered_length])
        await self._logger.log(
            IdempotencyError(
                message=(
                    f"Idempotency WAL has a torn tail or damaged entry at byte "
                    f"{recovered_length} of {len(data)} -- recovered "
                    f"{entry_count} complete entries; the "
                    f"{len(data) - recovered_length} bytes after them are preserved "
                    f"at {preserved_path}"
                ),
                component="manager-ledger",
            )
        )

    def _parse_wal_entries(self, frames: bytes) -> tuple[list[IdempotencyLedgerEntry], int]:
        """The entries in the bytes after the format header, up to the
        first torn or damaged frame, and how many bytes they span.

        A crash mid-``append_fsync`` leaves at most one partial frame, at
        the tail; a frame whose checksum fails, or that does not decode
        though its checksum holds, is damage. Nothing after either can be
        trusted, so replay stops there (``_replay_wal`` preserves the rest
        and says so).
        """
        entries: list[IdempotencyLedgerEntry] = []
        offset = 0
        while offset + FRAME_HEADER.size <= len(frames):
            if (frame_end := self._parse_next_frame(frames, offset, entries)) is None:
                break
            offset = frame_end
        return entries, offset

    def _parse_next_frame(self, frames: bytes, offset: int, entries: list[IdempotencyLedgerEntry]) -> int | None:
        """Append the intact entry framed at ``offset`` and return where
        its frame ends; None at a torn or damaged frame."""
        if (intact := self._intact_entry_bytes(frames, offset)) is None or not self._append_entry(
            entries, intact[0]
        ):
            return None
        return intact[1]

    @staticmethod
    def _intact_entry_bytes(frames: bytes, offset: int) -> tuple[bytes, int] | None:
        """The entry bytes framed at ``offset`` and the frame's end, when
        the frame is whole and its checksum holds."""
        checksum, entry_length = FRAME_HEADER.unpack_from(frames, offset)
        if (frame_end := offset + FRAME_HEADER.size + entry_length) > len(frames):
            return None
        entry_bytes = frames[offset + FRAME_HEADER.size : frame_end]
        if zlib.crc32(entry_bytes) != checksum:
            return None
        return entry_bytes, frame_end

    @staticmethod
    def _append_entry(entries: list[IdempotencyLedgerEntry], entry_bytes: bytes) -> bool:
        """Decode an entry onto ``entries``; whether it decoded."""
        try:
            entries.append(IdempotencyLedgerEntry.from_bytes(entry_bytes))
        except (struct.error, ValueError):
            return False
        return True

    async def _cleanup_loop(self) -> None:
        while not self._closed:
            await _DEFAULT_CLOCK.sleep(self._config.cleanup_interval_seconds)
            await self._cleanup_expired()

    async def _cleanup_expired(self) -> None:
        now = _DEFAULT_CLOCK.time()
        async with self._lock:
            self._drop_expired(now)
            await self._compact_wal()

    def _drop_expired(self, now: float) -> None:
        """Drop every entry expired at ``now`` (the caller holds the lock)."""
        for key, entry in self._expired_entries(now):
            self._index.pop(key, None)
            # A job id maps to its newest key: an older key lapsing
            # must not unmap a newer one.
            self._unmap_job_if_current(entry.job_id, key)

    def _expired_entries(self, now: float) -> list[tuple[IdempotencyKey, IdempotencyLedgerEntry]]:
        """The held entries expired at ``now``."""
        return [
            (key, entry)
            for key, entry in self._index.items()
            if self._is_expired(entry, now)
        ]

    async def _compact_wal(self) -> None:
        """Rewrite the WAL to the live entries once they fill no more than
        half of it (the caller holds the lock)."""
        # The WAL holds every entry ever appended -- each reservation
        # and its outcome -- and expiry drops them from memory only, so
        # it grew with every keyed submission for the manager's life
        # and replay read all of it. Once the live entries fill no more
        # than half of it, it is rewritten to them: the doubling bound,
        # each byte rewritten paid for by a byte appended.
        live_records = self._live_records()
        if self._committed_length <= 2 * len(live_records):
            return
        try:
            await self._filesystem.atomic_write(self._wal_path, live_records)
        except OSError as storage_error:
            await self._report_failed_compaction(storage_error, live_records)
            return
        self._record_compaction(live_records)

    def _live_records(self) -> bytes:
        """The WAL rewritten to the held entries: header, then a frame each."""
        return IDEMPOTENCY_WAL_FORMAT.header + b"".join(
            FRAME_HEADER.pack(zlib.crc32(payload := entry.to_bytes()), len(payload)) + payload
            for entry in self._index.values()
        )

    async def _report_failed_compaction(self, storage_error: OSError, live_records: bytes) -> None:
        """Record and log a compaction the device refused."""
        # The old WAL stands untouched (the rename never happened):
        # compaction is retried at the next sweep.
        if self._storage_health is not None:
            self._storage_health.record_failure(storage_error, len(live_records))
        await self._logger.log(
            IdempotencyError(
                message=(
                    f"Idempotency WAL compaction to {len(live_records)} "
                    f"of {self._committed_length} bytes failed: "
                    f"{storage_error}"
                ),
                component="manager-ledger",
            )
        )

    def _record_compaction(self, live_records: bytes) -> None:
        """Adopt a completed compaction's length and record its success."""
        self._committed_length = len(live_records)
        if self._storage_health is not None:
            self._storage_health.record_success(len(live_records))

    def _is_expired(self, entry: IdempotencyLedgerEntry, now: float) -> bool:
        ttl = self._get_ttl_for_status(entry.status)
        reference_time = (
            entry.committed_at if entry.committed_at is not None else entry.created_at
        )
        return now - reference_time > ttl

    def _get_ttl_for_status(self, status: IdempotencyStatus) -> float:
        if status == IdempotencyStatus.PENDING:
            return self._config.pending_ttl_seconds
        if status == IdempotencyStatus.COMMITTED:
            return self._config.committed_ttl_seconds
        return self._config.rejected_ttl_seconds
