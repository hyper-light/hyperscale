from __future__ import annotations

import asyncio
from pathlib import Path
import struct
from typing import Generic, TypeVar

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

        if self._cleanup_token is None:
            run = self._task_runner.run(self._cleanup_loop)
            if run:
                self._cleanup_token = f"{run.task_name}:{run.run_id}"

    async def close(self) -> None:
        """Stop cleanup and close the ledger."""
        self._closed = True
        cleanup_error: Exception | None = None
        if self._cleanup_token:
            try:
                await self._task_runner.cancel(self._cleanup_token)
            except Exception as exc:
                cleanup_error = exc
                await self._logger.log(
                    IdempotencyError(
                        message=f"Failed to cancel idempotency ledger cleanup: {exc}",
                        component="manager-ledger",
                    )
                )
            finally:
                self._cleanup_token = None

        if cleanup_error:
            raise cleanup_error

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
        record = struct.pack(">I", len(payload)) + payload
        # One durable unit per entry through the storage seam — the
        # same append+flush+fsync sequence as before, off-loop on the
        # filesystem's own executor.
        try:
            await self._filesystem.append_fsync(self._wal_path, record)
        except OSError as storage_error:
            # The device refused the append: cut the torn record back
            # (replay stops at the first incomplete frame, so it would
            # hide every later entry) and report the failure.
            if await self._filesystem.exists(self._wal_path):
                await self._filesystem.truncate(self._wal_path, self._committed_length)
            if self._storage_health is not None:
                self._storage_health.record_failure(storage_error, len(record))
            raise
        self._committed_length += len(record)
        if self._storage_health is not None:
            self._storage_health.record_success(len(record))

    async def _replay_wal(self) -> None:
        if not await self._filesystem.exists(self._wal_path):
            return

        data = await self._filesystem.read_bytes(self._wal_path)
        entries, torn_at_offset = self._parse_wal_entries(data)
        for entry in entries:
            self._index[entry.idempotency_key] = entry
            self._job_to_key[entry.job_id] = entry.idempotency_key

        self._committed_length = len(data) if torn_at_offset is None else torn_at_offset
        if torn_at_offset is not None:
            # Cut the debris so entries appended from now on are not
            # stranded behind it at the next replay.
            await self._filesystem.truncate(self._wal_path, torn_at_offset)
            await self._logger.log(
                IdempotencyError(
                    message=(
                        "Idempotency WAL has a torn tail at byte "
                        f"{torn_at_offset} of {len(data)} (crash debris "
                        "from an interrupted append) — recovered "
                        f"{len(entries)} complete entries and dropped "
                        "the tail"
                    ),
                    component="manager-ledger",
                )
            )

    def _parse_wal_entries(
        self, data: bytes
    ) -> tuple[list[IdempotencyLedgerEntry], int | None]:
        """Parse length-prefixed entries; tolerate a torn tail.

        A crash mid-``append_fsync`` legitimately leaves a partial
        record at the end of the file. This previously RAISED, which
        turned one crash into a permanent boot loop — every subsequent
        ``start()`` re-hit the same debris. Now, like NodeWAL and
        RaftWAL, parsing stops at the first incomplete or undecodable
        frame: complete entries are recovered, the tail's byte offset
        is returned so the caller can log the data loss loudly.
        """
        entries: list[IdempotencyLedgerEntry] = []
        offset = 0
        while offset < len(data):
            frame_start = offset
            if offset + 4 > len(data):
                return entries, frame_start
            entry_len = struct.unpack_from(">I", data, offset)[0]
            offset += 4
            if offset + entry_len > len(data):
                return entries, frame_start
            entry_bytes = data[offset : offset + entry_len]
            try:
                entries.append(IdempotencyLedgerEntry.from_bytes(entry_bytes))
            except Exception:
                # A complete-length frame with an undecodable payload —
                # bit rot or a torn write that landed inside the frame.
                # Nothing after it can be trusted without CRC resync.
                return entries, frame_start
            offset += entry_len
        return entries, None

    async def _cleanup_loop(self) -> None:
        while not self._closed:
            await _DEFAULT_CLOCK.sleep(self._config.cleanup_interval_seconds)
            await self._cleanup_expired()

    async def _cleanup_expired(self) -> None:
        now = _DEFAULT_CLOCK.time()
        async with self._lock:
            expired_entries = [
                (key, entry)
                for key, entry in self._index.items()
                if self._is_expired(entry, now)
            ]

            for key, entry in expired_entries:
                self._index.pop(key, None)
                self._job_to_key.pop(entry.job_id, None)

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
