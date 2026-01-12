from __future__ import annotations

import asyncio
import os
from pathlib import Path
import struct
import time
from typing import Generic, TypeVar

from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import IdempotencyError

from .idempotency_config import IdempotencyConfig
from .idempotency_key import IdempotencyKey
from .idempotency_status import IdempotencyStatus
from .ledger_entry import IdempotencyLedgerEntry

T = TypeVar("T")


class ManagerIdempotencyLedger(Generic[T]):
    """Manager-level idempotency ledger with WAL persistence."""

    def __init__(
        self,
        config: IdempotencyConfig,
        wal_path: str | Path,
        task_runner: TaskRunner,
        logger: Logger,
    ) -> None:
        self._config = config
        self._wal_path = Path(wal_path)
        self._task_runner = task_runner
        self._logger = logger
        self._index: dict[IdempotencyKey, IdempotencyLedgerEntry] = {}
        self._job_to_key: dict[str, IdempotencyKey] = {}
        self._lock = asyncio.Lock()
        self._cleanup_token: str | None = None
        self._closed = False

    async def start(self) -> None:
        """Start the ledger and replay the WAL."""
        self._wal_path.parent.mkdir(parents=True, exist_ok=True)
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
                created_at=time.time(),
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
                committed_at=time.time(),
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
                committed_at=time.time(),
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
        await asyncio.to_thread(self._write_wal_record, record)

    def _write_wal_record(self, record: bytes) -> None:
        with self._wal_path.open("ab") as wal_file:
            wal_file.write(record)
            wal_file.flush()
            os.fsync(wal_file.fileno())

    async def _replay_wal(self) -> None:
        if not self._wal_path.exists():
            return

        data = await asyncio.to_thread(self._wal_path.read_bytes)
        for entry in self._parse_wal_entries(data):
            self._index[entry.idempotency_key] = entry
            self._job_to_key[entry.job_id] = entry.idempotency_key

    def _parse_wal_entries(self, data: bytes) -> list[IdempotencyLedgerEntry]:
        entries: list[IdempotencyLedgerEntry] = []
        offset = 0
        while offset < len(data):
            if offset + 4 > len(data):
                raise ValueError("Incomplete WAL entry length")
            entry_len = struct.unpack_from(">I", data, offset)[0]
            offset += 4
            if offset + entry_len > len(data):
                raise ValueError("Incomplete WAL entry payload")
            entry_bytes = data[offset : offset + entry_len]
            entries.append(IdempotencyLedgerEntry.from_bytes(entry_bytes))
            offset += entry_len
        return entries

    async def _cleanup_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self._config.cleanup_interval_seconds)
            await self._cleanup_expired()

    async def _cleanup_expired(self) -> None:
        now = time.time()
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
