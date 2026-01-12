from __future__ import annotations

import asyncio
from collections import OrderedDict
import time
from typing import Generic, TypeVar

from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import IdempotencyError

from .idempotency_config import IdempotencyConfig
from .idempotency_entry import IdempotencyEntry
from .idempotency_key import IdempotencyKey
from .idempotency_status import IdempotencyStatus

T = TypeVar("T")


class GateIdempotencyCache(Generic[T]):
    """Gate-level idempotency cache for duplicate detection."""

    def __init__(
        self, config: IdempotencyConfig, task_runner: TaskRunner, logger: Logger
    ) -> None:
        self._config = config
        self._task_runner = task_runner
        self._logger = logger
        self._cache: OrderedDict[IdempotencyKey, IdempotencyEntry[T]] = OrderedDict()
        self._pending_waiters: dict[IdempotencyKey, list[asyncio.Future[T]]] = {}
        self._lock = asyncio.Lock()
        self._cleanup_token: str | None = None
        self._closed = False

    async def start(self) -> None:
        """Start the background cleanup loop."""
        if self._cleanup_token is not None:
            return

        self._closed = False
        run = self._task_runner.run(self._cleanup_loop)
        if run:
            self._cleanup_token = f"{run.task_name}:{run.run_id}"

    async def close(self) -> None:
        """Stop cleanup and clear cached state."""
        self._closed = True
        cleanup_error: Exception | None = None
        if self._cleanup_token:
            try:
                await self._task_runner.cancel(self._cleanup_token)
            except Exception as exc:
                cleanup_error = exc
                await self._logger.log(
                    IdempotencyError(
                        message=f"Failed to cancel idempotency cache cleanup: {exc}",
                        component="gate-cache",
                    )
                )
            finally:
                self._cleanup_token = None

        waiters = await self._drain_all_waiters()
        self._reject_waiters(waiters, RuntimeError("Idempotency cache closed"))

        async with self._lock:
            self._cache.clear()

        if cleanup_error:
            raise cleanup_error

    async def check_or_insert(
        self,
        key: IdempotencyKey,
        job_id: str,
        source_gate_id: str,
    ) -> tuple[bool, IdempotencyEntry[T] | None]:
        """Check if a key exists, inserting a PENDING entry if not."""
        entry = await self._get_entry(key)
        if entry:
            if entry.is_terminal() or not self._config.wait_for_pending:
                return True, entry
            await self._wait_for_pending(key)
            return True, await self._get_entry(key)

        await self._insert_entry(key, job_id, source_gate_id)
        return False, None

    async def commit(self, key: IdempotencyKey, result: T) -> None:
        """Commit a PENDING entry and notify waiters."""
        waiters: list[asyncio.Future[T]] = []
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None or entry.status != IdempotencyStatus.PENDING:
                return
            entry.status = IdempotencyStatus.COMMITTED
            entry.result = result
            entry.committed_at = time.time()
            self._cache.move_to_end(key)
            waiters = self._pending_waiters.pop(key, [])

        self._resolve_waiters(waiters, result)

    async def reject(self, key: IdempotencyKey, result: T) -> None:
        """Reject a PENDING entry and notify waiters."""
        waiters: list[asyncio.Future[T]] = []
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None or entry.status != IdempotencyStatus.PENDING:
                return
            entry.status = IdempotencyStatus.REJECTED
            entry.result = result
            entry.committed_at = time.time()
            self._cache.move_to_end(key)
            waiters = self._pending_waiters.pop(key, [])

        self._resolve_waiters(waiters, result)

    async def get(self, key: IdempotencyKey) -> IdempotencyEntry[T] | None:
        """Get an entry by key without altering waiters."""
        return await self._get_entry(key)

    async def stats(self) -> dict[str, int]:
        """Return cache statistics."""
        async with self._lock:
            status_counts = {status: 0 for status in IdempotencyStatus}
            for entry in self._cache.values():
                status_counts[entry.status] += 1

            return {
                "total_entries": len(self._cache),
                "pending_count": status_counts[IdempotencyStatus.PENDING],
                "committed_count": status_counts[IdempotencyStatus.COMMITTED],
                "rejected_count": status_counts[IdempotencyStatus.REJECTED],
                "pending_waiters": sum(
                    len(waiters) for waiters in self._pending_waiters.values()
                ),
                "max_entries": self._config.max_entries,
            }

    async def _get_entry(self, key: IdempotencyKey) -> IdempotencyEntry[T] | None:
        async with self._lock:
            entry = self._cache.get(key)
            if entry:
                self._cache.move_to_end(key)
            return entry

    async def _insert_entry(
        self, key: IdempotencyKey, job_id: str, source_gate_id: str
    ) -> None:
        entry = IdempotencyEntry(
            idempotency_key=key,
            status=IdempotencyStatus.PENDING,
            job_id=job_id,
            result=None,
            created_at=time.time(),
            committed_at=None,
            source_gate_id=source_gate_id,
        )

        evicted_waiters: list[asyncio.Future[T]] = []
        async with self._lock:
            evicted_waiters = self._evict_if_needed()
            self._cache[key] = entry

        if evicted_waiters:
            self._reject_waiters(
                evicted_waiters, TimeoutError("Idempotency entry evicted")
            )

    def _evict_if_needed(self) -> list[asyncio.Future[T]]:
        evicted_waiters: list[asyncio.Future[T]] = []
        while len(self._cache) >= self._config.max_entries:
            oldest_key, _ = self._cache.popitem(last=False)
            evicted_waiters.extend(self._pending_waiters.pop(oldest_key, []))
        return evicted_waiters

    async def _wait_for_pending(self, key: IdempotencyKey) -> T | None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[T] = loop.create_future()
        async with self._lock:
            self._pending_waiters.setdefault(key, []).append(future)

        try:
            return await asyncio.wait_for(
                future, timeout=self._config.pending_wait_timeout
            )
        except asyncio.TimeoutError:
            return None
        finally:
            async with self._lock:
                waiters = self._pending_waiters.get(key)
                if waiters and future in waiters:
                    waiters.remove(future)
                    if not waiters:
                        self._pending_waiters.pop(key, None)

    def _resolve_waiters(self, waiters: list[asyncio.Future[T]], result: T) -> None:
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(result)

    def _reject_waiters(
        self, waiters: list[asyncio.Future[T]], error: Exception
    ) -> None:
        for waiter in waiters:
            if not waiter.done():
                waiter.set_exception(error)

    async def _cleanup_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self._config.cleanup_interval_seconds)
            await self._cleanup_expired()

    async def _cleanup_expired(self) -> None:
        now = time.time()
        expired_waiters: list[asyncio.Future[T]] = []
        async with self._lock:
            expired_keys = [
                key
                for key, entry in self._cache.items()
                if self._is_expired(entry, now)
            ]

            for key in expired_keys:
                self._cache.pop(key, None)
                expired_waiters.extend(self._pending_waiters.pop(key, []))

        if expired_waiters:
            self._reject_waiters(
                expired_waiters, TimeoutError("Idempotency entry expired")
            )

    def _is_expired(self, entry: IdempotencyEntry[T], now: float) -> bool:
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

    async def _drain_all_waiters(self) -> list[asyncio.Future[T]]:
        async with self._lock:
            waiters = [
                waiter
                for waiter_list in self._pending_waiters.values()
                for waiter in waiter_list
            ]
            self._pending_waiters.clear()
            return waiters
