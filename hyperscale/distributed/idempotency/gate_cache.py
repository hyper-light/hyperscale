from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import Generic, TypeVar

from hyperscale.distributed.runtime import Runner
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import IdempotencyError

from .idempotency_config import IdempotencyConfig
from .idempotency_entry import IdempotencyEntry
from .idempotency_key import IdempotencyKey
from .idempotency_status import IdempotencyStatus

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

T = TypeVar("T")


class GateIdempotencyCache(Generic[T]):
    """Gate-level idempotency cache for duplicate detection."""

    def __init__(
        self, config: IdempotencyConfig, task_runner: Runner, logger: Logger
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
        cleanup_error = await self._cancel_cleanup()

        waiters = await self._drain_all_waiters()
        self._reject_waiters(waiters, RuntimeError("Idempotency cache closed"))

        async with self._lock:
            self._cache.clear()

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
                    message=f"Failed to cancel idempotency cache cleanup: {exc}",
                    component="gate-cache",
                )
            )
            return exc
        finally:
            self._cleanup_token = None
        return None

    async def check_or_insert(
        self,
        key: IdempotencyKey,
        job_id: str,
        source_gate_id: str,
    ) -> tuple[bool, IdempotencyEntry[T] | None]:
        entry, answers_immediately, evicted_waiters = await self._find_or_reserve(
            key, job_id, source_gate_id
        )
        if answers_immediately:
            return True, entry

        self._reject_evicted(evicted_waiters)

        # A held entry not answered at once is pending: wait for its outcome.
        if entry:
            await self._wait_for_pending(key)
            return True, await self._get_entry(key)

        return False, None

    async def _find_or_reserve(
        self,
        key: IdempotencyKey,
        job_id: str,
        source_gate_id: str,
    ) -> tuple[IdempotencyEntry[T] | None, bool, list[asyncio.Future[T]]]:
        """Under the lock, find ``key``'s entry -- and whether it answers
        at once -- or reserve it PENDING: the entry found, that verdict,
        and the waiters reserving evicted."""
        async with self._lock:
            entry = self._cache.get(key)
            if entry:
                self._cache.move_to_end(key)
                return entry, self._answers_immediately(entry), []
            return None, False, self._reserve(key, job_id, source_gate_id)

    def _answers_immediately(self, entry: IdempotencyEntry[T]) -> bool:
        """Whether a held entry is answered now rather than waited on."""
        return entry.is_terminal() or not self._config.wait_for_pending

    def _reserve(
        self, key: IdempotencyKey, job_id: str, source_gate_id: str
    ) -> list[asyncio.Future[T]]:
        """Hold ``key`` PENDING for ``job_id`` (the caller holds the lock):
        the waiters evicted to make room."""
        new_entry = IdempotencyEntry(
            idempotency_key=key,
            status=IdempotencyStatus.PENDING,
            job_id=job_id,
            result=None,
            created_at=_DEFAULT_CLOCK.time(),
            committed_at=None,
            source_gate_id=source_gate_id,
        )
        evicted_waiters = self._evict_if_needed()
        self._cache[key] = new_entry
        return evicted_waiters

    def _reject_evicted(self, evicted_waiters: list[asyncio.Future[T]]) -> None:
        """Fail the waiters of evicted entries."""
        if evicted_waiters:
            self._reject_waiters(
                evicted_waiters, TimeoutError("Idempotency entry evicted")
            )

    async def commit(self, key: IdempotencyKey, result: T) -> None:
        """Commit a PENDING entry and notify waiters."""
        waiters: list[asyncio.Future[T]] = []
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None or entry.status != IdempotencyStatus.PENDING:
                return
            entry.status = IdempotencyStatus.COMMITTED
            entry.result = result
            entry.committed_at = _DEFAULT_CLOCK.time()
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
            entry.committed_at = _DEFAULT_CLOCK.time()
            self._cache.move_to_end(key)
            waiters = self._pending_waiters.pop(key, [])

        self._resolve_waiters(waiters, result)

    async def adopt_committed(self, key: IdempotencyKey, job_id: str, source_gate_id: str) -> None:
        """Record ``key`` as decided for ``job_id`` by another gate -- its
        job's replica committed here (AD-40). A decision already held is
        kept, and so is this gate's own pending submission of the same job
        (its commit records the full answer); a pending submission of the
        key for another job is answered with this one -- that job cannot
        commit: gates refuse to prepare a second job under the key."""
        evicted_waiters, waiters = await self._adopt(key, job_id, source_gate_id)

        self._reject_evicted(evicted_waiters)
        self._resolve_waiters(waiters, None)

    async def _adopt(
        self, key: IdempotencyKey, job_id: str, source_gate_id: str
    ) -> tuple[list[asyncio.Future[T]], list[asyncio.Future[T]]]:
        """Under the lock, record ``key`` committed for ``job_id`` unless
        the held entry is kept: the waiters evicted and those to wake
        (none when the entry is kept)."""
        async with self._lock:
            entry = self._cache.get(key)
            if self._keeps_entry(entry, job_id):
                return [], []
            evicted_waiters = self._evict_for_new_entry(entry)
            self._cache[key] = IdempotencyEntry(
                idempotency_key=key,
                status=IdempotencyStatus.COMMITTED,
                job_id=job_id,
                result=None,
                created_at=self._adopted_created_at(entry),
                committed_at=_DEFAULT_CLOCK.time(),
                source_gate_id=source_gate_id,
            )
            self._cache.move_to_end(key)
            return evicted_waiters, self._pending_waiters.pop(key, [])

    @staticmethod
    def _keeps_entry(entry: IdempotencyEntry[T] | None, job_id: str) -> bool:
        """Whether a held entry stands against an adopted decision: it is
        decided, or it is this gate's own submission of the same job."""
        return entry is not None and (entry.is_terminal() or entry.job_id == job_id)

    def _evict_for_new_entry(self, entry: IdempotencyEntry[T] | None) -> list[asyncio.Future[T]]:
        """Make room when the key is new: the waiters evicted."""
        if entry is None:
            return self._evict_if_needed()
        return []

    @staticmethod
    def _adopted_created_at(entry: IdempotencyEntry[T] | None) -> float:
        """An adopted entry keeps a replaced entry's creation time."""
        return entry.created_at if entry is not None else _DEFAULT_CLOCK.time()

    async def release(self, key: IdempotencyKey) -> None:
        """Forget a PENDING entry whose request ended without an outcome
        worth replaying (a transient refusal): a retry with the key is
        processed afresh instead of replaying -- or waiting on -- it.
        Waiters are woken without a result."""
        waiters: list[asyncio.Future[T]] = []
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None or entry.status != IdempotencyStatus.PENDING:
                return
            del self._cache[key]
            waiters = self._pending_waiters.pop(key, [])

        self._resolve_waiters(waiters, None)

    async def get(self, key: IdempotencyKey) -> IdempotencyEntry[T] | None:
        """Get an entry by key without altering waiters."""
        return await self._get_entry(key)

    async def stats(self) -> dict[str, int]:
        """Return cache statistics."""
        async with self._lock:
            status_counts = self._count_by_status()

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

    def _count_by_status(self) -> dict[IdempotencyStatus, int]:
        """How many cached entries hold each status (caller holds the lock)."""
        status_counts = {status: 0 for status in IdempotencyStatus}
        for entry in self._cache.values():
            status_counts[entry.status] += 1
        return status_counts

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
            created_at=_DEFAULT_CLOCK.time(),
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
            return await _DEFAULT_CLOCK.wait_for(
                future, timeout=self._config.pending_wait_timeout
            )
        except asyncio.TimeoutError:
            return None
        finally:
            async with self._lock:
                self._discard_waiter(key, future)

    def _discard_waiter(self, key: IdempotencyKey, future: asyncio.Future[T]) -> None:
        """Forget a finished wait on ``key`` (the caller holds the lock)."""
        waiters = self._pending_waiters.get(key)
        if waiters and future in waiters:
            self._remove_waiter(key, waiters, future)

    def _remove_waiter(
        self, key: IdempotencyKey, waiters: list[asyncio.Future[T]], future: asyncio.Future[T]
    ) -> None:
        """Remove ``future`` from ``key``'s waiters, dropping an emptied list."""
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
            await _DEFAULT_CLOCK.sleep(self._config.cleanup_interval_seconds)
            await self._cleanup_expired()

    async def _cleanup_expired(self) -> None:
        now = _DEFAULT_CLOCK.time()
        expired_waiters: list[asyncio.Future[T]] = []
        async with self._lock:
            self._drop_expired(now, expired_waiters)

        if expired_waiters:
            self._reject_waiters(
                expired_waiters, TimeoutError("Idempotency entry expired")
            )

    def _drop_expired(self, now: float, expired_waiters: list[asyncio.Future[T]]) -> None:
        """Drop every entry expired at ``now``, collecting its waiters (the
        caller holds the lock)."""
        for key in self._expired_keys(now):
            self._cache.pop(key, None)
            expired_waiters.extend(self._pending_waiters.pop(key, []))

    def _expired_keys(self, now: float) -> list[IdempotencyKey]:
        """The cached keys whose entries expired at ``now``."""
        return [
            key
            for key, entry in self._cache.items()
            if self._is_expired(entry, now)
        ]

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
