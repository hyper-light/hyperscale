"""``JobLeaseManager`` -- pickled under the namespace
``hyperscale.distributed.leases.job_lease`` (see that module)."""

from __future__ import annotations

import asyncio

from .job_lease_shared import _DEFAULT_CLOCK
from .job_lease_model import JobLease
from .lease_state import LeaseState


class JobLeaseManager:
    __slots__ = (
        "_node_id",
        "_leases",
        "_fence_tokens",
        "_lock",
        "_default_duration",
        "_cleanup_interval",
        "_released_retention_seconds",
    )

    def __init__(
        self,
        node_id: str,
        default_duration: float = 30.0,
        cleanup_interval: float = 10.0,
        *,
        released_retention_seconds: float,
    ) -> None:
        """``released_retention_seconds``: how long a job's lease and fence
        token are kept once the lease ended -- released or expired -- before
        the cleanup forgets them; kept, every job ever leased stayed here
        for the owner's lifetime."""
        self._node_id = node_id
        self._leases: dict[str, JobLease] = {}
        self._fence_tokens: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._default_duration = default_duration
        self._cleanup_interval = cleanup_interval
        self._released_retention_seconds = released_retention_seconds

    @property
    def node_id(self) -> str:
        return self._node_id

    @node_id.setter
    def node_id(self, value: str) -> None:
        self._node_id = value

    def _get_next_fence_token(self, job_id: str) -> int:
        current = self._fence_tokens.get(job_id, 0)
        next_token = current + 1
        self._fence_tokens[job_id] = next_token
        return next_token

    async def acquire(self, job_id: str, duration: float | None = None) -> JobLease:
        """This gate's lease on the job: its active lease extended, else a
        new one under the job's next fence token. Every lease here is this
        gate's own -- none is imported from another gate -- so acquisition
        never meets another holder."""
        if duration is None:
            duration = self._default_duration

        async with self._lock:
            if (existing := self._leases.get(job_id)) is not None and existing.is_active():
                existing.extend(duration)
                return existing

            now = _DEFAULT_CLOCK.monotonic()
            lease = JobLease(
                job_id=job_id,
                owner_node=self._node_id,
                fence_token=self._get_next_fence_token(job_id),
                created_at=now,
                expires_at=now + duration,
                lease_duration=duration,
                state=LeaseState.ACTIVE,
            )
            self._leases[job_id] = lease
            return lease

    async def renew(self, job_id: str, duration: float | None = None) -> bool:
        if duration is None:
            duration = self._default_duration

        async with self._lock:
            lease = self._leases.get(job_id)

            if lease is None:
                return False

            if lease.owner_node != self._node_id:
                return False

            if lease.is_expired():
                return False

            lease.extend(duration)
            return True

    async def release(self, job_id: str) -> bool:
        async with self._lock:
            lease = self._leases.get(job_id)

            if lease is None:
                return False

            if lease.owner_node != self._node_id:
                return False

            lease.mark_released()
            return True

    async def get_lease(self, job_id: str) -> JobLease | None:
        async with self._lock:
            lease = self._leases.get(job_id)
            if lease and lease.is_active():
                return lease
            return None

    async def get_fence_token(self, job_id: str) -> int:
        async with self._lock:
            return self._fence_tokens.get(job_id, 0)

    async def is_owner(self, job_id: str) -> bool:
        async with self._lock:
            lease = self._leases.get(job_id)
            return (
                lease is not None
                and lease.owner_node == self._node_id
                and lease.is_active()
            )

    async def get_owned_jobs(self) -> list[str]:
        async with self._lock:
            return [
                job_id
                for job_id, lease in self._leases.items()
                if lease.owner_node == self._node_id and lease.is_active()
            ]

    async def cleanup_expired(self) -> list[JobLease]:
        expired: list[JobLease] = []
        now = _DEFAULT_CLOCK.monotonic()

        async with self._lock:
            for job_id, lease in list(self._leases.items()):
                if self._forget_if_ended_before(job_id, lease, now - self._released_retention_seconds):
                    continue
                if self._mark_if_expired(lease):
                    expired.append(lease)

        return expired

    def _forget_if_ended_before(self, job_id: str, lease: JobLease, cutoff: float) -> bool:
        """Forget a lease that ended -- released, or expired -- before
        ``cutoff`` (now less the retention), fence token and all."""
        if lease.state == LeaseState.ACTIVE or lease.expires_at >= cutoff:
            return False
        del self._leases[job_id]
        self._fence_tokens.pop(job_id, None)
        return True

    @staticmethod
    def _mark_if_expired(lease: JobLease) -> bool:
        """Mark a lease past its expiry that was not released EXPIRED."""
        if not lease.is_expired() or lease.state == LeaseState.RELEASED:
            return False
        lease.state = LeaseState.EXPIRED
        return True

    async def run_cleanup(self) -> None:
        """Expire leases, and forget those ended past the retention, every
        cleanup interval -- for as long as the owner runs it (under its task
        runner, cancelled with its other background loops)."""
        while True:
            await _DEFAULT_CLOCK.sleep(self._cleanup_interval)
            await self.cleanup_expired()

    async def lease_count(self) -> int:
        async with self._lock:
            return sum(1 for lease in self._leases.values() if lease.is_active())

    async def has_lease(self, job_id: str) -> bool:
        return await self.get_lease(job_id) is not None
