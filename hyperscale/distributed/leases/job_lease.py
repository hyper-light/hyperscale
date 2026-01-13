from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable


class LeaseState(Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    RELEASED = "released"


@dataclass(slots=True)
class JobLease:
    job_id: str
    owner_node: str
    fence_token: int
    created_at: float
    expires_at: float
    lease_duration: float = 30.0
    state: LeaseState = field(default=LeaseState.ACTIVE)

    def is_expired(self) -> bool:
        if self.state == LeaseState.RELEASED:
            return True
        return time.monotonic() >= self.expires_at

    def is_active(self) -> bool:
        return not self.is_expired() and self.state == LeaseState.ACTIVE

    def remaining_seconds(self) -> float:
        if self.is_expired():
            return 0.0
        return max(0.0, self.expires_at - time.monotonic())

    def extend(self, duration: float | None = None) -> None:
        if duration is None:
            duration = self.lease_duration
        now = time.monotonic()
        self.expires_at = now + duration

    def mark_released(self) -> None:
        self.state = LeaseState.RELEASED


@dataclass(slots=True)
class LeaseAcquisitionResult:
    success: bool
    lease: JobLease | None = None
    current_owner: str | None = None
    expires_in: float = 0.0


class JobLeaseManager:
    __slots__ = (
        "_node_id",
        "_leases",
        "_fence_tokens",
        "_lock",
        "_default_duration",
        "_cleanup_interval",
        "_cleanup_task",
        "_on_lease_expired",
        "_running",
    )

    def __init__(
        self,
        node_id: str,
        default_duration: float = 30.0,
        cleanup_interval: float = 10.0,
        on_lease_expired: Callable[[JobLease], None] | None = None,
    ) -> None:
        self._node_id = node_id
        self._leases: dict[str, JobLease] = {}
        self._fence_tokens: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._default_duration = default_duration
        self._cleanup_interval = cleanup_interval
        self._cleanup_task: asyncio.Task[None] | None = None
        self._on_lease_expired = on_lease_expired
        self._running = False

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

    async def acquire(
        self,
        job_id: str,
        duration: float | None = None,
        force: bool = False,
    ) -> LeaseAcquisitionResult:
        if duration is None:
            duration = self._default_duration

        async with self._lock:
            existing = self._leases.get(job_id)

            if existing and existing.owner_node == self._node_id:
                if existing.is_active():
                    existing.extend(duration)
                    return LeaseAcquisitionResult(
                        success=True,
                        lease=existing,
                    )

            if (
                existing
                and existing.is_active()
                and existing.owner_node != self._node_id
            ):
                if not force:
                    return LeaseAcquisitionResult(
                        success=False,
                        current_owner=existing.owner_node,
                        expires_in=existing.remaining_seconds(),
                    )

            now = time.monotonic()
            fence_token = self._get_next_fence_token(job_id)

            lease = JobLease(
                job_id=job_id,
                owner_node=self._node_id,
                fence_token=fence_token,
                created_at=now,
                expires_at=now + duration,
                lease_duration=duration,
                state=LeaseState.ACTIVE,
            )
            self._leases[job_id] = lease

            return LeaseAcquisitionResult(
                success=True,
                lease=lease,
            )

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

        async with self._lock:
            for job_id, lease in list(self._leases.items()):
                if lease.is_expired() and lease.state != LeaseState.RELEASED:
                    lease.state = LeaseState.EXPIRED
                    expired.append(lease)

        return expired

    async def import_lease(
        self,
        job_id: str,
        owner_node: str,
        fence_token: int,
        expires_at: float,
        lease_duration: float = 30.0,
    ) -> None:
        async with self._lock:
            current_token = self._fence_tokens.get(job_id, 0)

            if fence_token <= current_token:
                return

            now = time.monotonic()
            remaining = max(0.0, expires_at - now)

            lease = JobLease(
                job_id=job_id,
                owner_node=owner_node,
                fence_token=fence_token,
                created_at=now,
                expires_at=now + remaining,
                lease_duration=lease_duration,
                state=LeaseState.ACTIVE if remaining > 0 else LeaseState.EXPIRED,
            )
            self._leases[job_id] = lease
            self._fence_tokens[job_id] = fence_token

    async def export_leases(self) -> list[dict]:
        async with self._lock:
            result = []
            for job_id, lease in self._leases.items():
                if lease.is_active():
                    result.append(
                        {
                            "job_id": job_id,
                            "owner_node": lease.owner_node,
                            "fence_token": lease.fence_token,
                            "expires_in": lease.remaining_seconds(),
                            "lease_duration": lease.lease_duration,
                        }
                    )
            return result

    async def start_cleanup_task(self) -> None:
        if self._running:
            return

        self._running = True

        async def cleanup_loop() -> None:
            while self._running:
                try:
                    expired = await self.cleanup_expired()
                    if self._on_lease_expired:
                        for lease in expired:
                            try:
                                self._on_lease_expired(lease)
                            except Exception:
                                pass
                    await asyncio.sleep(self._cleanup_interval)
                except asyncio.CancelledError:
                    break
                except Exception:
                    await asyncio.sleep(self._cleanup_interval)

        self._cleanup_task = asyncio.create_task(cleanup_loop())

    async def stop_cleanup_task(self) -> None:
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    async def lease_count(self) -> int:
        async with self._lock:
            return sum(1 for lease in self._leases.values() if lease.is_active())

    async def has_lease(self, job_id: str) -> bool:
        return await self.get_lease(job_id) is not None
