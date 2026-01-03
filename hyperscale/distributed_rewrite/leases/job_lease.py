"""
Lease-Based Job Ownership for distributed gate coordination.

This implementation provides:
- Time-bounded ownership: leases expire automatically
- Fencing tokens: monotonically increasing tokens prevent stale writes
- Safe handoff: backup can claim after primary lease expires
- Explicit release: clean ownership transfer without waiting for expiry

Design Principles:
1. Leases are local state - no distributed consensus required
2. Fence tokens are globally monotonic per job (across lease holders)
3. Expiry is based on monotonic time (immune to clock drift)
4. Thread-safe for concurrent operations

Usage:
    manager = LeaseManager("gate-1:9000")

    # Acquire lease for a new job
    lease = manager.acquire("job-123")
    if lease:
        # We own this job
        fence_token = lease.fence_token

    # Renew before expiry
    if manager.renew("job-123"):
        # Lease extended

    # Release when done
    manager.release("job-123")
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable


class LeaseState(Enum):
    """State of a lease."""
    ACTIVE = "active"      # Lease is held and not expired
    EXPIRED = "expired"    # Lease has expired
    RELEASED = "released"  # Lease was explicitly released


@dataclass(slots=True)
class JobLease:
    """
    A time-bounded lease for job ownership.

    Attributes:
        job_id: The job this lease is for
        owner_node: Node ID of the current owner
        fence_token: Monotonically increasing token for fencing
        created_at: When the lease was first acquired (monotonic)
        expires_at: When the lease expires (monotonic)
        lease_duration: Duration in seconds
        state: Current state of the lease
    """
    job_id: str
    owner_node: str
    fence_token: int
    created_at: float  # time.monotonic()
    expires_at: float  # time.monotonic()
    lease_duration: float = 30.0
    state: LeaseState = field(default=LeaseState.ACTIVE)

    def is_expired(self) -> bool:
        """Check if the lease has expired."""
        if self.state == LeaseState.RELEASED:
            return True
        return time.monotonic() >= self.expires_at

    def is_active(self) -> bool:
        """Check if the lease is currently active (not expired)."""
        return not self.is_expired() and self.state == LeaseState.ACTIVE

    def remaining_seconds(self) -> float:
        """Get remaining time until expiry (0 if expired)."""
        if self.is_expired():
            return 0.0
        return max(0.0, self.expires_at - time.monotonic())

    def extend(self, duration: float | None = None) -> None:
        """Extend the lease by the specified duration."""
        if duration is None:
            duration = self.lease_duration
        now = time.monotonic()
        self.expires_at = now + duration

    def mark_released(self) -> None:
        """Mark the lease as explicitly released."""
        self.state = LeaseState.RELEASED


@dataclass(slots=True)
class LeaseAcquisitionResult:
    """Result of a lease acquisition attempt."""
    success: bool
    lease: JobLease | None = None
    current_owner: str | None = None  # If failed, who holds it
    expires_in: float = 0.0  # If failed, when current lease expires


class LeaseManager:
    """
    Manages job leases for a single node.

    Provides thread-safe lease operations with automatic expiry
    and fence token management.

    Attributes:
        node_id: This node's identifier
        default_duration: Default lease duration in seconds
        cleanup_interval: How often to clean up expired leases
    """

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
        """
        Initialize the lease manager.

        Args:
            node_id: This node's unique identifier
            default_duration: Default lease duration in seconds
            cleanup_interval: How often to clean expired leases
            on_lease_expired: Callback when a lease expires
        """
        self._node_id = node_id
        self._leases: dict[str, JobLease] = {}
        self._fence_tokens: dict[str, int] = {}  # Global fence token per job
        self._lock = threading.RLock()
        self._default_duration = default_duration
        self._cleanup_interval = cleanup_interval
        self._cleanup_task: asyncio.Task | None = None
        self._on_lease_expired = on_lease_expired
        self._running = False

    @property
    def node_id(self) -> str:
        """Get this node's ID."""
        return self._node_id

    def _get_next_fence_token(self, job_id: str) -> int:
        """Get and increment the fence token for a job."""
        current = self._fence_tokens.get(job_id, 0)
        next_token = current + 1
        self._fence_tokens[job_id] = next_token
        return next_token

    def acquire(
        self,
        job_id: str,
        duration: float | None = None,
        force: bool = False,
    ) -> LeaseAcquisitionResult:
        """
        Attempt to acquire a lease for a job.

        Args:
            job_id: The job to acquire lease for
            duration: Lease duration (uses default if not specified)
            force: If True, acquire even if held by another node (for failover)

        Returns:
            LeaseAcquisitionResult with success status and lease/owner info
        """
        if duration is None:
            duration = self._default_duration

        with self._lock:
            existing = self._leases.get(job_id)

            # Check if we already hold this lease
            if existing and existing.owner_node == self._node_id:
                if existing.is_active():
                    # Already own it - just extend
                    existing.extend(duration)
                    return LeaseAcquisitionResult(
                        success=True,
                        lease=existing,
                    )
                # Our lease expired, need to re-acquire with new token

            # Check if another node holds an active lease
            if existing and existing.is_active() and existing.owner_node != self._node_id:
                if not force:
                    return LeaseAcquisitionResult(
                        success=False,
                        current_owner=existing.owner_node,
                        expires_in=existing.remaining_seconds(),
                    )
                # Force acquisition - for failover scenarios

            # Acquire the lease
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

    def renew(self, job_id: str, duration: float | None = None) -> bool:
        """
        Renew a lease if we currently own it.

        Args:
            job_id: The job to renew
            duration: New duration (uses default if not specified)

        Returns:
            True if renewal succeeded, False if we don't own or it expired
        """
        if duration is None:
            duration = self._default_duration

        with self._lock:
            lease = self._leases.get(job_id)

            if lease is None:
                return False

            if lease.owner_node != self._node_id:
                return False

            if lease.is_expired():
                # Can't renew expired lease - need to re-acquire
                return False

            lease.extend(duration)
            return True

    def release(self, job_id: str) -> bool:
        """
        Explicitly release a lease.

        Args:
            job_id: The job to release

        Returns:
            True if we held the lease and released it
        """
        with self._lock:
            lease = self._leases.get(job_id)

            if lease is None:
                return False

            if lease.owner_node != self._node_id:
                return False

            lease.mark_released()
            # Don't remove from _leases - keep for fence token tracking
            return True

    def get_lease(self, job_id: str) -> JobLease | None:
        """
        Get the current lease for a job.

        Returns None if no lease exists or it's expired.
        """
        with self._lock:
            lease = self._leases.get(job_id)
            if lease and lease.is_active():
                return lease
            return None

    def get_fence_token(self, job_id: str) -> int:
        """
        Get the current fence token for a job.

        Returns 0 if no lease has ever been acquired.
        """
        with self._lock:
            return self._fence_tokens.get(job_id, 0)

    def is_owner(self, job_id: str) -> bool:
        """Check if we currently own the lease for a job."""
        with self._lock:
            lease = self._leases.get(job_id)
            return (
                lease is not None
                and lease.owner_node == self._node_id
                and lease.is_active()
            )

    def get_owned_jobs(self) -> list[str]:
        """Get list of job IDs we currently own."""
        with self._lock:
            return [
                job_id
                for job_id, lease in self._leases.items()
                if lease.owner_node == self._node_id and lease.is_active()
            ]

    def cleanup_expired(self) -> list[JobLease]:
        """
        Clean up expired leases.

        Returns list of leases that were cleaned up.
        Does not remove fence token tracking.
        """
        expired: list[JobLease] = []

        with self._lock:
            for job_id, lease in list(self._leases.items()):
                if lease.is_expired() and lease.state != LeaseState.RELEASED:
                    lease.state = LeaseState.EXPIRED
                    expired.append(lease)
                    # Keep in _leases for fence token tracking
                    # but mark as expired

        return expired

    def import_lease(
        self,
        job_id: str,
        owner_node: str,
        fence_token: int,
        expires_at: float,
        lease_duration: float = 30.0,
    ) -> None:
        """
        Import a lease from state sync.

        Used when receiving lease state from other nodes.
        Only updates if the incoming fence token is higher.

        Args:
            job_id: The job ID
            owner_node: The owner node ID
            fence_token: The fence token
            expires_at: Expiry time (monotonic)
            lease_duration: Lease duration
        """
        with self._lock:
            existing = self._leases.get(job_id)
            current_token = self._fence_tokens.get(job_id, 0)

            # Only accept if fence token is higher (prevents stale updates)
            if fence_token <= current_token:
                return

            now = time.monotonic()
            # Adjust expires_at relative to our monotonic clock
            # This is an approximation - true distributed leases need
            # clock sync, but for local tracking this works
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

    def export_leases(self) -> list[dict]:
        """
        Export all active leases for state sync.

        Returns list of lease dicts suitable for serialization.
        """
        with self._lock:
            result = []
            now = time.monotonic()
            for job_id, lease in self._leases.items():
                if lease.is_active():
                    result.append({
                        "job_id": job_id,
                        "owner_node": lease.owner_node,
                        "fence_token": lease.fence_token,
                        "expires_in": lease.remaining_seconds(),
                        "lease_duration": lease.lease_duration,
                    })
            return result

    async def start_cleanup_task(self) -> None:
        """Start the background cleanup task."""
        if self._running:
            return

        self._running = True

        async def cleanup_loop():
            while self._running:
                try:
                    expired = self.cleanup_expired()
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
        """Stop the background cleanup task."""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    def __len__(self) -> int:
        """Return number of active leases."""
        with self._lock:
            return sum(1 for lease in self._leases.values() if lease.is_active())

    def __contains__(self, job_id: str) -> bool:
        """Check if an active lease exists for a job."""
        return self.get_lease(job_id) is not None
