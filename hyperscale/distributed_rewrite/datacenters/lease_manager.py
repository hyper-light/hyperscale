"""
Lease Manager - At-most-once job delivery guarantees via leases.

This class manages leases for job dispatches to datacenters, ensuring
at-most-once delivery semantics through fencing tokens.

Key concepts:
- Lease: A time-limited grant for a gate to dispatch to a specific DC
- Fence Token: Monotonic counter to reject stale operations
- Lease Transfer: Handoff of lease from one gate to another

Leases provide:
- At-most-once semantics: Only the lease holder can dispatch
- Partition tolerance: Leases expire if holder becomes unresponsive
- Ordered operations: Fence tokens reject out-of-order requests
"""

import time
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed_rewrite.models import (
    DatacenterLease,
    LeaseTransfer,
)


@dataclass(slots=True)
class LeaseStats:
    """Statistics for lease operations."""

    total_created: int = 0
    total_renewed: int = 0
    total_expired: int = 0
    total_transferred: int = 0
    active_leases: int = 0


class LeaseManager:
    """
    Manages job-to-datacenter leases for at-most-once delivery.

    Each job-datacenter pair can have exactly one active lease.
    Only the lease holder can dispatch operations for that job to that DC.

    Example usage:
        manager = LeaseManager(
            node_id="gate-1",
            lease_timeout=30.0,
        )

        # Get or create lease for a job dispatch
        lease = manager.acquire_lease("job-123", "dc-1")

        # Check if we hold the lease
        if manager.is_lease_holder("job-123", "dc-1"):
            # Safe to dispatch
            pass

        # Transfer lease to another gate
        transfer = manager.create_transfer("job-123", "dc-1", "gate-2")

        # Cleanup expired leases
        expired = manager.cleanup_expired()
    """

    def __init__(
        self,
        node_id: str,
        lease_timeout: float = 30.0,
        get_fence_token: Callable[[], int] | None = None,
        get_state_version: Callable[[], int] | None = None,
    ):
        """
        Initialize LeaseManager.

        Args:
            node_id: ID of this node (lease holder identifier).
            lease_timeout: Lease duration in seconds.
            get_fence_token: Callback to get next fence token.
            get_state_version: Callback to get current state version.
        """
        self._node_id = node_id
        self._lease_timeout = lease_timeout
        self._get_fence_token = get_fence_token
        self._get_state_version = get_state_version

        # Leases: "job_id:dc_id" -> DatacenterLease
        self._leases: dict[str, DatacenterLease] = {}

        # Internal fence token counter (if no callback provided)
        self._internal_fence_token = 0

        # Statistics
        self._stats = LeaseStats()

    # =========================================================================
    # Configuration
    # =========================================================================

    def set_node_id(self, node_id: str) -> None:
        """Update the node ID (used as lease holder identifier)."""
        self._node_id = node_id

    def set_lease_timeout(self, timeout: float) -> None:
        """Update the lease timeout."""
        self._lease_timeout = timeout

    # =========================================================================
    # Lease Operations
    # =========================================================================

    def acquire_lease(
        self,
        job_id: str,
        datacenter: str,
    ) -> DatacenterLease:
        """
        Acquire or renew a lease for a job-datacenter pair.

        If a valid lease exists and we hold it, renews the lease.
        Otherwise creates a new lease.

        Args:
            job_id: Job ID.
            datacenter: Datacenter ID.

        Returns:
            The acquired/renewed lease.
        """
        key = f"{job_id}:{datacenter}"
        existing = self._leases.get(key)

        # If we have a valid lease, renew it
        if existing and existing.expires_at > time.monotonic():
            if existing.lease_holder == self._node_id:
                existing.expires_at = time.monotonic() + self._lease_timeout
                self._stats.total_renewed += 1
                return existing

        # Create new lease
        lease = DatacenterLease(
            job_id=job_id,
            datacenter=datacenter,
            lease_holder=self._node_id,
            fence_token=self._next_fence_token(),
            expires_at=time.monotonic() + self._lease_timeout,
            version=self._current_state_version(),
        )

        self._leases[key] = lease
        self._stats.total_created += 1
        self._stats.active_leases = len(self._leases)

        return lease

    def get_lease(
        self,
        job_id: str,
        datacenter: str,
    ) -> DatacenterLease | None:
        """
        Get an existing valid lease.

        Returns None if lease doesn't exist or is expired.

        Args:
            job_id: Job ID.
            datacenter: Datacenter ID.

        Returns:
            The lease if valid, None otherwise.
        """
        key = f"{job_id}:{datacenter}"
        lease = self._leases.get(key)

        if lease and lease.expires_at > time.monotonic():
            return lease

        return None

    def is_lease_holder(
        self,
        job_id: str,
        datacenter: str,
    ) -> bool:
        """
        Check if we hold a valid lease for a job-datacenter pair.

        Args:
            job_id: Job ID.
            datacenter: Datacenter ID.

        Returns:
            True if we hold a valid lease.
        """
        lease = self.get_lease(job_id, datacenter)
        return lease is not None and lease.lease_holder == self._node_id

    def release_lease(
        self,
        job_id: str,
        datacenter: str,
    ) -> DatacenterLease | None:
        """
        Release a lease (delete it).

        Args:
            job_id: Job ID.
            datacenter: Datacenter ID.

        Returns:
            The released lease, or None if not found.
        """
        key = f"{job_id}:{datacenter}"
        lease = self._leases.pop(key, None)
        self._stats.active_leases = len(self._leases)
        return lease

    def release_job_leases(self, job_id: str) -> list[DatacenterLease]:
        """
        Release all leases for a job (across all datacenters).

        Args:
            job_id: Job ID.

        Returns:
            List of released leases.
        """
        released: list[DatacenterLease] = []
        prefix = f"{job_id}:"

        to_remove = [key for key in self._leases.keys() if key.startswith(prefix)]

        for key in to_remove:
            lease = self._leases.pop(key, None)
            if lease:
                released.append(lease)

        self._stats.active_leases = len(self._leases)
        return released

    # =========================================================================
    # Lease Transfer
    # =========================================================================

    def create_transfer(
        self,
        job_id: str,
        datacenter: str,
        new_holder: str,
    ) -> LeaseTransfer | None:
        """
        Create a lease transfer message to hand off to another gate.

        Args:
            job_id: Job ID.
            datacenter: Datacenter ID.
            new_holder: Node ID of the new lease holder.

        Returns:
            LeaseTransfer message, or None if no valid lease.
        """
        lease = self.get_lease(job_id, datacenter)
        if not lease:
            return None

        if lease.lease_holder != self._node_id:
            return None  # Can't transfer a lease we don't hold

        transfer = LeaseTransfer(
            job_id=job_id,
            datacenter=datacenter,
            from_gate=self._node_id,
            to_gate=new_holder,
            new_fence_token=lease.fence_token,
            version=lease.version,
        )

        self._stats.total_transferred += 1

        return transfer

    def accept_transfer(
        self,
        transfer: LeaseTransfer,
    ) -> DatacenterLease:
        """
        Accept a lease transfer from another gate.

        Creates a new lease based on the transfer message.

        Args:
            transfer: The transfer message.

        Returns:
            The new lease.
        """
        key = f"{transfer.job_id}:{transfer.datacenter}"

        lease = DatacenterLease(
            job_id=transfer.job_id,
            datacenter=transfer.datacenter,
            lease_holder=self._node_id,  # We're the new holder
            fence_token=transfer.new_fence_token,
            expires_at=time.monotonic() + self._lease_timeout,
            version=transfer.version,
        )

        self._leases[key] = lease
        self._stats.active_leases = len(self._leases)

        return lease

    # =========================================================================
    # Fence Token Validation
    # =========================================================================

    def validate_fence_token(
        self,
        job_id: str,
        datacenter: str,
        token: int,
    ) -> bool:
        """
        Validate a fence token against the current lease.

        Used to reject stale operations.

        Args:
            job_id: Job ID.
            datacenter: Datacenter ID.
            token: Fence token to validate.

        Returns:
            True if token is valid (>= current lease token).
        """
        lease = self.get_lease(job_id, datacenter)
        if not lease:
            return True  # No lease, accept any token

        return token >= lease.fence_token

    # =========================================================================
    # Cleanup
    # =========================================================================

    def cleanup_expired(self) -> int:
        """
        Remove expired leases.

        Returns:
            Number of leases removed.
        """
        now = time.monotonic()
        to_remove: list[str] = []

        for key, lease in self._leases.items():
            if lease.expires_at <= now:
                to_remove.append(key)

        for key in to_remove:
            self._leases.pop(key, None)

        self._stats.total_expired += len(to_remove)
        self._stats.active_leases = len(self._leases)

        return len(to_remove)

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_stats(self) -> dict:
        """Get lease statistics."""
        return {
            "total_created": self._stats.total_created,
            "total_renewed": self._stats.total_renewed,
            "total_expired": self._stats.total_expired,
            "total_transferred": self._stats.total_transferred,
            "active_leases": len(self._leases),
            "lease_timeout": self._lease_timeout,
        }

    def get_all_leases(self) -> dict[str, DatacenterLease]:
        """Get all current leases."""
        return dict(self._leases)

    def get_job_leases(self, job_id: str) -> list[DatacenterLease]:
        """Get all leases for a specific job."""
        prefix = f"{job_id}:"
        return [
            lease
            for key, lease in self._leases.items()
            if key.startswith(prefix)
        ]

    # =========================================================================
    # Internal Helpers
    # =========================================================================

    def _next_fence_token(self) -> int:
        """Get the next fence token."""
        if self._get_fence_token:
            return self._get_fence_token()

        self._internal_fence_token += 1
        return self._internal_fence_token

    def _current_state_version(self) -> int:
        """Get the current state version."""
        if self._get_state_version:
            return self._get_state_version()
        return 0
