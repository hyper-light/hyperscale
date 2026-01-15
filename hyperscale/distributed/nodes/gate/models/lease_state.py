"""
Lease state tracking.

Tracks datacenter leases and fence tokens for at-most-once delivery.
"""

from dataclasses import dataclass, field

from hyperscale.distributed.models import DatacenterLease


@dataclass(slots=True)
class LeaseTracking:
    """Tracks a single lease state."""

    job_id: str
    datacenter_id: str
    lease: DatacenterLease
    fence_token: int


@dataclass(slots=True)
class LeaseState:
    """
    State container for lease management.

    Tracks:
    - Per-job-DC leases for at-most-once semantics
    - Global fence token counter
    - Lease timeout configuration

    Note: This is the legacy lease tracking. New code should use
    DatacenterLeaseManager which is instantiated separately.
    """

    # Per-job-DC leases (key: "job_id:dc_id" -> DatacenterLease)
    leases: dict[str, DatacenterLease] = field(default_factory=dict)

    # Global fence token counter
    fence_token: int = 0

    # Lease timeout (seconds)
    lease_timeout: float = 30.0

    def get_lease_key(self, job_id: str, datacenter_id: str) -> str:
        """Get the lease key for a job-DC pair."""
        return f"{job_id}:{datacenter_id}"

    def get_lease(self, job_id: str, datacenter_id: str) -> DatacenterLease | None:
        """Get the lease for a job-DC pair."""
        key = self.get_lease_key(job_id, datacenter_id)
        return self.leases.get(key)

    def set_lease(self, job_id: str, datacenter_id: str, lease: DatacenterLease) -> None:
        """Set the lease for a job-DC pair."""
        key = self.get_lease_key(job_id, datacenter_id)
        self.leases[key] = lease

    def remove_lease(self, job_id: str, datacenter_id: str) -> None:
        """Remove the lease for a job-DC pair."""
        key = self.get_lease_key(job_id, datacenter_id)
        self.leases.pop(key, None)

    def next_fence_token(self) -> int:
        """Get and increment the fence token."""
        self.fence_token += 1
        return self.fence_token
