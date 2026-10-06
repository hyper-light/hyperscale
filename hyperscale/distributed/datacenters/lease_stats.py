"""``LeaseStats`` -- pickled under the namespace
``hyperscale.distributed.datacenters.lease_manager`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class LeaseStats:
    """Statistics for lease operations."""

    total_created: int = 0
    total_renewed: int = 0
    total_expired: int = 0
    total_transferred: int = 0
    active_leases: int = 0
