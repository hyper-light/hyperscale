"""``DCFailureRecord`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class DCFailureRecord:
    """Record of a datacenter failure event."""

    datacenter_id: str
    timestamp: float
    failure_type: str  # "unhealthy", "timeout", "unreachable", etc.
    manager_count_affected: int = 0
