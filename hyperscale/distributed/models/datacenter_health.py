"""Wire model ``DatacenterHealth`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class DatacenterHealth(str, Enum):
    """
    Health classification for datacenter routing decisions.

    Key insight: BUSY ≠ UNHEALTHY
    - BUSY = transient, will clear when workflows complete → accept job (queued)
    - UNHEALTHY = structural problem, requires intervention → try fallback

    See AD-16 in docs/architecture.md for design rationale.
    """

    HEALTHY = "healthy"  # Managers responding, workers available, capacity exists
    BUSY = "busy"  # Managers responding, workers available, no immediate capacity
    DEGRADED = "degraded"  # Some managers responding, reduced capacity
    UNHEALTHY = "unhealthy"  # No managers responding OR all workers down
    # Configured but no manager heartbeat has EVER arrived — the warmup
    # window of DatacenterRegistrationStatus.AWAITING_INITIAL surfaced at
    # the health level. Distinct from UNHEALTHY (which means heartbeats
    # existed and stopped, or managers report a broken DC): submissions
    # against an INITIALIZING datacenter are rejected as transient so
    # clients retry, instead of jobs being accepted and insta-failed.
    INITIALIZING = "initializing"
