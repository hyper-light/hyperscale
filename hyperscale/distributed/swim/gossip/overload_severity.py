"""``OverloadSeverity`` -- pickled under the namespace
``hyperscale.distributed.swim.gossip.health_gossip_buffer`` (see that module)."""

from enum import IntEnum


class OverloadSeverity(IntEnum):
    """
    Severity ordering for health state prioritization.

    Higher severity = propagate faster (lower broadcast count threshold).
    This ensures overloaded nodes are known quickly across the cluster.
    """
    HEALTHY = 0
    BUSY = 1
    STRESSED = 2
    OVERLOADED = 3
    UNKNOWN = 0  # Treat unknown as healthy (don't prioritize)

# Pre-encode common strings for fast serialization
_OVERLOAD_STATE_TO_SEVERITY: dict[str, OverloadSeverity] = {
    "healthy": OverloadSeverity.HEALTHY,
    "busy": OverloadSeverity.BUSY,
    "stressed": OverloadSeverity.STRESSED,
    "overloaded": OverloadSeverity.OVERLOADED,
}
