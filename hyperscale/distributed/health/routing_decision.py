"""``RoutingDecision`` -- pickled under the namespace
``hyperscale.distributed.health.worker_health`` (see that module)."""

from enum import Enum


class RoutingDecision(Enum):
    """Routing decisions based on health signals."""

    ROUTE = "route"  # Healthy, send work
    DRAIN = "drain"  # Stop new work, let existing complete
    INVESTIGATE = "investigate"  # Check worker, possible issues
    EVICT = "evict"  # Remove from pool
