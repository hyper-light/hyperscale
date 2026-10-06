"""``DCReachability`` -- pickled under the namespace
``hyperscale.distributed.swim.health.federated_health_monitor`` (see that module)."""

from enum import Enum


class DCReachability(Enum):
    """Network reachability state for a datacenter."""

    UNKNOWN = "unknown"
    REACHABLE = "reachable"
    SUSPECTED = "suspected"
    UNREACHABLE = "unreachable"
