"""``PriorityLimits`` -- pickled under the namespace
``hyperscale.distributed.server.protocol.in_flight_tracker`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class PriorityLimits:
    """
    Per-priority concurrency limits.

    A limit of 0 means unlimited. The global_limit is the sum of all
    priorities that can be in flight simultaneously.
    """

    critical: int = 0  # 0 = unlimited for ungrouped CRITICAL traffic
    swim: int = 1000
    """Dedicated SWIM/control admission cap independent of DATA/NORMAL."""
    high: int = 500
    normal: int = 300
    low: int = 200
    global_limit: int = 1000
