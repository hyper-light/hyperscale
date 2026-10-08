"""``BackpressureLevel`` -- pickled under the namespace
``hyperscale.distributed.nodes.manager.stats`` (see that module)."""

from enum import Enum


class BackpressureLevel(Enum):
    """
    Backpressure levels for AD-23.

    Determines how aggressively to shed load.
    """

    NONE = "none"  # No backpressure
    THROTTLE = "throttle"  # Slow down incoming requests
    BATCH = "batch"  # Batch stats updates
    REJECT = "reject"  # Reject new stats updates
