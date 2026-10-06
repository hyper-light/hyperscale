"""``BackpressureLevel`` -- pickled under the namespace
``hyperscale.distributed.reliability.backpressure`` (see that module)."""

from enum import IntEnum


class BackpressureLevel(IntEnum):
    """Backpressure levels for stats updates."""

    NONE = 0  # Accept all updates
    THROTTLE = 1  # Reduce update frequency
    BATCH = 2  # Accept batched updates only
    REJECT = 3  # Reject non-critical updates
