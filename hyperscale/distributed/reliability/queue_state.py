"""``QueueState`` -- pickled under the namespace
``hyperscale.distributed.reliability.robust_queue`` (see that module)."""

from enum import IntEnum


class QueueState(IntEnum):
    """State of the queue for monitoring."""
    HEALTHY = 0      # Below throttle threshold
    THROTTLED = 1    # Above throttle, below batch
    BATCHING = 2     # Above batch, below reject
    OVERFLOW = 3     # Primary full, using overflow
    SATURATED = 4    # Both primary and overflow full
