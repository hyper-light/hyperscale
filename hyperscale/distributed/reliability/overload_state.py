"""``OverloadState`` -- pickled under the namespace
``hyperscale.distributed.reliability.overload`` (see that module)."""

from enum import Enum


class OverloadState(Enum):
    """
    Overload state levels.

    Each level has associated actions:
    - HEALTHY: Normal operation
    - BUSY: Reduce new work intake
    - STRESSED: Shed low-priority requests
    - OVERLOADED: Emergency shedding, only critical operations
    """

    HEALTHY = "healthy"
    BUSY = "busy"
    STRESSED = "stressed"
    OVERLOADED = "overloaded"

# State ordering for max() comparison
_STATE_ORDER = {
    OverloadState.HEALTHY: 0,
    OverloadState.BUSY: 1,
    OverloadState.STRESSED: 2,
    OverloadState.OVERLOADED: 3,
}
