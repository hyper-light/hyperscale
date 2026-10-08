"""``PeerLoadLevel`` -- pickled under the namespace
``hyperscale.distributed.swim.health.peer_health_awareness`` (see that module)."""

from enum import IntEnum


class PeerLoadLevel(IntEnum):
    """
    Peer load level classification for behavior adaptation.

    Higher values indicate more load - more accommodation needed.
    """
    UNKNOWN = 0   # No health info yet (treat as healthy)
    HEALTHY = 1   # Normal operation
    BUSY = 2      # Slightly elevated load
    STRESSED = 3  # Significant load - reduce traffic
    OVERLOADED = 4  # Critically loaded - minimal traffic only

# Map overload_state string to PeerLoadLevel
_OVERLOAD_STATE_TO_LEVEL: dict[str, PeerLoadLevel] = {
    "healthy": PeerLoadLevel.HEALTHY,
    "busy": PeerLoadLevel.BUSY,
    "stressed": PeerLoadLevel.STRESSED,
    "overloaded": PeerLoadLevel.OVERLOADED,
}
