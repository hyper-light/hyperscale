"""``PeerHealth`` -- pickled under the namespace
``hyperscale.distributed.discovery.models.peer_info`` (see that module)."""

from enum import Enum
from functools import total_ordering


@total_ordering
class PeerHealth(Enum):
    """
    Health status of a peer.

    Ordering: HEALTHY > UNKNOWN > DEGRADED > UNHEALTHY > EVICTED
    Higher values indicate better health.
    """
    EVICTED = ("evicted", 0)       # Removed from pool
    UNHEALTHY = ("unhealthy", 1)   # Failed consecutive probes
    DEGRADED = ("degraded", 2)     # High error rate or latency
    UNKNOWN = ("unknown", 3)       # Not yet probed
    HEALTHY = ("healthy", 4)       # Responding normally

    def __init__(self, label: str, order: int) -> None:
        self._label = label
        self._order = order

    @property
    def value(self) -> str:
        """Return the string value for serialization."""
        return self._label

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, PeerHealth):
            return NotImplemented
        return self._order < other._order

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PeerHealth):
            return NotImplemented
        return self._order == other._order

    def __hash__(self) -> int:
        return hash(self._label)
