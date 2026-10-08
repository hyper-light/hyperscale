"""``PeerHealthInfo`` -- pickled under the namespace
``hyperscale.distributed.swim.health.peer_health_awareness`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.health.tracker import HealthPiggyback
from hyperscale.distributed.runtime import Clock, RealClock

from .peer_load_level import _OVERLOAD_STATE_TO_LEVEL
from .peer_load_level import PeerLoadLevel

_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class PeerHealthInfo:
    """
    Cached health information for a single peer.

    Used to make adaptation decisions without requiring
    full HealthPiggyback lookups.
    """
    node_id: str
    load_level: PeerLoadLevel
    accepting_work: bool
    capacity: int
    throughput: float
    expected_throughput: float
    last_update: float

    @property
    def is_overloaded(self) -> bool:
        """Check if peer is in overloaded state."""
        return self.load_level >= PeerLoadLevel.OVERLOADED

    @property
    def is_stressed(self) -> bool:
        """Check if peer is stressed or worse."""
        return self.load_level >= PeerLoadLevel.STRESSED

    @property
    def is_healthy(self) -> bool:
        """Check if peer is healthy."""
        return self.load_level <= PeerLoadLevel.HEALTHY

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Check if this info is stale."""
        return (_DEFAULT_CLOCK.monotonic() - self.last_update) > max_age_seconds

    @classmethod
    def from_piggyback(cls, piggyback: HealthPiggyback) -> "PeerHealthInfo":
        """Create PeerHealthInfo from HealthPiggyback."""
        load_level = _OVERLOAD_STATE_TO_LEVEL.get(
            piggyback.overload_state,
            PeerLoadLevel.UNKNOWN,
        )

        return cls(
            node_id=piggyback.node_id,
            load_level=load_level,
            accepting_work=piggyback.accepting_work,
            capacity=piggyback.capacity,
            throughput=piggyback.throughput,
            expected_throughput=piggyback.expected_throughput,
            last_update=_DEFAULT_CLOCK.monotonic(),
        )
