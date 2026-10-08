"""
Health state carried on SWIM messages (AD-19): ``HealthPiggyback``.
"""

from dataclasses import dataclass, field

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class HealthPiggyback:
    """
    Health information for SWIM message embedding.

    This data structure is designed to be embedded in SWIM protocol
    messages to propagate health information alongside membership updates.
    """

    node_id: str
    node_type: str  # "worker", "manager", "gate"

    # Liveness signal
    is_alive: bool = True

    # Readiness signals
    accepting_work: bool = True
    capacity: int = 0  # Available capacity (cores, slots, etc.)

    # Progress signals
    throughput: float = 0.0  # Actual throughput
    expected_throughput: float = 0.0  # Expected throughput

    # Overload state (from HybridOverloadDetector)
    overload_state: str = "healthy"

    # Timestamp for staleness detection
    timestamp: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())

    def to_dict(self) -> dict:
        """Serialize to dictionary for embedding."""
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "is_alive": self.is_alive,
            "accepting_work": self.accepting_work,
            "capacity": self.capacity,
            "throughput": self.throughput,
            "expected_throughput": self.expected_throughput,
            "overload_state": self.overload_state,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HealthPiggyback":
        """Deserialize from dictionary."""
        return cls(
            node_id=data["node_id"],
            node_type=data["node_type"],
            is_alive=data.get("is_alive", True),
            accepting_work=data.get("accepting_work", True),
            capacity=data.get("capacity", 0),
            throughput=data.get("throughput", 0.0),
            expected_throughput=data.get("expected_throughput", 0.0),
            overload_state=data.get("overload_state", "healthy"),
            timestamp=data.get("timestamp", _DEFAULT_CLOCK.monotonic()),
        )

    def is_stale(self, max_age_seconds: float = 60.0) -> bool:
        """Check if this piggyback data is stale."""
        return (_DEFAULT_CLOCK.monotonic() - self.timestamp) > max_age_seconds
