from dataclasses import dataclass, field
import time


@dataclass(slots=True)
class HealthPiggyback:
    """Health information embedded in SWIM messages."""

    node_id: str
    node_type: str
    is_alive: bool = True
    accepting_work: bool = True
    capacity: int = 0
    throughput: float = 0.0
    expected_throughput: float = 0.0
    overload_state: str = "healthy"
    timestamp: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict:
        """Serialize the piggyback to a dictionary."""
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
        """Deserialize the piggyback from a dictionary."""
        return cls(
            node_id=data["node_id"],
            node_type=data["node_type"],
            is_alive=data.get("is_alive", True),
            accepting_work=data.get("accepting_work", True),
            capacity=data.get("capacity", 0),
            throughput=data.get("throughput", 0.0),
            expected_throughput=data.get("expected_throughput", 0.0),
            overload_state=data.get("overload_state", "healthy"),
            timestamp=data.get("timestamp", time.monotonic()),
        )

    def is_stale(self, max_age_seconds: float = 60.0) -> bool:
        """Return True if this piggyback is older than max_age_seconds."""
        return (time.monotonic() - self.timestamp) > max_age_seconds
