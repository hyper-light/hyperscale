"""``HealthGossipEntry`` -- pickled under the namespace
``hyperscale.distributed.swim.gossip.health_gossip_buffer`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.health.tracker import HealthPiggyback

from .health_gossip_buffer_shared import _DEFAULT_CLOCK
from .overload_severity import _OVERLOAD_STATE_TO_SEVERITY
from .overload_severity import OverloadSeverity


@dataclass(slots=True)
class HealthGossipEntry:
    """
    A health update entry in the gossip buffer.

    Uses __slots__ for memory efficiency since many instances may exist.
    """
    health: HealthPiggyback
    timestamp: float
    broadcast_count: int = 0
    max_broadcasts: int = 5  # Fewer than membership (health is less critical)

    @property
    def severity(self) -> OverloadSeverity:
        """Get severity for prioritization."""
        return _OVERLOAD_STATE_TO_SEVERITY.get(
            self.health.overload_state,
            OverloadSeverity.UNKNOWN,
        )

    def should_broadcast(self) -> bool:
        """Check if this entry should still be broadcast."""
        return self.broadcast_count < self.max_broadcasts

    def mark_broadcast(self) -> None:
        """Mark that this entry was broadcast."""
        self.broadcast_count += 1

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Check if this entry is stale based on its own timestamp."""
        return self.health.is_stale(max_age_seconds)

    def to_bytes(self) -> bytes:
        """
        Serialize entry for transmission.

        Format: node_id|node_type|overload_state|accepting_work|capacity|throughput|expected|timestamp

        Uses compact format to maximize entries per message.
        Field separator: '|' (pipe)
        """
        health = self.health
        # Convert overload_state enum to its string name
        overload_state_str = (
            health.overload_state.name
            if hasattr(health.overload_state, 'name')
            else str(health.overload_state)
        )
        parts = [
            health.node_id,
            health.node_type,
            overload_state_str,
            "1" if health.accepting_work else "0",
            str(health.capacity),
            f"{health.throughput:.2f}",
            f"{health.expected_throughput:.2f}",
            f"{health.timestamp:.2f}",
        ]
        return "|".join(parts).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "HealthGossipEntry | None":
        """
        Deserialize entry from bytes.

        Returns None if data is invalid or malformed.
        """
        try:
            text = data.decode()
            parts = text.split("|", maxsplit=7)
            if len(parts) < 8:
                return None

            node_id = parts[0]
            node_type = parts[1]
            overload_state = parts[2]
            accepting_work = parts[3] == "1"
            capacity = int(parts[4])
            throughput = float(parts[5])
            expected_throughput = float(parts[6])
            timestamp = float(parts[7])

            health = HealthPiggyback(
                node_id=node_id,
                node_type=node_type,
                overload_state=overload_state,
                accepting_work=accepting_work,
                capacity=capacity,
                throughput=throughput,
                expected_throughput=expected_throughput,
                timestamp=timestamp,
            )

            return cls(
                health=health,
                timestamp=_DEFAULT_CLOCK.monotonic(),
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None
