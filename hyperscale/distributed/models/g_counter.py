"""Wire model ``GCounter`` -- pickled under the wire namespace
``hyperscale.distributed.models.crdt`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class GCounter:
    """
    Grow-only Counter (G-Counter) CRDT.

    Each node/datacenter has its own slot that it can only increment.
    The total value is the sum of all slots. Merge takes the max of
    each slot, making it commutative, associative, and idempotent.

    Perfect for monotonically increasing counters like:
    - completed_count
    - failed_count
    - total_requests

    Example:
        counter = GCounter()
        counter.increment("dc-east", 5)
        counter.increment("dc-west", 3)
        assert counter.value == 8

        # Merge from another replica
        other = GCounter(counts={"dc-east": 10, "dc-south": 2})
        merged = counter.merge(other)
        assert merged.value == 15  # max(5,10) + 3 + 2
    """

    counts: dict[str, int] = field(default_factory=dict)

    def increment(self, node_id: str, amount: int = 1) -> None:
        """
        Increment this node's counter by the given amount.

        Args:
            node_id: The node/datacenter incrementing the counter
            amount: Amount to increment (must be positive)

        Raises:
            ValueError: If amount is negative
        """
        if amount < 0:
            raise ValueError("GCounter can only be incremented, not decremented")
        self.counts[node_id] = self.counts.get(node_id, 0) + amount

    def merge(self, other: GCounter) -> GCounter:
        """
        Merge with another GCounter.

        This operation is:
        - Commutative: a.merge(b) == b.merge(a)
        - Associative: a.merge(b.merge(c)) == a.merge(b).merge(c)
        - Idempotent: a.merge(a) == a

        Args:
            other: Another GCounter to merge with

        Returns:
            A new GCounter containing the merged state
        """
        merged = GCounter()
        all_nodes = set(self.counts.keys()) | set(other.counts.keys())
        for node_id in all_nodes:
            merged.counts[node_id] = max(
                self.counts.get(node_id, 0), other.counts.get(node_id, 0)
            )
        return merged

    def merge_in_place(self, other: GCounter) -> None:
        """Merge another GCounter into this one (mutating)."""
        for node_id, count in other.counts.items():
            self.counts[node_id] = max(self.counts.get(node_id, 0), count)

    @property
    def value(self) -> int:
        """Get the total counter value (sum of all node counts)."""
        return sum(self.counts.values())

    def get_node_value(self, node_id: str) -> int:
        """Get the counter value for a specific node."""
        return self.counts.get(node_id, 0)

    def to_dict(self) -> dict[str, int]:
        """Serialize to a dictionary."""
        return dict(self.counts)

    @classmethod
    def from_dict(cls, data: dict[str, int]) -> GCounter:
        """Deserialize from a dictionary."""
        return cls(counts=dict(data))
