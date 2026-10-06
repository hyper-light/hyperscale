"""Wire model ``LWWRegister`` -- pickled under the wire namespace
``hyperscale.distributed.models.crdt`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from .lww_register_snapshot import LWWRegisterSnapshot

ValueT = TypeVar("ValueT")


@dataclass(slots=True)
class LWWRegister(Generic[ValueT]):
    """
    Last-Writer-Wins Register (LWW-Register) CRDT.

    Each update is tagged with a Lamport timestamp. The value with
    the highest timestamp wins during merge. Ties are broken by
    comparing the node_id lexicographically.

    Suitable for values that can be overwritten:
    - rate_per_second
    - status
    - last_error

    Example:
        reg = LWWRegister()
        reg.set(100.5, 1, "dc-east")  # value=100.5, timestamp=1
        reg.set(200.0, 2, "dc-west")  # value=200.0, timestamp=2
        assert reg.value == 200.0  # higher timestamp wins
    """

    _value: ValueT | None = None
    _timestamp: int = 0
    _node_id: str = ""

    def set(self, value: ValueT, timestamp: int, node_id: str) -> bool:
        """
        Set the value if the timestamp is newer.

        Args:
            value: The new value
            timestamp: Lamport timestamp for this update
            node_id: Node making the update (for tiebreaking)

        Returns:
            True if the value was updated, False if it was stale
        """
        if self._should_accept(timestamp, node_id):
            self._value = value
            self._timestamp = timestamp
            self._node_id = node_id
            return True
        return False

    def _should_accept(self, timestamp: int, node_id: str) -> bool:
        """Check if a new value should be accepted."""
        if timestamp > self._timestamp:
            return True
        if timestamp == self._timestamp:
            # Tie-breaker: higher node_id wins (deterministic)
            return node_id > self._node_id
        return False

    def merge(self, other: LWWRegister[ValueT]) -> LWWRegister[ValueT]:
        """
        Merge with another LWWRegister.

        Returns a new register with the winning value.
        """
        if other._should_accept(self._timestamp, self._node_id):
            # self wins
            return LWWRegister(
                _value=self._value,
                _timestamp=self._timestamp,
                _node_id=self._node_id,
            )
        else:
            # other wins
            return LWWRegister(
                _value=other._value,
                _timestamp=other._timestamp,
                _node_id=other._node_id,
            )

    def merge_in_place(self, other: LWWRegister[ValueT]) -> None:
        """Merge another LWWRegister into this one (mutating)."""
        if other._timestamp > self._timestamp or (
            other._timestamp == self._timestamp and other._node_id > self._node_id
        ):
            self._value = other._value
            self._timestamp = other._timestamp
            self._node_id = other._node_id

    @property
    def value(self) -> ValueT | None:
        """Get the current value."""
        return self._value

    @property
    def timestamp(self) -> int:
        """Get the current timestamp."""
        return self._timestamp

    def to_dict(self) -> LWWRegisterSnapshot[ValueT]:
        """Serialize to a dictionary."""
        return {
            "value": self._value,
            "timestamp": self._timestamp,
            "node_id": self._node_id,
        }

    @classmethod
    def from_dict(cls, data: LWWRegisterSnapshot[ValueT]) -> LWWRegister[ValueT]:
        """Deserialize from a dictionary."""
        return cls(
            _value=data.get("value"),
            _timestamp=data.get("timestamp", 0),
            _node_id=data.get("node_id", ""),
        )
