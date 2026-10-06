"""Wire model ``LWWMap`` -- pickled under the wire namespace
``hyperscale.distributed.models.crdt`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from .lww_register import LWWRegister
from .lww_register_snapshot import LWWRegisterSnapshot

KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")
DefaultT = TypeVar("DefaultT")


@dataclass(slots=True)
class LWWMap(Generic[KeyT, ValueT]):
    """
    Last-Writer-Wins Map (LWW-Map) CRDT.

    A map where each key is a LWWRegister. Useful for tracking
    per-entity values that can be overwritten.

    Example:
        status_map = LWWMap()
        status_map.set("dc-east", "RUNNING", 1, "manager-1")
        status_map.set("dc-west", "COMPLETED", 2, "manager-2")
    """

    _entries: dict[KeyT, LWWRegister[ValueT]] = field(default_factory=dict)

    def set(self, key: KeyT, value: ValueT, timestamp: int, node_id: str) -> bool:
        """Set a value for a key if the timestamp is newer."""
        if key not in self._entries:
            self._entries[key] = LWWRegister()
        return self._entries[key].set(value, timestamp, node_id)

    def get(self, key: KeyT, default: DefaultT | None = None) -> ValueT | DefaultT | None:
        """Get the value for a key."""
        if key in self._entries:
            return self._entries[key].value
        return default

    def get_with_metadata(self, key: KeyT) -> tuple[ValueT | None, int, str] | None:
        """Get value with timestamp and node_id, or None if not present."""
        if key in self._entries:
            reg = self._entries[key]
            return (reg.value, reg.timestamp, reg._node_id)
        return None

    def merge(self, other: LWWMap[KeyT, ValueT]) -> LWWMap[KeyT, ValueT]:
        """Merge with another LWWMap."""
        merged: LWWMap[KeyT, ValueT] = LWWMap()
        all_keys = set(self._entries.keys()) | set(other._entries.keys())

        for key in all_keys:
            if key in self._entries and key in other._entries:
                merged._entries[key] = self._entries[key].merge(other._entries[key])
            elif key in self._entries:
                merged._entries[key] = LWWRegister(
                    _value=self._entries[key]._value,
                    _timestamp=self._entries[key]._timestamp,
                    _node_id=self._entries[key]._node_id,
                )
            else:
                merged._entries[key] = LWWRegister(
                    _value=other._entries[key]._value,
                    _timestamp=other._entries[key]._timestamp,
                    _node_id=other._entries[key]._node_id,
                )

        return merged

    def merge_in_place(self, other: LWWMap[KeyT, ValueT]) -> None:
        """Merge another LWWMap into this one (mutating)."""
        for key, reg in other._entries.items():
            if key in self._entries:
                self._entries[key].merge_in_place(reg)
            else:
                self._entries[key] = LWWRegister(
                    _value=reg._value,
                    _timestamp=reg._timestamp,
                    _node_id=reg._node_id,
                )

    def keys(self) -> list[KeyT]:
        """Get all keys."""
        return list(self._entries.keys())

    def values(self) -> list[ValueT | None]:
        """Get all values."""
        return [reg.value for reg in self._entries.values()]

    def items(self) -> list[tuple[KeyT, ValueT | None]]:
        """Get all key-value pairs."""
        return [(k, reg.value) for k, reg in self._entries.items()]

    def to_dict(self) -> dict[KeyT, LWWRegisterSnapshot[ValueT]]:
        """Serialize to a dictionary."""
        return {key: reg.to_dict() for key, reg in self._entries.items()}

    @classmethod
    def from_dict(cls, data: dict[KeyT, LWWRegisterSnapshot[ValueT]]) -> LWWMap[KeyT, ValueT]:
        """Deserialize from a dictionary."""
        entries = {key: LWWRegister.from_dict(val) for key, val in data.items()}
        return cls(_entries=entries)
