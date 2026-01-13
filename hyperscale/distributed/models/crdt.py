"""
Conflict-free Replicated Data Types (CRDTs) for cross-datacenter synchronization.

CRDTs allow coordination-free updates with guaranteed eventual consistency.
They are particularly useful for cross-DC stat aggregation where network
latency makes tight coordination prohibitively expensive.

See AD-14 in docs/architecture.md for design rationale.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


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


@dataclass(slots=True)
class LWWRegister:
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

    _value: Any = None
    _timestamp: int = 0
    _node_id: str = ""

    def set(self, value: Any, timestamp: int, node_id: str) -> bool:
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

    def merge(self, other: LWWRegister) -> LWWRegister:
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

    def merge_in_place(self, other: LWWRegister) -> None:
        """Merge another LWWRegister into this one (mutating)."""
        if other._timestamp > self._timestamp or (
            other._timestamp == self._timestamp and other._node_id > self._node_id
        ):
            self._value = other._value
            self._timestamp = other._timestamp
            self._node_id = other._node_id

    @property
    def value(self) -> Any:
        """Get the current value."""
        return self._value

    @property
    def timestamp(self) -> int:
        """Get the current timestamp."""
        return self._timestamp

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary."""
        return {
            "value": self._value,
            "timestamp": self._timestamp,
            "node_id": self._node_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LWWRegister:
        """Deserialize from a dictionary."""
        return cls(
            _value=data.get("value"),
            _timestamp=data.get("timestamp", 0),
            _node_id=data.get("node_id", ""),
        )


@dataclass(slots=True)
class LWWMap:
    """
    Last-Writer-Wins Map (LWW-Map) CRDT.

    A map where each key is a LWWRegister. Useful for tracking
    per-entity values that can be overwritten.

    Example:
        status_map = LWWMap()
        status_map.set("dc-east", "RUNNING", 1, "manager-1")
        status_map.set("dc-west", "COMPLETED", 2, "manager-2")
    """

    _entries: dict[str, LWWRegister] = field(default_factory=dict)

    def set(self, key: str, value: Any, timestamp: int, node_id: str) -> bool:
        """Set a value for a key if the timestamp is newer."""
        if key not in self._entries:
            self._entries[key] = LWWRegister()
        return self._entries[key].set(value, timestamp, node_id)

    def get(self, key: str, default: Any = None) -> Any:
        """Get the value for a key."""
        if key in self._entries:
            return self._entries[key].value
        return default

    def get_with_metadata(self, key: str) -> tuple[Any, int, str] | None:
        """Get value with timestamp and node_id, or None if not present."""
        if key in self._entries:
            reg = self._entries[key]
            return (reg.value, reg.timestamp, reg._node_id)
        return None

    def merge(self, other: LWWMap) -> LWWMap:
        """Merge with another LWWMap."""
        merged = LWWMap()
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

    def merge_in_place(self, other: LWWMap) -> None:
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

    def keys(self) -> list[str]:
        """Get all keys."""
        return list(self._entries.keys())

    def values(self) -> list[Any]:
        """Get all values."""
        return [reg.value for reg in self._entries.values()]

    def items(self) -> list[tuple[str, Any]]:
        """Get all key-value pairs."""
        return [(k, reg.value) for k, reg in self._entries.items()]

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """Serialize to a dictionary."""
        return {key: reg.to_dict() for key, reg in self._entries.items()}

    @classmethod
    def from_dict(cls, data: dict[str, dict[str, Any]]) -> LWWMap:
        """Deserialize from a dictionary."""
        entries = {key: LWWRegister.from_dict(val) for key, val in data.items()}
        return cls(_entries=entries)


@dataclass(slots=True)
class JobStatsCRDT:
    """
    CRDT-based job statistics for cross-datacenter aggregation.

    Uses G-Counters for monotonic stats and LWW registers for
    non-monotonic values. Safe to merge from any subset of DCs
    at any time without coordination.

    Concurrency:
        The merge_in_place() method is NOT safe for concurrent coroutines.
        For concurrent access in async contexts, use AsyncSafeJobStatsCRDT
        wrapper which provides asyncio.Lock protection around merge operations.

        The immutable merge() method returns a new instance and is
        inherently safe for concurrent reads (but concurrent merge +
        mutation of the same target instance still requires coordination).

    Example:
        stats = JobStatsCRDT(job_id="job-123")

        # DC-east reports
        stats.record_completed("dc-east", 100)
        stats.record_rate("dc-east", 500.0, timestamp=1)

        # DC-west reports
        stats.record_completed("dc-west", 50)
        stats.record_failed("dc-west", 2)

        # Merge from another gate's view
        other_stats = get_stats_from_peer()
        stats.merge_in_place(other_stats)

        print(stats.total_completed)  # Sum of all DCs
        print(stats.total_rate)       # Sum of latest rates
    """

    job_id: str
    completed: GCounter = field(default_factory=GCounter)
    failed: GCounter = field(default_factory=GCounter)
    rates: LWWMap = field(default_factory=LWWMap)  # dc -> rate
    statuses: LWWMap = field(default_factory=LWWMap)  # dc -> status

    def record_completed(self, dc_id: str, count: int) -> None:
        """Record completed actions from a datacenter."""
        self.completed.increment(dc_id, count)

    def record_failed(self, dc_id: str, count: int) -> None:
        """Record failed actions from a datacenter."""
        self.failed.increment(dc_id, count)

    def record_rate(self, dc_id: str, rate: float, timestamp: int) -> None:
        """Record the current rate from a datacenter."""
        self.rates.set(dc_id, rate, timestamp, dc_id)

    def record_status(self, dc_id: str, status: str, timestamp: int) -> None:
        """Record the current status from a datacenter."""
        self.statuses.set(dc_id, status, timestamp, dc_id)

    @property
    def total_completed(self) -> int:
        """Get total completed across all DCs."""
        return self.completed.value

    @property
    def total_failed(self) -> int:
        """Get total failed across all DCs."""
        return self.failed.value

    @property
    def total_rate(self) -> float:
        """Get aggregate rate across all DCs."""
        return sum(r for r in self.rates.values() if isinstance(r, (int, float)))

    def get_dc_completed(self, dc_id: str) -> int:
        """Get completed count for a specific DC."""
        return self.completed.get_node_value(dc_id)

    def get_dc_failed(self, dc_id: str) -> int:
        """Get failed count for a specific DC."""
        return self.failed.get_node_value(dc_id)

    def get_dc_rate(self, dc_id: str) -> float:
        """Get rate for a specific DC."""
        rate = self.rates.get(dc_id)
        return rate if isinstance(rate, (int, float)) else 0.0

    def get_dc_status(self, dc_id: str) -> str | None:
        """Get status for a specific DC."""
        return self.statuses.get(dc_id)

    def merge(self, other: JobStatsCRDT) -> JobStatsCRDT:
        """Merge with another JobStatsCRDT."""
        if self.job_id != other.job_id:
            raise ValueError(
                f"Cannot merge stats for different jobs: {self.job_id} vs {other.job_id}"
            )

        return JobStatsCRDT(
            job_id=self.job_id,
            completed=self.completed.merge(other.completed),
            failed=self.failed.merge(other.failed),
            rates=self.rates.merge(other.rates),
            statuses=self.statuses.merge(other.statuses),
        )

    def merge_in_place(self, other: JobStatsCRDT) -> None:
        """Merge another JobStatsCRDT into this one (mutating)."""
        if self.job_id != other.job_id:
            raise ValueError(
                f"Cannot merge stats for different jobs: {self.job_id} vs {other.job_id}"
            )

        self.completed.merge_in_place(other.completed)
        self.failed.merge_in_place(other.failed)
        self.rates.merge_in_place(other.rates)
        self.statuses.merge_in_place(other.statuses)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary."""
        return {
            "job_id": self.job_id,
            "completed": self.completed.to_dict(),
            "failed": self.failed.to_dict(),
            "rates": self.rates.to_dict(),
            "statuses": self.statuses.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JobStatsCRDT:
        """Deserialize from a dictionary."""
        return cls(
            job_id=data["job_id"],
            completed=GCounter.from_dict(data.get("completed", {})),
            failed=GCounter.from_dict(data.get("failed", {})),
            rates=LWWMap.from_dict(data.get("rates", {})),
            statuses=LWWMap.from_dict(data.get("statuses", {})),
        )


class AsyncSafeJobStatsCRDT:
    """
    Async-safe wrapper around JobStatsCRDT for concurrent coroutine access.

    Provides asyncio.Lock protection around merge operations to prevent
    race conditions when multiple coroutines merge stats concurrently.

    All read operations are lock-free since they access immutable snapshots
    or atomic Python operations. Only merge_in_place requires the lock.
    """

    __slots__ = ("_crdt", "_lock")

    def __init__(self, job_id: str):
        self._crdt = JobStatsCRDT(job_id=job_id)
        self._lock = asyncio.Lock()

    @property
    def job_id(self) -> str:
        return self._crdt.job_id

    @property
    def total_completed(self) -> int:
        return self._crdt.total_completed

    @property
    def total_failed(self) -> int:
        return self._crdt.total_failed

    @property
    def total_rate(self) -> float:
        return self._crdt.total_rate

    def record_completed(self, dc_id: str, count: int) -> None:
        self._crdt.record_completed(dc_id, count)

    def record_failed(self, dc_id: str, count: int) -> None:
        self._crdt.record_failed(dc_id, count)

    def record_rate(self, dc_id: str, rate: float, timestamp: int) -> None:
        self._crdt.record_rate(dc_id, rate, timestamp)

    def record_status(self, dc_id: str, status: str, timestamp: int) -> None:
        self._crdt.record_status(dc_id, status, timestamp)

    def get_dc_completed(self, dc_id: str) -> int:
        return self._crdt.get_dc_completed(dc_id)

    def get_dc_failed(self, dc_id: str) -> int:
        return self._crdt.get_dc_failed(dc_id)

    def get_dc_rate(self, dc_id: str) -> float:
        return self._crdt.get_dc_rate(dc_id)

    def get_dc_status(self, dc_id: str) -> str | None:
        return self._crdt.get_dc_status(dc_id)

    async def merge_in_place(self, other: JobStatsCRDT | AsyncSafeJobStatsCRDT) -> None:
        other_crdt = other._crdt if isinstance(other, AsyncSafeJobStatsCRDT) else other
        async with self._lock:
            self._crdt.merge_in_place(other_crdt)

    def merge(self, other: JobStatsCRDT | AsyncSafeJobStatsCRDT) -> JobStatsCRDT:
        other_crdt = other._crdt if isinstance(other, AsyncSafeJobStatsCRDT) else other
        return self._crdt.merge(other_crdt)

    def to_dict(self) -> dict[str, Any]:
        return self._crdt.to_dict()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AsyncSafeJobStatsCRDT:
        instance = cls(job_id=data["job_id"])
        instance._crdt = JobStatsCRDT.from_dict(data)
        return instance

    def get_inner(self) -> JobStatsCRDT:
        return self._crdt
