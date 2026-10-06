"""Wire model ``JobStatsCRDT`` -- pickled under the wire namespace
``hyperscale.distributed.models.crdt`` (see that module)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass, field

from .g_counter import GCounter
from .job_stats_crdt_snapshot import JobStatsCRDTSnapshot
from .lww_map import LWWMap

if TYPE_CHECKING:
    from .async_safe_job_stats_crdt import AsyncSafeJobStatsCRDT


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
    rates: LWWMap[str, float] = field(default_factory=LWWMap)  # dc -> rate
    statuses: LWWMap[str, str] = field(default_factory=LWWMap)  # dc -> status

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

    def to_dict(self) -> JobStatsCRDTSnapshot:
        """Serialize to a dictionary."""
        return {
            "job_id": self.job_id,
            "completed": self.completed.to_dict(),
            "failed": self.failed.to_dict(),
            "rates": self.rates.to_dict(),
            "statuses": self.statuses.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: JobStatsCRDTSnapshot) -> JobStatsCRDT:
        """Deserialize from a dictionary."""
        return cls(
            job_id=data["job_id"],
            completed=GCounter.from_dict(data.get("completed", {})),
            failed=GCounter.from_dict(data.get("failed", {})),
            rates=LWWMap.from_dict(data.get("rates", {})),
            statuses=LWWMap.from_dict(data.get("statuses", {})),
        )
