"""``StatsEntry`` -- pickled under the namespace
``hyperscale.distributed.reliability.backpressure`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class StatsEntry:
    """A single stats entry with timestamp."""

    timestamp: float
    value: float
    count: int = 1  # Number of entries aggregated (1 for raw, >1 for aggregated)
    min_value: float | None = None
    max_value: float | None = None
    sum_value: float | None = None

    def __post_init__(self) -> None:
        if self.min_value is None:
            self.min_value = self.value
        if self.max_value is None:
            self.max_value = self.value
        if self.sum_value is None:
            self.sum_value = self.value

    @classmethod
    def aggregate(cls, entries: list["StatsEntry"]) -> "StatsEntry":
        """Aggregate multiple entries into a single entry."""
        if not entries:
            raise ValueError("Cannot aggregate empty list")

        total_count = sum(e.count for e in entries)
        total_sum = sum(e.sum_value or e.value for e in entries)
        min_val = min(e.min_value or e.value for e in entries)
        max_val = max(e.max_value or e.value for e in entries)

        return cls(
            timestamp=entries[-1].timestamp,  # Use latest timestamp
            value=total_sum / total_count,  # Average value
            count=total_count,
            min_value=min_val,
            max_value=max_val,
            sum_value=total_sum,
        )
