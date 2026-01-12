"""
Observed latency state for adaptive route learning (AD-45).
"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic


@dataclass(slots=True)
class ObservedLatencyState:
    """
    Tracks observed job completion latency per datacenter using EWMA.
    """

    datacenter_id: str
    ewma_ms: float = 0.0
    sample_count: int = 0
    last_update: float = 0.0
    ewma_variance: float = 0.0

    def record_latency(
        self,
        latency_ms: float,
        alpha: float,
        now: float | None = None,
    ) -> None:
        """
        Record an observed job completion latency.

        Args:
            latency_ms: Observed latency in milliseconds.
            alpha: EWMA decay factor (0.0-1.0, higher = more responsive).
            now: Current monotonic time for testing.
        """
        current_time = now or monotonic()

        if self.sample_count == 0:
            self.ewma_ms = latency_ms
            self.ewma_variance = 0.0
        else:
            delta = latency_ms - self.ewma_ms
            self.ewma_ms = self.ewma_ms + alpha * delta
            self.ewma_variance = (1 - alpha) * (
                self.ewma_variance + alpha * delta * delta
            )

        self.sample_count += 1
        self.last_update = current_time

    def get_confidence(self, min_samples: int) -> float:
        """
        Get confidence in observed latency estimate.
        """
        if self.sample_count == 0:
            return 0.0
        if min_samples <= 0:
            return 1.0
        return min(1.0, self.sample_count / min_samples)

    def get_stddev_ms(self) -> float:
        """Get estimated standard deviation in milliseconds."""
        if self.ewma_variance <= 0.0:
            return 0.0
        return self.ewma_variance**0.5

    def is_stale(self, max_age_seconds: float, now: float | None = None) -> bool:
        """Return True when observations are stale."""
        current_time = now or monotonic()
        if self.last_update == 0.0:
            return True
        return (current_time - self.last_update) > max_age_seconds
