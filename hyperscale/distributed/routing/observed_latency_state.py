"""
Observed latency state for adaptive route learning (AD-45).
"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic


import statistics
from collections import deque
from typing import Deque


@dataclass(slots=True)
class ObservedLatencyState:
    """
    Tracks observed job completion latency per datacenter using EWMA.
    Includes percentile tracking and jitter detection (Task 61).
    """

    datacenter_id: str
    ewma_ms: float = 0.0
    sample_count: int = 0
    last_update: float = 0.0
    ewma_variance: float = 0.0

    # Percentile tracking (Task 61)
    # We keep a sliding window of recent samples for percentile calculation
    _recent_samples: Deque[float] | None = None
    _max_samples: int = 100
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0

    # Jitter tracking (Task 61)
    jitter_ms: float = 0.0  # Running jitter (mean absolute deviation)
    _last_latency_ms: float = 0.0

    def __post_init__(self) -> None:
        if self._recent_samples is None:
            object.__setattr__(self, "_recent_samples", deque(maxlen=self._max_samples))

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

        # Jitter tracking (Task 61)
        if self._last_latency_ms > 0:
            instant_jitter = abs(latency_ms - self._last_latency_ms)
            self.jitter_ms = self.jitter_ms + alpha * (instant_jitter - self.jitter_ms)
        self._last_latency_ms = latency_ms

        # Percentile tracking (Task 61)
        if self._recent_samples is not None:
            self._recent_samples.append(latency_ms)
            self._update_percentiles()

    def _update_percentiles(self) -> None:
        """Update percentile calculations from recent samples (Task 61)."""
        if self._recent_samples is None or len(self._recent_samples) < 2:
            return

        sorted_samples = sorted(self._recent_samples)
        n = len(sorted_samples)

        p50_idx = int(n * 0.50)
        p95_idx = min(int(n * 0.95), n - 1)
        p99_idx = min(int(n * 0.99), n - 1)

        self.p50_ms = sorted_samples[p50_idx]
        self.p95_ms = sorted_samples[p95_idx]
        self.p99_ms = sorted_samples[p99_idx]

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
