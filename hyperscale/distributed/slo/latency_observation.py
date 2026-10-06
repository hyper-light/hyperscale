from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class LatencyObservation:
    """Observed latency percentiles for a target."""

    target_id: str
    p50_ms: float
    p95_ms: float
    p99_ms: float
    sample_count: int
    window_start: float
    window_end: float

    def is_stale(self, max_age_seconds: float, now: float) -> bool:
        """Return True when the observation is older than max_age_seconds
        at ``now`` (the observing node's monotonic time)."""
        return (now - self.window_end) > max_age_seconds
