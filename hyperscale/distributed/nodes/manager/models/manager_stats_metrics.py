"""``ManagerStatsCoordinator.get_stats_metrics`` -- the manager's AD-19 progress and stats-buffer readings."""

from __future__ import annotations

from typing import TypedDict


class ManagerStatsMetrics(TypedDict):
    """Dispatch throughput against its expectation, the progress state and how long it has held,
    the stats buffer's backpressure level (``BackpressureLevel`` value) and hot-tier fill, and the
    dispatches counted in the current throughput interval."""

    dispatch_throughput: float
    expected_throughput: float
    progress_state: str
    progress_state_duration: float
    backpressure_level: int
    stats_buffer_count: int
    throughput_count: int
