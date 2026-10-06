"""``WindowedStatsMetrics`` -- pickled under the namespace
``hyperscale.distributed.jobs.windowed_stats_collector`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class WindowedStatsMetrics:
    windows_flushed: int = 0
    windows_dropped_late: int = 0
    stats_recorded: int = 0
    stats_dropped_late: int = 0
    duplicates_detected: int = 0
