"""Statistics shape produced by ``EventLoopHealthMonitor.get_stats``."""

from typing import TypedDict


class EventLoopHealthStats(TypedDict):
    """Event-loop lag measurements and monitor counters."""

    is_degraded: bool
    current_lag_ratio: float
    average_lag_ratio: float
    max_lag_ratio: float
    p99_lag_ratio: float
    health_score: float
    total_samples: int
    lag_samples: int
    critical_samples: int
    degraded_transitions: int
    log_write_failures: int
    consecutive_lag: int
    consecutive_ok: int
    unmanaged_tasks_created: int
    pending_callback_tasks: int
