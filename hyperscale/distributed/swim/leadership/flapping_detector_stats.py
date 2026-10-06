"""Statistics shape produced by ``FlappingDetector.get_stats``."""

from typing import TypedDict


class FlappingDetectorStats(TypedDict):
    """Leadership-change rate, cooldown, and flapping episode counters."""

    is_flapping: bool
    current_cooldown: float
    changes_in_window: int
    change_rate_per_min: float
    total_changes: int
    flapping_episodes: int
    log_write_failures: int
    total_flapping_duration: float
    window_seconds: float
    max_changes_per_window: int
