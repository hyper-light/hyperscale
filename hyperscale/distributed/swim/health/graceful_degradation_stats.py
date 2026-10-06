"""Statistics shape produced by ``GracefulDegradation.get_stats``."""

from typing import TypedDict


class GracefulDegradationStats(TypedDict):
    """Current degradation level, its policy, and skip counters."""

    level: str
    level_value: int
    description: str
    probe_rate: float
    gossip_rate: float
    timeout_multiplier: float
    level_changes: int
    probes_skipped: int
    gossips_skipped: int
    log_write_failures: int
    time_at_level: float
