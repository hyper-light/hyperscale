"""``StatsBufferConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.backpressure`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from .stats_buffer import StatsBuffer


@dataclass(slots=True)
class StatsBufferConfig:
    """Configuration for StatsBuffer."""

    # HOT tier settings
    hot_max_entries: int = 1000
    hot_max_age_seconds: float = 60.0

    # WARM tier settings (10s aggregates)
    warm_max_entries: int = 360
    warm_aggregate_seconds: float = 10.0
    warm_max_age_seconds: float = 3600.0  # 1 hour

    # COLD tier settings (1min aggregates)
    cold_max_entries: int = 1440
    cold_aggregate_seconds: float = 60.0
    cold_max_age_seconds: float = 86400.0  # 24 hours

    # Backpressure thresholds (as fraction of hot tier capacity)
    throttle_threshold: float = 0.70
    batch_threshold: float = 0.85
    reject_threshold: float = 0.95
