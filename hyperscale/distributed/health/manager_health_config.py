"""``ManagerHealthConfig`` -- pickled under the namespace
``hyperscale.distributed.health.manager_health`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class ManagerHealthConfig:
    """Configuration for manager health thresholds."""

    # Liveness thresholds
    liveness_timeout_seconds: float = 30.0
    max_consecutive_liveness_failures: int = 3

    # Progress rate thresholds (as fraction of expected)
    normal_rate_threshold: float = 0.8  # >= 80% of expected = normal
    slow_rate_threshold: float = 0.3  # >= 30% of expected = slow
