"""
Spillover configuration for capacity-aware routing (AD-43).
"""

from dataclasses import dataclass

from hyperscale.distributed.env.env import Env


@dataclass(slots=True)
class SpilloverConfig:
    """
    Configuration for spillover evaluation thresholds.
    """

    max_wait_seconds: float = 60.0
    max_latency_penalty_ms: float = 100.0
    min_improvement_ratio: float = 0.5
    spillover_enabled: bool = True
    capacity_staleness_threshold_seconds: float = 30.0

    @classmethod
    def from_env(cls, env: Env):
        """
        Create a configuration instance from environment settings.
        """
        return cls(
            max_wait_seconds=env.SPILLOVER_MAX_WAIT_SECONDS,
            max_latency_penalty_ms=env.SPILLOVER_MAX_LATENCY_PENALTY_MS,
            min_improvement_ratio=env.SPILLOVER_MIN_IMPROVEMENT_RATIO,
            spillover_enabled=env.SPILLOVER_ENABLED,
            capacity_staleness_threshold_seconds=env.CAPACITY_STALENESS_THRESHOLD_SECONDS,
        )
