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
            max_wait_seconds=getattr(
                env, "SPILLOVER_MAX_WAIT_SECONDS", cls.max_wait_seconds
            ),
            max_latency_penalty_ms=getattr(
                env, "SPILLOVER_MAX_LATENCY_PENALTY_MS", cls.max_latency_penalty_ms
            ),
            min_improvement_ratio=getattr(
                env, "SPILLOVER_MIN_IMPROVEMENT_RATIO", cls.min_improvement_ratio
            ),
            spillover_enabled=getattr(env, "SPILLOVER_ENABLED", cls.spillover_enabled),
            capacity_staleness_threshold_seconds=getattr(
                env,
                "CAPACITY_STALENESS_THRESHOLD_SECONDS",
                cls.capacity_staleness_threshold_seconds,
            ),
        )
