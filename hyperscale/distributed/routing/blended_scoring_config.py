"""
Blended scoring configuration for adaptive routing (AD-45).
"""

from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.env.env import Env


@dataclass(slots=True)
class BlendedScoringConfig:
    """
    Configuration for adaptive route learning.
    """

    adaptive_routing_enabled: bool = True
    ewma_alpha: float = 0.2
    min_samples_for_confidence: int = 10
    max_staleness_seconds: float = 300.0
    latency_cap_ms: float = 60000.0

    @classmethod
    def from_env(cls, env: Env) -> "BlendedScoringConfig":
        """
        Create a configuration instance from environment settings.
        """
        return cls(
            adaptive_routing_enabled=getattr(
                env, "ADAPTIVE_ROUTING_ENABLED", cls.adaptive_routing_enabled
            ),
            ewma_alpha=getattr(env, "ADAPTIVE_ROUTING_EWMA_ALPHA", cls.ewma_alpha),
            min_samples_for_confidence=getattr(
                env, "ADAPTIVE_ROUTING_MIN_SAMPLES", cls.min_samples_for_confidence
            ),
            max_staleness_seconds=getattr(
                env, "ADAPTIVE_ROUTING_MAX_STALENESS_SECONDS", cls.max_staleness_seconds
            ),
            latency_cap_ms=getattr(
                env, "ADAPTIVE_ROUTING_LATENCY_CAP_MS", cls.latency_cap_ms
            ),
        )
