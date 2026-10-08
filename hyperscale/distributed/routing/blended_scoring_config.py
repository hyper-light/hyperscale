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

    adaptive_routing_enabled: bool
    ewma_alpha: float
    min_samples_for_confidence: int
    max_staleness_seconds: float
    latency_cap_ms: float

    @classmethod
    def from_env(cls, env: Env) -> "BlendedScoringConfig":
        """
        Create a configuration instance from environment settings.
        """
        return cls(
            adaptive_routing_enabled=env.ADAPTIVE_ROUTING_ENABLED,
            ewma_alpha=env.ADAPTIVE_ROUTING_EWMA_ALPHA,
            min_samples_for_confidence=env.ADAPTIVE_ROUTING_MIN_SAMPLES,
            max_staleness_seconds=env.ADAPTIVE_ROUTING_MAX_STALENESS_SECONDS,
            latency_cap_ms=env.ADAPTIVE_ROUTING_LATENCY_CAP_MS,
        )
