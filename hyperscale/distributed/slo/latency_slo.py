from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.env import Env

from .slo_config import SLOConfig


@dataclass(frozen=True, slots=True)
class LatencySLO:
    """Latency SLO definition with Env-configurable defaults."""

    p50_target_ms: float
    p95_target_ms: float
    p99_target_ms: float
    p50_weight: float
    p95_weight: float
    p99_weight: float
    min_sample_count: int
    evaluation_window_seconds: float

    @classmethod
    def from_env(cls, env: Env | None = None) -> "LatencySLO":
        config = SLOConfig.from_env(env)
        return cls(
            p50_target_ms=config.p50_target_ms,
            p95_target_ms=config.p95_target_ms,
            p99_target_ms=config.p99_target_ms,
            p50_weight=config.p50_weight,
            p95_weight=config.p95_weight,
            p99_weight=config.p99_weight,
            min_sample_count=config.min_sample_count,
            evaluation_window_seconds=config.evaluation_window_seconds,
        )
