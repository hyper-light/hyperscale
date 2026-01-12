from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable, TypeVar

from hyperscale.distributed.env import Env


T = TypeVar("T")


def _parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_env_value(
    env: Env | None,
    name: str,
    default: T,
    cast: Callable[[object], T],
) -> T:
    env_value = getattr(env, name, None) if env is not None else None
    if env_value is not None:
        return cast(env_value)
    raw_value = os.getenv(name)
    if raw_value is not None:
        return cast(raw_value)
    return default


@dataclass(slots=True)
class SLOConfig:
    """Configuration defaults for SLO-aware routing and health."""

    tdigest_delta: float = 100.0
    tdigest_max_unmerged: int = 2048
    window_duration_seconds: float = 60.0
    max_windows: int = 5
    evaluation_window_seconds: float = 300.0
    p50_target_ms: float = 50.0
    p95_target_ms: float = 200.0
    p99_target_ms: float = 500.0
    p50_weight: float = 0.2
    p95_weight: float = 0.5
    p99_weight: float = 0.3
    min_sample_count: int = 100
    factor_min: float = 0.5
    factor_max: float = 3.0
    score_weight: float = 0.4
    busy_p50_ratio: float = 1.5
    degraded_p95_ratio: float = 2.0
    degraded_p99_ratio: float = 3.0
    unhealthy_p99_ratio: float = 5.0
    busy_window_seconds: float = 60.0
    degraded_window_seconds: float = 180.0
    unhealthy_window_seconds: float = 300.0
    enable_resource_prediction: bool = True
    cpu_latency_correlation: float = 0.7
    memory_latency_correlation: float = 0.4
    prediction_blend_weight: float = 0.4
    gossip_summary_ttl_seconds: float = 30.0
    gossip_max_jobs_per_heartbeat: int = 100

    @classmethod
    def from_env(cls, env: Env | None = None) -> "SLOConfig":
        return cls(
            tdigest_delta=_resolve_env_value(env, "SLO_TDIGEST_DELTA", 100.0, float),
            tdigest_max_unmerged=_resolve_env_value(
                env, "SLO_TDIGEST_MAX_UNMERGED", 2048, int
            ),
            window_duration_seconds=_resolve_env_value(
                env, "SLO_WINDOW_DURATION_SECONDS", 60.0, float
            ),
            max_windows=_resolve_env_value(env, "SLO_MAX_WINDOWS", 5, int),
            evaluation_window_seconds=_resolve_env_value(
                env,
                "SLO_EVALUATION_WINDOW_SECONDS",
                300.0,
                float,
            ),
            p50_target_ms=_resolve_env_value(env, "SLO_P50_TARGET_MS", 50.0, float),
            p95_target_ms=_resolve_env_value(env, "SLO_P95_TARGET_MS", 200.0, float),
            p99_target_ms=_resolve_env_value(env, "SLO_P99_TARGET_MS", 500.0, float),
            p50_weight=_resolve_env_value(env, "SLO_P50_WEIGHT", 0.2, float),
            p95_weight=_resolve_env_value(env, "SLO_P95_WEIGHT", 0.5, float),
            p99_weight=_resolve_env_value(env, "SLO_P99_WEIGHT", 0.3, float),
            min_sample_count=_resolve_env_value(env, "SLO_MIN_SAMPLE_COUNT", 100, int),
            factor_min=_resolve_env_value(env, "SLO_FACTOR_MIN", 0.5, float),
            factor_max=_resolve_env_value(env, "SLO_FACTOR_MAX", 3.0, float),
            score_weight=_resolve_env_value(env, "SLO_SCORE_WEIGHT", 0.4, float),
            busy_p50_ratio=_resolve_env_value(env, "SLO_BUSY_P50_RATIO", 1.5, float),
            degraded_p95_ratio=_resolve_env_value(
                env, "SLO_DEGRADED_P95_RATIO", 2.0, float
            ),
            degraded_p99_ratio=_resolve_env_value(
                env, "SLO_DEGRADED_P99_RATIO", 3.0, float
            ),
            unhealthy_p99_ratio=_resolve_env_value(
                env, "SLO_UNHEALTHY_P99_RATIO", 5.0, float
            ),
            busy_window_seconds=_resolve_env_value(
                env, "SLO_BUSY_WINDOW_SECONDS", 60.0, float
            ),
            degraded_window_seconds=_resolve_env_value(
                env, "SLO_DEGRADED_WINDOW_SECONDS", 180.0, float
            ),
            unhealthy_window_seconds=_resolve_env_value(
                env, "SLO_UNHEALTHY_WINDOW_SECONDS", 300.0, float
            ),
            enable_resource_prediction=_resolve_env_value(
                env,
                "SLO_ENABLE_RESOURCE_PREDICTION",
                True,
                _parse_bool,
            ),
            cpu_latency_correlation=_resolve_env_value(
                env, "SLO_CPU_LATENCY_CORRELATION", 0.7, float
            ),
            memory_latency_correlation=_resolve_env_value(
                env, "SLO_MEMORY_LATENCY_CORRELATION", 0.4, float
            ),
            prediction_blend_weight=_resolve_env_value(
                env,
                "SLO_PREDICTION_BLEND_WEIGHT",
                0.4,
                float,
            ),
            gossip_summary_ttl_seconds=_resolve_env_value(
                env,
                "SLO_GOSSIP_SUMMARY_TTL_SECONDS",
                30.0,
                float,
            ),
            gossip_max_jobs_per_heartbeat=_resolve_env_value(
                env,
                "SLO_GOSSIP_MAX_JOBS_PER_HEARTBEAT",
                100,
                int,
            ),
        )
