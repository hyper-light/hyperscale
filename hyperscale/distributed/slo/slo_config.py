from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.env import Env


@dataclass(slots=True)
class SLOConfig:
    """SLO-aware routing and health settings (AD-42), read from Env."""

    tdigest_delta: float
    tdigest_max_unmerged: int
    window_duration_seconds: float
    max_windows: int
    evaluation_window_seconds: float
    p50_target_ms: float
    p95_target_ms: float
    p99_target_ms: float
    p50_weight: float
    p95_weight: float
    p99_weight: float
    min_sample_count: int
    factor_min: float
    factor_max: float
    score_weight: float
    busy_p50_ratio: float
    degraded_p95_ratio: float
    degraded_p99_ratio: float
    unhealthy_p99_ratio: float
    busy_window_seconds: float
    degraded_window_seconds: float
    unhealthy_window_seconds: float
    enable_resource_prediction: bool
    cpu_latency_correlation: float
    memory_latency_correlation: float
    prediction_blend_weight: float
    gossip_summary_ttl_seconds: float
    gossip_max_jobs_per_heartbeat: int

    @classmethod
    def from_env(cls, env: Env) -> "SLOConfig":
        return cls(
            tdigest_delta=env.SLO_TDIGEST_DELTA,
            tdigest_max_unmerged=env.SLO_TDIGEST_MAX_UNMERGED,
            window_duration_seconds=env.SLO_WINDOW_DURATION_SECONDS,
            max_windows=env.SLO_MAX_WINDOWS,
            evaluation_window_seconds=env.SLO_EVALUATION_WINDOW_SECONDS,
            p50_target_ms=env.SLO_P50_TARGET_MS,
            p95_target_ms=env.SLO_P95_TARGET_MS,
            p99_target_ms=env.SLO_P99_TARGET_MS,
            p50_weight=env.SLO_P50_WEIGHT,
            p95_weight=env.SLO_P95_WEIGHT,
            p99_weight=env.SLO_P99_WEIGHT,
            min_sample_count=env.SLO_MIN_SAMPLE_COUNT,
            factor_min=env.SLO_FACTOR_MIN,
            factor_max=env.SLO_FACTOR_MAX,
            score_weight=env.SLO_SCORE_WEIGHT,
            busy_p50_ratio=env.SLO_BUSY_P50_RATIO,
            degraded_p95_ratio=env.SLO_DEGRADED_P95_RATIO,
            degraded_p99_ratio=env.SLO_DEGRADED_P99_RATIO,
            unhealthy_p99_ratio=env.SLO_UNHEALTHY_P99_RATIO,
            busy_window_seconds=env.SLO_BUSY_WINDOW_SECONDS,
            degraded_window_seconds=env.SLO_DEGRADED_WINDOW_SECONDS,
            unhealthy_window_seconds=env.SLO_UNHEALTHY_WINDOW_SECONDS,
            enable_resource_prediction=env.SLO_ENABLE_RESOURCE_PREDICTION,
            cpu_latency_correlation=env.SLO_CPU_LATENCY_CORRELATION,
            memory_latency_correlation=env.SLO_MEMORY_LATENCY_CORRELATION,
            prediction_blend_weight=env.SLO_PREDICTION_BLEND_WEIGHT,
            gossip_summary_ttl_seconds=env.SLO_GOSSIP_SUMMARY_TTL_SECONDS,
            gossip_max_jobs_per_heartbeat=env.SLO_GOSSIP_MAX_JOBS_PER_HEARTBEAT,
        )
