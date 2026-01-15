from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.env import Env

from .latency_observation import LatencyObservation
from .latency_slo import LatencySLO
from .slo_compliance_level import SLOComplianceLevel
from .slo_config import SLOConfig


@dataclass(slots=True)
class SLOComplianceScore:
    """Computed SLO compliance for a target."""

    target_id: str
    p50_ratio: float
    p95_ratio: float
    p99_ratio: float
    composite_score: float
    confidence: float
    compliance_level: SLOComplianceLevel
    routing_factor: float

    @classmethod
    def calculate(
        cls,
        target_id: str,
        observation: LatencyObservation,
        slo: LatencySLO,
        env: Env | None = None,
    ) -> "SLOComplianceScore":
        """Calculate compliance score from observation."""
        config = SLOConfig.from_env(env)
        p50_ratio = observation.p50_ms / slo.p50_target_ms
        p95_ratio = observation.p95_ms / slo.p95_target_ms
        p99_ratio = observation.p99_ms / slo.p99_target_ms

        composite_score = (
            slo.p50_weight * p50_ratio
            + slo.p95_weight * p95_ratio
            + slo.p99_weight * p99_ratio
        )

        min_samples = max(slo.min_sample_count, 1)
        confidence = min(1.0, observation.sample_count / min_samples)
        if confidence < 1.0:
            composite_score = composite_score * confidence + 1.0 * (1.0 - confidence)

        if composite_score < 0.8:
            compliance_level = SLOComplianceLevel.EXCEEDING
        elif composite_score < 1.0:
            compliance_level = SLOComplianceLevel.MEETING
        elif composite_score < 1.2:
            compliance_level = SLOComplianceLevel.WARNING
        elif composite_score < 1.5:
            compliance_level = SLOComplianceLevel.VIOLATING
        else:
            compliance_level = SLOComplianceLevel.CRITICAL

        routing_factor = 1.0 + config.score_weight * (composite_score - 1.0)
        routing_factor = max(config.factor_min, min(config.factor_max, routing_factor))

        return cls(
            target_id=target_id,
            p50_ratio=p50_ratio,
            p95_ratio=p95_ratio,
            p99_ratio=p99_ratio,
            composite_score=composite_score,
            confidence=confidence,
            compliance_level=compliance_level,
            routing_factor=routing_factor,
        )
