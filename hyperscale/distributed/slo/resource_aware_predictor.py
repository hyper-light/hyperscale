from __future__ import annotations

from dataclasses import dataclass, field

from hyperscale.distributed.env import Env

from .slo_config import SLOConfig


@dataclass(slots=True)
class ResourceAwareSLOPredictor:
    """Predicts SLO violations from AD-41 resource metrics."""

    _config: SLOConfig = field(default_factory=SLOConfig.from_env)

    @classmethod
    def from_env(cls, env: Env | None = None) -> "ResourceAwareSLOPredictor":
        return cls(_config=SLOConfig.from_env(env))

    def predict_slo_risk(
        self,
        cpu_pressure: float,
        cpu_uncertainty: float,
        memory_pressure: float,
        memory_uncertainty: float,
        current_slo_score: float,
    ) -> float:
        """Return predicted SLO risk factor (1.0 = normal, >1.0 = risk)."""
        if not self._config.enable_resource_prediction:
            return current_slo_score

        cpu_confidence = 1.0 / (1.0 + cpu_uncertainty / 20.0)
        memory_confidence = 1.0 / (1.0 + memory_uncertainty / 1e8)

        cpu_contribution = (
            cpu_pressure * self._config.cpu_latency_correlation * cpu_confidence
        )
        memory_contribution = (
            memory_pressure
            * self._config.memory_latency_correlation
            * memory_confidence
        )

        predicted_risk = 1.0 + cpu_contribution + memory_contribution
        blend_weight = self._config.prediction_blend_weight
        return (1.0 - blend_weight) * current_slo_score + blend_weight * predicted_risk
