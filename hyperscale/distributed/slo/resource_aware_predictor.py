from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.env import Env

from .slo_config import SLOConfig


@dataclass(slots=True)
class ResourceAwareSLOPredictor:
    """Predicts SLO violations from AD-41 resource metrics."""

    _config: SLOConfig

    @classmethod
    def from_env(cls, env: Env) -> "ResourceAwareSLOPredictor":
        return cls(_config=SLOConfig.from_env(env))

    def predict_slo_risk(
        self,
        cpu_pressure: float,
        cpu_uncertainty_pressure: float,
        memory_pressure: float,
        memory_uncertainty_pressure: float,
        current_slo_score: float,
    ) -> float:
        """Return predicted SLO risk factor (1.0 = normal, >1.0 = risk).

        Each pressure is the workload's share of the datacenter's capacity,
        and its uncertainty is measured the same way (the estimate's
        standard deviation over that capacity): a pressure counts in full
        when its estimate is certain, and half when the estimate is
        uncertain by the whole capacity."""
        if not self._config.enable_resource_prediction:
            return current_slo_score

        cpu_confidence = 1.0 / (1.0 + cpu_uncertainty_pressure)
        memory_confidence = 1.0 / (1.0 + memory_uncertainty_pressure)

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
