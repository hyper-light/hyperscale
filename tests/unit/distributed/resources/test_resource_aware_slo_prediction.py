"""
AD-42 Part 9: a datacenter's resource pressure raises its predicted SLO
risk in proportion to how sure the estimate behind it is.

The confidence divisors were fixed numbers (20 CPU-percent, 1e8 bytes),
so the same estimate counted fully on one datacenter and barely on
another depending only on its size. Uncertainty is now measured on the
pressure's own scale -- a share of the datacenter's capacity -- so a
pressure counts in full when certain and half when its estimate is
uncertain by the whole capacity, at any size.
"""

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.slo.resource_aware_predictor import ResourceAwareSLOPredictor
from hyperscale.distributed.slo.slo_config import SLOConfig

SETTINGS = Env()
CONFIG = SLOConfig.from_env(SETTINGS)
NEUTRAL_SCORE = 1.0


def predicted_risk(cpu_pressure: float, cpu_uncertainty_pressure: float) -> float:
    return ResourceAwareSLOPredictor.from_env(SETTINGS).predict_slo_risk(
        cpu_pressure=cpu_pressure,
        cpu_uncertainty_pressure=cpu_uncertainty_pressure,
        memory_pressure=0.0,
        memory_uncertainty_pressure=0.0,
        current_slo_score=NEUTRAL_SCORE,
    )


def cpu_term(cpu_pressure: float, confidence: float) -> float:
    return CONFIG.prediction_blend_weight * cpu_pressure * CONFIG.cpu_latency_correlation * confidence


@pytest.mark.parametrize("cpu_pressure", [0.25, 0.5, 0.9])
def test_a_certain_pressure_counts_in_full_and_one_uncertain_by_the_whole_capacity_counts_half(
    cpu_pressure: float,
) -> None:
    assert predicted_risk(cpu_pressure, 0.0) == pytest.approx(NEUTRAL_SCORE + cpu_term(cpu_pressure, 1.0))
    assert predicted_risk(cpu_pressure, 1.0) == pytest.approx(NEUTRAL_SCORE + cpu_term(cpu_pressure, 0.5))


def test_an_unmeasurable_uncertainty_contributes_nothing() -> None:
    assert predicted_risk(0.9, float("inf")) == pytest.approx(NEUTRAL_SCORE)
