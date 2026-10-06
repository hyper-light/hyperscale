"""``WitnessVerdict`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.throughput_witness`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass

from .witness_verdict_kind import WitnessVerdictKind


@dataclass(slots=True, frozen=True)
class WitnessVerdict:
    """Structured outcome from ``ThroughputWitness.observe``.

    Attributes:
        kind: Categorical outcome per ``WitnessVerdictKind``.
        change_point_probability: ``P(r_t = 0 | x_1:t)`` from the
            BOCPD posterior at this observation.
        alpha_workflow: The per-workflow false-positive budget the
            outcome was evaluated against.
        observation: The throughput sample that produced this
            verdict (carried for forensics / outcome feedback).
        observation_count: How many samples the per-stream BOCPD
            has processed including this one.
        predictive_mean_before: Posterior-marginalised predictive
            mean *before* this observation (i.e. the prior
            expectation we're comparing against).
        predictive_mean_after: Posterior-marginalised predictive
            mean *after* the update.
    """

    kind: WitnessVerdictKind
    change_point_probability: float
    alpha_workflow: float
    observation: float
    observation_count: int
    predictive_mean_before: float
    predictive_mean_after: float
