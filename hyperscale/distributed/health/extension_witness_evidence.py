"""``ExtensionWitnessEvidence`` -- pickled under the namespace
``hyperscale.distributed.health.extension_decision`` (see that module)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass
from hyperscale.distributed.health.progress_witness import WitnessVerdictKind

if TYPE_CHECKING:
    from .extension_decision_model import ExtensionDecision


@dataclass(slots=True, frozen=True)
class ExtensionWitnessEvidence:
    """Forensic record of every witness check for one decision.

    Carried in ``ExtensionDecision.evidence`` and replicated in the
    Phase H7 ``ExtensionDecisionEvent`` ledger so any manager in the
    cluster can answer "why was this decision made" from
    AD-48-disseminated state alone.

    All fields are scalars or enum kinds — safe to ship over the
    wire and persist in ``TimeoutTrackingState``.
    """

    progress_meaningful: bool
    progress_all_non_regressed: bool
    progress_any_advanced: bool
    throughput_verdict_kind: WitnessVerdictKind
    throughput_change_point_probability: float
    throughput_alpha_workflow: float
    throughput_predictive_mean_before: float
    throughput_predictive_mean_after: float
    overload_state: str
    seconds_since_last_extension: float
    extension_count_pre_decision: int
