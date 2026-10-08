"""``ExtensionDecision`` -- pickled under the namespace
``hyperscale.distributed.health.extension_decision`` (see that module)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass

from .extension_denial_code import ExtensionDenialCode
from .extension_witness_evidence import ExtensionWitnessEvidence

if TYPE_CHECKING:
    from .extension_decision_evaluator import ExtensionDecisionEvaluator


@dataclass(slots=True, frozen=True)
class ExtensionDecision:
    """Outcome of ``ExtensionDecisionEvaluator.decide``.

    Pure value — no side effects. The caller (a manager-side path
    like ``WorkerHealthManager.handle_extension_request`` or the
    HFD wrapper) commits the decision via ``ExtensionTracker.
    commit_grant`` / ``commit_deny`` after the AD-48 dissemination
    handler has propagated the corresponding ``ExtensionDecisionEvent``.
    """

    granted: bool
    extension_seconds: float
    denial_reason_code: ExtensionDenialCode
    denial_message: str | None
    evidence: ExtensionWitnessEvidence
    is_exhaustion_warning: bool
