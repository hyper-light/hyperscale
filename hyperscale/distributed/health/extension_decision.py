"""
Multi-witness manager-side extension decision (AD-26 Phase H5).

Composes the worker-level ``ExtensionTracker`` (geometric grant decay
+ max_extensions cap from H1) with three independent per-workflow
witnesses:

1. **Counter monotonicity** — ``WorkflowProgressSnapshot.is_meaningful_
   progress`` (H3). Every dimension non-regressed, at least one
   strictly advanced. Tamper-resistant by construction.
2. **Throughput witness** — H6 BOCPD over the per-(worker, workflow)
   throughput stream. Detects sustained regime changes via MAP run
   length, classified UP / DOWN by the predictive-mean shift.
3. **Overload state** — AD-19 ``health_overload_state`` from the
   worker's heartbeat. ``"overloaded"`` triggers an immediate deny
   regardless of progress: an overloaded worker should drain or be
   rescheduled, not extended.

Plus two worker-level pre-checks routed through ``ExtensionTracker``:

4. **Max-extensions cap** (existing AD-26 line 41).
5. **Rate-limit** — ``min_between_extensions_seconds`` since the
   previous grant. Prevents spam when a worker repeatedly requests
   within the same heartbeat interval.

Every check is independently overrideable via deployment policy and
every denial path emits a structured ``ExtensionDenialCode`` plus a
forensic ``ExtensionWitnessEvidence`` snapshot — both are
disseminated through the AD-48 channel in Phase H7 so any manager in
the cluster can audit the decision.

Outcome contract: ``ExtensionDecisionEvaluator.decide`` is pure (no
state mutation) — callers commit the decision via
``ExtensionTracker.commit_grant`` / ``commit_deny`` after acting on
it. This separation lets the evaluator be safely re-run for
observability or shadow-mode scoring without affecting the live
tracker state.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.health.extension_tracker import ExtensionTracker
from hyperscale.distributed.health.progress_witness import ThroughputWitness, WitnessVerdictKind
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot

from .extension_decision_evaluator import _DEFAULT_CLOCK
from .extension_decision_model import ExtensionDecision
from .extension_decision_config import ExtensionDecisionConfig
from .extension_decision_evaluator import ExtensionDecisionEvaluator
from .extension_denial_code import ExtensionDenialCode
from .extension_witness_evidence import ExtensionWitnessEvidence

_REHOMED = (
    ExtensionDenialCode,
    ExtensionWitnessEvidence,
    ExtensionDecision,
    ExtensionDecisionConfig,
    ExtensionDecisionEvaluator,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
