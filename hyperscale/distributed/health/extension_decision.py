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
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from hyperscale.distributed.health.extension_tracker import ExtensionTracker
from hyperscale.distributed.health.progress_witness import (
    ThroughputWitness,
    WitnessVerdictKind,
)
from hyperscale.distributed.health.workflow_progress_snapshot import (
    WorkflowProgressSnapshot,
)


# ============================================================================
# Public types
# ============================================================================


class ExtensionDenialCode(Enum):
    """Structured denial codes for AD-26 H5 multi-witness decisions.

    Each code is observable end-to-end: emitted in the
    ``HealthcheckExtensionResponse.denial_reason_code`` wire field
    (Phase H5 wire-format extension), recorded in the
    ``ExtensionDecisionEvent`` ledger entry (H7), and consumed by
    the H8 outcome-feedback loop for Bayesian alpha tuning.
    """

    NONE = "none"
    """Granted — no denial."""

    MAX_EXHAUSTED = "max_exhausted"
    """``extension_count >= max_extensions``. AD-26 hard cap."""

    COUNTER_REGRESSION = "counter_regression"
    """``WorkflowProgressSnapshot.all_non_regressed`` failed —
    counters went backward. Most-likely worker bug or attempted
    gaming."""

    NO_ADVANCEMENT = "no_advancement"
    """``WorkflowProgressSnapshot.any_advanced`` failed — every
    dimension stalled. Truly stuck workflow; AD-26 progress
    requirement violated."""

    THROUGHPUT_REGIME_DOWN = "throughput_regime_down"
    """H6 ThroughputWitness flagged a sustained throughput drop.
    Deny so the workflow's hard timeout can fire and the dispatcher
    can reschedule somewhere healthier."""

    OVERLOADED_STATE = "overloaded_state"
    """Worker reported AD-19 ``health_overload_state == "overloaded"``.
    Adding an extension to an already-overloaded worker delays
    eviction; deny and let drain logic engage."""

    RATE_LIMITED = "rate_limited"
    """Last grant was too recent
    (< ``min_between_extensions_seconds`` ago). Prevents extension
    spam within a single heartbeat interval."""


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


@dataclass(slots=True, frozen=True)
class ExtensionDecisionConfig:
    """Configuration for ``ExtensionDecisionEvaluator``."""

    # Rate-limit between successive extension grants on the same
    # worker. Defaults to half the AD-26 base_deadline (15s) so that
    # the cumulative time-to-exhaust spans at least a heartbeat-
    # interval-multiple even when every extension is granted as
    # soon as the rate limit allows. Prevents extension storms
    # within a single heartbeat-processing window.
    min_between_extensions_seconds: float = 15.0


# ============================================================================
# Evaluator
# ============================================================================


class ExtensionDecisionEvaluator:
    """Orchestrates the AD-26 H5 multi-witness extension decision.

    Stateless (per-call) orchestrator. Holds references to the
    deployment-shared throughput witness and the per-decision config;
    each ``decide(...)`` invocation is a pure function of those plus
    the per-(worker, workflow) inputs.

    Thread-safety: NOT thread-safe with respect to the throughput
    witness, which mutates per-stream state on each ``observe()``
    call. The manager serializes through the existing
    ``WorkerHealthManager`` extension lock.
    """

    def __init__(
        self,
        throughput_witness: ThroughputWitness,
        config: ExtensionDecisionConfig | None = None,
        time_source: Callable[[], float] | None = None,
    ) -> None:
        self._throughput_witness: ThroughputWitness = throughput_witness
        self._config: ExtensionDecisionConfig = (
            config if config is not None else ExtensionDecisionConfig()
        )
        self._now: Callable[[], float] = (
            time_source if time_source is not None else time.monotonic
        )

    @property
    def throughput_witness(self) -> ThroughputWitness:
        return self._throughput_witness

    @property
    def config(self) -> ExtensionDecisionConfig:
        return self._config

    def decide(
        self,
        *,
        tracker: ExtensionTracker,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
        active_in_cluster: int,
        active_in_dc: int,
        active_on_manager: int,
        active_on_worker: int,
    ) -> ExtensionDecision:
        """Run all five witnesses and return a structured decision.

        Args:
            tracker: Per-worker AD-26 ``ExtensionTracker``. Read-only
                from this call; the caller commits via
                ``commit_grant``/``commit_deny`` after handling.
            snapshot: Current ``WorkflowProgressSnapshot`` reported
                by the worker for this workflow (H3).
            last_snapshot: The previously-accepted snapshot for this
                workflow (manager-side state). ``None`` on the very
                first request — treated as the zero-progress
                baseline.
            throughput: ``WorkerHeartbeat.health_throughput`` for
                this worker. Fed into the H6 BOCPD detector.
            overload_state: AD-19 ``health_overload_state`` reported
                by this worker. ``"overloaded"`` short-circuits to
                deny.
            active_in_cluster, active_in_dc, active_on_manager,
            active_on_worker: Concurrent-workflow counts feeding the
                H6 hierarchical α-budget allocator.
        """
        now = self._now()

        # Witness 1 — worker-level: max-extensions cap (existing AD-26)
        if tracker.is_exhausted:
            return self._deny(
                tracker,
                ExtensionDenialCode.MAX_EXHAUSTED,
                f"Maximum extensions ({tracker.max_extensions}) exceeded",
                now,
                snapshot,
                last_snapshot,
                throughput=throughput,
                overload_state=overload_state,
                witness_verdict=WitnessVerdictKind.STATIONARY,
                witness_change_p=0.0,
                witness_alpha=0.0,
                witness_mean_before=0.0,
                witness_mean_after=0.0,
            )

        # Witness 2 — worker-level: rate-limit
        seconds_since_last = (
            now - tracker.last_extension_time
            if tracker.extension_count > 0
            else float("inf")
        )
        if (
            tracker.extension_count > 0
            and seconds_since_last < self._config.min_between_extensions_seconds
        ):
            return self._deny(
                tracker,
                ExtensionDenialCode.RATE_LIMITED,
                (
                    f"Last extension was {seconds_since_last:.1f}s ago; "
                    f"min_between_extensions_seconds = "
                    f"{self._config.min_between_extensions_seconds:.1f}"
                ),
                now,
                snapshot,
                last_snapshot,
                throughput=throughput,
                overload_state=overload_state,
                witness_verdict=WitnessVerdictKind.STATIONARY,
                witness_change_p=0.0,
                witness_alpha=0.0,
                witness_mean_before=0.0,
                witness_mean_after=0.0,
            )

        # Witness 3 — worker-level: overload-state guard
        if overload_state == "overloaded":
            return self._deny(
                tracker,
                ExtensionDenialCode.OVERLOADED_STATE,
                "Worker reports health_overload_state=overloaded",
                now,
                snapshot,
                last_snapshot,
                throughput=throughput,
                overload_state=overload_state,
                witness_verdict=WitnessVerdictKind.STATIONARY,
                witness_change_p=0.0,
                witness_alpha=0.0,
                witness_mean_before=0.0,
                witness_mean_after=0.0,
            )

        # Witness 4 — workflow-level: progress monotonicity (H3)
        baseline = last_snapshot if last_snapshot is not None else (
            WorkflowProgressSnapshot.initial(
                workflow_id=snapshot.workflow_id,
                cores_total=snapshot.cores_total,
            )
        )
        all_non_regressed = snapshot.all_non_regressed(baseline)
        any_advanced = snapshot.any_advanced(baseline)

        if not all_non_regressed:
            return self._deny(
                tracker,
                ExtensionDenialCode.COUNTER_REGRESSION,
                self._counter_regression_message(snapshot, baseline),
                now,
                snapshot,
                last_snapshot,
                throughput=throughput,
                overload_state=overload_state,
                witness_verdict=WitnessVerdictKind.STATIONARY,
                witness_change_p=0.0,
                witness_alpha=0.0,
                witness_mean_before=0.0,
                witness_mean_after=0.0,
                progress_all_non_regressed=False,
                progress_any_advanced=any_advanced,
            )

        if not any_advanced:
            return self._deny(
                tracker,
                ExtensionDenialCode.NO_ADVANCEMENT,
                (
                    "No progress dimension advanced since last snapshot "
                    f"(cores={snapshot.cores_completed}/{baseline.cores_completed}, "
                    f"steps={snapshot.step_transitions}/{baseline.step_transitions}, "
                    f"actions={snapshot.actions_completed}/{baseline.actions_completed})"
                ),
                now,
                snapshot,
                last_snapshot,
                throughput=throughput,
                overload_state=overload_state,
                witness_verdict=WitnessVerdictKind.STATIONARY,
                witness_change_p=0.0,
                witness_alpha=0.0,
                witness_mean_before=0.0,
                witness_mean_after=0.0,
                progress_all_non_regressed=True,
                progress_any_advanced=False,
            )

        # Witness 5 — workflow-level: throughput witness (H6 BOCPD)
        verdict = self._throughput_witness.observe(
            worker_id=tracker.worker_id,
            workflow_id=snapshot.workflow_id,
            throughput=throughput,
            active_in_cluster=active_in_cluster,
            active_in_dc=active_in_dc,
            active_on_manager=active_on_manager,
            active_on_worker=active_on_worker,
        )

        if verdict.kind == WitnessVerdictKind.REGIME_CHANGE_DOWN:
            return self._deny(
                tracker,
                ExtensionDenialCode.THROUGHPUT_REGIME_DOWN,
                (
                    "Throughput regime shifted down: "
                    f"P(change-point | history) = {verdict.change_point_probability:.4f} "
                    f"> α_workflow = {verdict.alpha_workflow:.4f} "
                    f"({verdict.predictive_mean_before:.2f} -> "
                    f"{verdict.predictive_mean_after:.2f})"
                ),
                now,
                snapshot,
                last_snapshot,
                throughput=throughput,
                overload_state=overload_state,
                witness_verdict=verdict.kind,
                witness_change_p=verdict.change_point_probability,
                witness_alpha=verdict.alpha_workflow,
                witness_mean_before=verdict.predictive_mean_before,
                witness_mean_after=verdict.predictive_mean_after,
                progress_all_non_regressed=True,
                progress_any_advanced=True,
            )

        # All witnesses passed — grant the geometric-decay extension.
        grant_seconds = max(
            tracker.min_grant,
            tracker.base_deadline / (2 ** tracker.extension_count),
        )
        return self._grant(
            tracker=tracker,
            grant_seconds=grant_seconds,
            now=now,
            snapshot=snapshot,
            last_snapshot=last_snapshot,
            throughput=throughput,
            overload_state=overload_state,
            verdict_kind=verdict.kind,
            witness_change_p=verdict.change_point_probability,
            witness_alpha=verdict.alpha_workflow,
            witness_mean_before=verdict.predictive_mean_before,
            witness_mean_after=verdict.predictive_mean_after,
        )

    # --------------------------------------------------------------
    # Internal helpers — deny / grant constructors
    # --------------------------------------------------------------

    def _deny(
        self,
        tracker: ExtensionTracker,
        code: ExtensionDenialCode,
        message: str,
        now: float,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
        witness_verdict: WitnessVerdictKind,
        witness_change_p: float,
        witness_alpha: float,
        witness_mean_before: float,
        witness_mean_after: float,
        progress_all_non_regressed: bool = True,
        progress_any_advanced: bool = True,
    ) -> ExtensionDecision:
        evidence = self._build_evidence(
            tracker=tracker,
            snapshot=snapshot,
            last_snapshot=last_snapshot,
            now=now,
            overload_state=overload_state,
            verdict_kind=witness_verdict,
            change_p=witness_change_p,
            alpha=witness_alpha,
            mean_before=witness_mean_before,
            mean_after=witness_mean_after,
            progress_all_non_regressed=progress_all_non_regressed,
            progress_any_advanced=progress_any_advanced,
        )
        return ExtensionDecision(
            granted=False,
            extension_seconds=0.0,
            denial_reason_code=code,
            denial_message=message,
            evidence=evidence,
            is_exhaustion_warning=False,
        )

    def _grant(
        self,
        *,
        tracker: ExtensionTracker,
        grant_seconds: float,
        now: float,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
        verdict_kind: WitnessVerdictKind,
        witness_change_p: float,
        witness_alpha: float,
        witness_mean_before: float,
        witness_mean_after: float,
    ) -> ExtensionDecision:
        # Predict whether this grant will trip the AD-26 exhaustion
        # warning. The actual transition happens inside
        # ``commit_grant``; we surface the prediction here so the
        # manager can include it in the response.
        post_grant_remaining = max(
            0, tracker.max_extensions - (tracker.extension_count + 1)
        )
        is_warning = (
            post_grant_remaining <= tracker.warning_threshold
            and not tracker.warning_sent
        )
        evidence = self._build_evidence(
            tracker=tracker,
            snapshot=snapshot,
            last_snapshot=last_snapshot,
            now=now,
            overload_state=overload_state,
            verdict_kind=verdict_kind,
            change_p=witness_change_p,
            alpha=witness_alpha,
            mean_before=witness_mean_before,
            mean_after=witness_mean_after,
            progress_all_non_regressed=True,
            progress_any_advanced=True,
        )
        return ExtensionDecision(
            granted=True,
            extension_seconds=grant_seconds,
            denial_reason_code=ExtensionDenialCode.NONE,
            denial_message=None,
            evidence=evidence,
            is_exhaustion_warning=is_warning,
        )

    def _build_evidence(
        self,
        *,
        tracker: ExtensionTracker,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        now: float,
        overload_state: str,
        verdict_kind: WitnessVerdictKind,
        change_p: float,
        alpha: float,
        mean_before: float,
        mean_after: float,
        progress_all_non_regressed: bool,
        progress_any_advanced: bool,
    ) -> ExtensionWitnessEvidence:
        seconds_since_last = (
            now - tracker.last_extension_time
            if tracker.extension_count > 0
            else 0.0
        )
        progress_meaningful = (
            progress_all_non_regressed and progress_any_advanced
        )
        return ExtensionWitnessEvidence(
            progress_meaningful=progress_meaningful,
            progress_all_non_regressed=progress_all_non_regressed,
            progress_any_advanced=progress_any_advanced,
            throughput_verdict_kind=verdict_kind,
            throughput_change_point_probability=change_p,
            throughput_alpha_workflow=alpha,
            throughput_predictive_mean_before=mean_before,
            throughput_predictive_mean_after=mean_after,
            overload_state=overload_state,
            seconds_since_last_extension=seconds_since_last,
            extension_count_pre_decision=tracker.extension_count,
        )

    @staticmethod
    def _counter_regression_message(
        current: WorkflowProgressSnapshot,
        baseline: WorkflowProgressSnapshot,
    ) -> str:
        regressions: list[str] = []
        if current.cores_completed < baseline.cores_completed:
            regressions.append(
                f"cores_completed {baseline.cores_completed} -> "
                f"{current.cores_completed}"
            )
        if current.step_transitions < baseline.step_transitions:
            regressions.append(
                f"step_transitions {baseline.step_transitions} -> "
                f"{current.step_transitions}"
            )
        if current.actions_completed < baseline.actions_completed:
            regressions.append(
                f"actions_completed {baseline.actions_completed} -> "
                f"{current.actions_completed}"
            )
        return "Counter regression: " + "; ".join(regressions)
