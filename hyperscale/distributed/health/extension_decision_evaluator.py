"""``ExtensionDecisionEvaluator`` -- pickled under the namespace
``hyperscale.distributed.health.extension_decision`` (see that module)."""

from __future__ import annotations

from typing import Callable
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.health.extension_tracker import ExtensionTracker
from hyperscale.distributed.health.hierarchical_alpha_tuner import HierarchicalAlphaTuner
from hyperscale.distributed.health.progress_witness import ThroughputWitness, WitnessVerdictKind
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot

from .extension_decision_model import ExtensionDecision
from .extension_decision_config import ExtensionDecisionConfig
from .extension_denial_code import ExtensionDenialCode
from .extension_witness_evidence import ExtensionWitnessEvidence

_DEFAULT_CLOCK: Clock = RealClock()

# Progress counters ``_counter_regression_message`` reports, in report order.
_PROGRESS_COUNTER_NAMES: tuple[str, ...] = (
    "cores_completed",
    "step_transitions",
    "actions_completed",
)


class ExtensionDecisionEvaluator:
    """Orchestrates the AD-26 H5 multi-witness extension decision.

    Stateless (per-call) orchestrator. Holds references to the
    deployment-shared throughput witness, the H8 outcome tuner and the
    per-decision config; each ``decide(...)`` invocation is a pure
    function of those plus the per-(worker, workflow) inputs.

    Thread-safety: NOT thread-safe with respect to the throughput
    witness, which mutates per-stream state on each ``observe()``
    call. The manager serializes through the existing
    ``WorkerHealthManager`` extension lock.
    """

    def __init__(
        self,
        throughput_witness: ThroughputWitness,
        config: ExtensionDecisionConfig | None = None,
        *,
        alpha_tuner: HierarchicalAlphaTuner,
        clock: Clock | None = None,
    ) -> None:
        # Phase 5 DI seam — replaces the prior ``time_source:
        # Callable[[], float] | None`` parameter. ``clock.monotonic``
        # is bound to ``self._now`` so the existing ``self._now()``
        # call sites in ``decide`` keep working unchanged; Phase 6 SIM
        # mode passes ``VirtualClock`` here and ``decide`` sees the
        # simulated timeline.
        self._throughput_witness: ThroughputWitness = throughput_witness
        # AD-26 H8: the per-workflow-class outcome posterior. Owned by
        # ``WorkerHealthManager``, which feeds it every outcome; read
        # here to weight each throughput-witness test's α.
        self._alpha_tuner: HierarchicalAlphaTuner = alpha_tuner
        self._config: ExtensionDecisionConfig = (
            config if config is not None else ExtensionDecisionConfig()
        )
        self._clock: Clock = clock if clock is not None else _DEFAULT_CLOCK
        self._now: Callable[[], float] = self._clock.monotonic

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
        workflow_class: str,
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
            workflow_class: The workflow's class name -- the key of
                its H8 outcome posterior, which re-weights the H6 α.
        """
        now = self._now()

        if (
            worker_denial := self._worker_witness_denial(
                tracker, now, snapshot, last_snapshot, throughput, overload_state
            )
        ) is not None:
            return worker_denial

        if (
            progress_denial := self._progress_witness_denial(
                tracker, now, snapshot, last_snapshot, throughput, overload_state
            )
        ) is not None:
            return progress_denial

        return self._throughput_witness_decision(
            tracker=tracker,
            now=now,
            snapshot=snapshot,
            last_snapshot=last_snapshot,
            throughput=throughput,
            overload_state=overload_state,
            active_in_cluster=active_in_cluster,
            active_in_dc=active_in_dc,
            active_on_manager=active_on_manager,
            active_on_worker=active_on_worker,
            workflow_class=workflow_class,
        )

    def _worker_witness_denial(
        self,
        tracker: ExtensionTracker,
        now: float,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
    ) -> ExtensionDecision | None:
        """Witnesses 1-3 (AD-26 H5 worker-level): the denial of the first
        one that fails, or ``None`` when all three pass."""
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

        if (
            rate_limit_denial := self._rate_limit_denial(
                tracker, now, snapshot, last_snapshot, throughput, overload_state
            )
        ) is not None:
            return rate_limit_denial

        return self._overload_state_denial(
            tracker, now, snapshot, last_snapshot, throughput, overload_state
        )

    def _rate_limit_denial(
        self,
        tracker: ExtensionTracker,
        now: float,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
    ) -> ExtensionDecision | None:
        """Witness 2 (AD-26 H5): deny an extension requested sooner than
        ``min_between_extensions_seconds`` after the previous grant; a
        worker with no prior extension always passes."""
        # Witness 2 — worker-level: rate-limit
        if not tracker.extension_count > 0:
            return None
        seconds_since_last = now - tracker.last_extension_time
        if seconds_since_last < self._config.min_between_extensions_seconds:
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
        return None

    def _overload_state_denial(
        self,
        tracker: ExtensionTracker,
        now: float,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
    ) -> ExtensionDecision | None:
        """Witness 3 (AD-26 H5, AD-19 overload state): deny a worker that
        reports itself overloaded."""
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
        return None

    @staticmethod
    def _progress_baseline(
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
    ) -> WorkflowProgressSnapshot:
        """The snapshot witness 4 (H3) compares against: the last accepted
        one, or the zero-progress baseline on a first request."""
        return last_snapshot if last_snapshot is not None else (
            WorkflowProgressSnapshot.initial(
                workflow_id=snapshot.workflow_id,
                cores_total=snapshot.cores_total,
            )
        )

    def _progress_witness_denial(
        self,
        tracker: ExtensionTracker,
        now: float,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
    ) -> ExtensionDecision | None:
        """Witness 4 (AD-26 H3 progress monotonicity): deny when a counter
        regressed or none advanced, else ``None``."""
        # Witness 4 — workflow-level: progress monotonicity (H3)
        baseline = self._progress_baseline(snapshot, last_snapshot)
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
        return None

    def _throughput_witness_decision(
        self,
        *,
        tracker: ExtensionTracker,
        now: float,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
        active_in_cluster: int,
        active_in_dc: int,
        active_on_manager: int,
        active_on_worker: int,
        workflow_class: str,
    ) -> ExtensionDecision:
        """Witness 5 (AD-26 H6 BOCPD): deny on a confirmed downward
        throughput regime change, otherwise grant the geometric-decay
        extension.

        The test's level is the H6 hierarchical α composed with the
        workflow class's H8 outcome posterior
        (``HierarchicalAlphaTuner.alpha_budget``), clamped to the H6
        floor and ceiling: a class whose workflows fail more often
        needs less evidence before a deny, one that rarely fails
        needs more."""
        budget = self._throughput_witness.budget
        alpha_workflow = self._alpha_tuner.alpha_budget(
            workflow_class,
            budget.workflow_alpha_from_counts(
                active_in_cluster=active_in_cluster,
                active_in_dc=active_in_dc,
                active_on_manager=active_on_manager,
                active_on_worker=active_on_worker,
            ),
            budget.config.alpha_workflow_floor,
            budget.config.alpha_workflow_ceiling,
        )
        # Witness 5 — workflow-level: throughput witness (H6 BOCPD), fed
        # the workflow's own progress rate by the manager's progress path.
        verdict = self._throughput_witness.assess(
            worker_id=tracker.worker_id,
            workflow_id=snapshot.workflow_id,
            alpha_workflow=alpha_workflow,
        )

        if verdict.kind == WitnessVerdictKind.REGIME_CHANGE_DOWN:
            return self._deny(
                tracker,
                ExtensionDenialCode.THROUGHPUT_REGIME_DOWN,
                (
                    "Throughput regime shifted down: BOCPD change point "
                    f"confirmed by K-S at α_workflow = {verdict.alpha_workflow:.6f} "
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
        regressions: list[str] = [
            f"{counter_name} {getattr(baseline, counter_name)} -> "
            f"{getattr(current, counter_name)}"
            for counter_name in _PROGRESS_COUNTER_NAMES
            if getattr(current, counter_name) < getattr(baseline, counter_name)
        ]
        return "Counter regression: " + "; ".join(regressions)
