"""
Worker Health Manager for Adaptive Healthcheck Extensions (AD-26).

This module provides the WorkerHealthManager class that managers use
to track worker health and handle deadline extension requests.

Key responsibilities:
- Track ExtensionTracker per worker
- Handle extension requests with proper validation
- Reset trackers when workers become healthy
- Coordinate with the three-signal health model (AD-19)
"""

from dataclasses import dataclass, field
import time

from hyperscale.distributed.health.extension_decision import (
    ExtensionDecision,
    ExtensionDecisionConfig,
    ExtensionDecisionEvaluator,
    ExtensionDenialCode,
)
from hyperscale.distributed.health.extension_ledger import (
    ExtensionDecisionEvent,
    ExtensionLedger,
    ExtensionLedgerConfig,
)
from hyperscale.distributed.health.extension_tracker import (
    ExtensionTracker,
    ExtensionTrackerConfig,
)
from hyperscale.distributed.health.progress_witness import ThroughputWitness
from hyperscale.distributed.health.workflow_progress_snapshot import (
    WorkflowProgressSnapshot,
)
from hyperscale.distributed.models import (
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
)


@dataclass(slots=True)
class WorkerHealthManagerConfig:
    """
    Configuration for WorkerHealthManager.

    Attributes:
        base_deadline: Base deadline in seconds for extensions.
        min_grant: Minimum extension grant in seconds.
        max_extensions: Maximum extensions per worker per cycle.
        eviction_threshold: Number of failed extensions before eviction.
        warning_threshold: Remaining extensions to trigger warning notification.
        grace_period: Seconds of grace after exhaustion before kill.
    """

    base_deadline: float = 30.0
    min_grant: float = 1.0
    max_extensions: int = 5
    eviction_threshold: int = 3
    warning_threshold: int = 1
    grace_period: float = 10.0


class WorkerHealthManager:
    """
    Manages worker health and deadline extensions.

    This class is used by managers to:
    1. Track ExtensionTracker instances for each worker
    2. Handle extension requests from workers
    3. Reset trackers when workers become healthy
    4. Determine when workers should be evicted

    Thread Safety:
    - The manager should ensure proper locking when accessing this class
    - Each worker has its own ExtensionTracker instance

    Usage:
        manager = WorkerHealthManager(config)

        # When worker requests extension
        response = manager.handle_extension_request(request, current_deadline)

        # When worker becomes healthy
        manager.on_worker_healthy(worker_id)

        # When checking if worker should be evicted
        should_evict, reason = manager.should_evict_worker(worker_id)
    """

    def __init__(
        self,
        config: WorkerHealthManagerConfig | None = None,
        throughput_witness: ThroughputWitness | None = None,
        decision_config: ExtensionDecisionConfig | None = None,
    ):
        """
        Initialize the WorkerHealthManager.

        Args:
            config: Configuration for extension tracking. Uses defaults if None.
            throughput_witness: Optional H6 BOCPD throughput witness.
                When provided, ``handle_extension_request_with_witnesses``
                runs the multi-witness H5 decision; otherwise the legacy
                ``handle_extension_request`` path runs the H1 progress-
                only check. The H7 ledger and H8 outcome feedback both
                require the witness be wired through.
            decision_config: Per-evaluator config for the H5 decision
                orchestrator (rate limit, etc.). Defaults if None.
        """
        self._config = config or WorkerHealthManagerConfig()
        self._extension_config = ExtensionTrackerConfig(
            base_deadline=self._config.base_deadline,
            min_grant=self._config.min_grant,
            max_extensions=self._config.max_extensions,
            warning_threshold=self._config.warning_threshold,
            grace_period=self._config.grace_period,
        )

        # Per-worker extension trackers
        self._trackers: dict[str, ExtensionTracker] = {}

        # Track consecutive extension failures for eviction decisions
        self._extension_failures: dict[str, int] = {}

        # Phase H5 — multi-witness decision orchestrator. Lazy: a manager
        # without a witness wired in (e.g. unit tests) gets the legacy
        # path; the production HealthAwareServer construction passes a
        # witness so the full multi-witness logic activates.
        self._throughput_witness: ThroughputWitness | None = throughput_witness
        self._decision_evaluator: ExtensionDecisionEvaluator | None = (
            ExtensionDecisionEvaluator(
                throughput_witness=throughput_witness,
                config=decision_config,
            )
            if throughput_witness is not None
            else None
        )

        # Phase H7 — local authoritative ledger of every extension
        # decision made on this manager. Always present (even on
        # the legacy single-witness path) so observability /
        # cross-DC correlation tooling can query consistently.
        self._ledger: ExtensionLedger = ExtensionLedger(ExtensionLedgerConfig())

    def _get_tracker(self, worker_id: str) -> ExtensionTracker:
        """Get or create an ExtensionTracker for a worker."""
        if worker_id not in self._trackers:
            self._trackers[worker_id] = self._extension_config.create_tracker(worker_id)
        return self._trackers[worker_id]

    def handle_extension_request(
        self,
        request: HealthcheckExtensionRequest,
        current_deadline: float,
    ) -> HealthcheckExtensionResponse:
        """
        Handle a deadline extension request from a worker.

        Args:
            request: The extension request from the worker.
            current_deadline: The worker's current deadline timestamp.

        Returns:
            HealthcheckExtensionResponse with the decision.

        Includes graceful exhaustion handling:
        - is_exhaustion_warning set when close to running out of extensions
        - grace_period_remaining shows time left after exhaustion before eviction
        - in_grace_period indicates if worker is in final grace period
        """
        tracker = self._get_tracker(request.worker_id)

        # Attempt to grant extension
        # AD-26 Issue 4: Pass absolute metrics to prioritize over relative progress
        granted, extension_seconds, denial_reason, is_warning = (
            tracker.request_extension(
                reason=request.reason,
                current_progress=request.current_progress,
                completed_items=request.completed_items,
                total_items=request.total_items,
            )
        )

        if granted:
            # Clear extension failure count on successful grant
            self._extension_failures.pop(request.worker_id, None)

            new_deadline = tracker.get_new_deadline(current_deadline, extension_seconds)

            return HealthcheckExtensionResponse(
                granted=True,
                extension_seconds=extension_seconds,
                new_deadline=new_deadline,
                remaining_extensions=tracker.get_remaining_extensions(),
                denial_reason=None,
                is_exhaustion_warning=is_warning,
                grace_period_remaining=0.0,
                in_grace_period=False,
            )
        else:
            # Track extension failures
            failures = self._extension_failures.get(request.worker_id, 0) + 1
            self._extension_failures[request.worker_id] = failures

            # Check if worker is in grace period after exhaustion
            in_grace = tracker.is_in_grace_period
            grace_remaining = tracker.grace_period_remaining

            return HealthcheckExtensionResponse(
                granted=False,
                extension_seconds=0.0,
                new_deadline=current_deadline,  # Unchanged
                remaining_extensions=tracker.get_remaining_extensions(),
                denial_reason=denial_reason,
                is_exhaustion_warning=False,
                grace_period_remaining=grace_remaining,
                in_grace_period=in_grace,
            )

    def handle_extension_request_with_witnesses(
        self,
        request: HealthcheckExtensionRequest,
        current_deadline: float,
        snapshot: WorkflowProgressSnapshot,
        last_snapshot: WorkflowProgressSnapshot | None,
        throughput: float,
        overload_state: str,
        active_in_cluster: int,
        active_in_dc: int,
        active_on_manager: int,
        active_on_worker: int,
        job_id: str = "",
        fence_token: int = 0,
        leader_term: int = 0,
    ) -> tuple[
        HealthcheckExtensionResponse,
        ExtensionDecision,
        ExtensionDecisionEvent,
    ]:
        """Phase H5 multi-witness path.

        Runs the full ``ExtensionDecisionEvaluator`` over all five
        witnesses (counter monotonicity, throughput, overload-state,
        rate-limit, max-extensions). Commits the decision via
        ``ExtensionTracker.commit_grant``/``commit_deny`` so tracker
        state stays consistent with what's gossipped through the
        AD-48 channel in H7.

        Returns both the wire response (for the worker) and the
        full ``ExtensionDecision`` value (for H7 ledger replication
        and H8 outcome feedback).

        Falls back to the legacy ``handle_extension_request`` path
        when no throughput witness is wired (i.e. the manager was
        constructed without one — typically unit-test surfaces).
        """
        if self._decision_evaluator is None:
            # Legacy single-witness path; preserve behavior for callers
            # that didn't wire a throughput witness.
            response = self.handle_extension_request(request, current_deadline)
            # Synthesize a minimal ExtensionDecision matching the
            # legacy outcome so the caller's H7/H8 hooks see a
            # consistent shape.
            tracker = self._get_tracker(request.worker_id)
            from hyperscale.distributed.health.extension_decision import (
                ExtensionWitnessEvidence,
            )
            from hyperscale.distributed.health.progress_witness import (
                WitnessVerdictKind,
            )
            evidence = ExtensionWitnessEvidence(
                progress_meaningful=response.granted,
                progress_all_non_regressed=response.granted,
                progress_any_advanced=response.granted,
                throughput_verdict_kind=WitnessVerdictKind.COLD_START,
                throughput_change_point_probability=0.0,
                throughput_alpha_workflow=0.0,
                throughput_predictive_mean_before=0.0,
                throughput_predictive_mean_after=0.0,
                overload_state=overload_state,
                seconds_since_last_extension=0.0,
                extension_count_pre_decision=tracker.extension_count,
            )
            decision = ExtensionDecision(
                granted=response.granted,
                extension_seconds=response.extension_seconds,
                denial_reason_code=ExtensionDenialCode(
                    response.denial_reason_code or "none"
                ),
                denial_message=response.denial_reason,
                evidence=evidence,
                is_exhaustion_warning=response.is_exhaustion_warning,
            )
            event = self._record_decision_event(
                job_id=job_id,
                worker_id=request.worker_id,
                decision=decision,
                snapshot=snapshot,
                fence_token=fence_token,
                leader_term=leader_term,
            )
            return response, decision, event

        tracker = self._get_tracker(request.worker_id)
        decision = self._decision_evaluator.decide(
            tracker=tracker,
            snapshot=snapshot,
            last_snapshot=last_snapshot,
            throughput=throughput,
            overload_state=overload_state,
            active_in_cluster=active_in_cluster,
            active_in_dc=active_in_dc,
            active_on_manager=active_on_manager,
            active_on_worker=active_on_worker,
        )

        # Commit tracker state mutation.
        if decision.granted:
            tracker.commit_grant(
                grant_seconds=decision.extension_seconds,
                completed_items=request.completed_items,
                current_progress=request.current_progress,
            )
            self._extension_failures.pop(request.worker_id, None)
            new_deadline = tracker.get_new_deadline(
                current_deadline, decision.extension_seconds
            )
            response = HealthcheckExtensionResponse(
                granted=True,
                extension_seconds=decision.extension_seconds,
                new_deadline=new_deadline,
                remaining_extensions=tracker.get_remaining_extensions(),
                denial_reason=None,
                is_exhaustion_warning=decision.is_exhaustion_warning,
                grace_period_remaining=0.0,
                in_grace_period=False,
                denial_reason_code=ExtensionDenialCode.NONE.value,
            )
        else:
            tracker.commit_deny(decision.denial_reason_code.value)
            failures = self._extension_failures.get(request.worker_id, 0) + 1
            self._extension_failures[request.worker_id] = failures
            response = HealthcheckExtensionResponse(
                granted=False,
                extension_seconds=0.0,
                new_deadline=current_deadline,
                remaining_extensions=tracker.get_remaining_extensions(),
                denial_reason=decision.denial_message,
                is_exhaustion_warning=False,
                grace_period_remaining=tracker.grace_period_remaining,
                in_grace_period=tracker.is_in_grace_period,
                denial_reason_code=decision.denial_reason_code.value,
            )

        event = self._record_decision_event(
            job_id=job_id,
            worker_id=request.worker_id,
            decision=decision,
            snapshot=snapshot,
            fence_token=fence_token,
            leader_term=leader_term,
        )
        return response, decision, event

    def _record_decision_event(
        self,
        *,
        job_id: str,
        worker_id: str,
        decision: ExtensionDecision,
        snapshot: WorkflowProgressSnapshot,
        fence_token: int,
        leader_term: int,
    ) -> ExtensionDecisionEvent:
        """Build the ledger event, persist it, and return it for
        downstream H7b dissemination.

        Cumulative-extended bookkeeping pulls the previous total from
        the ledger (defaults to 0 on first decision) so a denial
        carries the running sum forward unchanged and a grant
        increases it by ``decision.extension_seconds``. This way the
        event self-describes the post-decision state without the
        ledger needing a second-pass mutation.
        """
        prior_entry = self._ledger.get_workflow_entry(snapshot.workflow_id)
        prior_cumulative = (
            prior_entry.cumulative_extended if prior_entry is not None else 0.0
        )
        post_cumulative = (
            prior_cumulative + decision.extension_seconds
            if decision.granted
            else prior_cumulative
        )
        event = ExtensionDecisionEvent.from_decision(
            job_id=job_id,
            workflow_id=snapshot.workflow_id,
            worker_id=worker_id,
            decision=decision,
            cumulative_extended=post_cumulative,
            progress_snapshot=snapshot,
            fence_token=fence_token,
            timestamp=time.monotonic(),
            leader_term=leader_term,
        )
        self._ledger.record(event)
        return event

    @property
    def ledger(self) -> ExtensionLedger:
        """Read-only access to the H7 extension decision ledger."""
        return self._ledger

    def ingest_remote_decision_event(
        self, event: ExtensionDecisionEvent
    ) -> None:
        """Apply an ``ExtensionDecisionEvent`` received from a peer
        manager via AD-48 dissemination.

        The ledger is the source of truth, and ``record`` itself is
        idempotent + stale-term-rejecting, so this is just a thin
        forwarder that exists so callers don't reach into ``_ledger``
        directly.
        """
        self._ledger.record(event)

    def forget_workflow(self, workflow_id: str) -> None:
        """Drop H7 ledger state for a terminated workflow."""
        self._ledger.forget_workflow(workflow_id)

    def forget_job(self, job_id: str) -> None:
        """Cascade-drop H7 ledger state for a terminated job."""
        self._ledger.forget_job(job_id)

    @property
    def throughput_witness(self) -> ThroughputWitness | None:
        return self._throughput_witness

    def on_worker_healthy(self, worker_id: str) -> None:
        """
        Reset extension tracking when a worker becomes healthy.

        Call this when:
        - Worker responds to liveness probe
        - Worker completes a workflow successfully
        - Worker's health signals indicate recovery

        Args:
            worker_id: ID of the worker that became healthy.
        """
        tracker = self._trackers.get(worker_id)
        if tracker:
            tracker.reset()

        # Clear extension failures
        self._extension_failures.pop(worker_id, None)

    def on_worker_removed(self, worker_id: str) -> None:
        """
        Clean up tracking state when a worker is removed.

        Call this when:
        - Worker is evicted
        - Worker leaves the cluster
        - Worker is marked as dead

        Args:
            worker_id: ID of the worker being removed.
        """
        self._trackers.pop(worker_id, None)
        self._extension_failures.pop(worker_id, None)
        # Phase H7 — cascade-evict the ledger so a reaped worker's
        # workflow entries don't leak. This is also the path that
        # closes the H8 outcome-feedback loop: every workflow owned
        # by the worker is implicitly resolved as "worker_lost".
        self._ledger.forget_worker(worker_id)

    def should_evict_worker(self, worker_id: str) -> tuple[bool, str | None]:
        """
        Determine if a worker should be evicted based on extension failures.

        A worker should be evicted if:
        1. It has exceeded the consecutive failure threshold, OR
        2. It has exhausted all extensions AND the grace period has expired

        The grace period allows the worker time to checkpoint/save state
        before being forcefully evicted.

        Args:
            worker_id: ID of the worker to check.

        Returns:
            Tuple of (should_evict, reason).
        """
        failures = self._extension_failures.get(worker_id, 0)

        if failures >= self._config.eviction_threshold:
            return (
                True,
                f"Worker exhausted {failures} extension requests without progress",
            )

        tracker = self._trackers.get(worker_id)
        if tracker and tracker.should_evict:
            # Extensions exhausted AND grace period expired
            return (
                True,
                f"Worker exhausted all {self._config.max_extensions} extensions "
                f"and {self._config.grace_period}s grace period",
            )

        return (False, None)

    def get_worker_extension_state(self, worker_id: str) -> dict:
        """
        Get the extension tracking state for a worker.

        Useful for debugging and observability.

        Args:
            worker_id: ID of the worker.

        Returns:
            Dict with extension tracking information.
        """
        tracker = self._trackers.get(worker_id)
        if not tracker:
            return {
                "worker_id": worker_id,
                "has_tracker": False,
            }

        return {
            "worker_id": worker_id,
            "has_tracker": True,
            "extension_count": tracker.extension_count,
            "remaining_extensions": tracker.get_remaining_extensions(),
            "total_extended": tracker.total_extended,
            "last_progress": tracker.last_progress,
            "is_exhausted": tracker.is_exhausted,
            "in_grace_period": tracker.is_in_grace_period,
            "grace_period_remaining": tracker.grace_period_remaining,
            "should_evict": tracker.should_evict,
            "warning_sent": tracker.warning_sent,
            "extension_failures": self._extension_failures.get(worker_id, 0),
        }

    def get_all_extension_states(self) -> dict[str, dict]:
        """
        Get extension tracking state for all workers.

        Returns:
            Dict mapping worker_id to extension state.
        """
        return {
            worker_id: self.get_worker_extension_state(worker_id)
            for worker_id in self._trackers
        }

    @property
    def base_deadline(self) -> float:
        return self._config.base_deadline

    @property
    def tracked_worker_count(self) -> int:
        return len(self._trackers)

    @property
    def workers_with_active_extensions(self) -> int:
        """
        Get the count of workers that have requested at least one extension.

        Used for cross-DC correlation to distinguish load from failures.
        Workers with active extensions are busy with legitimate work,
        not necessarily unhealthy.
        """
        return sum(
            1 for tracker in self._trackers.values() if tracker.extension_count > 0
        )
