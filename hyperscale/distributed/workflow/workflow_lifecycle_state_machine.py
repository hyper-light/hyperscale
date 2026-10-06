"""
Workflow lifecycle state machine (AD-54, AD-34).

The single source of truth for where each workflow of each job is in its
lifecycle. Transitions are validated against ``VALID_TRANSITIONS`` and
applied synchronously -- no await between the check and the write -- so
they are atomic under asyncio without a lock, and callers apply them
inside their own critical sections. Publishing (logs, observers such as
AD-34 progress) is the separate async step callers run once those
sections are released.
"""

from collections import deque
from typing import Awaitable, Callable

from hyperscale.distributed.runtime import Clock
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    WorkflowLifecycleCallbackFailed,
    WorkflowLifecycleTransitionRefused,
    WorkflowLifecycleTransitionTaken,
)

from .state_transition import StateTransition
from .workflow_lifecycle_record import WorkflowLifecycleRecord
from .workflow_state import (
    CANCELLATION_TRANSITIONS,
    FINAL_RUN_TRANSITIONS,
    REGISTRATION_TRANSITIONS,
    RETRY_CYCLE_TRANSITIONS,
    VALID_TRANSITIONS,
    WorkflowState,
)

TransitionObserver = Callable[[StateTransition], Awaitable[None]]


class WorkflowLifecycleStateMachine:
    """
    Every workflow's lifecycle, keyed by (job id, workflow id).

    A workflow's history keeps every transition of a lifecycle whose
    budgeted retries stay within ``max_budgeted_retries``: registration,
    each retry cycle, the final run and a cancellation. Beyond that (a
    workflow requeued by losses its retry budget does not count) the
    oldest transitions fall off, so memory stays bounded per workflow.
    """

    def __init__(
        self,
        clock: Clock,
        max_budgeted_retries: int,
        logger: Logger,
        manager_id: str,
        datacenter: str,
    ) -> None:
        self._clock = clock
        self._history_limit = (
            REGISTRATION_TRANSITIONS
            + max_budgeted_retries * RETRY_CYCLE_TRANSITIONS
            + FINAL_RUN_TRANSITIONS
            + CANCELLATION_TRANSITIONS
        )
        self._logger = logger
        self._manager_id = manager_id
        self._datacenter = datacenter
        self._records: dict[str, dict[str, WorkflowLifecycleRecord]] = {}
        self._observers: list[TransitionObserver] = []
        self._rejected_transition_count = 0

    @property
    def history_limit(self) -> int:
        return self._history_limit

    @property
    def rejected_transition_count(self) -> int:
        """Transitions refused since start: an edge the table does not
        allow, or a workflow the machine does not hold."""
        return self._rejected_transition_count

    @property
    def record_count(self) -> int:
        return sum(len(job_records) for job_records in self._records.values())

    def register_workflow(self, job_id: str, workflow_id: str, reason: str) -> StateTransition:
        """Start a workflow's lifecycle in PENDING. Registering a workflow
        the machine already holds is refused and changes nothing."""
        now = self._clock.monotonic()
        job_records = self._records.setdefault(job_id, {})
        if (record := job_records.get(workflow_id)) is not None:
            self._rejected_transition_count += 1
            return StateTransition(
                job_id, workflow_id, record.state, WorkflowState.PENDING, now, reason, False, False
            )

        transition = StateTransition(job_id, workflow_id, None, WorkflowState.PENDING, now, reason, True, False)
        job_records[workflow_id] = WorkflowLifecycleRecord(
            state=WorkflowState.PENDING,
            history=deque((transition,), maxlen=self._history_limit),
            last_transition_at=now,
        )
        return transition

    def install_state(
        self,
        job_id: str,
        workflow_id: str,
        state: WorkflowState,
        retry_generation: int,
        reason: str,
    ) -> StateTransition | None:
        """Set a workflow's state outright, without the table: a follower
        taking its job leader's snapshot, or a manager rebuilding a job.
        None when the workflow already holds that state and generation --
        a snapshot repeated on every sync changes nothing."""
        job_records = self._records.setdefault(job_id, {})
        record = job_records.get(workflow_id)
        if record is not None and record.state == state and record.retry_generation == retry_generation:
            return None

        now = self._clock.monotonic()
        transition = StateTransition(
            job_id,
            workflow_id,
            None if record is None else record.state,
            state,
            now,
            reason,
            True,
            True,
        )
        if record is None:
            job_records[workflow_id] = WorkflowLifecycleRecord(
                state=state,
                history=deque((transition,), maxlen=self._history_limit),
                last_transition_at=now,
                retry_generation=retry_generation,
            )
            return transition

        record.state = state
        record.retry_generation = retry_generation
        record.last_transition_at = now
        record.history.append(transition)
        return transition

    def apply_transition(
        self,
        job_id: str,
        workflow_id: str,
        to_state: WorkflowState,
        reason: str,
    ) -> StateTransition:
        """Move a workflow along one edge of the table, or refuse. Requeueing
        (FAILED_READY_FOR_RETRY -> PENDING) starts a new retry generation."""
        now = self._clock.monotonic()
        record = self._records.get(job_id, {}).get(workflow_id)
        if record is None:
            self._rejected_transition_count += 1
            return StateTransition(job_id, workflow_id, None, to_state, now, reason, False, False)

        from_state = record.state
        if to_state not in VALID_TRANSITIONS[from_state]:
            self._rejected_transition_count += 1
            return StateTransition(job_id, workflow_id, from_state, to_state, now, reason, False, False)

        transition = StateTransition(job_id, workflow_id, from_state, to_state, now, reason, True, False)
        record.state = to_state
        record.last_transition_at = now
        record.history.append(transition)
        if from_state == WorkflowState.FAILED_READY_FOR_RETRY:
            record.retry_generation += 1
        return transition

    async def publish_transitions(self, transitions: list[StateTransition]) -> None:
        """Log each transition and hand it to every observer -- taken or
        refused, so observers judge the whole history. Run outside the
        callers' critical sections: observers and logs may await."""
        for transition in transitions:
            from_state_name = "none" if transition.from_state is None else transition.from_state.value
            if transition.accepted:
                await self._logger.log(
                    WorkflowLifecycleTransitionTaken(
                        message=(
                            f"Workflow lifecycle {from_state_name} -> "
                            f"{transition.to_state.value} ({transition.reason})"
                        ),
                        manager_id=self._manager_id,
                        datacenter=self._datacenter,
                        job_id=transition.job_id,
                        workflow_id=transition.workflow_id,
                        from_state=from_state_name,
                        to_state=transition.to_state.value,
                        reason=transition.reason,
                    )
                )
            else:
                await self._logger.log(
                    WorkflowLifecycleTransitionRefused(
                        message=(
                            f"Refused workflow lifecycle transition {from_state_name} -> "
                            f"{transition.to_state.value} ({transition.reason})"
                        ),
                        manager_id=self._manager_id,
                        datacenter=self._datacenter,
                        job_id=transition.job_id,
                        workflow_id=transition.workflow_id,
                        from_state=from_state_name,
                        to_state=transition.to_state.value,
                        reason=transition.reason,
                    )
                )

            for observer in self._observers:
                try:
                    await observer(transition)
                except Exception as observer_error:
                    await self._logger.log(
                        WorkflowLifecycleCallbackFailed(
                            message=(
                                f"Workflow lifecycle observer failed: "
                                f"{type(observer_error).__name__}: {observer_error}"
                            ),
                            manager_id=self._manager_id,
                            datacenter=self._datacenter,
                            job_id=transition.job_id,
                            workflow_id=transition.workflow_id,
                            error_type=type(observer_error).__name__,
                        )
                    )

    def register_observer(self, observer: TransitionObserver) -> None:
        """Be handed every published transition (AD-34 progress, SIM
        oracles)."""
        if observer not in self._observers:
            self._observers.append(observer)

    def get_state(self, job_id: str, workflow_id: str) -> WorkflowState | None:
        """The workflow's state, or None when the machine does not hold it."""
        if (record := self._records.get(job_id, {}).get(workflow_id)) is None:
            return None
        return record.state

    def get_record(self, job_id: str, workflow_id: str) -> WorkflowLifecycleRecord | None:
        return self._records.get(job_id, {}).get(workflow_id)

    def forget_workflow(self, job_id: str, workflow_id: str) -> None:
        if (job_records := self._records.get(job_id)) is None:
            return
        job_records.pop(workflow_id, None)
        if not job_records:
            del self._records[job_id]

    def forget_job(self, job_id: str) -> None:
        self._records.pop(job_id, None)

    def get_state_counts(self) -> dict[WorkflowState, int]:
        counts = {state: 0 for state in WorkflowState}
        for job_records in self._records.values():
            for record in job_records.values():
                counts[record.state] += 1
        return counts

    def get_stuck_workflows(
        self,
        states: frozenset[WorkflowState],
        threshold_seconds: float,
    ) -> list[tuple[str, str, WorkflowState, float]]:
        """Workflows in one of ``states`` that have not moved for at least
        ``threshold_seconds``, longest-stuck first, as (job id, workflow id,
        state, seconds since their last transition)."""
        now = self._clock.monotonic()
        stuck = [
            (job_id, workflow_id, record.state, now - record.last_transition_at)
            for job_id, job_records in self._records.items()
            for workflow_id, record in job_records.items()
            if record.state in states and now - record.last_transition_at >= threshold_seconds
        ]
        stuck.sort(key=lambda entry: entry[3], reverse=True)
        return stuck
