"""
JobMakesProgress (simulation_framework.md §13; SCENARIOS.md §11
"Acknowledged jobs reach a terminal state -- no indefinite RUNNING").

The design's form -- the workflow completion count rises at least every N
seconds -- is wrong for a correct cluster: one long workflow completes
nothing for its whole duration. The bound the protocol itself enforces is
AD-34's stuck rule: a job with work in flight (a workflow DISPATCHED or
RUNNING) that makes no progress -- no workflow advancing its lifecycle or
its completed/failed counts -- for ``stuck_threshold`` plus every AD-26
extension granted since its last progress is timed out by its leader's
timeout strategy, checked every ``JOB_TIMEOUT_CHECK_INTERVAL``. So the
invariant: on every responsive leader, a job with work in flight shows
progress (or leaves flight) within that bound, read from the job's own
``TimeoutTrackingState`` and the leader's Env.

Progress is observed independently of the strategy's bookkeeping: the
job's status and counts, each workflow's status and lifecycle state, and
each dispatch's reported completed/failed actions and finished cores. A
paused leader owes nothing while paused; its watch restarts when it is
observed again.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from hyperscale.distributed.models.jobs import JobInfo
from hyperscale.distributed.workflow.workflow_state import WorkflowState

from tests.simulation.harness.invariant_checks.job_progress_mark import (
    JobProgressMark,
    JobProgressSignature,
)
from tests.simulation.harness.invariant_checks.live_nodes import responsive_handles
from tests.simulation.harness.invariant_result import InvariantResult
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness

WatchKey = tuple[str, str]
IN_FLIGHT_STATES = frozenset((WorkflowState.DISPATCHED, WorkflowState.RUNNING))


class JobProgressWatch:
    """Stateful evaluator: when each in-flight job last showed progress."""

    def __init__(self, clock: Callable[[], float]) -> None:
        self._clock = clock
        self._marks: dict[WatchKey, JobProgressMark] = {}

    def evaluate(self, harness: "ClusterHarness") -> InvariantResult:
        """Holds while every in-flight job progressed within its AD-34 stuck bound."""
        now = self._clock()
        observations = _in_flight_jobs(harness)
        self._keep_only(_watch_keys(observations))
        detail = next(filter(None, (self._stall(handle, job, now) for handle, job in observations)), "")
        return InvariantResult(holds=not detail, detail=detail)

    def _keep_only(self, observed_keys: set[WatchKey]) -> None:
        self._marks = {key: mark for key, mark in self._marks.items() if key in observed_keys}

    def _stall(self, handle: ServerHandle, job: JobInfo, now: float) -> str:
        key = (handle.node_id, job.job_id)
        signature = _progress_signature(handle, job)
        held = self._marks.get(key)
        if _is_fresh(held, handle.instance, signature):
            self._marks[key] = JobProgressMark(handle.instance, signature, now)
            return ""
        return _overdue_detail(handle, job, now - held.changed_at)


def _in_flight_jobs(harness: "ClusterHarness") -> list[tuple[ServerHandle, JobInfo]]:
    return [
        (handle, job)
        for handle in responsive_handles(harness, ServerKind.MANAGER)
        for job in _led_jobs_in_flight(handle)
    ]


def _watch_keys(observations: list[tuple[ServerHandle, JobInfo]]) -> set[WatchKey]:
    return {(handle.node_id, job.job_id) for handle, job in observations}


def _led_jobs_in_flight(handle: ServerHandle) -> list[JobInfo]:
    return [job for job in handle.instance._job_manager.iter_jobs() if _is_led_in_flight(handle, job)]


def _is_led_in_flight(handle: ServerHandle, job: JobInfo) -> bool:
    return handle.instance._leases.is_job_leader(job.job_id) and _has_work_in_flight(handle, job)


def _has_work_in_flight(handle: ServerHandle, job: JobInfo) -> bool:
    """Mirrors the stuck rule's own test (``LocalAuthorityTimeout._has_work_in_flight``)."""
    return any(state in IN_FLIGHT_STATES for state in _lifecycle_states(handle, job))


def _lifecycle_states(handle: ServerHandle, job: JobInfo) -> list[WorkflowState | None]:
    lifecycle = handle.instance._job_manager.workflow_lifecycle
    return [
        lifecycle.get_state(job.job_id, workflow.token.workflow_id or "")
        for workflow in list(job.workflows.values())
    ]


def _progress_signature(handle: ServerHandle, job: JobInfo) -> JobProgressSignature:
    return (
        job.status,
        job.workflows_completed,
        job.workflows_failed,
        tuple(_workflow_marks(job)),
        tuple(_lifecycle_states(handle, job)),
        tuple(_dispatch_marks(job)),
    )


def _workflow_marks(job: JobInfo) -> list[tuple[str, str]]:
    return sorted((workflow.token_str, str(workflow.status)) for workflow in list(job.workflows.values()))


def _dispatch_marks(job: JobInfo) -> list[tuple[str, int, int, int]]:
    return sorted(
        (token, sub_workflow.progress.completed_count, sub_workflow.progress.failed_count, sub_workflow.progress.cores_completed)
        for token, sub_workflow in list(job.sub_workflows.items())
        if sub_workflow.progress is not None
    )


def _is_fresh(held: JobProgressMark | None, leader_instance: object, signature: JobProgressSignature) -> bool:
    return held is None or held.leader_instance is not leader_instance or held.signature != signature


def _overdue_detail(handle: ServerHandle, job: JobInfo, stalled_seconds: float) -> str:
    stuck_bound = _stuck_bound(handle, job)
    if stalled_seconds <= stuck_bound:
        return ""
    return (
        f"job {job.job_id!r} led by {handle.node_id} made no progress for "
        f"{stalled_seconds:.1f}s with work in flight (AD-34 stuck bound {stuck_bound:.1f}s)"
    )


def _stuck_bound(handle: ServerHandle, job: JobInfo) -> float:
    """``stuck_threshold`` + extensions since progress + one check interval."""
    env = handle.instance.env
    tracking = job.timeout_tracking
    if tracking is None:
        return env.JOB_STUCK_THRESHOLD + env.JOB_TIMEOUT_CHECK_INTERVAL
    return tracking.stuck_threshold + tracking.extension_seconds_since_progress + env.JOB_TIMEOUT_CHECK_INTERVAL
