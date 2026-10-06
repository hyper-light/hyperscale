"""
Job Manager - Thread-safe job and workflow state management.

This class encapsulates all job-related state and operations with proper
synchronization using per-job locks. It provides race-condition safe access
to job data structures using defaultdict and asyncio locks.

Key responsibilities:
- Job lifecycle management (submission, progress, completion, failure)
- Workflow tracking (dispatch, progress, results)
- Sub-workflow aggregation
- Per-job locking for concurrent access safety

Tracking Token Format:
======================
All workflow tracking uses globally unique tokens with the format:

    <DATACENTER>:<MANAGER_NODE_ID>:<JOB_ID>:<WORKFLOW_ID>:<WORKER_NODE_ID>

Components:
- DATACENTER: Datacenter/region identifier (e.g., "DC-EAST")
- MANAGER_NODE_ID: Short node ID of the manager that owns the job
- JOB_ID: Unique job identifier
- WORKFLOW_ID: Unique workflow identifier within the job
- WORKER_NODE_ID: Short node ID of the worker (for sub-workflows only)

Examples:
- Job token:      DC-EAST:mgr-abc123:job-def456
- Workflow token: DC-EAST:mgr-abc123:job-def456:wf-001
- Sub-workflow:   DC-EAST:mgr-abc123:job-def456:wf-001:wrk-xyz789

Benefits:
- Globally unique across datacenters
- Self-describing (know DC, manager, worker from ID alone)
- Easy log correlation (grep by any component)
- Supports failover detection (identify orphaned workflows)
- Gate routing (parse datacenter prefix)
"""

import asyncio
from collections import deque
from operator import attrgetter
from typing import Awaitable, Callable, Iterator

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.state.context import Context
from hyperscale.distributed.models import (
    JobInfo,
    JobStatus,
    JobSubmission,
    SubWorkflowInfo,
    TrackingToken,
    WorkflowFinalResult,
    WorkflowInfo,
    WorkflowProgress,
    WorkflowStatus,
)
from hyperscale.distributed.models.message import Message
from hyperscale.distributed.models.sub_workflow_state_snapshot import SubWorkflowStateSnapshot
from hyperscale.distributed.models.workflow_state_snapshot import WorkflowStateSnapshot
from hyperscale.distributed.jobs.job_status_order import JobStatusOrder
from hyperscale.distributed.jobs.logging_models import (
    JobManagerError,
    JobManagerInfo,
)
from hyperscale.distributed.runtime import Clock
from hyperscale.distributed.workflow import (
    WORKFLOW_STATE_BY_WORKFLOW_STATUS,
    WORKFLOW_STATE_RANK,
    WORKFLOW_STATUS_BY_WORKFLOW_STATE,
    StateTransition,
    WorkflowLifecycleRecord,
    WorkflowLifecycleStateMachine,
    WorkflowState,
)
from hyperscale.logging import Logger
import traceback as _tb


# A job in one of these statuses has finished: hydration stamps its
# completion time, and a follower's mirror no longer overwrites it.
TERMINAL_JOB_STATUS_VALUES = frozenset(
    {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
        JobStatus.TIMEOUT.value,
    }
)

# A workflow in one of these statuses stays finished: no result, failure or
# aggregation failure arriving later changes it.
FINISHED_WORKFLOW_STATUSES = (
    WorkflowStatus.COMPLETED,
    WorkflowStatus.FAILED,
    WorkflowStatus.CANCELLED,
)

# A workflow moving out of one of these statuses was already counted toward
# its job's completed or failed workflows by ``update_workflow_status``.
COUNTED_TERMINAL_WORKFLOW_STATUSES = (
    WorkflowStatus.COMPLETED,
    WorkflowStatus.FAILED,
    WorkflowStatus.AGGREGATED,
    WorkflowStatus.AGGREGATION_FAILED,
)

# A parent workflow in one of these statuses has no sub-workflow left to
# reassign from a lost worker.
UNREASSIGNABLE_WORKFLOW_STATUSES = frozenset(
    {
        WorkflowStatus.COMPLETED,
        WorkflowStatus.FAILED,
        WorkflowStatus.AGGREGATED,
        WorkflowStatus.AGGREGATION_FAILED,
        WorkflowStatus.CANCELLED,
    }
)

SubWorkflowResultOutcome = tuple[bool, bool, bool, bool, str | None]


def _parent_is_unknown(
    parent: WorkflowInfo | None,
    sub_workflow: SubWorkflowInfo,
    result: WorkflowFinalResult,
) -> bool:
    """The sub-workflow's parent workflow is not held by its job."""
    return not parent


def _parent_terminal_already_pushed(
    parent: WorkflowInfo,
    sub_workflow: SubWorkflowInfo,
    result: WorkflowFinalResult,
) -> bool:
    """The parent's terminal outcome was already decided and pushed."""
    return parent.terminal_pushed


def _sub_workflow_already_answered(
    parent: WorkflowInfo,
    sub_workflow: SubWorkflowInfo,
    result: WorkflowFinalResult,
) -> bool:
    """The sub-workflow's result was already recorded."""
    return sub_workflow.result is not None


def _sub_workflow_superseded(
    parent: WorkflowInfo,
    sub_workflow: SubWorkflowInfo,
    result: WorkflowFinalResult,
) -> bool:
    """The sub-workflow's worker was lost and its run superseded."""
    return sub_workflow.superseded


def _result_from_another_worker(
    parent: WorkflowInfo,
    sub_workflow: SubWorkflowInfo,
    result: WorkflowFinalResult,
) -> bool:
    """Both sides name a worker, and the result's is not the sub-workflow's."""
    return bool(
        result.worker_id
        and sub_workflow.worker_id
        and result.worker_id != sub_workflow.worker_id
    )


def _result_carries_stale_fence_token(
    parent: WorkflowInfo,
    sub_workflow: SubWorkflowInfo,
    result: WorkflowFinalResult,
) -> bool:
    """Both sides carry a fence token, and the result's is not the
    sub-workflow's current dispatch's."""
    return bool(
        sub_workflow.fence_token
        and result.fence_token
        and result.fence_token != sub_workflow.fence_token
    )


# Why a sub-workflow's result is not recorded, checked in this order: the
# first predicate that holds names the outcome returned for the result.
SUB_WORKFLOW_RESULT_REJECTIONS: tuple[
    tuple[
        Callable[[WorkflowInfo, SubWorkflowInfo, WorkflowFinalResult], bool],
        SubWorkflowResultOutcome,
    ],
    ...,
] = (
    (_parent_is_unknown, (False, False, False, False, "unknown_parent_workflow")),
    (_parent_terminal_already_pushed, (False, False, True, False, "parent_terminal_already_pushed")),
    (_sub_workflow_already_answered, (False, False, True, False, "duplicate_sub_workflow_result")),
    (_sub_workflow_superseded, (False, False, False, True, "superseded_sub_workflow")),
    (_result_from_another_worker, (False, False, False, True, "worker_mismatch")),
    (_result_carries_stale_fence_token, (False, False, False, True, "stale_fence_token")),
)


class JobManager:
    """
    Thread-safe job and workflow state management.

    Uses per-job locks to ensure race-condition safe access to job state.
    All public methods that modify state should be called with the job lock held,
    or use the provided async context manager methods.

    All tracking uses TrackingToken format:
        <datacenter>:<manager_id>:<job_id>:<workflow_id>:<worker_id>

    Event-driven notifications:
        - on_workflow_completed callback is called when a workflow reaches terminal state
        - This enables event-driven dependency resolution in WorkflowDispatcher
    """

    def __init__(
        self,
        datacenter: str,
        manager_id: str,
        clock: Clock,
        max_budgeted_retries: int,
        on_workflow_completed: Callable[[str, str], Awaitable[None]]
        | None = None,
    ):
        """
        Initialize JobManager.

        Args:
            datacenter: Datacenter identifier (e.g., "DC-EAST")
            manager_id: This manager's node ID (short form)
            clock: The node's clock; every timestamp JobManager records reads it
            max_budgeted_retries: The most retries a workflow's AD-44 budget
                                  allows; bounds each workflow's lifecycle history
            on_workflow_completed: Optional async callback called when workflow completes
                                  Takes (job_id, workflow_id) and is awaited
        """
        self._datacenter = datacenter
        self._manager_id = manager_id
        self._clock = clock
        self._on_workflow_completed = on_workflow_completed
        self._logger = Logger()

        # Every workflow's AD-54 lifecycle: the authority on where each one is.
        self.workflow_lifecycle = WorkflowLifecycleStateMachine(
            clock=clock,
            max_budgeted_retries=max_budgeted_retries,
            logger=self._logger,
            manager_id=manager_id,
            datacenter=datacenter,
        )

        # Main job storage - job token string -> JobInfo
        self._jobs: dict[str, JobInfo] = {}

        # Quick lookup for workflow/sub-workflow -> job token mapping
        self._workflow_to_job: dict[
            str, str
        ] = {}  # workflow_token_str -> job_token_str
        self._sub_workflow_to_job: dict[
            str, str
        ] = {}  # sub_workflow_token_str -> job_token_str

        # Fence token tracking for at-most-once dispatch
        # Monotonically increasing per job to ensure workers can reject stale dispatches
        self._job_fence_tokens: dict[str, int] = {}
        self._fence_token_lock: asyncio.Lock | None = None

        # Progress sequence tracking for per-job progress update ordering (FIX 2.8)
        # Monotonically increasing per job to ensure gates can reject out-of-order updates
        self._job_progress_sequences: dict[str, int] = {}
        self._progress_sequence_lock: asyncio.Lock | None = None

        # Global lock for job creation/deletion (not per-job operations)
        self._global_lock = asyncio.Lock()

    def set_on_workflow_completed(
        self,
        callback: Callable[[str, str], Awaitable[None]],
    ) -> None:
        """
        Set the workflow completion callback.

        This allows setting the callback after initialization, which is useful
        when JobManager and WorkflowDispatcher need to reference each other.

        Args:
            callback: Async callback called when workflow completes
                     Takes (job_id, workflow_id) and is awaited
        """
        self._on_workflow_completed = callback

    # =========================================================================
    # Token Generation
    # =========================================================================

    def create_job_token(self, job_id: str) -> TrackingToken:
        """Create a job-level tracking token."""
        return TrackingToken.for_job(self._datacenter, self._manager_id, job_id)

    def create_workflow_token(self, job_id: str, workflow_id: str) -> TrackingToken:
        """Create a workflow-level tracking token."""
        return TrackingToken.for_workflow(
            self._datacenter, self._manager_id, job_id, workflow_id
        )

    def create_sub_workflow_token(
        self, job_id: str, workflow_id: str, worker_id: str
    ) -> TrackingToken:
        """Create a sub-workflow tracking token."""
        return TrackingToken.for_sub_workflow(
            self._datacenter, self._manager_id, job_id, workflow_id, worker_id
        )

    # =========================================================================
    # Fence Token Management (AD-10 compliant)
    # =========================================================================

    def _get_fence_token_lock(self) -> asyncio.Lock:
        """Get the fence token lock, creating lazily if needed."""
        if self._fence_token_lock is None:
            self._fence_token_lock = asyncio.Lock()
        return self._fence_token_lock

    async def get_next_fence_token(self, job_id: str, leader_term: int = 0) -> int:
        """
        Get the next fence token for a job, incorporating leader term (AD-10).

        Token format: (term << 32) | per_job_counter

        This ensures:
        1. Any fence token from term N+1 is always > any token from term N
        2. Within a term, per-job counters provide uniqueness
        3. Workers can validate tokens by comparing against previously seen tokens

        The high 32 bits contain the leader election term, ensuring term-level
        monotonicity. The low 32 bits contain a per-job counter for dispatch-level
        uniqueness within a term.

        Args:
            job_id: Job ID
            leader_term: Current leader election term (AD-10 requirement)

        Returns:
            Fence token incorporating term and job-specific counter

        Thread-safe: uses async lock to ensure atomic read-modify-write.
        """
        async with self._get_fence_token_lock():
            current = self._job_fence_tokens.get(job_id, 0)
            # Extract current counter (low 32 bits) and increment
            current_counter = current & 0xFFFFFFFF
            next_counter = current_counter + 1
            # Combine term (high bits) with counter (low bits)
            next_token = (leader_term << 32) | next_counter
            self._job_fence_tokens[job_id] = next_token
            return next_token

    def get_current_fence_token(self, job_id: str) -> int:
        """Get the current fence token for a job without incrementing."""
        return self._job_fence_tokens.get(job_id, 0)

    @staticmethod
    def extract_term_from_fence_token(fence_token: int) -> int:
        """
        Extract leader term from a fence token (AD-10).

        Args:
            fence_token: A fence token in format (term << 32) | counter

        Returns:
            The leader term from the high 32 bits
        """
        return fence_token >> 32

    @staticmethod
    def extract_counter_from_fence_token(fence_token: int) -> int:
        """
        Extract per-job counter from a fence token (AD-10).

        Args:
            fence_token: A fence token in format (term << 32) | counter

        Returns:
            The per-job counter from the low 32 bits
        """
        return fence_token & 0xFFFFFFFF

    # =========================================================================
    # Progress Sequence Management (FIX 2.8)
    # =========================================================================

    def _get_progress_sequence_lock(self) -> asyncio.Lock:
        if self._progress_sequence_lock is None:
            self._progress_sequence_lock = asyncio.Lock()
        return self._progress_sequence_lock

    async def get_next_progress_sequence(self, job_id: str) -> int:
        async with self._get_progress_sequence_lock():
            current = self._job_progress_sequences.get(job_id, 0)
            next_sequence = current + 1
            self._job_progress_sequences[job_id] = next_sequence
            return next_sequence

    def get_current_progress_sequence(self, job_id: str) -> int:
        return self._job_progress_sequences.get(job_id, 0)

    def cleanup_progress_sequence(self, job_id: str) -> None:
        self._job_progress_sequences.pop(job_id, None)

    # =========================================================================
    # Job Lifecycle
    # =========================================================================

    async def create_job(
        self,
        submission: JobSubmission,
        callback_addr: tuple[str, int] | None = None,
    ) -> JobInfo:
        """
        Create a new job from a submission.

        Thread-safe: uses global lock for job creation.
        """
        job_token = self.create_job_token(submission.job_id)
        job_token_str = str(job_token)

        async with self._global_lock:
            if job_token_str in self._jobs:
                return self._jobs[job_token_str]

            job = JobInfo(
                token=job_token,
                submission=submission,
                status=JobStatus.QUEUED.value,
                timestamp=self._clock.time(),
                callback_addr=callback_addr,
            )

            self._jobs[job_token_str] = job

            return job

    async def track_remote_job(
        self,
        job_id: str,
        leader_node_id: str,
        leader_addr: tuple[str, int],
    ) -> JobInfo:
        """
        Create a tracking entry for a job led by another manager.

        Non-leader managers use this to track jobs they've been notified about.
        This enables query routing and state awareness without full submission data.

        Thread-safe: uses global lock for job creation.
        """
        job_token = self.create_job_token(job_id)
        job_token_str = str(job_token)

        async with self._global_lock:
            if job_token_str in self._jobs:
                # Update leader info if job already exists
                job = self._jobs[job_token_str]
                job.leader_node_id = leader_node_id
                job.leader_addr = leader_addr
                return job

            job = JobInfo(
                token=job_token,
                submission=None,  # Non-leader doesn't have submission
                status=JobStatus.QUEUED.value,
                timestamp=self._clock.time(),
                leader_node_id=leader_node_id,
                leader_addr=leader_addr,
            )

            self._jobs[job_token_str] = job
            return job

    async def hydrate_remote_job_state(
        self,
        *,
        job_id: str,
        leader_node_id: str,
        leader_addr: tuple[str, int],
        status: str,
        workflows_total: int,
        workflows_completed: int,
        workflows_failed: int,
        fencing_token: int,
        callback_addr: tuple[str, int] | None,
        workflow_snapshots: dict[str, WorkflowStateSnapshot],
        sub_workflow_snapshots: dict[str, SubWorkflowStateSnapshot],
        layer_version: int,
        elapsed_seconds: float,
        timestamp: float,
        merge_forward_only: bool,
        replace_existing: bool = True,
    ) -> JobInfo:
        """Hydrate executable job state replicated from a peer manager, or
        reported by a worker.

        Peer managers must preserve the original workflow/sub-workflow token
        strings. Workers send terminal results using those dispatch-time
        tokens, so recreating local-manager tokens during failover would make
        the new leader authoritative but unable to record results.

        ``merge_forward_only`` decides whose view wins (AD-54):

        * False -- a follower mirrors its fenced leader: each workflow takes
          the snapshot's lifecycle state and fields, and ``replace_existing``
          drops what the leader no longer holds;
        * True -- the job's own leader (or a worker's report, which is only
          evidence) moves forward only: a workflow adopts the snapshot's
          state and fields when the snapshot is ahead of it -- a later retry
          generation, or further along the same one -- and a known sub only
          gains what it lacks. A stale view never regresses the job, its
          counters or its workflows, and nothing it holds is dropped.
        """
        job_token = self.create_job_token(job_id)
        job_token_str = str(job_token)

        async with self._global_lock:
            job = self._install_or_refresh_hydrated_job(
                job_token=job_token,
                job_token_str=job_token_str,
                leader_node_id=leader_node_id,
                leader_addr=leader_addr,
                status=status,
                fencing_token=fencing_token,
                callback_addr=callback_addr,
                timestamp=timestamp,
            )

        mirror_snapshot = not merge_forward_only
        replaces_with_mirror = replace_existing and mirror_snapshot
        hydration_transitions: list[StateTransition] = []
        async with job.lock:
            self._merge_hydrated_job_status(
                job, status, merge_forward_only, replace_existing, timestamp
            )
            self._merge_hydrated_job_counters(
                job,
                replaces_with_mirror,
                workflows_total,
                workflows_completed,
                workflows_failed,
            )
            self._merge_hydrated_job_timing(job, timestamp, layer_version, elapsed_seconds)
            self._hydrate_workflow_snapshots(
                job,
                job_id,
                job_token_str,
                workflow_snapshots,
                merge_forward_only,
                replaces_with_mirror,
                hydration_transitions,
            )
            self._hydrate_sub_workflow_snapshots(
                job,
                job_token_str,
                sub_workflow_snapshots,
                mirror_snapshot,
                replaces_with_mirror,
            )

            job.workflows_total = max(job.workflows_total, len(job.workflows))

        await self.workflow_lifecycle.publish_transitions(hydration_transitions)
        return job

    def _install_or_refresh_hydrated_job(
        self,
        *,
        job_token: TrackingToken,
        job_token_str: str,
        leader_node_id: str,
        leader_addr: tuple[str, int],
        status: str,
        fencing_token: int,
        callback_addr: tuple[str, int] | None,
        timestamp: float,
    ) -> JobInfo:
        """Under the global lock: create the hydrated job when this manager
        does not hold it yet, or point the held one at its leader -- the
        higher fence token kept, and a callback address only replaced by
        one the snapshot carries."""
        job = self._jobs.get(job_token_str)
        if job is None:
            job = JobInfo(
                token=job_token,
                submission=None,
                status=status,
                timestamp=timestamp,
                leader_node_id=leader_node_id,
                leader_addr=leader_addr,
                fencing_token=fencing_token,
                callback_addr=callback_addr,
            )
            self._jobs[job_token_str] = job
        else:
            job.leader_node_id = leader_node_id
            job.leader_addr = leader_addr
            job.fencing_token = max(job.fencing_token, fencing_token)
            if callback_addr is not None:
                job.callback_addr = callback_addr
        return job

    def _merge_hydrated_job_status(
        self,
        job: JobInfo,
        status: str,
        merge_forward_only: bool,
        replace_existing: bool,
        timestamp: float,
    ) -> None:
        """Take the snapshot's job status -- forward only for the job's own
        leader, or as a follower's mirror unless a terminal status it holds
        is kept -- then stamp a terminal job's completion time."""
        if merge_forward_only:
            self._advance_hydrated_job_status(job, status)
        elif self._mirror_may_overwrite_job_status(job, replace_existing):
            job.status = status

        # Followers never ran the leader's terminal path, so without
        # this stamp a hydrated terminal job kept completed_at == 0 and
        # the retention sweep (which requires completed_at > 0) never
        # removed it.
        self._stamp_hydrated_terminal_completion(job, timestamp)

    def _advance_hydrated_job_status(self, job: JobInfo, status: str) -> None:
        """Take the snapshot's job status only when it ranks further along
        the job lifecycle than the held one (an unranked status ranks
        lowest)."""
        job_status_order = JobStatusOrder()
        if self._job_status_rank(job_status_order, status) > self._job_status_rank(
            job_status_order, job.status
        ):
            job.status = status

    @staticmethod
    def _job_status_rank(job_status_order: JobStatusOrder, status: str) -> int:
        """The status's lifecycle rank, with an unranked (or zero-ranked)
        status ranking -1."""
        return job_status_order.rank(status) or -1

    @staticmethod
    def _mirror_may_overwrite_job_status(job: JobInfo, replace_existing: bool) -> bool:
        """A follower's mirror overwrites the job status when it replaces
        the held state, or when the held status is not terminal."""
        return replace_existing or job.status not in TERMINAL_JOB_STATUS_VALUES

    @staticmethod
    def _stamp_hydrated_terminal_completion(job: JobInfo, timestamp: float) -> None:
        """Stamp a terminal job never stamped with the snapshot's time."""
        if job.status in TERMINAL_JOB_STATUS_VALUES and job.completed_at <= 0:
            job.completed_at = timestamp

    @staticmethod
    def _merge_hydrated_job_counters(
        job: JobInfo,
        replaces_with_mirror: bool,
        workflows_total: int,
        workflows_completed: int,
        workflows_failed: int,
    ) -> None:
        """Take the snapshot's workflow counters as they are when a mirror
        replaces the held state; otherwise never lower a held counter."""
        if replaces_with_mirror:
            job.workflows_total = workflows_total
            job.workflows_completed = workflows_completed
            job.workflows_failed = workflows_failed
        else:
            job.workflows_total = max(job.workflows_total, workflows_total)
            job.workflows_completed = max(
                job.workflows_completed,
                workflows_completed,
            )
            job.workflows_failed = max(job.workflows_failed, workflows_failed)

    def _merge_hydrated_job_timing(
        self,
        job: JobInfo,
        timestamp: float,
        layer_version: int,
        elapsed_seconds: float,
    ) -> None:
        """Take the snapshot's timestamp, never lower the layer version, and
        adopt the start time its elapsed seconds imply when that is earlier."""
        job.timestamp = timestamp
        job.layer_version = max(job.layer_version, layer_version)
        if elapsed_seconds > 0:
            self._adopt_earlier_hydrated_start(job, elapsed_seconds)

    def _adopt_earlier_hydrated_start(self, job: JobInfo, elapsed_seconds: float) -> None:
        """Start the job ``elapsed_seconds`` ago on this manager's monotonic
        clock, unless it already started earlier."""
        hydrated_started_at = self._clock.monotonic() - elapsed_seconds
        if self._hydrated_start_is_earlier(job, hydrated_started_at):
            job.started_at = hydrated_started_at

    @staticmethod
    def _hydrated_start_is_earlier(job: JobInfo, hydrated_started_at: float) -> bool:
        """The job has no start time yet, or the hydrated one precedes it."""
        return job.started_at == 0.0 or hydrated_started_at < job.started_at

    def _hydrate_workflow_snapshots(
        self,
        job: JobInfo,
        job_id: str,
        job_token_str: str,
        workflow_snapshots: dict[str, WorkflowStateSnapshot],
        merge_forward_only: bool,
        replaces_with_mirror: bool,
        hydration_transitions: list[StateTransition],
    ) -> None:
        """Drop the workflows a replacing mirror no longer holds, then
        hydrate each snapshotted workflow in snapshot order."""
        if replaces_with_mirror:
            self._drop_workflows_missing_from_snapshot(job, job_id, workflow_snapshots)

        for workflow_token_str, workflow_snapshot in workflow_snapshots.items():
            self._hydrate_workflow_snapshot(
                job,
                job_id,
                job_token_str,
                workflow_token_str,
                workflow_snapshot,
                merge_forward_only,
                hydration_transitions,
            )

    def _drop_workflows_missing_from_snapshot(
        self,
        job: JobInfo,
        job_id: str,
        workflow_snapshots: dict[str, WorkflowStateSnapshot],
    ) -> None:
        """Forget every held workflow the leader's snapshot no longer
        carries: its record, its lifecycle and its job lookup."""
        current_workflow_tokens = set(job.workflows)
        snapshot_workflow_tokens = set(workflow_snapshots)
        for stale_workflow_token in (
            current_workflow_tokens - snapshot_workflow_tokens
        ):
            if (stale_workflow := job.workflows.pop(stale_workflow_token, None)) is not None:
                self.workflow_lifecycle.forget_workflow(
                    job_id, self._workflow_id_of_token(stale_workflow.token)
                )
            self._workflow_to_job.pop(stale_workflow_token, None)

    def _hydrate_workflow_snapshot(
        self,
        job: JobInfo,
        job_id: str,
        job_token_str: str,
        workflow_token_str: str,
        workflow_snapshot: WorkflowStateSnapshot,
        merge_forward_only: bool,
        hydration_transitions: list[StateTransition],
    ) -> None:
        """Hydrate one workflow: decide whether its snapshot's lifecycle
        wins (AD-54), install the state that does, and take the snapshot's
        fields unless the held workflow is ahead of it."""
        workflow_token = TrackingToken.parse(
            str(workflow_snapshot["token"])
        )
        workflow_id = self._workflow_id_of_token(workflow_token)
        workflow_status = self._workflow_status_from_value(
            str(workflow_snapshot["status"])
        )
        # The snapshot carries the workflow's lifecycle state; an
        # older peer's carries only the status, read as the state it
        # projects from.
        snapshot_lifecycle_state = workflow_snapshot.get("lifecycle_state")
        snapshot_state = self._snapshot_workflow_state(
            snapshot_lifecycle_state, workflow_status
        )
        snapshot_retry_generation = int(workflow_snapshot.get("retry_generation", 0))
        hydrated_record = self.workflow_lifecycle.get_record(job_id, workflow_id)
        adopt_snapshot = self._adopts_workflow_snapshot(
            hydrated_record,
            merge_forward_only,
            snapshot_state,
            snapshot_retry_generation,
            snapshot_lifecycle_state,
            workflow_status,
        )
        hydrated_state = self._install_hydrated_workflow_state(
            job_id,
            workflow_id,
            adopt_snapshot,
            snapshot_state,
            snapshot_retry_generation,
            hydrated_record,
            hydration_transitions,
        )

        if (
            workflow := self._workflow_to_hydrate(
                job,
                job_token_str,
                workflow_token_str,
                workflow_token,
                workflow_snapshot,
                hydrated_state,
                self._workflow_keeps_own_view(merge_forward_only, adopt_snapshot),
            )
        ) is not None:
            self._apply_workflow_snapshot_fields(
                workflow, workflow_snapshot, hydrated_state, not merge_forward_only
            )

    @staticmethod
    def _snapshot_workflow_state(
        snapshot_lifecycle_state: str | None,
        workflow_status: WorkflowStatus,
    ) -> WorkflowState:
        """The snapshot's lifecycle state, or for a status-only snapshot the
        state its status projects from."""
        return (
            WorkflowState(snapshot_lifecycle_state)
            if snapshot_lifecycle_state
            else WORKFLOW_STATE_BY_WORKFLOW_STATUS[workflow_status]
        )

    def _adopts_workflow_snapshot(
        self,
        hydrated_record: WorkflowLifecycleRecord | None,
        merge_forward_only: bool,
        snapshot_state: WorkflowState,
        snapshot_retry_generation: int,
        snapshot_lifecycle_state: str | None,
        workflow_status: WorkflowStatus,
    ) -> bool:
        """Whether the snapshot's lifecycle state wins over the held one: an
        unknown workflow always takes it; the job's own leader only when the
        snapshot is ahead; a follower whenever the snapshot disagrees."""
        if hydrated_record is None:
            return True
        if merge_forward_only:
            return self._snapshot_is_ahead_of_record(
                hydrated_record, snapshot_state, snapshot_retry_generation
            )
        return self._snapshot_disagrees_with_record(
            hydrated_record, snapshot_lifecycle_state, workflow_status
        )

    @staticmethod
    def _snapshot_is_ahead_of_record(
        hydrated_record: WorkflowLifecycleRecord,
        snapshot_state: WorkflowState,
        snapshot_retry_generation: int,
    ) -> bool:
        """The snapshot is of a later retry generation, or further along the
        same one."""
        return (
            snapshot_retry_generation,
            WORKFLOW_STATE_RANK[snapshot_state],
        ) > (
            hydrated_record.retry_generation,
            WORKFLOW_STATE_RANK[hydrated_record.state],
        )

    @staticmethod
    def _snapshot_disagrees_with_record(
        hydrated_record: WorkflowLifecycleRecord,
        snapshot_lifecycle_state: str | None,
        workflow_status: WorkflowStatus,
    ) -> bool:
        """A snapshot carrying a lifecycle state always installs. A
        status-only snapshot agrees with any local state that projects to
        it (AGGREGATED and COMPLETED both read COMPLETED); only a
        disagreement installs."""
        return (
            snapshot_lifecycle_state is not None
            or WORKFLOW_STATUS_BY_WORKFLOW_STATE[hydrated_record.state] != workflow_status
        )

    def _install_hydrated_workflow_state(
        self,
        job_id: str,
        workflow_id: str,
        adopt_snapshot: bool,
        snapshot_state: WorkflowState,
        snapshot_retry_generation: int,
        hydrated_record: WorkflowLifecycleRecord | None,
        hydration_transitions: list[StateTransition],
    ) -> WorkflowState:
        """Install an adopted snapshot's state, collecting the transition
        it makes; returns the workflow's state after hydration."""
        if not adopt_snapshot:
            return hydrated_record.state
        if (
            hydrated_transition := self.workflow_lifecycle.install_state(
                job_id,
                workflow_id,
                snapshot_state,
                snapshot_retry_generation,
                "hydrated from a snapshot",
            )
        ) is not None:
            hydration_transitions.append(hydrated_transition)
        return snapshot_state

    @staticmethod
    def _workflow_keeps_own_view(merge_forward_only: bool, adopt_snapshot: bool) -> bool:
        """A held workflow ahead of a forward-only snapshot keeps its own
        view: none of the snapshot's fields are taken."""
        return merge_forward_only and not adopt_snapshot

    def _workflow_to_hydrate(
        self,
        job: JobInfo,
        job_token_str: str,
        workflow_token_str: str,
        workflow_token: TrackingToken,
        workflow_snapshot: WorkflowStateSnapshot,
        hydrated_state: WorkflowState,
        keeps_own_view: bool,
    ) -> WorkflowInfo | None:
        """Map the workflow to its job and return the workflow whose fields
        the snapshot sets: a new one for a workflow the job does not hold,
        the held one, or None when the held one keeps its own view."""
        workflow = job.workflows.get(workflow_token_str)
        self._workflow_to_job[workflow_token_str] = job_token_str
        if workflow is None:
            workflow = WorkflowInfo(
                token=workflow_token,
                name=str(workflow_snapshot["name"]),
                workflow=None,
                status=WORKFLOW_STATUS_BY_WORKFLOW_STATE[hydrated_state],
                # Whether its per-core results merge as load-test
                # stats is known only from its definition: a
                # workflow learned from a worker's report (no
                # ``is_test``) keeps its results unmerged --
                # merging is an aggregation, unmerged loses nothing.
                is_test=bool(workflow_snapshot.get("is_test", False)),
            )
            job.workflows[workflow_token_str] = workflow
            return workflow
        if keeps_own_view:
            # Ahead of this snapshot: the workflow keeps its own view.
            return None
        return workflow

    def _apply_workflow_snapshot_fields(
        self,
        workflow: WorkflowInfo,
        workflow_snapshot: WorkflowStateSnapshot,
        hydrated_state: WorkflowState,
        mirror_snapshot: bool,
    ) -> None:
        """Set the workflow's fields from its snapshot: a mirror takes the
        snapshot's sub-workflow tokens, a forward merge adds them to the
        held ones in order."""
        workflow.name = str(workflow_snapshot["name"])
        workflow.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[hydrated_state]
        workflow.sub_workflow_tokens = (
            self._mirrored_sub_workflow_tokens(workflow_snapshot)
            if mirror_snapshot
            else self._merged_sub_workflow_tokens(workflow, workflow_snapshot)
        )
        workflow.error = workflow_snapshot["error"]
        workflow.aggregation_error = workflow_snapshot["aggregation_error"]
        workflow.terminal_pushed = bool(workflow_snapshot["terminal_pushed"])
        workflow.terminal_status = workflow_snapshot["terminal_status"]
        if (dependency_workflow_ids := workflow_snapshot.get("dependency_workflow_ids")) is not None:
            workflow.dependency_workflow_ids = frozenset(dependency_workflow_ids)

    @staticmethod
    def _mirrored_sub_workflow_tokens(workflow_snapshot: WorkflowStateSnapshot) -> list[str]:
        """The snapshot's sub-workflow tokens, as strings."""
        return [str(token) for token in workflow_snapshot["sub_workflow_tokens"]]

    @staticmethod
    def _merged_sub_workflow_tokens(
        workflow: WorkflowInfo,
        workflow_snapshot: WorkflowStateSnapshot,
    ) -> list[str]:
        """The held sub-workflow tokens followed by the snapshot's ones the
        workflow does not hold yet, each once."""
        return list(
            dict.fromkeys(
                [
                    *workflow.sub_workflow_tokens,
                    *(str(token) for token in workflow_snapshot["sub_workflow_tokens"]),
                ]
            )
        )

    def _hydrate_sub_workflow_snapshots(
        self,
        job: JobInfo,
        job_token_str: str,
        sub_workflow_snapshots: dict[str, SubWorkflowStateSnapshot],
        mirror_snapshot: bool,
        replaces_with_mirror: bool,
    ) -> None:
        """Drop the sub-workflows a replacing mirror no longer holds, then
        hydrate each snapshotted sub-workflow in snapshot order."""
        if replaces_with_mirror:
            self._drop_sub_workflows_missing_from_snapshot(job, sub_workflow_snapshots)

        for (
            sub_workflow_token_str,
            sub_workflow_snapshot,
        ) in sub_workflow_snapshots.items():
            self._hydrate_sub_workflow_snapshot(
                job,
                job_token_str,
                sub_workflow_token_str,
                sub_workflow_snapshot,
                mirror_snapshot,
            )

    def _drop_sub_workflows_missing_from_snapshot(
        self,
        job: JobInfo,
        sub_workflow_snapshots: dict[str, SubWorkflowStateSnapshot],
    ) -> None:
        """Forget every held sub-workflow the leader's snapshot no longer
        carries, with its job lookup."""
        current_sub_workflow_tokens = set(job.sub_workflows)
        snapshot_sub_workflow_tokens = set(sub_workflow_snapshots)
        for stale_sub_workflow_token in (
            current_sub_workflow_tokens - snapshot_sub_workflow_tokens
        ):
            job.sub_workflows.pop(stale_sub_workflow_token, None)
            self._sub_workflow_to_job.pop(stale_sub_workflow_token, None)

    def _hydrate_sub_workflow_snapshot(
        self,
        job: JobInfo,
        job_token_str: str,
        sub_workflow_token_str: str,
        sub_workflow_snapshot: SubWorkflowStateSnapshot,
        mirror_snapshot: bool,
    ) -> None:
        """Hydrate one sub-workflow -- created when the job does not hold
        it, mirrored by a follower, or only gaining what it lacks under a
        forward merge -- and link it to its parent workflow."""
        sub_workflow_token = TrackingToken.parse(
            str(sub_workflow_snapshot["token"])
        )
        parent_token = TrackingToken.parse(
            str(sub_workflow_snapshot["parent_token"])
        )
        sub_workflow = job.sub_workflows.get(sub_workflow_token_str)
        self._sub_workflow_to_job[sub_workflow_token_str] = job_token_str
        if sub_workflow is None:
            self._create_hydrated_sub_workflow(
                job,
                sub_workflow_token_str,
                sub_workflow_token,
                parent_token,
                sub_workflow_snapshot,
            )
        elif mirror_snapshot:
            self._mirror_sub_workflow_snapshot(
                sub_workflow, parent_token, sub_workflow_snapshot
            )
        else:
            self._merge_sub_workflow_snapshot_forward(sub_workflow, sub_workflow_snapshot)

        self._link_hydrated_sub_workflow_to_parent(
            job, sub_workflow_token_str, parent_token
        )

    def _create_hydrated_sub_workflow(
        self,
        job: JobInfo,
        sub_workflow_token_str: str,
        sub_workflow_token: TrackingToken,
        parent_token: TrackingToken,
        sub_workflow_snapshot: SubWorkflowStateSnapshot,
    ) -> None:
        """Create a sub-workflow the job does not hold from its snapshot."""
        sub_workflow = SubWorkflowInfo(
            token=sub_workflow_token,
            parent_token=parent_token,
            cores_allocated=int(sub_workflow_snapshot["cores_allocated"]),
            fence_token=int(sub_workflow_snapshot["fence_token"]),
            # The snapshot carries no start time; "now" over-
            # states remaining work, the safe side for AD-43's
            # wait estimate.
            dispatched_at=self._clock.monotonic(),
        )
        job.sub_workflows[sub_workflow_token_str] = sub_workflow
        sub_workflow.progress = sub_workflow_snapshot["progress"]
        sub_workflow.result = sub_workflow_snapshot["result"]
        sub_workflow.dispatched_context = sub_workflow_snapshot["dispatched_context"]
        sub_workflow.dispatched_version = int(sub_workflow_snapshot["dispatched_version"])
        sub_workflow.superseded = bool(sub_workflow_snapshot["superseded"])

    @staticmethod
    def _mirror_sub_workflow_snapshot(
        sub_workflow: SubWorkflowInfo,
        parent_token: TrackingToken,
        sub_workflow_snapshot: SubWorkflowStateSnapshot,
    ) -> None:
        """A follower takes every field of the leader's sub-workflow."""
        sub_workflow.parent_token = parent_token
        sub_workflow.cores_allocated = int(
            sub_workflow_snapshot["cores_allocated"]
        )
        sub_workflow.fence_token = int(sub_workflow_snapshot["fence_token"])
        sub_workflow.progress = sub_workflow_snapshot["progress"]
        sub_workflow.result = sub_workflow_snapshot["result"]
        sub_workflow.dispatched_context = sub_workflow_snapshot[
            "dispatched_context"
        ]
        sub_workflow.dispatched_version = int(
            sub_workflow_snapshot["dispatched_version"]
        )
        sub_workflow.superseded = bool(sub_workflow_snapshot["superseded"])

    def _merge_sub_workflow_snapshot_forward(
        self,
        sub_workflow: SubWorkflowInfo,
        sub_workflow_snapshot: SubWorkflowStateSnapshot,
    ) -> None:
        """A known sub only gains what it lacks: a result, progress
        further along, the knowledge that it was superseded."""
        self._gain_missing_sub_workflow_result_and_progress(
            sub_workflow, sub_workflow_snapshot
        )
        sub_workflow.superseded = sub_workflow.superseded or bool(
            sub_workflow_snapshot["superseded"]
        )

    def _gain_missing_sub_workflow_result_and_progress(
        self,
        sub_workflow: SubWorkflowInfo,
        sub_workflow_snapshot: SubWorkflowStateSnapshot,
    ) -> None:
        """Take the snapshot's result when the sub has none, and its
        progress when that is further along than the held progress."""
        if sub_workflow.result is None:
            sub_workflow.result = sub_workflow_snapshot["result"]
        snapshot_progress = sub_workflow_snapshot["progress"]
        if self._snapshot_progress_is_further(sub_workflow.progress, snapshot_progress):
            sub_workflow.progress = snapshot_progress

    @staticmethod
    def _snapshot_progress_is_further(
        held_progress: WorkflowProgress | None,
        snapshot_progress: WorkflowProgress | None,
    ) -> bool:
        """The snapshot carries progress, and the sub holds none or has
        completed and failed fewer actions than it."""
        return snapshot_progress is not None and (
            held_progress is None
            or snapshot_progress.completed_count + snapshot_progress.failed_count
            > held_progress.completed_count + held_progress.failed_count
        )

    @staticmethod
    def _link_hydrated_sub_workflow_to_parent(
        job: JobInfo,
        sub_workflow_token_str: str,
        parent_token: TrackingToken,
    ) -> None:
        """List the sub-workflow under its parent, when the job holds the
        parent and it does not list the sub yet."""
        parent = job.workflows.get(str(parent_token))
        if (
            parent is not None
            and sub_workflow_token_str not in parent.sub_workflow_tokens
        ):
            parent.sub_workflow_tokens.append(sub_workflow_token_str)

    async def hydrate_worker_active_workflow(
        self,
        *,
        progress: WorkflowProgress,
        worker_id: str,
        leader_node_id: str,
        leader_addr: tuple[str, int],
        fencing_token: int,
        callback_addr: tuple[str, int] | None,
    ) -> JobInfo | None:
        """Learn a workflow a worker reports running -- evidence only
        (AD-54): an unknown workflow is RUNNING, a DISPATCHED one is now
        RUNNING, and any other keeps its state (a PENDING workflow is not
        promoted by a run the job is no longer waiting on). A run of an
        earlier incarnation of a workflow the job re-registered under a new
        token (its manager restarted) is not this job's to track. Returns
        the job, or None when the report was not taken."""
        sub_workflow_token = self._reported_sub_workflow_token(progress, worker_id)

        job_id = progress.job_id or sub_workflow_token.job_id
        workflow_id = sub_workflow_token.workflow_id
        workflow_token = sub_workflow_token.to_parent_workflow_token()
        if self._reported_run_is_of_an_earlier_incarnation(
            job_id, workflow_id, workflow_token
        ):
            return None

        known_record = self.workflow_lifecycle.get_record(job_id, workflow_id)
        evidenced_state = self._evidenced_workflow_state(known_record)
        cores_allocated = self._reported_cores_allocated(progress)
        workflow_snapshot = self._worker_evidence_workflow_snapshot(
            progress, workflow_token, evidenced_state, known_record
        )
        sub_workflow_snapshot: SubWorkflowStateSnapshot = {
            "token": progress.workflow_id,
            "parent_token": str(workflow_token),
            "cores_allocated": cores_allocated,
            "fence_token": fencing_token,
            "progress": progress,
            "result": None,
            "dispatched_context": b"",
            "dispatched_version": 0,
            "superseded": False,
        }

        return await self.hydrate_remote_job_state(
            job_id=job_id,
            leader_node_id=leader_node_id,
            leader_addr=leader_addr,
            status=JobStatus.RUNNING.value,
            workflows_total=1,
            workflows_completed=0,
            workflows_failed=0,
            fencing_token=fencing_token,
            callback_addr=callback_addr,
            workflow_snapshots={str(workflow_token): workflow_snapshot},
            sub_workflow_snapshots={progress.workflow_id: sub_workflow_snapshot},
            layer_version=0,
            elapsed_seconds=0.0,
            timestamp=self._clock.time(),
            merge_forward_only=True,
            replace_existing=False,
        )

    @staticmethod
    def _reported_sub_workflow_token(
        progress: WorkflowProgress,
        worker_id: str,
    ) -> TrackingToken:
        """The sub-workflow token a worker's report names; raises
        ``ValueError`` when it carries no parent workflow."""
        sub_workflow_token = TrackingToken.parse(progress.workflow_id)
        if not sub_workflow_token.workflow_id:
            raise ValueError(
                f"Worker {worker_id} active workflow has no parent workflow token: "
                f"{progress.workflow_id}"
            )
        return sub_workflow_token

    def _reported_run_is_of_an_earlier_incarnation(
        self,
        job_id: str,
        workflow_id: str,
        workflow_token: TrackingToken,
    ) -> bool:
        """The job is held and re-registered the workflow under another
        token (its manager restarted): the reported run is not its own."""
        return (
            job := self.get_job_by_id(job_id)
        ) is not None and self._job_holds_workflow_under_another_token(
            job, workflow_id, workflow_token
        )

    @staticmethod
    def _job_holds_workflow_under_another_token(
        job: JobInfo,
        workflow_id: str,
        workflow_token: TrackingToken,
    ) -> bool:
        """Some workflow of the job has this workflow id but another token."""
        return any(
            workflow_info.token.workflow_id == workflow_id
            and str(workflow_info.token) != str(workflow_token)
            for workflow_info in job.workflows.values()
        )

    @staticmethod
    def _evidenced_workflow_state(
        known_record: WorkflowLifecycleRecord | None,
    ) -> WorkflowState:
        """The state a worker's report evidences: RUNNING for an unknown or
        DISPATCHED workflow, the known state otherwise."""
        return (
            WorkflowState.RUNNING
            if known_record is None or known_record.state == WorkflowState.DISPATCHED
            else known_record.state
        )

    @staticmethod
    def _reported_cores_allocated(progress: WorkflowProgress) -> int:
        """The cores the worker reports for the run, at least one."""
        return (
            progress.worker_workflow_assigned_cores
            or len(progress.assigned_cores)
            or 1
        )

    @staticmethod
    def _worker_evidence_workflow_snapshot(
        progress: WorkflowProgress,
        workflow_token: TrackingToken,
        evidenced_state: WorkflowState,
        known_record: WorkflowLifecycleRecord | None,
    ) -> WorkflowStateSnapshot:
        """The workflow snapshot a worker's report evidences, in the known
        record's retry generation (0 for an unknown workflow)."""
        return {
            "token": str(workflow_token),
            "name": progress.workflow_name,
            "status": WORKFLOW_STATUS_BY_WORKFLOW_STATE[evidenced_state].value,
            "lifecycle_state": evidenced_state.value,
            "retry_generation": 0 if known_record is None else known_record.retry_generation,
            "sub_workflow_tokens": [progress.workflow_id],
            "error": None,
            "aggregation_error": None,
            "terminal_pushed": False,
            "terminal_status": None,
        }

    @staticmethod
    def _workflow_id_of_token(token: TrackingToken) -> str:
        """The token's workflow id, or "" for a token naming none."""
        return token.workflow_id or ""

    @staticmethod
    def _workflow_status_from_value(status: str) -> WorkflowStatus:
        """Convert a wire status value into ``WorkflowStatus``."""
        for candidate in WorkflowStatus:
            if candidate.value == status:
                return candidate

        raise ValueError(f"Unknown workflow status: {status}")

    def get_job(self, job_token_or_id: str | TrackingToken) -> JobInfo | None:
        """Get job info by token-string or bare job_id.

        Storage is keyed by token-string (``"<DC>:<MANAGER>:<JOB>"``).
        Callers historically split between two helpers:

        * ``get_job(token)`` for callers that already had a token.
        * ``get_job_by_id(job_id)`` for callers that had only the
          bare job_id.

        Many call sites (cancel handler, dispatch handler, query
        handlers) passed a bare job_id to ``get_job`` and silently
        got ``None`` — masking real job state behind "Job not
        found" responses. To eliminate that footgun, ``get_job``
        now accepts both forms: if the input doesn't carry the
        token-segment delimiter ``":"`` it is assumed to be a
        bare job_id and routed through ``create_job_token`` first.

        ``get_job_by_id`` is preserved as the explicit-by-id
        accessor for callers that want to make their intent
        unambiguous.
        """
        token_str = str(job_token_or_id)
        if ":" not in token_str:
            token_str = str(self.create_job_token(token_str))
        return self._jobs.get(token_str)

    def get_job_by_id(self, job_id: str) -> JobInfo | None:
        """Get job info by job_id (creates token internally)."""
        token = self.create_job_token(job_id)
        return self._jobs.get(str(token))

    def get_job_for_workflow(
        self, workflow_token: str | TrackingToken
    ) -> JobInfo | None:
        """Get job info by workflow token."""
        token_str = str(workflow_token)
        job_token_str = self._workflow_to_job.get(token_str)
        if job_token_str:
            return self._jobs.get(job_token_str)
        return None

    def get_job_for_sub_workflow(
        self, sub_workflow_token: str | TrackingToken
    ) -> JobInfo | None:
        """Get job info by sub-workflow token."""
        token_str = str(sub_workflow_token)
        job_token_str = self._sub_workflow_to_job.get(token_str)
        if job_token_str:
            return self._jobs.get(job_token_str)
        return None

    async def remove_job(self, job_id: str) -> bool:
        """
        Remove a job and everything kept for it: its lookups, fence tokens,
        progress sequences and workflow lifecycles. The one removal, so a
        job dropped at completion and one dropped at the retention sweep
        leave nothing different behind.

        Thread-safe: uses global lock for job deletion.

        Returns True if the job was found and removed.
        """
        async with self._global_lock:
            job = self._jobs.pop(str(self.create_job_token(job_id)), None)
            if not job:
                return False

            self._forget_job_lookups(job)
            self._job_fence_tokens.pop(job_id, None)
            self._job_progress_sequences.pop(job_id, None)
            self.workflow_lifecycle.forget_job(job_id)

            return True

    def _forget_job_lookups(self, job: JobInfo) -> None:
        """Drop the workflow and sub-workflow -> job lookups of a removed
        job."""
        for workflow_token_str in job.workflows:
            self._workflow_to_job.pop(workflow_token_str, None)
        for sub_workflow_token_str in job.sub_workflows:
            self._sub_workflow_to_job.pop(sub_workflow_token_str, None)

    # =========================================================================
    # Workflow Registration
    # =========================================================================

    async def register_workflow(
        self,
        job_id: str,
        workflow_id: str,
        name: str,
        workflow: Workflow | None = None,
        *,
        dependency_workflow_ids: frozenset[str],
        is_test: bool,
    ) -> WorkflowInfo | None:
        """
        Register a workflow for a job.

        Args:
            job_id: The job ID
            workflow_id: Unique workflow identifier within the job
            name: Human-readable workflow name (for display/logging)
            workflow: Optional workflow instance
            dependency_workflow_ids: The workflow ids it waits on
            is_test: Whether it drives load (has a TEST hook)

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if not job:
            await self._logger.log(
                JobManagerError(
                    message=f"[register_workflow] FAILED: job not found for job_id={job_id}",
                    manager_id=self._manager_id,
                    datacenter=self._datacenter,
                    job_id=job_id,
                    workflow_id=workflow_id,
                )
            )
            return None

        workflow_token = self.create_workflow_token(job_id, workflow_id)
        workflow_token_str = str(workflow_token)

        async with job.lock:
            if workflow_token_str in job.workflows:
                return job.workflows[workflow_token_str]

            info = WorkflowInfo(
                token=workflow_token,
                name=name,
                workflow=workflow,
                status=WorkflowStatus.PENDING,
                dependency_workflow_ids=dependency_workflow_ids,
                is_test=is_test,
            )
            job.workflows[workflow_token_str] = info
            self._workflow_to_job[workflow_token_str] = str(job.token)
            registration = self.workflow_lifecycle.register_workflow(
                job_id, workflow_id, "registered"
            )

            # Update job progress
            job.workflows_total = len(job.workflows)

        await self.workflow_lifecycle.publish_transitions([registration])
        return info

    async def register_sub_workflow(
        self,
        job_id: str,
        workflow_id: str,
        worker_id: str,
        cores_allocated: int,
        fence_token: int = 0,
    ) -> SubWorkflowInfo | None:
        """
        Register a sub-workflow dispatch to a worker.

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if not job:
            await self._logger.log(
                JobManagerError(
                    message=f"[register_sub_workflow] FAILED: job not found for job_id={job_id}",
                    manager_id=self._manager_id,
                    datacenter=self._datacenter,
                    job_id=job_id,
                    workflow_id=workflow_id,
                )
            )
            return None

        workflow_token = self.create_workflow_token(job_id, workflow_id)
        workflow_token_str = str(workflow_token)
        sub_workflow_token = self.create_sub_workflow_token(
            job_id, workflow_id, worker_id
        )
        sub_workflow_token_str = str(sub_workflow_token)

        async with job.lock:
            # Get parent workflow
            parent = job.workflows.get(workflow_token_str)
            if not parent:
                await self._logger.log(
                    JobManagerError(
                        message=f"[register_sub_workflow] FAILED: parent workflow not found for workflow_token={workflow_token_str}, job.workflows keys={list(job.workflows.keys())}",
                        manager_id=self._manager_id,
                        datacenter=self._datacenter,
                        job_id=job_id,
                        workflow_id=workflow_id,
                    )
                )
                return None

            info = self._register_sub_workflow_dispatch(
                job,
                parent,
                workflow_token,
                sub_workflow_token,
                sub_workflow_token_str,
                cores_allocated,
                fence_token,
            )

        return info

    def _register_sub_workflow_dispatch(
        self,
        job: JobInfo,
        parent: WorkflowInfo,
        workflow_token: TrackingToken,
        sub_workflow_token: TrackingToken,
        sub_workflow_token_str: str,
        cores_allocated: int,
        fence_token: int,
    ) -> SubWorkflowInfo:
        """Under the job lock: re-dispatch the sub-workflow the job already
        holds, or create it."""
        if existing := job.sub_workflows.get(sub_workflow_token_str):
            return self._redispatch_sub_workflow(
                job, parent, existing, sub_workflow_token_str, cores_allocated, fence_token
            )

        # Create sub-workflow info
        info = SubWorkflowInfo(
            token=sub_workflow_token,
            parent_token=workflow_token,
            cores_allocated=cores_allocated,
            fence_token=fence_token,
            dispatched_at=self._clock.monotonic(),
        )

        # Register in both places
        job.sub_workflows[sub_workflow_token_str] = info
        parent.sub_workflow_tokens.append(sub_workflow_token_str)
        self._sub_workflow_to_job[sub_workflow_token_str] = str(job.token)
        return info

    def _redispatch_sub_workflow(
        self,
        job: JobInfo,
        parent: WorkflowInfo,
        existing: SubWorkflowInfo,
        sub_workflow_token_str: str,
        cores_allocated: int,
        fence_token: int,
    ) -> SubWorkflowInfo:
        """Reset a held sub-workflow for a new dispatch -- unless it holds a
        live result, which stands."""
        if self._sub_workflow_holds_live_result(existing):
            return existing
        existing.cores_allocated = cores_allocated
        existing.fence_token = fence_token
        existing.result = None
        existing.progress = None
        existing.superseded = False
        # A re-dispatch starts executing now: AD-43's remaining-time
        # estimate measures from here, not from the first dispatch.
        existing.dispatched_at = self._clock.monotonic()
        if sub_workflow_token_str not in parent.sub_workflow_tokens:
            parent.sub_workflow_tokens.append(sub_workflow_token_str)
        self._sub_workflow_to_job[sub_workflow_token_str] = str(job.token)
        return existing

    @staticmethod
    def _sub_workflow_holds_live_result(sub_workflow: SubWorkflowInfo) -> bool:
        """The sub-workflow has a result and was not superseded."""
        return sub_workflow.result is not None and not sub_workflow.superseded

    async def remove_unstarted_sub_workflow(
        self,
        sub_workflow_token: str,
    ) -> bool:
        """Remove a planned sub-workflow no worker took. Returns False --
        keeping it -- when its worker already reported progress or a result
        for it: the worker did take it, whatever its answer said."""
        token_str = str(sub_workflow_token)
        job = self.get_job_for_sub_workflow(token_str)
        if not job:
            return True

        async with job.lock:
            return self._remove_sub_workflow_no_worker_took(job, token_str)

    def _remove_sub_workflow_no_worker_took(self, job: JobInfo, token_str: str) -> bool:
        """Under the job lock: remove the sub-workflow unless its worker
        reported progress or a result for it. True when it is gone."""
        sub_workflow = job.sub_workflows.get(token_str)
        if sub_workflow is None:
            return True
        if self._sub_workflow_was_taken(sub_workflow):
            return False

        self._unlist_sub_workflow_from_parent(job, sub_workflow, token_str)
        job.sub_workflows.pop(token_str, None)
        self._sub_workflow_to_job.pop(token_str, None)
        return True

    @staticmethod
    def _sub_workflow_was_taken(sub_workflow: SubWorkflowInfo) -> bool:
        """Its worker reported a result or progress for it."""
        return sub_workflow.result is not None or sub_workflow.progress is not None

    def _unlist_sub_workflow_from_parent(
        self,
        job: JobInfo,
        sub_workflow: SubWorkflowInfo,
        token_str: str,
    ) -> None:
        """Drop the sub-workflow's token from its parent's list, when the
        job holds the parent."""
        parent = job.workflows.get(str(sub_workflow.parent_token))
        if parent:
            parent.sub_workflow_tokens = self._tokens_without(
                parent.sub_workflow_tokens, token_str
            )

    @staticmethod
    def _tokens_without(tokens: list[str], removed_token: str) -> list[str]:
        """The tokens, in order, without ``removed_token``."""
        return [token for token in tokens if token != removed_token]

    async def claim_workflow_for_dispatch(self, job_id: str, workflow_id: str) -> bool:
        """
        Take a PENDING workflow to send it to workers: PENDING -> DISPATCHED,
        before anything is sent, so a cancellation racing the send sees it
        dispatched. False -- send nothing -- when it is no longer PENDING:
        something (a dependency's cascade, a timeout, a cancellation)
        finished it while its cores were being allocated.

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if job is None:
            return False

        async with job.lock:
            workflow = job.workflows.get(str(self.create_workflow_token(job_id, workflow_id)))
            if self._workflow_is_unclaimable(job_id, workflow_id, workflow):
                return False

            claim_transition = self.workflow_lifecycle.apply_transition(
                job_id, workflow_id, WorkflowState.DISPATCHED, "claimed for dispatch"
            )
            self._project_accepted_transition(workflow, claim_transition)

        await self.workflow_lifecycle.publish_transitions([claim_transition])
        return claim_transition.accepted

    def _workflow_is_unclaimable(
        self,
        job_id: str,
        workflow_id: str,
        workflow: WorkflowInfo | None,
    ) -> bool:
        """The job does not hold the workflow, or it is no longer PENDING."""
        return (
            workflow is None
            or self.workflow_lifecycle.get_state(job_id, workflow_id) != WorkflowState.PENDING
        )

    @staticmethod
    def _project_accepted_transition(
        workflow: WorkflowInfo,
        transition: StateTransition,
    ) -> None:
        """An accepted transition sets the workflow's status to the
        projection of the state it reached."""
        if transition.accepted:
            workflow.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[transition.to_state]

    async def return_workflow_to_pending(
        self,
        job_id: str,
        workflow_id: str,
        reason: str,
    ) -> bool:
        """
        Put a workflow whose attempt failed -- no worker took it, or every
        worker it ran on was lost -- back in the queue, in one step:
        DISPATCHED|RUNNING -> FAILED -> FAILED_CANCELING_DEPENDENTS ->
        FAILED_READY_FOR_RETRY -> PENDING. Its dependents wait on its
        completion, so none of them started. False when it is no longer
        DISPATCHED or RUNNING: something else finished it meanwhile.

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if job is None:
            return False

        async with job.lock:
            workflow = job.workflows.get(str(self.create_workflow_token(job_id, workflow_id)))
            if self._workflow_is_not_out_for_dispatch(job_id, workflow_id, workflow):
                return False

            retry_transitions = [
                self.workflow_lifecycle.apply_transition(
                    job_id, workflow_id, WorkflowState.FAILED, reason
                ),
                self.workflow_lifecycle.apply_transition(
                    job_id, workflow_id, WorkflowState.FAILED_CANCELING_DEPENDENTS, "retrying"
                ),
                self.workflow_lifecycle.apply_transition(
                    job_id, workflow_id, WorkflowState.FAILED_READY_FOR_RETRY, "no dependent started"
                ),
                requeue_transition := self.workflow_lifecycle.apply_transition(
                    job_id, workflow_id, WorkflowState.PENDING, "requeued"
                ),
            ]
            self._project_accepted_transition(workflow, requeue_transition)

        await self.workflow_lifecycle.publish_transitions(retry_transitions)
        return requeue_transition.accepted

    def _workflow_is_not_out_for_dispatch(
        self,
        job_id: str,
        workflow_id: str,
        workflow: WorkflowInfo | None,
    ) -> bool:
        """The job does not hold the workflow, or it is neither DISPATCHED
        nor RUNNING."""
        return workflow is None or self.workflow_lifecycle.get_state(job_id, workflow_id) not in (
            WorkflowState.DISPATCHED,
            WorkflowState.RUNNING,
        )

    async def apply_workflow_reassignment(
        self,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
    ) -> tuple[bool, bool]:
        """
        Supersede the workflow's sub-workflows on a lost worker: their late
        progress and results are stale from now on.

        Returns ``(superseded, lost_every_sub)``: whether any sub was newly
        superseded (a repeated report of the same loss supersedes nothing),
        and whether none of the workflow's subs remains -- every worker it
        ran on is gone, so whether it runs again (or fails for good) is the
        job leader's decision. Its lifecycle is left as it is.
        """
        if (
            reassignment_target := await self._reassignment_target(
                job_id, workflow_id, sub_workflow_token
            )
        ) is None:
            return False, False
        job, reassignment_token = reassignment_target

        async with job.lock:
            supersession = await self._supersede_subs_on_lost_worker(
                job,
                reassignment_token,
                job_id,
                workflow_id,
                sub_workflow_token,
                failed_worker_id,
            )
        if supersession is None:
            return False, False
        newly_superseded, lost_every_sub = supersession

        await self._log_applied_reassignment(
            newly_superseded, job_id, workflow_id, sub_workflow_token, failed_worker_id
        )

        return bool(newly_superseded), lost_every_sub

    async def _reassignment_target(
        self,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
    ) -> tuple[JobInfo, TrackingToken] | None:
        """The job and the parsed sub-workflow token a reassignment names,
        or None -- logged -- when the job is not held or the token is
        invalid or names another job or workflow."""
        job = self.get_job_by_id(job_id)
        if not job:
            await self._log_reassignment_error(
                f"[apply_workflow_reassignment] FAILED: job not found for job_id={job_id}",
                job_id,
                workflow_id,
                sub_workflow_token,
            )
            return None

        if (
            reassignment_token := await self._validated_reassignment_token(
                job_id, workflow_id, sub_workflow_token
            )
        ) is None:
            return None
        return job, reassignment_token

    async def _validated_reassignment_token(
        self,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
    ) -> TrackingToken | None:
        """The parsed sub-workflow token, or None -- logged -- when it does
        not parse or names another job or workflow."""
        try:
            reassignment_token = TrackingToken.parse(sub_workflow_token)
        except ValueError as error:
            await self._log_reassignment_error(
                f"[apply_workflow_reassignment] FAILED: invalid sub_workflow_token {sub_workflow_token}: {error}",
                job_id,
                workflow_id,
                sub_workflow_token,
            )
            return None

        if self._reassignment_token_mismatches(reassignment_token, job_id, workflow_id):
            await self._log_reassignment_error(
                (
                    "[apply_workflow_reassignment] FAILED: token mismatch "
                    f"job_id={job_id}, workflow_id={workflow_id}, token={sub_workflow_token}"
                ),
                job_id,
                workflow_id,
                sub_workflow_token,
            )
            return None
        return reassignment_token

    @staticmethod
    def _reassignment_token_mismatches(
        reassignment_token: TrackingToken,
        job_id: str,
        workflow_id: str,
    ) -> bool:
        """The token names another job or another workflow."""
        return (
            reassignment_token.job_id != job_id
            or reassignment_token.workflow_id != workflow_id
        )

    async def _supersede_subs_on_lost_worker(
        self,
        job: JobInfo,
        reassignment_token: TrackingToken,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
    ) -> tuple[list[SubWorkflowInfo], bool] | None:
        """Under the job lock: supersede the parent's live subs on the lost
        worker. Returns the newly superseded subs and whether none of the
        parent's subs remains, or None -- logged -- when the parent is not
        held."""
        parent, parent_token_str = self._reassignment_parent(
            job, reassignment_token, job_id, workflow_id
        )
        if not parent:
            await self._log_reassignment_error(
                f"[apply_workflow_reassignment] FAILED: parent workflow not found for token={parent_token_str}",
                job_id,
                workflow_id,
                sub_workflow_token,
            )
            return None

        newly_superseded = self._live_sub_workflows_on_worker(job, parent, failed_worker_id)
        self._mark_superseded(newly_superseded)
        return newly_superseded, self._lost_every_sub_workflow(job, parent)

    def _reassignment_parent(
        self,
        job: JobInfo,
        reassignment_token: TrackingToken,
        job_id: str,
        workflow_id: str,
    ) -> tuple[WorkflowInfo | None, str]:
        """The parent workflow the token names -- or, when the job does not
        hold it, the job's workflow of this id under this manager's token --
        with the token it was looked up by."""
        parent_token_str = reassignment_token.workflow_token or ""
        parent = job.workflows.get(parent_token_str)
        if not parent:
            fallback_token_str = str(
                self.create_workflow_token(job_id, workflow_id)
            )
            parent = job.workflows.get(fallback_token_str)
            parent_token_str = fallback_token_str
        return parent, parent_token_str

    def _live_sub_workflows_on_worker(
        self,
        job: JobInfo,
        parent: WorkflowInfo,
        worker_id: str,
    ) -> list[SubWorkflowInfo]:
        """The parent's held, not yet superseded subs on the worker."""
        return [
            sub_workflow
            for token_str in parent.sub_workflow_tokens
            if self._is_live_sub_workflow_on_worker(
                sub_workflow := job.sub_workflows.get(token_str), worker_id
            )
        ]

    @staticmethod
    def _is_live_sub_workflow_on_worker(
        sub_workflow: SubWorkflowInfo | None,
        worker_id: str,
    ) -> bool:
        """The sub is held, runs on the worker, and is not superseded."""
        return (
            sub_workflow is not None
            and sub_workflow.token.worker_id == worker_id
            and not sub_workflow.superseded
        )

    @staticmethod
    def _mark_superseded(sub_workflows: list[SubWorkflowInfo]) -> None:
        """Supersede each sub: its late progress and results are stale."""
        for sub_workflow in sub_workflows:
            sub_workflow.superseded = True

    @staticmethod
    def _lost_every_sub_workflow(job: JobInfo, parent: WorkflowInfo) -> bool:
        """None of the parent's held subs remains unsuperseded."""
        return not any(
            not sub_workflow.superseded
            for token_str in parent.sub_workflow_tokens
            if (sub_workflow := job.sub_workflows.get(token_str)) is not None
        )

    async def _log_applied_reassignment(
        self,
        newly_superseded: list[SubWorkflowInfo],
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
    ) -> None:
        """Log a reassignment that superseded at least one sub."""
        if newly_superseded:
            await self._logger.log(
                JobManagerInfo(
                    message=(
                        "Applied workflow reassignment "
                        f"from worker {failed_worker_id[:8]}... for workflow {workflow_id[:8]}..."
                    ),
                    manager_id=self._manager_id,
                    datacenter=self._datacenter,
                    job_id=job_id,
                    workflow_id=workflow_id,
                    sub_workflow_token=sub_workflow_token,
                )
            )

    async def _log_reassignment_error(
        self,
        message: str,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
    ) -> None:
        """Log why a workflow reassignment was not applied."""
        await self._logger.log(
            JobManagerError(
                message=message,
                manager_id=self._manager_id,
                datacenter=self._datacenter,
                job_id=job_id,
                workflow_id=workflow_id,
                sub_workflow_token=sub_workflow_token,
            )
        )

    # =========================================================================
    # Progress Updates
    # =========================================================================

    async def update_workflow_progress(
        self,
        sub_workflow_token: str | TrackingToken,
        progress: WorkflowProgress,
    ) -> bool:
        """
        Update progress for a sub-workflow.

        Thread-safe: acquires job lock.
        The first progress from the sub-workflow currently running the
        parent moves it DISPATCHED -> RUNNING; a superseded sub's late
        report proves nothing about the run that replaced it.
        Returns True when the sub-workflow's work advanced -- its first
        report, or more actions completed or failed than its last one (the
        progress AD-34's stuck detection counts); False when it did not, or
        the sub-workflow is not found, or its final result (with its final
        progress) is already recorded.
        """
        token_str = str(sub_workflow_token)
        job = self.get_job_for_sub_workflow(token_str)
        if not job:
            return False

        async with job.lock:
            if (sub_wf := self._sub_workflow_awaiting_result(job, token_str)) is None:
                return False

            previous_progress = sub_wf.progress
            sub_wf.progress = progress
            running_transition = self._start_parent_running_on_progress(job, sub_wf)

        await self._publish_transition_if_any(running_transition)
        return self._progress_advanced(previous_progress, progress)

    @staticmethod
    def _sub_workflow_awaiting_result(job: JobInfo, token_str: str) -> SubWorkflowInfo | None:
        """The held sub-workflow, unless its final result -- whose final
        progress is authoritative -- is already recorded: an in-flight
        report arriving later is stale."""
        sub_wf = job.sub_workflows.get(token_str)
        if not sub_wf:
            return None
        if sub_wf.result is not None:
            return None
        return sub_wf

    def _start_parent_running_on_progress(
        self,
        job: JobInfo,
        sub_wf: SubWorkflowInfo,
    ) -> StateTransition | None:
        """The sub-workflow currently running an ASSIGNED parent moves it
        DISPATCHED -> RUNNING; returns that transition, or None when no
        transition was attempted."""
        parent = job.workflows.get(str(sub_wf.parent_token))
        if not self._progress_starts_parent(parent, sub_wf):
            return None
        running_transition = self.workflow_lifecycle.apply_transition(
            sub_wf.token.job_id,
            self._workflow_id_of_token(parent.token),
            WorkflowState.RUNNING,
            "progress reported",
        )
        self._project_accepted_transition(parent, running_transition)
        return running_transition

    @staticmethod
    def _progress_starts_parent(parent: WorkflowInfo | None, sub_wf: SubWorkflowInfo) -> bool:
        """The parent is held and ASSIGNED, and the reporting sub was not
        superseded (a superseded sub's late report proves nothing about the
        run that replaced it)."""
        return (
            parent is not None
            and parent.status == WorkflowStatus.ASSIGNED
            and not sub_wf.superseded
        )

    async def _publish_transition_if_any(self, transition: StateTransition | None) -> None:
        """Publish the transition, when one was made."""
        if transition is not None:
            await self.workflow_lifecycle.publish_transitions([transition])

    @staticmethod
    def _progress_advanced(
        previous_progress: WorkflowProgress | None,
        progress: WorkflowProgress,
    ) -> bool:
        """The report is the sub's first, or completed or failed more
        actions than its last one."""
        return previous_progress is None or (
            progress.completed_count + progress.failed_count
            > previous_progress.completed_count + previous_progress.failed_count
        )

    async def set_final_workflow_progress(
        self,
        sub_workflow_token: str | TrackingToken,
        progress: WorkflowProgress,
    ) -> bool:
        """Record a sub-workflow's final progress, carried by its final
        result. Returns False if the sub-workflow is not found."""
        token_str = str(sub_workflow_token)
        if (job := self.get_job_for_sub_workflow(token_str)) is None:
            return False

        async with job.lock:
            if (sub_wf := job.sub_workflows.get(token_str)) is None:
                return False
            sub_wf.progress = progress
            return True

    async def record_sub_workflow_result(
        self,
        sub_workflow_token: str | TrackingToken,
        result: WorkflowFinalResult,
    ) -> tuple[bool, bool]:
        """
        Record a final result for a sub-workflow.

        Thread-safe: acquires job lock.

        Returns:
            (result_recorded, parent_complete):
            - result_recorded: True if result was stored
            - parent_complete: True if all sub-workflows for parent are now complete
        """
        result_recorded, parent_complete, _duplicate, _stale, _reason = (
            await self.record_sub_workflow_result_checked(
                sub_workflow_token,
                result,
            )
        )
        return result_recorded, parent_complete

    async def record_sub_workflow_result_checked(
        self,
        sub_workflow_token: str | TrackingToken,
        result: WorkflowFinalResult,
    ) -> tuple[bool, bool, bool, bool, str | None]:
        """Record a sub-workflow result with duplicate/stale classification."""
        token_str = str(sub_workflow_token)
        job = self.get_job_for_sub_workflow(token_str)
        if not job:
            await self._logger.log(
                JobManagerError(
                    message=(
                        "[record_sub_workflow_result] FAILED: job not found "
                        f"for token={token_str}, manager={self._manager_id}, "
                        "_sub_workflow_to_job keys="
                        f"{list(self._sub_workflow_to_job.keys())[:10]}..."
                    ),
                    manager_id=self._manager_id,
                    datacenter=self._datacenter,
                    sub_workflow_token=token_str,
                )
            )
            return False, False, False, False, "unknown_sub_workflow"

        async with job.lock:
            return await self._record_sub_workflow_result_locked(job, token_str, result)

    async def _record_sub_workflow_result_locked(
        self,
        job: JobInfo,
        token_str: str,
        result: WorkflowFinalResult,
    ) -> SubWorkflowResultOutcome:
        """Under the job lock: record the result unless the sub-workflow is
        not held (logged) or a ``SUB_WORKFLOW_RESULT_REJECTIONS`` check
        rejects it; then report whether every active sub of the parent is
        answered."""
        sub_wf = job.sub_workflows.get(token_str)
        if not sub_wf:
            await self._logger.log(
                JobManagerError(
                    message=(
                        "[record_sub_workflow_result] FAILED: sub_wf not "
                        f"found for token={token_str}, job.sub_workflows "
                        f"keys={list(job.sub_workflows.keys())}"
                    ),
                    manager_id=self._manager_id,
                    datacenter=self._datacenter,
                    job_id=job.job_id,
                    sub_workflow_token=token_str,
                )
            )
            return False, False, False, False, "unknown_sub_workflow"

        # Check if all sub-workflows for parent are complete
        parent = job.workflows.get(str(sub_wf.parent_token))
        if (rejection := self._sub_workflow_result_rejection(parent, sub_wf, result)) is not None:
            return rejection

        return self._record_answer_and_check_parent(job, parent, sub_wf, result)

    @staticmethod
    def _sub_workflow_result_rejection(
        parent: WorkflowInfo | None,
        sub_wf: SubWorkflowInfo,
        result: WorkflowFinalResult,
    ) -> SubWorkflowResultOutcome | None:
        """The outcome of the first rejection that holds for the result, or
        None when it is to be recorded."""
        for rejects, rejection in SUB_WORKFLOW_RESULT_REJECTIONS:
            if rejects(parent, sub_wf, result):
                return rejection
        return None

    def _record_answer_and_check_parent(
        self,
        job: JobInfo,
        parent: WorkflowInfo,
        sub_wf: SubWorkflowInfo,
        result: WorkflowFinalResult,
    ) -> SubWorkflowResultOutcome:
        """Record the result; the parent is complete when it has active
        subs and every one of them is answered."""
        sub_wf.result = result
        active_sub_workflow_tokens = self._active_sub_workflow_tokens(job, parent)
        if not active_sub_workflow_tokens:
            return True, False, False, False, None

        all_complete = self._every_sub_workflow_answered(job, active_sub_workflow_tokens)

        return True, all_complete, False, False, None

    def _active_sub_workflow_tokens(self, job: JobInfo, parent: WorkflowInfo) -> list[str]:
        """The tokens of the parent's held, unsuperseded subs."""
        return [
            sid
            for sid in parent.sub_workflow_tokens
            if self._is_active_sub_workflow(job.sub_workflows.get(sid))
        ]

    @staticmethod
    def _is_active_sub_workflow(sub_workflow: SubWorkflowInfo | None) -> bool:
        """The sub is held and not superseded."""
        return sub_workflow is not None and not sub_workflow.superseded

    @staticmethod
    def _every_sub_workflow_answered(job: JobInfo, sub_workflow_tokens: list[str]) -> bool:
        """Every one of the subs is held and has a result."""
        return all(
            job.sub_workflows.get(sid)
            and job.sub_workflows[sid].result is not None
            for sid in sub_workflow_tokens
        )

    async def aggregate_parent_workflow_outcome(
        self,
        sub_workflow_token: str,
    ) -> tuple[str, str | None, list[dict]] | None:
        """Compute the parent's aggregate terminal state across sub-workflows.

        A parent workflow's terminal status is *not* the status of whichever
        sub-workflow happened to be recorded last — that's a race when a
        worker dies mid-execution and one sub-workflow comes back
        ``CANCELLED`` while another comes back ``COMPLETED``. The right
        answer is to look at every sub-workflow's stored result and reduce:

          * any sub-workflow ``COMPLETED`` => the parent COMPLETED (the
            cluster delivered usable results)
          * else if any sub-workflow ``FAILED`` => the parent FAILED with
            that error
          * else (everything CANCELLED) => the parent CANCELLED with the
            first cancellation reason

        Returns ``(status, error, aggregated_results)`` or ``None`` if the
        parent cannot be located. ``aggregated_results`` concatenates the
        ``results`` payloads from every sub-workflow that produced one so
        the caller can forward a single combined push.
        """
        if (located := self._locate_sub_workflow(str(sub_workflow_token))) is None:
            return None
        job, sub_wf = located

        async with job.lock:
            parent = job.workflows.get(str(sub_wf.parent_token))
            if self._parent_is_unknown_or_decided(parent):
                return None

            outcome_sub_workflows = self._answered_active_sub_workflows(job, parent)
            aggregated_results = self._concatenated_sub_workflow_results(outcome_sub_workflows)
            terminal_status, terminal_error = self._reduce_sub_workflow_outcomes(
                outcome_sub_workflows
            )
            parent.terminal_pushed = True
            parent.terminal_status = terminal_status
            return terminal_status, terminal_error, aggregated_results

    def _locate_sub_workflow(
        self,
        token_str: str,
    ) -> tuple[JobInfo, SubWorkflowInfo] | None:
        """The job holding the sub-workflow and the sub-workflow, or None
        when either is not held."""
        job = self.get_job_for_sub_workflow(token_str)
        if not job:
            return None

        sub_wf = job.sub_workflows.get(token_str)
        if not sub_wf:
            return None
        return job, sub_wf

    @staticmethod
    def _parent_is_unknown_or_decided(parent: WorkflowInfo | None) -> bool:
        """The parent is not held, or its terminal outcome was pushed."""
        return not parent or parent.terminal_pushed

    def _answered_active_sub_workflows(
        self,
        job: JobInfo,
        parent: WorkflowInfo,
    ) -> list[SubWorkflowInfo]:
        """The parent's held, unsuperseded subs that have a result, in the
        parent's order."""
        return [
            sub_workflow
            for sub_workflow_token in parent.sub_workflow_tokens
            if self._is_answered_active_sub_workflow(
                sub_workflow := job.sub_workflows.get(sub_workflow_token)
            )
        ]

    @staticmethod
    def _is_answered_active_sub_workflow(sub_workflow: SubWorkflowInfo | None) -> bool:
        """The sub is held, not superseded, and has a result."""
        return (
            sub_workflow is not None
            and not sub_workflow.superseded
            and sub_workflow.result is not None
        )

    @staticmethod
    def _concatenated_sub_workflow_results(
        sub_workflows: list[SubWorkflowInfo],
    ) -> list[dict]:
        """Every sub's ``results`` payload, concatenated in order."""
        aggregated_results: list[dict] = []
        for sub_workflow in sub_workflows:
            if sub_workflow.result.results:
                aggregated_results.extend(sub_workflow.result.results)
        return aggregated_results

    def _reduce_sub_workflow_outcomes(
        self,
        sub_workflows: list[SubWorkflowInfo],
    ) -> tuple[str, str | None]:
        """The parent's terminal status and error: COMPLETED when any sub
        completed; else FAILED, with the first failed sub's error, when any
        failed; else CANCELLED, with the first cancellation reason."""
        if self._any_sub_workflow_reports(sub_workflows, WorkflowStatus.COMPLETED.value):
            return WorkflowStatus.COMPLETED.value, None
        if self._any_sub_workflow_reports(sub_workflows, WorkflowStatus.FAILED.value):
            return WorkflowStatus.FAILED.value, self._first_error(
                self._failed_sub_workflow_errors(sub_workflows)
            )
        return WorkflowStatus.CANCELLED.value, self._first_cancellation_reason(sub_workflows)

    @staticmethod
    def _any_sub_workflow_reports(sub_workflows: list[SubWorkflowInfo], status: str) -> bool:
        """Some sub's result reports the status."""
        return any(sub_workflow.result.status == status for sub_workflow in sub_workflows)

    @staticmethod
    def _failed_sub_workflow_errors(sub_workflows: list[SubWorkflowInfo]) -> list[str | None]:
        """The errors of the failed subs' results, in order."""
        return [
            sub_workflow.result.error
            for sub_workflow in sub_workflows
            if sub_workflow.result.status == WorkflowStatus.FAILED.value
        ]

    @staticmethod
    def _first_error(errors: list[str | None]) -> str | None:
        """The first error that is not None, if any."""
        return next((error for error in errors if error is not None), None)

    @staticmethod
    def _first_cancellation_reason(sub_workflows: list[SubWorkflowInfo]) -> str | None:
        """The first non-empty error among the subs' results, if any."""
        return next(
            (
                sub_workflow.result.error
                for sub_workflow in sub_workflows
                if sub_workflow.result.error
            ),
            None,
        )

    async def get_parent_ready_result(
        self,
        job_id: str,
        workflow_id: str,
    ) -> WorkflowFinalResult | None:
        """Return an existing active result when a parent is complete."""
        job = self.get_job_by_id(job_id)
        if not job:
            return None

        workflow_token = self.create_workflow_token(job_id, workflow_id)
        async with job.lock:
            parent = job.workflows.get(str(workflow_token))
            if self._parent_is_unknown_or_decided(parent):
                return None

            return self._shared_ready_result(self._active_sub_workflows(job, parent))

    def _active_sub_workflows(
        self,
        job: JobInfo,
        parent: WorkflowInfo,
    ) -> list[SubWorkflowInfo]:
        """The parent's held, unsuperseded subs, in the parent's order."""
        return [
            sub_workflow
            for sid in parent.sub_workflow_tokens
            if self._is_active_sub_workflow(sub_workflow := job.sub_workflows.get(sid))
        ]

    def _shared_ready_result(
        self,
        active_sub_workflows: list[SubWorkflowInfo],
    ) -> WorkflowFinalResult | None:
        """The first active sub's result, once every active sub has one."""
        if not active_sub_workflows:
            return None
        if not self._every_active_sub_workflow_answered(active_sub_workflows):
            return None
        return active_sub_workflows[0].result

    @staticmethod
    def _every_active_sub_workflow_answered(
        active_sub_workflows: list[SubWorkflowInfo],
    ) -> bool:
        """No active sub is still without a result."""
        return not any(sub_workflow.result is None for sub_workflow in active_sub_workflows)

    async def has_active_sub_workflows(
        self,
        job_id: str,
        workflow_id: str,
    ) -> bool:
        """Return whether the parent still has non-superseded sub-workflows."""
        job = self.get_job_by_id(job_id)
        if not job:
            return False

        workflow_token = self.create_workflow_token(job_id, workflow_id)
        async with job.lock:
            parent = job.workflows.get(str(workflow_token))
            if self._parent_is_unknown_or_decided(parent):
                return False

            return self._holds_active_sub_workflow(job, parent)

    def _holds_active_sub_workflow(self, job: JobInfo, parent: WorkflowInfo) -> bool:
        """Some sub of the parent is held and not superseded."""
        return any(
            self._is_active_sub_workflow(job.sub_workflows.get(sid))
            for sid in parent.sub_workflow_tokens
        )

    # =========================================================================
    # Workflow Completion
    # =========================================================================

    async def mark_workflow_completed(
        self,
        workflow_token: str | TrackingToken,
        from_worker: bool = True,
    ) -> bool:
        """
        Mark a workflow as completed based on worker status.

        This is separate from aggregation - a workflow can be "completed"
        (all workers finished) but aggregation may still fail.

        Thread-safe: acquires job lock.
        Notifies on_workflow_completed callback for event-driven dispatch.
        """
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        workflow_id = ""
        completion_transitions: list[StateTransition] = []

        async with job.lock:
            wf = job.workflows.get(token_str)
            if not wf:
                return False

            # Use workflow_id (e.g., "wf-0001") not name - dependencies are tracked by ID
            workflow_id = self._workflow_id_of_token(wf.token)
            should_notify = self._complete_workflow(job, wf, workflow_id, completion_transitions)

        await self.workflow_lifecycle.publish_transitions(completion_transitions)

        # Notify callback outside lock to avoid deadlocks
        await self._announce_workflow_terminal(should_notify, job.job_id, workflow_id)

        return True

    def _complete_workflow(
        self,
        job: JobInfo,
        wf: WorkflowInfo,
        workflow_id: str,
        completion_transitions: list[StateTransition],
    ) -> bool:
        """Under the job lock: complete a workflow not yet finished,
        collecting the transitions made. A finished workflow stays
        finished: a cancelled one is not completed by a result that raced
        its cancellation. True when the completion was accepted (the
        workflow's completion is to be announced)."""
        if wf.status in FINISHED_WORKFLOW_STATUSES:
            return False
        return self._complete_unfinished_workflow(job, wf, workflow_id, completion_transitions)

    def _complete_unfinished_workflow(
        self,
        job: JobInfo,
        wf: WorkflowInfo,
        workflow_id: str,
        completion_transitions: list[StateTransition],
    ) -> bool:
        """A result proves the workflow ran, even one that finished before
        its first progress report: an ASSIGNED workflow turns RUNNING
        first. Results merged from more than one sub-workflow aggregate."""
        if wf.status == WorkflowStatus.ASSIGNED:
            completion_transitions.append(
                self.workflow_lifecycle.apply_transition(
                    job.job_id, workflow_id, WorkflowState.RUNNING, "result reported"
                )
            )
        completion_transitions.append(
            completion_transition := self.workflow_lifecycle.apply_transition(
                job.job_id,
                workflow_id,
                self._completion_state(job, wf),
                "completed",
            )
        )
        if not completion_transition.accepted:
            return False

        wf.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[completion_transition.to_state]
        wf.completion_event.set()

        # Update job progress
        job.workflows_completed += 1
        return True

    def _completion_state(self, job: JobInfo, wf: WorkflowInfo) -> WorkflowState:
        """AGGREGATED when more than one active sub answered, else
        COMPLETED."""
        return (
            WorkflowState.AGGREGATED
            if self._answered_active_sub_workflow_count(job, wf) > 1
            else WorkflowState.COMPLETED
        )

    def _answered_active_sub_workflow_count(self, job: JobInfo, wf: WorkflowInfo) -> int:
        """How many of the workflow's held, unsuperseded subs answered."""
        return sum(
            1
            for sub_workflow_token in wf.sub_workflow_tokens
            if self._is_answered_active_sub_workflow(job.sub_workflows.get(sub_workflow_token))
        )

    async def _announce_workflow_terminal(
        self,
        should_notify: bool,
        job_id: str,
        workflow_id: str,
    ) -> None:
        """Await the workflow-completion callback, when the workflow's
        terminal is to be announced and a callback is set."""
        if should_notify and self._on_workflow_completed:
            await self._on_workflow_completed(job_id, workflow_id)

    async def _announce_accepted_terminal(
        self,
        terminal_transition: StateTransition,
        job_id: str,
        workflow_id: str,
    ) -> bool:
        """Announce the workflow's terminal once its transition was
        accepted (failed workflows still trigger dispatch -- dependent
        workflows may need to fail). False when it was not."""
        if not terminal_transition.accepted:
            return False

        # Notify callback outside lock to avoid deadlocks
        if self._on_workflow_completed:
            await self._on_workflow_completed(job_id, workflow_id)
        return True

    def _unfinished_workflow(self, job: JobInfo, token_str: str) -> WorkflowInfo | None:
        """The job's workflow under this token, unless it is not held or
        already finished."""
        wf = job.workflows.get(token_str)
        if not wf:
            return None

        if wf.status in FINISHED_WORKFLOW_STATUSES:
            return None
        return wf

    async def mark_workflow_failed(
        self,
        workflow_token: str | TrackingToken,
        error: str,
    ) -> bool:
        """
        Mark a workflow as failed.

        Thread-safe: acquires job lock.
        Notifies on_workflow_completed callback for event-driven dispatch.
        """
        token_str = str(workflow_token)
        await self._log_marked_failed(token_str, error)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        async with job.lock:
            if (wf := self._unfinished_workflow(job, token_str)) is None:
                return False

            # Use workflow_id (e.g., "wf-0001") not name - dependencies are tracked by ID
            workflow_id = self._workflow_id_of_token(wf.token)
            failure_transition = self._fail_unfinished_workflow(job, wf, workflow_id, error)

        await self.workflow_lifecycle.publish_transitions([failure_transition])
        return await self._announce_accepted_terminal(failure_transition, job.job_id, workflow_id)

    async def _log_marked_failed(self, token_str: str, error: str) -> None:
        """Log a workflow being marked failed, with the five calls that led
        to ``mark_workflow_failed`` (this frame's caller)."""
        if self._logger is not None:
            await self._logger.log(
                JobManagerError(
                    message=(
                        f"[MARK-FAILED] token={token_str} error={error!r} "
                        f"stack={'/'.join(map(attrgetter('name'), _tb.extract_stack()[-7:-2]))}"
                    ),
                    manager_id=self._manager_id,
                    datacenter=self._datacenter,
                )
            )

    def _fail_unfinished_workflow(
        self,
        job: JobInfo,
        wf: WorkflowInfo,
        workflow_id: str,
        error: str,
    ) -> StateTransition:
        """Fail the workflow with the error as its reason; an accepted
        failure counts toward the job's failed workflows."""
        failure_transition = self.workflow_lifecycle.apply_transition(
            job.job_id, workflow_id, WorkflowState.FAILED, error
        )
        if failure_transition.accepted:
            wf.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[failure_transition.to_state]
            wf.error = error
            wf.completion_event.set()

            # Update job progress
            job.workflows_failed += 1
        return failure_transition

    async def cancel_workflows(
        self,
        job_id: str,
        workflow_ids: set[str] | None,
        reason: str,
    ) -> tuple[list[str], list[str]]:
        """
        Begin cancelling the job's unfinished workflows -- those in
        ``workflow_ids``, or all of them when it is None (AD-20, AD-54).

        A PENDING workflow is cancelled at once, nothing runs it: PENDING ->
        CANCELLING -> CANCELLED. A DISPATCHED or RUNNING one turns
        CANCELLING until its workers confirm they stopped it
        (``finish_workflow_cancellation``); its late progress and results
        change nothing meanwhile. A workflow cancelled while its job goes
        on (the job itself is not cancelled) is one the job did not
        complete: it counts toward the job's failed workflows, with the
        reason as its error.

        Returns ``(cancelled, cancelling)``: the workflow ids cancelled now,
        and those awaiting their workers.

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if job is None:
            return [], []

        cancellation_transitions: list[StateTransition] = []
        cancelled_workflow_ids: list[str] = []
        cancelling_workflow_ids: list[str] = []
        async with job.lock:
            self._begin_job_workflow_cancellations(
                job,
                job_id,
                workflow_ids,
                reason,
                cancellation_transitions,
                cancelled_workflow_ids,
                cancelling_workflow_ids,
            )

        await self.workflow_lifecycle.publish_transitions(cancellation_transitions)
        return cancelled_workflow_ids, cancelling_workflow_ids

    def _begin_job_workflow_cancellations(
        self,
        job: JobInfo,
        job_id: str,
        workflow_ids: set[str] | None,
        reason: str,
        cancellation_transitions: list[StateTransition],
        cancelled_workflow_ids: list[str],
        cancelling_workflow_ids: list[str],
    ) -> None:
        """Under the job lock: begin cancelling each selected workflow still
        PENDING, DISPATCHED or RUNNING, in the job's workflow order."""
        job_goes_on = job.status != JobStatus.CANCELLED.value
        for workflow in job.workflows.values():
            workflow_id = self._workflow_id_of_token(workflow.token)
            if (
                state := self._cancellable_workflow_state(job_id, workflow_id, workflow_ids)
            ) is None:
                continue

            self._begin_workflow_cancellation(
                job,
                job_id,
                workflow,
                workflow_id,
                state,
                reason,
                job_goes_on,
                cancellation_transitions,
                cancelled_workflow_ids,
                cancelling_workflow_ids,
            )

    def _cancellable_workflow_state(
        self,
        job_id: str,
        workflow_id: str,
        workflow_ids: set[str] | None,
    ) -> WorkflowState | None:
        """The workflow's state when it is selected and still PENDING,
        DISPATCHED or RUNNING; None otherwise."""
        if self._workflow_not_selected(workflow_id, workflow_ids):
            return None
        if (state := self.workflow_lifecycle.get_state(job_id, workflow_id)) not in (
            WorkflowState.PENDING,
            WorkflowState.DISPATCHED,
            WorkflowState.RUNNING,
        ):
            return None
        return state

    @staticmethod
    def _workflow_not_selected(workflow_id: str, workflow_ids: set[str] | None) -> bool:
        """A selection is given and does not name the workflow."""
        return workflow_ids is not None and workflow_id not in workflow_ids

    def _begin_workflow_cancellation(
        self,
        job: JobInfo,
        job_id: str,
        workflow: WorkflowInfo,
        workflow_id: str,
        state: WorkflowState,
        reason: str,
        job_goes_on: bool,
        cancellation_transitions: list[StateTransition],
        cancelled_workflow_ids: list[str],
        cancelling_workflow_ids: list[str],
    ) -> None:
        """Turn the workflow CANCELLING; a DISPATCHED or RUNNING one then
        awaits its workers, a PENDING one is cancelled at once."""
        cancellation_transitions.append(
            cancelling_transition := self.workflow_lifecycle.apply_transition(
                job_id, workflow_id, WorkflowState.CANCELLING, reason
            )
        )
        if not cancelling_transition.accepted:
            return
        if state != WorkflowState.PENDING:
            workflow.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[cancelling_transition.to_state]
            cancelling_workflow_ids.append(workflow_id)
            return

        cancellation_transitions.append(
            cancelled_transition := self.workflow_lifecycle.apply_transition(
                job_id, workflow_id, WorkflowState.CANCELLED, "nothing ran it"
            )
        )
        workflow.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[cancelled_transition.to_state]
        workflow.completion_event.set()
        self._count_cancellation_while_job_goes_on(job, workflow, job_goes_on, reason)
        cancelled_workflow_ids.append(workflow_id)

    @staticmethod
    def _count_cancellation_while_job_goes_on(
        job: JobInfo,
        workflow: WorkflowInfo,
        job_goes_on: bool,
        reason: str,
    ) -> None:
        """A workflow cancelled while its job goes on is one the job did not
        complete: it counts toward the job's failed workflows, with the
        cancellation's reason as its error."""
        if job_goes_on:
            workflow.error = f"cancelled: {reason}"
            job.workflows_failed += 1

    async def finish_workflow_cancellation(self, job_id: str, workflow_id: str) -> bool:
        """
        A CANCELLING workflow's workers stopped it: CANCELLING -> CANCELLED.
        While its job goes on it counts toward the job's failed workflows,
        with its cancellation's reason as its error. False when it was not
        CANCELLING.

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if job is None:
            return False

        async with job.lock:
            workflow = self._workflow_with_id(job, workflow_id)
            if (record := self._cancelling_record(job_id, workflow_id, workflow)) is None:
                return False

            cancellation_reason = record.history[-1].reason
            cancelled_transition = self.workflow_lifecycle.apply_transition(
                job_id, workflow_id, WorkflowState.CANCELLED, "its workers stopped it"
            )
            workflow.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[cancelled_transition.to_state]
            workflow.completion_event.set()
            self._count_cancellation_while_job_goes_on(
                job,
                workflow,
                job.status != JobStatus.CANCELLED.value,
                cancellation_reason,
            )

        await self.workflow_lifecycle.publish_transitions([cancelled_transition])
        return cancelled_transition.accepted

    @staticmethod
    def _workflow_with_id(job: JobInfo, workflow_id: str) -> WorkflowInfo | None:
        """The first of the job's workflows with this workflow id."""
        return next(
            (
                workflow_info
                for workflow_info in job.workflows.values()
                if workflow_info.token.workflow_id == workflow_id
            ),
            None,
        )

    def _cancelling_record(
        self,
        job_id: str,
        workflow_id: str,
        workflow: WorkflowInfo | None,
    ) -> WorkflowLifecycleRecord | None:
        """The held workflow's lifecycle record while it is CANCELLING."""
        if workflow is None:
            return None
        return self._record_if_cancelling(self.workflow_lifecycle.get_record(job_id, workflow_id))

    @staticmethod
    def _record_if_cancelling(
        record: WorkflowLifecycleRecord | None,
    ) -> WorkflowLifecycleRecord | None:
        """The record, when there is one and it is CANCELLING."""
        return record if record is not None and record.state == WorkflowState.CANCELLING else None

    async def fail_workflow_dependents(
        self,
        job_id: str,
        workflow_id: str,
        error: str,
    ) -> list[str]:
        """
        Fail every workflow still waiting, directly or transitively, on
        ``workflow_id`` -- which failed for good, so none of them can ever
        run -- in one sweep under the job lock, each counted toward the
        job's failures. Returns the ids of the dependents it failed.

        Dependents wait on their dependencies' completion, so only PENDING
        ones are failed; the sweep walks through finished ones to reach
        theirs. Their completion is not announced: the caller drops them
        from dispatch together with the failed workflow, which is what the
        announcement would have done.

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if job is None:
            return []

        cascade_transitions: list[StateTransition] = []
        failed_dependent_ids: list[str] = []
        async with job.lock:
            self._fail_pending_dependents(
                job, job_id, workflow_id, error, cascade_transitions, failed_dependent_ids
            )

        await self.workflow_lifecycle.publish_transitions(cascade_transitions)
        return failed_dependent_ids

    def _fail_pending_dependents(
        self,
        job: JobInfo,
        job_id: str,
        workflow_id: str,
        error: str,
        cascade_transitions: list[StateTransition],
        failed_dependent_ids: list[str],
    ) -> None:
        """Under the job lock: walk the workflow's dependents breadth-first,
        failing each PENDING one as it is reached."""
        dependents_by_dependency = self._dependents_by_dependency(job)
        for dependent in self._dependents_reached_from(dependents_by_dependency, workflow_id):
            if dependent.status != WorkflowStatus.PENDING:
                continue

            self._fail_cascaded_dependent(
                job, job_id, dependent, error, cascade_transitions, failed_dependent_ids
            )

    @staticmethod
    def _dependents_by_dependency(job: JobInfo) -> dict[str, list[WorkflowInfo]]:
        """Each workflow id the job's workflows depend on, with the
        workflows that depend on it, in job order."""
        dependents_by_dependency: dict[str, list[WorkflowInfo]] = {}
        for workflow in job.workflows.values():
            for dependency_workflow_id in workflow.dependency_workflow_ids:
                dependents_by_dependency.setdefault(dependency_workflow_id, []).append(workflow)
        return dependents_by_dependency

    def _dependents_reached_from(
        self,
        dependents_by_dependency: dict[str, list[WorkflowInfo]],
        workflow_id: str,
    ) -> Iterator[WorkflowInfo]:
        """Yield each workflow depending, directly or transitively, on
        ``workflow_id``, once, breadth-first -- lazily, so each is yielded
        before the walk goes further."""
        unvisited_dependencies = deque((workflow_id,))
        reached_workflow_ids = {workflow_id}
        while unvisited_dependencies:
            yield from self._newly_reached_dependents(
                dependents_by_dependency.get(unvisited_dependencies.popleft(), ()),
                reached_workflow_ids,
                unvisited_dependencies,
            )

    def _newly_reached_dependents(
        self,
        dependents: list[WorkflowInfo] | tuple[()],
        reached_workflow_ids: set[str],
        unvisited_dependencies: deque[str],
    ) -> Iterator[WorkflowInfo]:
        """Yield each dependent not reached before, marking it reached and
        queueing its own dependents for the walk."""
        for dependent in dependents:
            if (dependent_workflow_id := self._workflow_id_of_token(dependent.token)) in reached_workflow_ids:
                continue
            reached_workflow_ids.add(dependent_workflow_id)
            unvisited_dependencies.append(dependent_workflow_id)
            yield dependent

    def _fail_cascaded_dependent(
        self,
        job: JobInfo,
        job_id: str,
        dependent: WorkflowInfo,
        error: str,
        cascade_transitions: list[StateTransition],
        failed_dependent_ids: list[str],
    ) -> None:
        """Fail a PENDING dependent with the cascade's error; an accepted
        failure counts toward the job's failed workflows."""
        dependent_workflow_id = self._workflow_id_of_token(dependent.token)
        cascade_transitions.append(
            cascade_transition := self.workflow_lifecycle.apply_transition(
                job_id, dependent_workflow_id, WorkflowState.FAILED, error
            )
        )
        if not cascade_transition.accepted:
            return
        dependent.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[cascade_transition.to_state]
        dependent.error = error
        dependent.completion_event.set()
        job.workflows_failed += 1
        failed_dependent_ids.append(dependent_workflow_id)

    async def mark_aggregation_failed(
        self,
        workflow_token: str | TrackingToken,
        error: str,
    ) -> bool:
        """
        Mark workflow aggregation as failed.

        The workers finished but their results could not be aggregated:
        the workflow fails (AD-54 has no aggregation-failed state), with
        the aggregation error kept beside it. A workflow already finished
        stays finished.

        Thread-safe: acquires job lock.
        Notifies on_workflow_completed callback for event-driven dispatch.
        """
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        async with job.lock:
            if (wf := self._unfinished_workflow(job, token_str)) is None:
                return False

            workflow_id = self._workflow_id_of_token(wf.token)
            aggregation_transition = self._fail_workflow_aggregation(job, wf, workflow_id, error)

        await self.workflow_lifecycle.publish_transitions([aggregation_transition])
        return await self._announce_accepted_terminal(
            aggregation_transition, job.job_id, workflow_id
        )

    def _fail_workflow_aggregation(
        self,
        job: JobInfo,
        wf: WorkflowInfo,
        workflow_id: str,
        error: str,
    ) -> StateTransition:
        """Fail the workflow for its aggregation error, kept beside it; an
        accepted failure counts toward the job's failed workflows."""
        aggregation_transition = self.workflow_lifecycle.apply_transition(
            job.job_id,
            workflow_id,
            WorkflowState.FAILED,
            f"aggregation failed: {error}",
        )
        if aggregation_transition.accepted:
            wf.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[aggregation_transition.to_state]
            wf.aggregation_error = error
            wf.completion_event.set()
            job.workflows_failed += 1
        return aggregation_transition

    async def update_workflow_status(
        self,
        job_id: str,
        workflow_token: str | TrackingToken,
        new_status: WorkflowStatus,
        error: str | None = None,
    ) -> bool:
        """
        Update workflow status directly.

        This is a general-purpose status update method that handles the job
        progress counters correctly based on status transitions.

        Thread-safe: acquires job lock.
        Notifies on_workflow_completed callback for event-driven dispatch.

        Args:
            job_id: The job ID
            workflow_token: Workflow token (can be token string or TrackingToken)
            new_status: New WorkflowStatus to set
            error: Optional error message (for FAILED states)

        Returns:
            True if status was updated, False if workflow not found
        """
        job = self.get_job_by_id(job_id)
        if not job:
            return False

        token_str = str(workflow_token)

        async with job.lock:
            wf = job.workflows.get(token_str)
            if not wf:
                return False

            old_status = wf.status
            # Use workflow_id (e.g., "wf-0001") not name - dependencies are tracked by ID
            workflow_id = self._workflow_id_of_token(wf.token)

            installed_state, installed_transition = self._install_decided_workflow_status(
                job_id, workflow_id, new_status
            )
            new_status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[installed_state]

            # Update status
            self._set_decided_workflow_status(wf, new_status, error)

            should_notify = self._count_decided_terminal_status(job, wf, old_status, new_status)

        await self._publish_transition_if_any(installed_transition)

        # Notify callback outside lock to avoid deadlocks
        await self._announce_workflow_terminal(should_notify, job.job_id, workflow_id)

        return True

    def _install_decided_workflow_status(
        self,
        job_id: str,
        workflow_id: str,
        new_status: WorkflowStatus,
    ) -> tuple[WorkflowState, StateTransition | None]:
        """A decided status installs (a status-only decision agrees with any
        state that projects to it); returns the workflow's state and the
        transition installing it, if one was made."""
        status_record = self.workflow_lifecycle.get_record(job_id, workflow_id)
        if self._record_projects_to_status(status_record, new_status):
            return status_record.state, None

        installed_state = WORKFLOW_STATE_BY_WORKFLOW_STATUS[new_status]
        return installed_state, self.workflow_lifecycle.install_state(
            job_id,
            workflow_id,
            installed_state,
            self._retry_generation_of(status_record),
            f"status set to {new_status.value}",
        )

    @staticmethod
    def _record_projects_to_status(
        status_record: WorkflowLifecycleRecord | None,
        status: WorkflowStatus,
    ) -> bool:
        """The record exists and its state projects to the status."""
        return (
            status_record is not None
            and WORKFLOW_STATUS_BY_WORKFLOW_STATE[status_record.state] == status
        )

    @staticmethod
    def _retry_generation_of(status_record: WorkflowLifecycleRecord | None) -> int:
        """The record's retry generation, 0 without a record."""
        return 0 if status_record is None else status_record.retry_generation

    @staticmethod
    def _set_decided_workflow_status(
        wf: WorkflowInfo,
        new_status: WorkflowStatus,
        error: str | None,
    ) -> None:
        """Set the workflow's status, and its error when one is given."""
        wf.status = new_status
        if error:
            wf.error = error

    def _count_decided_terminal_status(
        self,
        job: JobInfo,
        wf: WorkflowInfo,
        old_status: WorkflowStatus,
        new_status: WorkflowStatus,
    ) -> bool:
        """Count a move TO a terminal status toward the job's progress, not
        one from it, and complete the workflow. True when it was counted
        (its terminal is to be announced)."""
        if old_status in COUNTED_TERMINAL_WORKFLOW_STATUSES:
            return False
        if not self._count_terminal_workflow_status(job, new_status):
            return False
        wf.completion_event.set()
        return True

    @staticmethod
    def _count_terminal_workflow_status(job: JobInfo, new_status: WorkflowStatus) -> bool:
        """Count a COMPLETED or FAILED status toward the job's completed or
        failed workflows; False for any other status."""
        if new_status == WorkflowStatus.COMPLETED:
            job.workflows_completed += 1
        elif new_status == WorkflowStatus.FAILED:
            job.workflows_failed += 1
        else:
            return False
        return True

    # =========================================================================
    # Job Status
    # =========================================================================

    def get_job_status(self, job_token: str | TrackingToken) -> str:
        """Get current job status string."""
        job = self.get_job(job_token)
        if not job:
            return JobStatus.UNKNOWN.value

        return job.status

    async def update_job_status(
        self,
        job_token: str | TrackingToken,
        status: str,
        timestamp: float | None = None,
    ) -> bool:
        """
        Update job status.

        Thread-safe: acquires job lock.

        ``timestamp`` is the wall-clock seconds value to record on
        ``job.timestamp``. When called from the Raft apply path, the caller
        passes ``entry.timestamp`` (the HLC-derived value replicated in the log)
        so every follower converges on identical state. When called outside
        the apply path -- e.g. local manager handlers updating their own view --
        the default ``self._clock.time()`` records the current wall-clock seconds.
        Never call ``self._clock.monotonic()`` here: ``job.timestamp`` is a wall-clock
        field, consumed by readers that compare against ``Clock.time()``.
        """
        job = self.get_job(job_token)
        if not job:
            return False

        async with job.lock:
            job.status = status
            job.timestamp = self._clock.time() if timestamp is None else timestamp
            return True

    # =========================================================================
    # Context Management
    # =========================================================================

    def get_context(self, job_token: str | TrackingToken) -> Context | None:
        """Get job context. Returns None if job not found."""
        job = self.get_job(job_token)
        if not job:
            return None
        return job.context

    async def get_layer_version(self, job_id: str) -> int:
        job = self.get_job_by_id(job_id)
        if job is None:
            return 0
        async with job.lock:
            return job.layer_version

    async def increment_layer_version(self, job_id: str) -> int:
        job = self.get_job_by_id(job_id)
        if job is None:
            return 0
        async with job.lock:
            job.layer_version += 1
            return job.layer_version

    async def get_job_context(self, job_id: str) -> dict[str, dict[str, object]]:
        """The job's context by workflow namespace -- what a dispatched
        workflow's run starts from.

        Core's semantics, kept across machines: a ``Provide`` hook writes
        into each workflow namespace it targets, and a ``Use`` hook reads
        the namespaces it names (its own workflow's by default), all from
        one shared context. A run is therefore handed every namespace, not
        a flattening of its dependencies' -- which merged the providers'
        own namespaces into the consumer's and lost what was provided for
        it.
        """
        job = self.get_job_by_id(job_id)
        if job is None:
            return {}

        async with job.lock:
            return job.context.dict()

    async def apply_workflow_context(
        self,
        job_id: str,
        context_updates_bytes: bytes,
    ) -> bool:
        """Merge a finished run's context -- every namespace it holds
        (``Context.dict()``: workflow name -> values) -- into the job's.

        The bytes come off the wire from a worker, so they are unpickled
        under the same restrictions as every other message.
        """
        if (job := self.get_job_by_id(job_id)) is None:
            return False

        context_updates: dict[str, dict[str, object]] = Message.load(context_updates_bytes)

        async with job.lock:
            for workflow_name, values in context_updates.items():
                await self._merge_workflow_context_namespace(job, workflow_name, values)
            job.layer_version += 1
            return True

    @staticmethod
    async def _merge_workflow_context_namespace(
        job: JobInfo,
        workflow_name: str,
        values: dict[str, object],
    ) -> None:
        """Set each of a run's values into the job's namespace of the
        workflow, in order."""
        workflow_context = job.context[workflow_name]
        for key, value in values.items():
            await workflow_context.set(key, value)

    async def set_sub_workflow_dispatched_context(
        self,
        sub_workflow_token: str | TrackingToken,
        context_bytes: bytes,
        layer_version: int,
    ) -> bool:
        token_str = str(sub_workflow_token)
        if (job := self.get_job_for_sub_workflow(token_str)) is None:
            return False

        async with job.lock:
            if sub_wf := job.sub_workflows.get(token_str):
                sub_wf.dispatched_context = context_bytes
                sub_wf.dispatched_version = layer_version
                return True
            return False

    async def get_stored_dispatched_context(
        self,
        job_id: str,
        workflow_id: str,
    ) -> tuple[bytes, int] | None:
        """
        Get stored dispatched context for a workflow (FIX 2.6).

        On requeue after worker failure, we should reuse the original dispatched
        context to maintain consistency rather than recomputing fresh context.

        Returns (context_bytes, layer_version) if found, None otherwise.
        """
        job_token = self.create_job_token(job_id)
        job = self._jobs.get(str(job_token))
        if not job:
            return None

        async with job.lock:
            return self._first_stored_dispatched_context(job, workflow_id)

    def _first_stored_dispatched_context(
        self,
        job: JobInfo,
        workflow_id: str,
    ) -> tuple[bytes, int] | None:
        """The dispatched context and its layer version of the job's first
        sub-workflow of the workflow that stored one."""
        for sub_wf in job.sub_workflows.values():
            if self._sub_workflow_stored_context_of(sub_wf, workflow_id):
                return (sub_wf.dispatched_context, sub_wf.dispatched_version)
        return None

    @staticmethod
    def _sub_workflow_stored_context_of(sub_wf: SubWorkflowInfo, workflow_id: str) -> bool:
        """The sub-workflow runs the workflow and stored a dispatched
        context."""
        return bool(
            sub_wf.parent_token.workflow_id == workflow_id and sub_wf.dispatched_context
        )

    # =========================================================================
    # Iteration Helpers
    # =========================================================================

    @property
    def job_count(self) -> int:
        """Get count of active jobs."""
        return len(self._jobs)

    def get_all_job_ids(self) -> list[str]:
        """Get list of all active job IDs."""
        return [job.job_id for job in self._jobs.values()]

    def iter_jobs(self) -> list[JobInfo]:
        """Get a snapshot of all jobs for iteration."""
        return list(self._jobs.values())

    def get_reassignable_sub_workflows_on_worker(
        self,
        worker_id: str,
        job_id: str | None = None,
    ) -> list[tuple[str, str, str]]:
        """Return unfinished sub-workflows on a worker that can be reassigned
        (of ``job_id`` alone when given): not yet answered, not superseded,
        and of a parent not yet terminal."""
        reassignable: list[tuple[str, str, str]] = []
        for job in self._jobs_to_scan_for_reassignment(job_id):
            reassignable.extend(self._reassignable_sub_workflows_of_job(job, worker_id))

        return reassignable

    def _jobs_to_scan_for_reassignment(self, job_id: str | None) -> list[JobInfo]:
        """Every held job when no job is named; else the named job, when it
        is held."""
        if job_id is None:
            return list(self._jobs.values())
        if (job := self.get_job_by_id(job_id)) is not None:
            return [job]
        return []

    def _reassignable_sub_workflows_of_job(
        self,
        job: JobInfo,
        worker_id: str,
    ) -> list[tuple[str, str, str]]:
        """The job's reassignable sub-workflows on the worker, as
        ``(job_id, workflow_id, sub_workflow_token)``."""
        return [
            reassignment
            for sub_workflow in list(job.sub_workflows.values())
            if (reassignment := self._sub_workflow_reassignment(job, sub_workflow, worker_id))
            is not None
        ]

    def _sub_workflow_reassignment(
        self,
        job: JobInfo,
        sub_workflow: SubWorkflowInfo,
        worker_id: str,
    ) -> tuple[str, str, str] | None:
        """The sub-workflow's reassignment entry, or None when it is not on
        the worker, is answered or superseded, or its parent is not held or
        already terminal."""
        if self._sub_workflow_is_settled_or_elsewhere(sub_workflow, worker_id):
            return None

        parent = job.workflows.get(str(sub_workflow.parent_token))
        if self._parent_rules_out_reassignment(parent):
            return None

        return (
            job.job_id,
            self._reassigned_workflow_id(parent, sub_workflow),
            sub_workflow.token_str,
        )

    @staticmethod
    def _sub_workflow_is_settled_or_elsewhere(
        sub_workflow: SubWorkflowInfo,
        worker_id: str,
    ) -> bool:
        """The sub runs on another worker, is answered, or was superseded."""
        return (
            sub_workflow.worker_id != worker_id
            or sub_workflow.result is not None
            or sub_workflow.superseded
        )

    @staticmethod
    def _parent_rules_out_reassignment(parent: WorkflowInfo | None) -> bool:
        """The parent is not held, pushed its terminal, or is terminal."""
        return (
            parent is None
            or parent.terminal_pushed
            or parent.status in UNREASSIGNABLE_WORKFLOW_STATUSES
        )

    @staticmethod
    def _reassigned_workflow_id(parent: WorkflowInfo, sub_workflow: SubWorkflowInfo) -> str:
        """The parent's workflow id, else the sub's, else ""."""
        return (
            parent.token.workflow_id
            or sub_workflow.token.workflow_id
            or ""
        )

