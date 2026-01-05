"""
Job-related models for internal manager tracking.

These models are used by the manager's job tracking system for
internal state management. They are not wire protocol messages.

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
"""

import asyncio
import time
from dataclasses import dataclass, field

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority
from hyperscale.core.state.context import Context
from hyperscale.distributed_rewrite.models.distributed import (
    JobProgress,
    JobStatus,
    JobSubmission,
    WorkflowProgress,
    WorkflowFinalResult,
    WorkflowStatus,
)


def _create_event() -> asyncio.Event:
    """Factory for creating asyncio.Event in dataclass field."""
    return asyncio.Event()


@dataclass(frozen=True)
class TrackingToken:
    """
    Globally unique tracking token for jobs, workflows, and sub-workflows.

    Format: <datacenter>:<manager_id>:<job_id>:<workflow_id>:<worker_id>

    The token is hierarchical - each level includes all parent components:
    - Job:         datacenter:manager_id:job_id
    - Workflow:    datacenter:manager_id:job_id:workflow_id
    - Sub-workflow: datacenter:manager_id:job_id:workflow_id:worker_id
    """
    datacenter: str
    manager_id: str
    job_id: str
    workflow_id: str | None = None
    worker_id: str | None = None

    @classmethod
    def for_job(cls, datacenter: str, manager_id: str, job_id: str) -> "TrackingToken":
        """Create a job-level token."""
        return cls(datacenter=datacenter, manager_id=manager_id, job_id=job_id)

    @classmethod
    def for_workflow(
        cls,
        datacenter: str,
        manager_id: str,
        job_id: str,
        workflow_id: str,
    ) -> "TrackingToken":
        """Create a workflow-level token."""
        return cls(
            datacenter=datacenter,
            manager_id=manager_id,
            job_id=job_id,
            workflow_id=workflow_id,
        )

    @classmethod
    def for_sub_workflow(
        cls,
        datacenter: str,
        manager_id: str,
        job_id: str,
        workflow_id: str,
        worker_id: str,
    ) -> "TrackingToken":
        """Create a sub-workflow token (dispatched to specific worker)."""
        return cls(
            datacenter=datacenter,
            manager_id=manager_id,
            job_id=job_id,
            workflow_id=workflow_id,
            worker_id=worker_id,
        )

    @classmethod
    def parse(cls, token_str: str) -> "TrackingToken":
        """
        Parse a token string back into a TrackingToken.

        Raises ValueError if the format is invalid.
        """
        parts = token_str.split(":")
        if len(parts) < 3:
            raise ValueError(f"Invalid token format (need at least 3 parts): {token_str}")

        datacenter = parts[0]
        manager_id = parts[1]
        job_id = parts[2]
        workflow_id = parts[3] if len(parts) > 3 else None
        worker_id = parts[4] if len(parts) > 4 else None

        return cls(
            datacenter=datacenter,
            manager_id=manager_id,
            job_id=job_id,
            workflow_id=workflow_id,
            worker_id=worker_id,
        )

    def __str__(self) -> str:
        """Convert to string format."""
        if self.worker_id:
            return f"{self.datacenter}:{self.manager_id}:{self.job_id}:{self.workflow_id}:{self.worker_id}"
        elif self.workflow_id:
            return f"{self.datacenter}:{self.manager_id}:{self.job_id}:{self.workflow_id}"
        else:
            return f"{self.datacenter}:{self.manager_id}:{self.job_id}"

    @property
    def job_token(self) -> str:
        """Get the job-level token string."""
        return f"{self.datacenter}:{self.manager_id}:{self.job_id}"

    @property
    def workflow_token(self) -> str | None:
        """Get the workflow-level token string, or None if this is a job token."""
        if not self.workflow_id:
            return None
        return f"{self.datacenter}:{self.manager_id}:{self.job_id}:{self.workflow_id}"

    @property
    def is_job_token(self) -> bool:
        """True if this is a job-level token."""
        return self.workflow_id is None

    @property
    def is_workflow_token(self) -> bool:
        """True if this is a workflow-level token (not sub-workflow)."""
        return self.workflow_id is not None and self.worker_id is None

    @property
    def is_sub_workflow_token(self) -> bool:
        """True if this is a sub-workflow token."""
        return self.worker_id is not None

    def to_workflow_token(self, workflow_id: str) -> "TrackingToken":
        """Create a workflow token from this job token."""
        return TrackingToken(
            datacenter=self.datacenter,
            manager_id=self.manager_id,
            job_id=self.job_id,
            workflow_id=workflow_id,
        )

    def to_sub_workflow_token(self, worker_id: str) -> "TrackingToken":
        """Create a sub-workflow token from this workflow token."""
        if not self.workflow_id:
            raise ValueError("Cannot create sub-workflow token from job token")
        return TrackingToken(
            datacenter=self.datacenter,
            manager_id=self.manager_id,
            job_id=self.job_id,
            workflow_id=self.workflow_id,
            worker_id=worker_id,
        )

    def to_parent_workflow_token(self) -> "TrackingToken":
        """Get the parent workflow token from a sub-workflow token."""
        if not self.is_sub_workflow_token:
            raise ValueError("Not a sub-workflow token")
        return TrackingToken(
            datacenter=self.datacenter,
            manager_id=self.manager_id,
            job_id=self.job_id,
            workflow_id=self.workflow_id,
        )


@dataclass
class WorkflowInfo:
    """Information about a workflow within a job."""
    token: TrackingToken          # Full tracking token (DC:manager:job:workflow)
    name: str
    workflow: Workflow | None = None
    status: WorkflowStatus = WorkflowStatus.PENDING
    sub_workflow_tokens: list[str] = field(default_factory=list)  # Sub-workflow token strings
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    error: str | None = None
    aggregation_error: str | None = None  # Separate from workflow error

    @property
    def token_str(self) -> str:
        """Get token as string."""
        return str(self.token)


@dataclass
class SubWorkflowInfo:
    """Information about a sub-workflow dispatched to a specific worker."""
    token: TrackingToken          # Full tracking token (DC:manager:job:workflow:worker)
    parent_token: TrackingToken   # Parent workflow token
    cores_allocated: int
    progress: WorkflowProgress | None = None
    result: WorkflowFinalResult | None = None

    @property
    def token_str(self) -> str:
        """Get token as string."""
        return str(self.token)

    @property
    def worker_id(self) -> str:
        """Get worker ID from token."""
        return self.token.worker_id or ""


@dataclass
class JobInfo:
    """All state for a single job, protected by its own lock."""
    token: TrackingToken          # Job-level token (DC:manager:job)
    submission: JobSubmission | None  # None for remote jobs tracked by non-leaders
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # Internal progress tracking (separate from wire protocol JobProgress)
    status: str = JobStatus.QUEUED.value
    workflows_total: int = 0
    workflows_completed: int = 0
    workflows_failed: int = 0
    started_at: float = 0.0       # time.monotonic() when job started
    timestamp: float = 0.0        # Last update time

    # Workflow tracking - keyed by token string for fast lookup
    workflows: dict[str, WorkflowInfo] = field(default_factory=dict)  # workflow_token_str -> info
    sub_workflows: dict[str, SubWorkflowInfo] = field(default_factory=dict)  # sub_workflow_token_str -> info

    # Context for dependent workflows
    context: Context = field(default_factory=Context)
    layer_version: int = 0

    # Job leadership (for multi-manager setups)
    leader_node_id: str | None = None
    leader_addr: tuple[str, int] | None = None
    fencing_token: int = 0

    # Callbacks
    callback_addr: tuple[str, int] | None = None

    @property
    def job_id(self) -> str:
        """Get job_id from token."""
        return self.token.job_id

    @property
    def datacenter(self) -> str:
        """Get datacenter from token."""
        return self.token.datacenter

    def elapsed_seconds(self) -> float:
        """Calculate elapsed time since job started."""
        if self.started_at == 0.0:
            return 0.0
        return time.monotonic() - self.started_at

    def to_wire_progress(self) -> JobProgress:
        """
        Convert internal JobInfo to wire protocol JobProgress.

        Used for state sync between managers and progress reporting to gates.
        """
        # Convert internal workflow state to wire protocol WorkflowProgress
        workflow_progresses = []
        for wf_token_str, wf_info in self.workflows.items():
            wf_progress = WorkflowProgress(
                job_id=self.job_id,
                workflow_id=wf_info.token.workflow_id or "",
                workflow_name=wf_info.name,
                status=wf_info.status.value,
                completed_count=0,  # TODO: aggregate from sub-workflows
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=self.elapsed_seconds(),
                timestamp=self.timestamp,
            )
            workflow_progresses.append(wf_progress)

        return JobProgress(
            job_id=self.job_id,
            datacenter=self.datacenter,
            status=self.status,
            workflows=workflow_progresses,
            total_completed=self.workflows_completed,
            total_failed=self.workflows_failed,
            overall_rate=0.0,
            elapsed_seconds=self.elapsed_seconds(),
            timestamp=self.timestamp,
        )


@dataclass
class PendingWorkflow:
    """
    A workflow waiting to be dispatched.

    Used by WorkflowDispatcher to track workflows that are registered
    but not yet dispatched to workers. Tracks dependency completion,
    dispatch state, timeout for eviction, and retry state.

    Event-driven dispatch:
    - ready_event: Set when dependencies are satisfied AND workflow is ready for dispatch
    - Dispatch loop waits on ready_event instead of polling
    """
    job_id: str
    workflow_id: str
    workflow_name: str
    workflow: Workflow
    vus: int
    priority: StagePriority
    is_test: bool
    dependencies: set[str]           # workflow_ids this depends on
    completed_dependencies: set[str] = field(default_factory=set)
    dispatched: bool = False
    cores_allocated: int = 0

    # Event-driven dispatch: set when dependencies satisfied and ready for dispatch attempt
    ready_event: asyncio.Event = field(default_factory=_create_event)

    # Timeout tracking
    registered_at: float = 0.0           # time.monotonic() when registered
    dispatched_at: float = 0.0           # time.monotonic() when dispatched
    timeout_seconds: float = 300.0       # Max seconds before eviction

    # Dispatch attempt tracking (for the dispatch flag race fix)
    dispatch_in_progress: bool = False   # True while async dispatch is in progress

    # Retry tracking with exponential backoff
    dispatch_attempts: int = 0           # Number of dispatch attempts
    last_dispatch_attempt: float = 0.0   # time.monotonic() of last attempt
    next_retry_delay: float = 1.0        # Seconds until next retry allowed
    max_dispatch_attempts: int = 5       # Max retries before marking failed

    def check_and_signal_ready(self) -> bool:
        """
        Check if workflow is ready for dispatch and signal if so.

        A workflow is ready when:
        - All dependencies are satisfied
        - Not already dispatched
        - Not currently being dispatched
        - Haven't exceeded max retries

        Returns True if workflow is ready (and signals the event).
        """
        if self.dispatched:
            return False
        if self.dispatch_in_progress:
            return False
        if self.dispatch_attempts >= self.max_dispatch_attempts:
            return False
        if not (self.dependencies <= self.completed_dependencies):
            return False

        # Ready - signal the event
        self.ready_event.set()
        return True

    def clear_ready(self) -> None:
        """Clear the ready event (called when dispatch starts or fails)."""
        self.ready_event.clear()
