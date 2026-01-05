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
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine

import cloudpickle

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.state.context import Context
from hyperscale.distributed_rewrite.models import (
    JobProgress,
    JobStatus,
    JobSubmission,
    WorkflowProgress,
    WorkflowFinalResult,
)


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


class WorkflowState(Enum):
    """State of a workflow in the job manager."""
    PENDING = "pending"           # Registered but not yet dispatched
    DISPATCHED = "dispatched"     # Dispatched to worker(s)
    RUNNING = "running"           # At least one worker reports running
    COMPLETED = "completed"       # Worker(s) reported success
    FAILED = "failed"             # Worker(s) reported failure
    AGGREGATED = "aggregated"     # Results successfully aggregated
    AGGREGATION_FAILED = "aggregation_failed"  # Aggregation failed (manager error)


@dataclass
class WorkflowInfo:
    """Information about a workflow within a job."""
    token: TrackingToken          # Full tracking token (DC:manager:job:workflow)
    name: str
    workflow: Workflow | None = None
    state: WorkflowState = WorkflowState.PENDING
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
    submission: JobSubmission
    progress: JobProgress
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

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


class JobManager:
    """
    Thread-safe job and workflow state management.

    Uses per-job locks to ensure race-condition safe access to job state.
    All public methods that modify state should be called with the job lock held,
    or use the provided async context manager methods.

    All tracking uses TrackingToken format:
        <datacenter>:<manager_id>:<job_id>:<workflow_id>:<worker_id>
    """

    def __init__(self, datacenter: str, manager_id: str):
        """
        Initialize JobManager.

        Args:
            datacenter: Datacenter identifier (e.g., "DC-EAST")
            manager_id: This manager's node ID (short form)
        """
        self._datacenter = datacenter
        self._manager_id = manager_id

        # Main job storage - job token string -> JobInfo
        self._jobs: dict[str, JobInfo] = {}

        # Quick lookup for workflow/sub-workflow -> job token mapping
        self._workflow_to_job: dict[str, str] = {}  # workflow_token_str -> job_token_str
        self._sub_workflow_to_job: dict[str, str] = {}  # sub_workflow_token_str -> job_token_str

        # Global lock for job creation/deletion (not per-job operations)
        self._global_lock = asyncio.Lock()

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

            progress = JobProgress(
                job_id=submission.job_id,
                status=JobStatus.QUEUED.value,
                workflows_total=0,
                workflows_completed=0,
                workflows_failed=0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
            )

            job = JobInfo(
                token=job_token,
                submission=submission,
                progress=progress,
                callback_addr=callback_addr,
            )

            self._jobs[job_token_str] = job
            return job

    def get_job(self, job_token: str | TrackingToken) -> JobInfo | None:
        """Get job info by token. Returns None if not found."""
        token_str = str(job_token)
        return self._jobs.get(token_str)

    def get_job_by_id(self, job_id: str) -> JobInfo | None:
        """Get job info by job_id (creates token internally)."""
        token = self.create_job_token(job_id)
        return self._jobs.get(str(token))

    def get_job_for_workflow(self, workflow_token: str | TrackingToken) -> JobInfo | None:
        """Get job info by workflow token."""
        token_str = str(workflow_token)
        job_token_str = self._workflow_to_job.get(token_str)
        if job_token_str:
            return self._jobs.get(job_token_str)
        return None

    def get_job_for_sub_workflow(self, sub_workflow_token: str | TrackingToken) -> JobInfo | None:
        """Get job info by sub-workflow token."""
        token_str = str(sub_workflow_token)
        job_token_str = self._sub_workflow_to_job.get(token_str)
        if job_token_str:
            return self._jobs.get(job_token_str)
        return None

    async def remove_job(self, job_token: str | TrackingToken) -> None:
        """
        Remove a job and all its associated state.

        Thread-safe: uses global lock for job deletion.
        """
        token_str = str(job_token)

        async with self._global_lock:
            job = self._jobs.pop(token_str, None)
            if not job:
                return

            # Clean up lookup mappings
            for wf_token_str in job.workflows:
                self._workflow_to_job.pop(wf_token_str, None)
            for sub_wf_token_str in job.sub_workflows:
                self._sub_workflow_to_job.pop(sub_wf_token_str, None)

    # =========================================================================
    # Workflow Registration
    # =========================================================================

    async def register_workflow(
        self,
        job_id: str,
        workflow_id: str,
        name: str,
        workflow: Workflow | None = None,
    ) -> WorkflowInfo | None:
        """
        Register a workflow for a job.

        Args:
            job_id: The job ID
            workflow_id: Unique workflow identifier within the job
            name: Human-readable workflow name (for display/logging)
            workflow: Optional workflow instance

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if not job:
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
                state=WorkflowState.PENDING,
            )
            job.workflows[workflow_token_str] = info
            self._workflow_to_job[workflow_token_str] = str(job.token)

            # Update job progress
            job.progress.workflows_total = len(job.workflows)

            return info

    async def register_sub_workflow(
        self,
        job_id: str,
        workflow_id: str,
        worker_id: str,
        cores_allocated: int,
    ) -> SubWorkflowInfo | None:
        """
        Register a sub-workflow dispatch to a worker.

        Thread-safe: acquires job lock.
        """
        job = self.get_job_by_id(job_id)
        if not job:
            return None

        workflow_token = self.create_workflow_token(job_id, workflow_id)
        workflow_token_str = str(workflow_token)
        sub_workflow_token = self.create_sub_workflow_token(job_id, workflow_id, worker_id)
        sub_workflow_token_str = str(sub_workflow_token)

        async with job.lock:
            # Get parent workflow
            parent = job.workflows.get(workflow_token_str)
            if not parent:
                return None

            # Create sub-workflow info
            info = SubWorkflowInfo(
                token=sub_workflow_token,
                parent_token=workflow_token,
                cores_allocated=cores_allocated,
            )

            # Register in both places
            job.sub_workflows[sub_workflow_token_str] = info
            parent.sub_workflow_tokens.append(sub_workflow_token_str)
            self._sub_workflow_to_job[sub_workflow_token_str] = str(job.token)

            # Mark parent as dispatched
            if parent.state == WorkflowState.PENDING:
                parent.state = WorkflowState.DISPATCHED

            return info

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
        Returns True if update was applied, False if sub-workflow not found.
        """
        token_str = str(sub_workflow_token)
        job = self.get_job_for_sub_workflow(token_str)
        if not job:
            return False

        async with job.lock:
            sub_wf = job.sub_workflows.get(token_str)
            if not sub_wf:
                return False

            sub_wf.progress = progress

            # Update parent workflow state if needed
            parent_token_str = str(sub_wf.parent_token)
            parent = job.workflows.get(parent_token_str)
            if parent and parent.state == WorkflowState.DISPATCHED:
                parent.state = WorkflowState.RUNNING

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
        token_str = str(sub_workflow_token)
        job = self.get_job_for_sub_workflow(token_str)
        if not job:
            return False, False

        async with job.lock:
            sub_wf = job.sub_workflows.get(token_str)
            if not sub_wf:
                return False, False

            sub_wf.result = result

            # Check if all sub-workflows for parent are complete
            parent_token_str = str(sub_wf.parent_token)
            parent = job.workflows.get(parent_token_str)
            if not parent:
                return True, False

            all_complete = all(
                job.sub_workflows.get(sid) and job.sub_workflows[sid].result is not None
                for sid in parent.sub_workflow_tokens
            )

            return True, all_complete

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
        """
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        async with job.lock:
            wf = job.workflows.get(token_str)
            if not wf:
                return False

            if wf.state not in (WorkflowState.COMPLETED, WorkflowState.FAILED,
                                WorkflowState.AGGREGATED, WorkflowState.AGGREGATION_FAILED):
                wf.state = WorkflowState.COMPLETED
                wf.completion_event.set()

                # Update job progress
                job.progress.workflows_completed += 1

            return True

    async def mark_workflow_failed(
        self,
        workflow_token: str | TrackingToken,
        error: str,
    ) -> bool:
        """
        Mark a workflow as failed.

        Thread-safe: acquires job lock.
        """
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        async with job.lock:
            wf = job.workflows.get(token_str)
            if not wf:
                return False

            wf.state = WorkflowState.FAILED
            wf.error = error
            wf.completion_event.set()

            # Update job progress
            job.progress.workflows_failed += 1

            return True

    async def mark_aggregation_failed(
        self,
        workflow_token: str | TrackingToken,
        error: str,
    ) -> bool:
        """
        Mark workflow aggregation as failed.

        This is separate from workflow failure - the workers completed
        successfully but we couldn't aggregate the results.

        Thread-safe: acquires job lock.
        """
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        async with job.lock:
            wf = job.workflows.get(token_str)
            if not wf:
                return False

            wf.state = WorkflowState.AGGREGATION_FAILED
            wf.aggregation_error = error

            return True

    # =========================================================================
    # Sub-workflow Queries
    # =========================================================================

    def get_sub_workflow_tokens(self, workflow_token: str | TrackingToken) -> list[str]:
        """
        Get all sub-workflow token strings for a parent workflow.

        Note: For thread-safety, caller should hold job lock if iterating results.
        """
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return []

        wf = job.workflows.get(token_str)
        if not wf:
            return []

        return list(wf.sub_workflow_tokens)

    def get_sub_workflow_results(
        self,
        workflow_token: str | TrackingToken,
    ) -> list[WorkflowFinalResult]:
        """
        Get all sub-workflow results for a parent workflow.

        Note: For thread-safety, caller should hold job lock if modifying results.
        """
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return []

        wf = job.workflows.get(token_str)
        if not wf:
            return []

        results = []
        for sub_token_str in wf.sub_workflow_tokens:
            sub_wf = job.sub_workflows.get(sub_token_str)
            if sub_wf and sub_wf.result:
                results.append(sub_wf.result)

        return results

    def are_all_sub_workflows_complete(self, workflow_token: str | TrackingToken) -> bool:
        """Check if all sub-workflows for a parent have results."""
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        wf = job.workflows.get(token_str)
        if not wf:
            return False

        if not wf.sub_workflow_tokens:
            return False

        return all(
            job.sub_workflows.get(sid) and job.sub_workflows[sid].result is not None
            for sid in wf.sub_workflow_tokens
        )

    # =========================================================================
    # Job Status
    # =========================================================================

    def is_job_complete(self, job_token: str | TrackingToken) -> bool:
        """Check if all workflows in a job are complete (success or failure)."""
        job = self.get_job(job_token)
        if not job:
            return False

        if not job.workflows:
            return False

        return all(
            wf.state in (WorkflowState.COMPLETED, WorkflowState.FAILED,
                        WorkflowState.AGGREGATED, WorkflowState.AGGREGATION_FAILED)
            for wf in job.workflows.values()
        )

    def get_job_status(self, job_token: str | TrackingToken) -> str:
        """Get current job status string."""
        job = self.get_job(job_token)
        if not job:
            return JobStatus.UNKNOWN.value

        return job.progress.status

    async def update_job_status(self, job_token: str | TrackingToken, status: str) -> bool:
        """
        Update job status.

        Thread-safe: acquires job lock.
        """
        job = self.get_job(job_token)
        if not job:
            return False

        async with job.lock:
            job.progress.status = status
            job.progress.timestamp = time.monotonic()
            return True

    # =========================================================================
    # Context Management
    # =========================================================================

    async def update_context(
        self,
        job_token: str | TrackingToken,
        updates: dict[str, Any],
    ) -> bool:
        """
        Update job context with new values.

        Thread-safe: acquires job lock.
        """
        job = self.get_job(job_token)
        if not job:
            return False

        async with job.lock:
            for key, value in updates.items():
                job.context[key] = value
            job.layer_version += 1
            return True

    def get_context(self, job_token: str | TrackingToken) -> Context | None:
        """Get job context. Returns None if job not found."""
        job = self.get_job(job_token)
        if not job:
            return None
        return job.context

    # =========================================================================
    # Iteration Helpers
    # =========================================================================

    def iter_jobs(self) -> list[JobInfo]:
        """Get a snapshot of all jobs for iteration."""
        return list(self._jobs.values())

    def iter_workflows(self, job_token: str | TrackingToken) -> list[WorkflowInfo]:
        """Get a snapshot of all workflows for a job."""
        job = self.get_job(job_token)
        if not job:
            return []
        return list(job.workflows.values())
