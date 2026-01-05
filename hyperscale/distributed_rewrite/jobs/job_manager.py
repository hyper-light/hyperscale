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
from typing import Any, Callable, Coroutine

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.state.context import Context
from hyperscale.distributed_rewrite.models import (
    JobInfo,
    JobProgress,
    JobStatus,
    JobSubmission,
    SubWorkflowInfo,
    TrackingToken,
    WorkflowFinalResult,
    WorkflowInfo,
    WorkflowProgress,
    WorkflowStatus,
)
from hyperscale.distributed_rewrite.jobs.logging_models import (
    JobManagerError,
    JobManagerInfo,
)
from hyperscale.distributed_rewrite.jobs.workflow_state_machine import (
    WorkflowStateMachine,
)
from hyperscale.logging import Logger


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
        on_workflow_completed: Callable[[str, str], Coroutine[Any, Any, None]] | None = None,
    ):
        """
        Initialize JobManager.

        Args:
            datacenter: Datacenter identifier (e.g., "DC-EAST")
            manager_id: This manager's node ID (short form)
            on_workflow_completed: Optional async callback called when workflow completes
                                  Takes (job_id, workflow_id) and is awaited
        """
        self._datacenter = datacenter
        self._manager_id = manager_id
        self._on_workflow_completed = on_workflow_completed
        self._logger = Logger()

        # Main job storage - job token string -> JobInfo
        self._jobs: dict[str, JobInfo] = {}

        # Quick lookup for workflow/sub-workflow -> job token mapping
        self._workflow_to_job: dict[str, str] = {}  # workflow_token_str -> job_token_str
        self._sub_workflow_to_job: dict[str, str] = {}  # sub_workflow_token_str -> job_token_str

        # Global lock for job creation/deletion (not per-job operations)
        self._global_lock = asyncio.Lock()

    def set_on_workflow_completed(
        self,
        callback: Callable[[str, str], Coroutine[Any, Any, None]],
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
                timestamp=time.monotonic(),
                callback_addr=callback_addr,
            )

            self._jobs[job_token_str] = job

            await self._logger.log(JobManagerError(
                message=f"[create_job] SUCCESS: created job_token={job_token_str}, job_id={submission.job_id}, JobManager id={id(self)}",
                manager_id=self._manager_id,
                datacenter=self._datacenter,
                job_id=submission.job_id,
            ))

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
                timestamp=time.monotonic(),
                leader_node_id=leader_node_id,
                leader_addr=leader_addr,
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
            await self._logger.log(JobManagerError(
                message=f"[register_workflow] FAILED: job not found for job_id={job_id}",
                manager_id=self._manager_id,
                datacenter=self._datacenter,
                job_id=job_id,
                workflow_id=workflow_id,
            ))
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
            )
            job.workflows[workflow_token_str] = info
            self._workflow_to_job[workflow_token_str] = str(job.token)

            # Update job progress
            job.workflows_total = len(job.workflows)

            await self._logger.log(JobManagerError(
                message=f"[register_workflow] SUCCESS: registered workflow_token={workflow_token_str}, name={name}",
                manager_id=self._manager_id,
                datacenter=self._datacenter,
                job_id=job_id,
                workflow_id=workflow_id,
            ))

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
            await self._logger.log(JobManagerError(
                message=f"[register_sub_workflow] FAILED: job not found for job_id={job_id}",
                manager_id=self._manager_id,
                datacenter=self._datacenter,
                job_id=job_id,
                workflow_id=workflow_id,
            ))
            return None

        workflow_token = self.create_workflow_token(job_id, workflow_id)
        workflow_token_str = str(workflow_token)
        sub_workflow_token = self.create_sub_workflow_token(job_id, workflow_id, worker_id)
        sub_workflow_token_str = str(sub_workflow_token)

        async with job.lock:
            # Get parent workflow
            parent = job.workflows.get(workflow_token_str)
            if not parent:
                await self._logger.log(JobManagerError(
                    message=f"[register_sub_workflow] FAILED: parent workflow not found for workflow_token={workflow_token_str}, job.workflows keys={list(job.workflows.keys())}",
                    manager_id=self._manager_id,
                    datacenter=self._datacenter,
                    job_id=job_id,
                    workflow_id=workflow_id,
                ))
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

            await self._logger.log(JobManagerError(
                message=f"[register_sub_workflow] SUCCESS: registered sub_workflow_token={sub_workflow_token_str}, parent={workflow_token_str}, JobManager id={id(self)}, _sub_workflow_to_job has {len(self._sub_workflow_to_job)} keys",
                manager_id=self._manager_id,
                datacenter=self._datacenter,
                job_id=job_id,
                workflow_id=workflow_id,
                sub_workflow_token=sub_workflow_token_str,
            ))

            # Mark parent as dispatched (assigned)
            if parent.status == WorkflowStatus.PENDING:
                parent.status = WorkflowStatus.ASSIGNED

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
        Uses WorkflowStateMachine to ensure parent state only advances, never regresses.
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

            print(progress.status, progress.workflow_name)

            # Update parent workflow state using state machine (prevents regression)
            parent_token_str = str(sub_wf.parent_token)
            parent = job.workflows.get(parent_token_str)
            if parent:
                # Receiving progress means workflow is running
                parent.status = WorkflowStateMachine.advance_state(
                    parent.status, WorkflowStatus.RUNNING
                )

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
            await self._logger.log(JobManagerError(
                message=f"[record_sub_workflow_result] FAILED: job not found for token={token_str}, JobManager id={id(self)}, _sub_workflow_to_job keys={list(self._sub_workflow_to_job.keys())[:10]}...",
                manager_id=self._manager_id,
                datacenter=self._datacenter,
                sub_workflow_token=token_str,
            ))
            return False, False

        async with job.lock:
            sub_wf = job.sub_workflows.get(token_str)
            if not sub_wf:
                await self._logger.log(JobManagerError(
                    message=f"[record_sub_workflow_result] FAILED: sub_wf not found for token={token_str}, job.sub_workflows keys={list(job.sub_workflows.keys())}",
                    manager_id=self._manager_id,
                    datacenter=self._datacenter,
                    job_id=job.job_id,
                    sub_workflow_token=token_str,
                ))
                return False, False

            sub_wf.result = result
            await self._logger.log(JobManagerError(
                message=f"[record_sub_workflow_result] SUCCESS: recorded result for token={token_str}",
                manager_id=self._manager_id,
                datacenter=self._datacenter,
                job_id=job.job_id,
                sub_workflow_token=token_str,
            ))

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
        Notifies on_workflow_completed callback for event-driven dispatch.
        """
        token_str = str(workflow_token)
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        should_notify = False
        workflow_id = ""

        async with job.lock:
            wf = job.workflows.get(token_str)
            if not wf:
                return False

            if wf.status not in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED,
                                WorkflowStatus.AGGREGATED, WorkflowStatus.AGGREGATION_FAILED):
                wf.status = WorkflowStatus.COMPLETED
                wf.completion_event.set()

                # Update job progress
                job.workflows_completed += 1

                # Mark for notification (do outside lock)
                # Use workflow_id (e.g., "wf-0001") not name - dependencies are tracked by ID
                should_notify = True
                workflow_id = wf.token.workflow_id or ""

        # Notify callback outside lock to avoid deadlocks
        if should_notify and self._on_workflow_completed:
            await self._on_workflow_completed(job.job_id, workflow_id)

        return True

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
        job = self.get_job_for_workflow(token_str)
        if not job:
            return False

        should_notify = False
        workflow_id = ""

        async with job.lock:
            wf = job.workflows.get(token_str)
            if not wf:
                return False

            wf.status = WorkflowStatus.FAILED
            wf.error = error
            wf.completion_event.set()

            # Update job progress
            job.workflows_failed += 1

            # Mark for notification (do outside lock)
            # Use workflow_id (e.g., "wf-0001") not name - dependencies are tracked by ID
            should_notify = True
            workflow_id = wf.token.workflow_id or ""

        # Notify callback outside lock to avoid deadlocks
        # Failed workflows still trigger dispatch - dependent workflows may need to fail
        if should_notify and self._on_workflow_completed:
            await self._on_workflow_completed(job.job_id, workflow_id)

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

            wf.status = WorkflowStatus.AGGREGATION_FAILED
            wf.aggregation_error = error

            return True

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
        should_notify = False
        workflow_id = ""

        async with job.lock:
            wf = job.workflows.get(token_str)
            if not wf:
                return False

            old_status = wf.status

            # Update status
            wf.status = new_status
            if error:
                wf.error = error

            # Update job progress counters based on status transition
            # Only count transitions TO terminal states, not from them
            if old_status not in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED,
                                 WorkflowStatus.AGGREGATED, WorkflowStatus.AGGREGATION_FAILED):
                if new_status == WorkflowStatus.COMPLETED:
                    job.workflows_completed += 1
                    wf.completion_event.set()
                    should_notify = True
                    # Use workflow_id (e.g., "wf-0001") not name - dependencies are tracked by ID
                    workflow_id = wf.token.workflow_id or ""
                elif new_status == WorkflowStatus.FAILED:
                    job.workflows_failed += 1
                    wf.completion_event.set()
                    should_notify = True
                    # Use workflow_id (e.g., "wf-0001") not name - dependencies are tracked by ID
                    workflow_id = wf.token.workflow_id or ""

        # Notify callback outside lock to avoid deadlocks
        if should_notify and self._on_workflow_completed:
            await self._on_workflow_completed(job.job_id, workflow_id)

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
            wf.status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED,
                        WorkflowStatus.AGGREGATED, WorkflowStatus.AGGREGATION_FAILED)
            for wf in job.workflows.values()
        )

    def get_job_status(self, job_token: str | TrackingToken) -> str:
        """Get current job status string."""
        job = self.get_job(job_token)
        if not job:
            return JobStatus.UNKNOWN.value

        return job.status

    async def update_job_status(self, job_token: str | TrackingToken, status: str) -> bool:
        """
        Update job status.

        Thread-safe: acquires job lock.
        """
        job = self.get_job(job_token)
        if not job:
            return False

        async with job.lock:
            job.status = status
            job.timestamp = time.monotonic()
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

    def iter_workflows(self, job_token: str | TrackingToken) -> list[WorkflowInfo]:
        """Get a snapshot of all workflows for a job."""
        job = self.get_job(job_token)
        if not job:
            return []
        return list(job.workflows.values())

    def get_jobs_as_wire_progress(self) -> dict[str, JobProgress]:
        """
        Get all jobs converted to wire protocol JobProgress.

        Used for state sync between managers.
        """
        return {
            job.job_id: job.to_wire_progress()
            for job in self._jobs.values()
        }

    # =========================================================================
    # Job Cleanup
    # =========================================================================

    async def complete_job(self, job_id: str) -> bool:
        """
        Mark a job as complete and clean up its tracking state.

        This removes the job from JobManager tracking. Called by the manager
        during job cleanup to prevent memory leaks.

        Thread-safe: uses global lock for job removal.

        Args:
            job_id: The job ID to complete/remove

        Returns:
            True if job was found and removed, False if not found
        """
        job_token = self.create_job_token(job_id)
        job_token_str = str(job_token)

        async with self._global_lock:
            job = self._jobs.pop(job_token_str, None)
            if not job:
                return False

            # Clean up lookup mappings to prevent memory leaks
            for wf_token_str in job.workflows:
                self._workflow_to_job.pop(wf_token_str, None)
            for sub_wf_token_str in job.sub_workflows:
                self._sub_workflow_to_job.pop(sub_wf_token_str, None)

            return True
