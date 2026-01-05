"""
Workflow Dispatcher - Manages workflow dispatch to workers.

This class handles the logic for dispatching workflows to workers,
including dependency tracking, eager dispatch, and resource allocation.

Key responsibilities:
- Workflow dependency graph management
- Eager dispatch (dispatch as soon as dependencies are satisfied)
- Core allocation coordination with WorkerPool
- Dispatch request building and sending
"""

import asyncio
from typing import Any, Callable, Coroutine

import cloudpickle
import networkx

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.graph.dependent_workflow import DependentWorkflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority
from hyperscale.distributed_rewrite.models import (
    JobSubmission,
    PendingWorkflow,
    WorkflowDispatch,
)
from hyperscale.distributed_rewrite.jobs.job_manager import (
    JobManager,
    TrackingToken,
)
from hyperscale.distributed_rewrite.jobs.worker_pool import WorkerPool


class WorkflowDispatcher:
    """
    Manages workflow dispatch to workers.

    Coordinates with JobManager for state tracking and WorkerPool
    for resource allocation. Handles dependency-based eager dispatch.
    """

    def __init__(
        self,
        job_manager: JobManager,
        worker_pool: WorkerPool,
        send_dispatch: Callable[[str, WorkflowDispatch], Coroutine[Any, Any, bool]],
        datacenter: str,
        manager_id: str,
    ):
        """
        Initialize WorkflowDispatcher.

        Args:
            job_manager: JobManager for state tracking
            worker_pool: WorkerPool for resource allocation
            send_dispatch: Async callback to send dispatch to a worker
                          Takes (worker_node_id, dispatch) and returns success bool
            datacenter: Datacenter identifier
            manager_id: This manager's node ID
        """
        self._job_manager = job_manager
        self._worker_pool = worker_pool
        self._send_dispatch = send_dispatch
        self._datacenter = datacenter
        self._manager_id = manager_id

        # Pending workflows waiting for dependencies/cores
        # Key: f"{job_id}:{workflow_id}"
        self._pending: dict[str, PendingWorkflow] = {}

        # Lock for pending workflow access
        self._pending_lock = asyncio.Lock()

        # Lock to prevent concurrent eager dispatch attempts
        self._dispatch_lock = asyncio.Lock()

    # =========================================================================
    # Workflow Registration
    # =========================================================================

    async def register_workflows(
        self,
        submission: JobSubmission,
        workflows: list[type[Workflow] | DependentWorkflow],
    ) -> bool:
        """
        Register all workflows from a job submission.

        Builds the dependency graph and registers workflows with
        JobManager. Workflows without dependencies are immediately
        eligible for dispatch.

        Returns True if registration succeeded.
        """
        job_id = submission.job_id

        # Build dependency graph
        graph = networkx.DiGraph()
        workflow_by_id: dict[str, tuple[str, Workflow, int]] = {}  # workflow_id -> (name, workflow, vus)
        priorities: dict[str, StagePriority] = {}
        is_test: dict[str, bool] = {}

        for i, wf in enumerate(workflows):
            try:
                # Instantiate if it's a class
                instance = wf() if isinstance(wf, type) else wf
                dependencies: list[str] = []

                # Unwrap DependentWorkflow
                if isinstance(instance, DependentWorkflow):
                    dependencies = instance.dependencies
                    instance = instance.dependent_workflow

                # Generate workflow ID
                workflow_id = f"wf-{i:04d}"
                name = getattr(instance, 'name', type(instance).__name__)
                vus = getattr(instance, 'vus', submission.vus)

                # Register with JobManager
                await self._job_manager.register_workflow(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    name=name,
                    workflow=instance,
                )

                # Store for graph building
                workflow_by_id[workflow_id] = (name, instance, vus)
                priorities[workflow_id] = self._get_workflow_priority(instance)
                is_test[workflow_id] = self._is_test_workflow(instance)

                # Add to graph
                graph.add_node(workflow_id)
                for dep_name in dependencies:
                    # Find dependency by name
                    dep_id = self._find_workflow_id_by_name(workflow_by_id, dep_name)
                    if dep_id:
                        graph.add_edge(dep_id, workflow_id)

            except Exception:
                # Registration failed - job should be marked failed by caller
                return False

        # Register pending workflows
        async with self._pending_lock:
            for workflow_id, (name, workflow, vus) in workflow_by_id.items():
                # Get dependencies from graph
                dependencies = set(graph.predecessors(workflow_id))

                key = f"{job_id}:{workflow_id}"
                self._pending[key] = PendingWorkflow(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    workflow_name=name,
                    workflow=workflow,
                    vus=vus,
                    priority=priorities[workflow_id],
                    is_test=is_test[workflow_id],
                    dependencies=dependencies,
                )

        return True

    def _find_workflow_id_by_name(
        self,
        workflow_by_id: dict[str, tuple[str, Workflow, int]],
        name: str,
    ) -> str | None:
        """Find workflow ID by name."""
        for wf_id, (wf_name, _, _) in workflow_by_id.items():
            if wf_name == name:
                return wf_id
        return None

    def _get_workflow_priority(self, workflow: Workflow) -> StagePriority:
        """Determine dispatch priority for a workflow."""
        priority = getattr(workflow, 'priority', None)
        if isinstance(priority, StagePriority):
            return priority
        return StagePriority.NORMAL

    def _is_test_workflow(self, workflow: Workflow) -> bool:
        """Check if a workflow is a test workflow."""
        # Check for test-related attributes or naming
        name = getattr(workflow, 'name', type(workflow).__name__)
        if 'test' in name.lower():
            return True
        return hasattr(workflow, 'is_test') and workflow.is_test

    # =========================================================================
    # Dependency Completion
    # =========================================================================

    async def mark_workflow_completed(
        self,
        job_id: str,
        workflow_id: str,
    ) -> None:
        """
        Mark a workflow as completed and update dependents.

        Called when a workflow completes successfully. Updates
        all pending workflows that depend on this one.
        """
        async with self._pending_lock:
            # Update all pending workflows that depend on this one
            for key, pending in self._pending.items():
                if pending.job_id != job_id:
                    continue
                if workflow_id in pending.dependencies:
                    pending.completed_dependencies.add(workflow_id)

    # =========================================================================
    # Eager Dispatch
    # =========================================================================

    async def try_dispatch(self, job_id: str, submission: JobSubmission) -> int:
        """
        Attempt to dispatch any workflows that are ready.

        Called when:
        1. A job is first submitted
        2. A workflow completes (dependencies may now be satisfied)
        3. Cores become available

        Returns number of workflows dispatched.
        """
        async with self._dispatch_lock:
            ready = self._get_ready_workflows(job_id)
            if not ready:
                return 0

            # Get available cores
            total_cores = self._worker_pool.get_total_available_cores()
            if total_cores <= 0:
                return 0

            dispatched = 0

            # Handle EXCLUSIVE workflows first
            exclusive = [p for p in ready if p.priority == StagePriority.EXCLUSIVE]
            if exclusive:
                pending = exclusive[0]
                success = await self._dispatch_workflow(
                    pending, submission, total_cores
                )
                if success:
                    dispatched += 1
                # Don't dispatch others while EXCLUSIVE is pending
                return dispatched

            # Dispatch non-exclusive workflows
            non_exclusive = [p for p in ready if p.priority != StagePriority.EXCLUSIVE]
            if not non_exclusive:
                return dispatched

            # Calculate core allocation
            allocations = self._calculate_allocations(non_exclusive, total_cores)

            # Dispatch each workflow
            for pending, cores in allocations:
                success = await self._dispatch_workflow(pending, submission, cores)
                if success:
                    dispatched += 1

            return dispatched

    def _get_ready_workflows(self, job_id: str) -> list[PendingWorkflow]:
        """Get workflows ready for dispatch (dependencies satisfied, not dispatched)."""
        ready = []
        for key, pending in self._pending.items():
            if pending.job_id != job_id:
                continue
            if pending.dispatched:
                continue
            # Check if all dependencies are satisfied
            if pending.dependencies <= pending.completed_dependencies:
                ready.append(pending)
        return ready

    def _calculate_allocations(
        self,
        workflows: list[PendingWorkflow],
        total_cores: int,
    ) -> list[tuple[PendingWorkflow, int]]:
        """
        Calculate core allocations for workflows.

        Distributes cores based on priority and VUs.
        """
        if not workflows:
            return []

        # Sort by priority (higher value = higher priority) then by VUs (higher first)
        # StagePriority: EXCLUSIVE=4, HIGH=3, NORMAL=2, LOW=1, AUTO=0
        workflows = sorted(
            workflows,
            key=lambda p: (-p.priority.value, -p.vus),
        )

        # Simple allocation: divide cores proportionally by VUs
        total_vus = sum(p.vus for p in workflows)
        if total_vus == 0:
            total_vus = len(workflows)  # Fallback: equal distribution

        allocations = []
        remaining_cores = total_cores

        for i, pending in enumerate(workflows):
            if remaining_cores <= 0:
                break

            # Calculate this workflow's share
            if i == len(workflows) - 1:
                # Last workflow gets remaining cores
                cores = remaining_cores
            else:
                # Proportional allocation
                share = pending.vus / total_vus if total_vus > 0 else 1 / len(workflows)
                cores = max(1, int(total_cores * share))
                cores = min(cores, remaining_cores)

            allocations.append((pending, cores))
            remaining_cores -= cores

        return allocations

    # =========================================================================
    # Single Workflow Dispatch
    # =========================================================================

    async def _dispatch_workflow(
        self,
        pending: PendingWorkflow,
        submission: JobSubmission,
        cores_needed: int,
    ) -> bool:
        """
        Dispatch a single workflow to workers.

        Allocates cores from the worker pool and sends dispatch
        messages to the selected workers.

        Returns True if dispatch succeeded.
        """
        # Mark as dispatched (prevents duplicate dispatch)
        pending.dispatched = True
        pending.cores_allocated = cores_needed

        # Allocate cores from worker pool
        allocations = await self._worker_pool.allocate_cores(
            cores_needed,
            timeout=submission.timeout_seconds,
        )

        if not allocations:
            # No cores available - revert dispatch status
            pending.dispatched = False
            return False

        total_allocated = sum(cores for _, cores in allocations)

        # Serialize workflow
        workflow_bytes = cloudpickle.dumps(pending.workflow)
        context_bytes = cloudpickle.dumps({})

        # Create tracking token
        workflow_token = TrackingToken.for_workflow(
            self._datacenter,
            self._manager_id,
            pending.job_id,
            pending.workflow_id,
        )

        # Dispatch to each worker
        successful = 0
        for worker_id, worker_cores in allocations:
            # Calculate VUs for this worker
            worker_vus = max(1, int(pending.vus * (worker_cores / total_allocated)))

            # Create sub-workflow token
            sub_token = workflow_token.to_sub_workflow_token(worker_id)

            # Register sub-workflow with JobManager
            await self._job_manager.register_sub_workflow(
                job_id=pending.job_id,
                workflow_id=pending.workflow_id,
                worker_id=worker_id,
                cores_allocated=worker_cores,
            )

            # Create dispatch message
            dispatch = WorkflowDispatch(
                job_id=pending.job_id,
                workflow_id=str(sub_token),  # Use full tracking token
                workflow=workflow_bytes,
                context=context_bytes,
                vus=worker_vus,
                cores=worker_cores,
                timeout_seconds=submission.timeout_seconds,
                fence_token=0,  # TODO: Get from quorum manager
                context_version=0,
            )

            # Send dispatch
            try:
                success = await self._send_dispatch(worker_id, dispatch)
                if success:
                    await self._worker_pool.confirm_allocation(worker_id, worker_cores)
                    successful += 1
                else:
                    await self._worker_pool.release_cores(worker_id, worker_cores)
            except Exception:
                await self._worker_pool.release_cores(worker_id, worker_cores)

        return successful > 0

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def cleanup_job(self, job_id: str) -> None:
        """Remove all pending workflows for a job."""
        async with self._pending_lock:
            keys_to_remove = [
                key for key in self._pending
                if key.startswith(f"{job_id}:")
            ]
            for key in keys_to_remove:
                self._pending.pop(key, None)
