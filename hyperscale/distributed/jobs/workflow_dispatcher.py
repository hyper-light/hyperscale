"""
Workflow Dispatcher - Manages workflow dispatch to workers.

This class handles the logic for dispatching workflows to workers,
including dependency tracking, eager dispatch, resource allocation,
and workflow timeout/eviction.

Key responsibilities:
- Workflow dependency graph management
- Eager dispatch (dispatch as soon as dependencies are satisfied)
- Core allocation coordination with WorkerPool
- Dispatch request building and sending
- Workflow timeout tracking and eviction
"""

import asyncio
import time
import traceback
from typing import Any, Callable, Coroutine

import cloudpickle
import networkx

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority
from hyperscale.distributed.health.deadline_resolver import (
    resolve_worker_deadline_seconds,
)
from hyperscale.distributed.models import (
    JobSubmission,
    PendingWorkflow,
    WorkflowDispatch,
)
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.models import TrackingToken
from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.jobs.logging_models import (
    DispatcherTrace,
    DispatcherDebug,
    DispatcherInfo,
    DispatcherWarning,
    DispatcherError,
    DispatcherCritical,
)
from hyperscale.distributed.reliability import (
    RetryBudgetManager,
    ReliabilityConfig,
    create_reliability_config_from_env,
)
from hyperscale.distributed.env import Env
from hyperscale.logging import Logger


def _serialize_context(context_dict: dict) -> bytes:
    return cloudpickle.dumps(context_dict)


class WorkflowDispatcher:
    """
    Manages workflow dispatch to workers.

    Coordinates with JobManager for state tracking and WorkerPool
    for resource allocation. Handles dependency-based eager dispatch.
    """

    # Exponential backoff constants
    INITIAL_RETRY_DELAY = 1.0  # seconds
    MAX_RETRY_DELAY = 60.0  # seconds
    BACKOFF_MULTIPLIER = 2.0  # double delay each retry

    def __init__(
        self,
        job_manager: JobManager,
        worker_pool: WorkerPool,
        send_dispatch: Callable[[str, WorkflowDispatch], Coroutine[Any, Any, bool]],
        datacenter: str,
        manager_id: str,
        default_timeout_seconds: float = 300.0,
        max_dispatch_attempts: int = 5,
        on_workflow_evicted: Callable[[str, str, str], Coroutine[Any, Any, None]]
        | None = None,
        on_dispatch_failed: Callable[[str, str, str], Coroutine[Any, Any, None]]
        | None = None,
        get_leader_term: Callable[[], int] | None = None,
        retry_budget_manager: RetryBudgetManager | None = None,
        env: Env | None = None,
        max_concurrent_dispatches: int = 16,
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
            default_timeout_seconds: Default timeout for workflows (can be overridden per-job)
            max_dispatch_attempts: Max dispatch retries before marking workflow failed
            on_workflow_evicted: Optional callback when a workflow is evicted due to timeout
                               Takes (job_id, workflow_id, reason) and is awaited
            on_dispatch_failed: Optional callback when dispatch permanently fails after retries
                               Takes (job_id, workflow_id, reason) and is awaited
            get_leader_term: Callback to get current leader election term (AD-10 requirement).
                            Returns the current term for fence token generation.
            retry_budget_manager: Optional retry budget manager (AD-44). If None, one is created.
            env: Optional environment config. Used to create retry budget manager if not provided.
            max_concurrent_dispatches: Maximum workers to dispatch to concurrently
                                       for one workflow fanout.
        """
        self._job_manager = job_manager
        self._worker_pool = worker_pool
        self._send_dispatch = send_dispatch
        self._datacenter = datacenter
        self._manager_id = manager_id
        self._default_timeout_seconds = default_timeout_seconds
        self._max_dispatch_attempts = max_dispatch_attempts
        self._on_workflow_evicted = on_workflow_evicted
        self._on_dispatch_failed = on_dispatch_failed
        self._get_leader_term = get_leader_term
        self._max_concurrent_dispatches = max(1, max_concurrent_dispatches)
        self._logger = Logger()

        # Phase H2: hold onto env for the deadline resolver. Falls back
        # to a fresh ``Env()`` so the constructor invariant "self._env
        # is never None" holds — simplifies all downstream call sites
        # that need the multiplier.
        self._env: Env = env if env is not None else Env()

        if retry_budget_manager is not None:
            self._retry_budget_manager = retry_budget_manager
        else:
            self._retry_budget_manager = RetryBudgetManager(
                config=create_reliability_config_from_env(self._env)
            )

        # Pending workflows waiting for dependencies/cores
        # Key: f"{job_id}:{workflow_id}"
        self._pending: dict[str, PendingWorkflow] = {}

        # Lock for pending workflow access
        self._pending_lock = asyncio.Lock()

        # Lock to prevent concurrent eager dispatch attempts
        self._dispatch_lock = asyncio.Lock()

        # Event-driven dispatch: signaled when dispatch should be attempted
        # Set when: workflow ready_event is set, cores become available, retry timer expires
        self._dispatch_trigger: asyncio.Event = asyncio.Event()

        # Active dispatch loops per job
        # Key: job_id -> asyncio.Task running the dispatch loop
        self._job_dispatch_tasks: dict[str, asyncio.Task] = {}

        # Job submissions cache for dispatch loop access
        self._job_submissions: dict[str, JobSubmission] = {}

        # Shutdown flag
        self._shutting_down: bool = False

        # Jobs currently being cancelled (prevents dispatch during cancellation)
        self._cancelling_jobs: set[str] = set()

    # =========================================================================
    # Workflow Registration
    # =========================================================================

    async def register_workflows(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
    ) -> bool:
        """
        Register all workflows from a job submission.

        Builds the dependency graph and registers workflows with
        JobManager. Workflows without dependencies are immediately
        eligible for dispatch.

        Args:
            submission: The job submission
            workflows: List of (workflow_id, dependencies, workflow) tuples
                       workflow_id is client-generated for cross-DC consistency

        Returns True if registration succeeded.
        """
        job_id = submission.job_id

        await self._retry_budget_manager.create_budget(
            job_id=job_id,
            total=getattr(submission, "retry_budget", 0),
            per_workflow=getattr(submission, "retry_budget_per_workflow", 0),
        )

        # Build dependency graph
        graph = networkx.DiGraph()
        workflow_by_id: dict[
            str, tuple[str, Workflow, int]
        ] = {}  # workflow_id -> (name, workflow, vus)
        priorities: dict[str, StagePriority] = {}
        is_test: dict[str, bool] = {}

        for wf_data in workflows:
            # Unpack with client-generated workflow_id
            workflow_id, dependencies, instance = wf_data
            try:
                # Use the client-provided workflow_id (globally unique across DCs)
                name = getattr(instance, "name", None) or type(instance).__name__
                vus = (
                    instance.vus
                    if instance.vus and instance.vus > 0
                    else submission.vus
                )

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

            except Exception as e:
                # Registration failed - job should be marked failed by caller
                await self._log_error(
                    f"Failed to register workflow {workflow_id} for job {job_id}: {e}",
                    job_id=job_id,
                    workflow_id=workflow_id,
                )
                return False

        # Register pending workflows
        now = time.monotonic()
        timeout = submission.timeout_seconds or self._default_timeout_seconds

        async with self._pending_lock:
            for workflow_id, (name, workflow, vus) in workflow_by_id.items():
                # Get dependencies from graph
                dependencies = set(graph.predecessors(workflow_id))

                key = f"{job_id}:{workflow_id}"
                pending = PendingWorkflow(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    workflow_name=name,
                    workflow=workflow,
                    vus=vus,
                    priority=priorities[workflow_id],
                    is_test=is_test[workflow_id],
                    dependencies=dependencies,
                    registered_at=now,
                    timeout_seconds=timeout,
                    next_retry_delay=self.INITIAL_RETRY_DELAY,
                    max_dispatch_attempts=self._max_dispatch_attempts,
                )
                self._pending[key] = pending

                # Signal workflows with no dependencies as ready immediately
                pending.check_and_signal_ready()

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
        priority = getattr(workflow, "priority", None)
        if isinstance(priority, StagePriority):
            return priority
        return StagePriority.AUTO

    def _is_test_workflow(self, workflow: Workflow) -> bool:
        """Check if a workflow is a test workflow."""
        # Check for test-related attributes or naming
        name = getattr(workflow, "name", type(workflow).__name__)
        if "test" in name.lower():
            return True
        return hasattr(workflow, "is_test") and workflow.is_test

    # =========================================================================
    # Dependency Completion
    # =========================================================================

    async def mark_workflow_completed(
        self,
        job_id: str,
        workflow_id: str,
    ) -> None:
        """
        Mark a workflow as completed successfully and update dependents.

        Called when a workflow completes successfully. Updates
        all pending workflows that depend on this one and signals
        any that are now ready for dispatch.

        This is event-driven: dependent workflows waiting on this
        completion will have their ready_event set if all their
        dependencies are now satisfied.
        """
        async with self._pending_lock:
            # Update all pending workflows that depend on this one
            for key, pending in self._pending.items():
                if pending.job_id != job_id:
                    continue
                if workflow_id in pending.dependencies:
                    pending.completed_dependencies.add(workflow_id)
                    # Check if this workflow is now ready and signal if so
                    pending.check_and_signal_ready()

    async def mark_workflow_failed(
        self,
        job_id: str,
        workflow_id: str,
    ) -> list[str]:
        """
        Mark a workflow as failed and fail all dependents.

        When a workflow fails, all workflows that depend on it (directly or
        transitively) cannot execute and should be marked as failed.

        Returns list of workflow_ids that were failed as a result.
        """
        failed_workflows: list[str] = []

        async with self._pending_lock:
            # Find all workflows that depend on the failed one (directly or transitively)
            to_fail: set[str] = set()
            queue = [workflow_id]

            while queue:
                failed_wf_id = queue.pop(0)
                for key, pending in self._pending.items():
                    if pending.job_id != job_id:
                        continue
                    if (
                        failed_wf_id in pending.dependencies
                        and pending.workflow_id not in to_fail
                    ):
                        to_fail.add(pending.workflow_id)
                        queue.append(pending.workflow_id)

            # Mark all dependent workflows as failed by setting max_dispatch_attempts
            # This prevents them from ever being dispatched
            for key in list(self._pending.keys()):
                pending = self._pending.get(key)
                if not pending or pending.job_id != job_id:
                    continue
                if pending.workflow_id in to_fail:
                    # Mark as exhausted so it won't dispatch
                    pending.dispatch_attempts = pending.max_dispatch_attempts
                    pending.clear_ready()
                    failed_workflows.append(pending.workflow_id)

        return failed_workflows

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
        now = time.monotonic()
        ready = []
        for key, pending in self._pending.items():
            if pending.job_id != job_id:
                continue
            if pending.dispatched:
                continue
            if pending.dispatch_in_progress:
                continue  # Skip workflows with dispatch already in progress

            # Check if we've exceeded max retries
            if pending.dispatch_attempts >= pending.max_dispatch_attempts:
                continue  # Will be cleaned up by check_timeouts or explicit failure handling

            # Check retry backoff - if we failed before, wait for backoff period
            if pending.dispatch_attempts > 0:
                time_since_last_attempt = now - pending.last_dispatch_attempt
                if time_since_last_attempt < pending.next_retry_delay:
                    continue  # Still in backoff period

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

        Allocation strategy:
        1. Explicit priority workflows (non-AUTO) are allocated first, proportionally by VUs
        2. AUTO priority workflows use no more cores than they can keep busy
        3. If not enough cores for all workflows, only allocate what fits - rest stay pending

        Returns only workflows that can actually be allocated within available cores.
        Workflows that don't fit remain pending and will be dispatched when cores free up.
        """
        if not workflows:
            return []

        # Separate explicit priority from AUTO workflows
        explicit = [p for p in workflows if p.priority != StagePriority.AUTO]
        auto = [p for p in workflows if p.priority == StagePriority.AUTO]

        allocations = []
        remaining_cores = total_cores

        # Step 1: Allocate explicit priority workflows first (by priority then VUs)
        if explicit:
            # Sort by priority (higher value = higher priority) then by VUs (higher first)
            # StagePriority: EXCLUSIVE=4, HIGH=3, NORMAL=2, LOW=1
            explicit = sorted(
                explicit,
                key=lambda p: (-p.priority.value, -p.vus),
            )

            # Proportional allocation by VUs for explicit workflows
            total_vus = sum(p.vus for p in explicit)
            if total_vus == 0:
                total_vus = len(explicit)

            for i, pending in enumerate(explicit):
                if remaining_cores <= 0:
                    # No more cores - remaining explicit workflows stay pending
                    break

                if i == len(explicit) - 1 and not auto:
                    # Last explicit workflow gets remaining if no AUTO workflows
                    cores = remaining_cores
                else:
                    # Proportional allocation
                    share = (
                        pending.vus / total_vus if total_vus > 0 else 1 / len(explicit)
                    )
                    cores = max(1, int(total_cores * share))
                    cores = min(cores, remaining_cores)

                allocations.append((pending, cores))
                remaining_cores -= cores

        # Step 2: AUTO uses up to one core per VU. A 1-VU workflow should
        # not fan out across every worker in a large cluster; that turns a
        # tiny workload into a cluster-wide dispatch handshake barrier and
        # adds no useful parallelism.
        if auto and remaining_cores > 0:
            auto = sorted(auto, key=lambda pending: pending.vus, reverse=True)
            auto_to_allocate = auto[:remaining_cores]
            auto_core_caps = [max(1, pending.vus) for pending in auto_to_allocate]
            total_auto_core_cap = sum(auto_core_caps)
            available_auto_cores = remaining_cores

            for index, pending in enumerate(auto_to_allocate):
                if remaining_cores <= 0:
                    break
                useful_cores = auto_core_caps[index]
                remaining_workflows = len(auto_to_allocate) - index
                reserved_for_rest = remaining_workflows - 1

                if index == len(auto_to_allocate) - 1:
                    cores = min(useful_cores, remaining_cores)
                else:
                    share = useful_cores / total_auto_core_cap
                    proportional_cores = max(1, int(available_auto_cores * share))
                    cores = min(
                        useful_cores,
                        proportional_cores,
                        remaining_cores - reserved_for_rest,
                    )

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

        Uses dispatch_in_progress flag to prevent race conditions:
        - Set flag BEFORE async allocation (prevents concurrent dispatch attempts)
        - Only set dispatched=True AFTER successful allocation
        - Clear flag on failure so workflow can be retried

        On failure, increments dispatch_attempts and applies exponential backoff.
        After max_dispatch_attempts, the workflow is marked as permanently failed.

        Returns True if dispatch succeeded.
        """
        if pending.job_id in self._cancelling_jobs:
            return False

        if pending.dispatch_in_progress:
            return False
        pending.dispatch_in_progress = True

        try:
            if pending.job_id in self._cancelling_jobs:
                return False

            is_retry = pending.dispatch_attempts > 0

            if is_retry:
                allowed, reason = await self._retry_budget_manager.check_and_consume(
                    pending.job_id, pending.workflow_id
                )
                if not allowed:
                    await self._log_warning(
                        f"Retry budget exhausted for workflow {pending.workflow_id}: {reason}",
                        job_id=pending.job_id,
                        workflow_id=pending.workflow_id,
                    )
                    pending.dispatch_attempts = pending.max_dispatch_attempts
                    return False

            pending.dispatch_attempts += 1
            pending.last_dispatch_attempt = time.monotonic()

            # Allocate cores from worker pool. The allocation budget is
            # capped at 30s regardless of per-job timeout — this is a
            # backpressure cap, not the workflow execution deadline.
            # Phase H2: when ``timeout_seconds_explicit`` is False the
            # wire value is 0 (sentinel); fall back to the 30s cap so
            # we never end up with a zero-budget allocation that
            # immediately fails.
            if submission.timeout_seconds > 0.0:
                allocator_budget = min(submission.timeout_seconds, 30.0)
            else:
                allocator_budget = 30.0
            allocations = await self._worker_pool.allocate_cores(
                cores_needed,
                timeout=allocator_budget,
                excluded_worker_ids=pending.excluded_worker_ids,
            )

            if not allocations:
                self._apply_backoff(pending)
                return False

            if pending.job_id in self._cancelling_jobs:
                for worker_id, worker_cores in allocations:
                    await self._worker_pool.release_cores(worker_id, worker_cores)
                return False

            total_allocated = sum(cores for _, cores in allocations)

            workflow_bytes = cloudpickle.dumps(pending.workflow)

            stored_context = await self._job_manager.get_stored_dispatched_context(
                pending.job_id,
                pending.workflow_id,
            )
            if stored_context is not None:
                context_bytes, layer_version = stored_context
            else:
                context_for_workflow = await self._job_manager.get_context_for_workflow(
                    pending.job_id,
                    pending.workflow_id,
                    pending.dependencies,
                )
                context_bytes = _serialize_context(context_for_workflow)
                layer_version = await self._job_manager.get_layer_version(
                    pending.job_id
                )

            workflow_token = TrackingToken.for_workflow(
                self._datacenter,
                self._manager_id,
                pending.job_id,
                pending.workflow_id,
            )

            # Dispatch to each worker, tracking success/failure for cleanup
            successful_dispatches: list[tuple[str, int]] = []  # (worker_id, cores)
            failed_dispatches: list[tuple[str, int]] = []  # (worker_id, cores)

            dispatch_plans: list[
                tuple[str, int, TrackingToken, WorkflowDispatch]
            ] = []
            for worker_id, worker_cores in allocations:
                # Calculate VUs for this worker
                worker_vus = max(1, int(pending.vus * (worker_cores / total_allocated)))

                # Create sub-workflow token
                sub_token = workflow_token.to_sub_workflow_token(worker_id)

                # Get fence token for at-most-once dispatch (AD-10: incorporate leader term)
                leader_term = self._get_leader_term() if self._get_leader_term else 0
                fence_token = await self._job_manager.get_next_fence_token(
                    pending.job_id, leader_term
                )

                # Phase H2: derive the worker-observed deadline via
                # the canonical override hierarchy (explicit submission
                # > class-level timeout override > default = duration ×
                # multiplier). All dispatch paths share this resolver
                # so deadlines stay consistent across gate, manager,
                # and timeout-strategy layers.
                resolved_timeout_seconds = resolve_worker_deadline_seconds(
                    workflow=pending.workflow,
                    submission_timeout_seconds=submission.timeout_seconds,
                    submission_timeout_explicit=submission.timeout_seconds_explicit,
                    default_multiplier=self._env.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
                )

                dispatch = WorkflowDispatch(
                    job_id=pending.job_id,
                    workflow_id=str(sub_token),
                    workflow=workflow_bytes,
                    context=context_bytes,
                    vus=worker_vus,
                    cores=worker_cores,
                    timeout_seconds=resolved_timeout_seconds,
                    fence_token=fence_token,
                    context_version=layer_version,
                )

                sub_workflow = await self._job_manager.register_sub_workflow(
                    job_id=pending.job_id,
                    workflow_id=pending.workflow_id,
                    worker_id=worker_id,
                    cores_allocated=worker_cores,
                    fence_token=fence_token,
                )
                if sub_workflow is None:
                    await self._worker_pool.release_cores(worker_id, worker_cores)
                    failed_dispatches.append((worker_id, worker_cores))
                    continue

                await self._job_manager.set_sub_workflow_dispatched_context(
                    sub_workflow_token=str(sub_token),
                    context_bytes=context_bytes,
                    layer_version=layer_version,
                )

                dispatch_plans.append((worker_id, worker_cores, sub_token, dispatch))

            dispatch_results = await self._send_dispatch_plans(
                pending,
                dispatch_plans,
            )

            for worker_id, worker_cores, sub_token, success in dispatch_results:
                if success:
                    await self._worker_pool.confirm_allocation(
                        worker_id, worker_cores
                    )
                    successful_dispatches.append((worker_id, worker_cores))
                else:
                    await self._record_failed_dispatch_plan(
                        worker_id=worker_id,
                        worker_cores=worker_cores,
                        sub_token=sub_token,
                        failed_dispatches=failed_dispatches,
                    )

            pending.cores_allocated = sum(cores for _, cores in successful_dispatches)

            # Determine outcome based on dispatch results
            if len(successful_dispatches) == 0:
                # ALL dispatches failed - revert dispatch status and apply backoff
                pending.dispatched = False
                pending.dispatched_at = 0.0
                self._apply_backoff(pending)
                return False

            pending.dispatched = True
            pending.dispatched_at = time.monotonic()

            if len(failed_dispatches) > 0:
                # PARTIAL success - some dispatches succeeded, some failed
                # This is still considered a success, but we log the partial failure
                # The workflow will complete with reduced parallelism
                dispatch_count = len(successful_dispatches) + len(failed_dispatches)
                await self._log_warning(
                    f"Partial dispatch for workflow {pending.workflow_id}: "
                    f"{len(successful_dispatches)}/{dispatch_count} workers succeeded",
                    job_id=pending.job_id,
                    workflow_id=pending.workflow_id,
                )

            return True

        finally:
            # Always clear the in-progress flag
            pending.dispatch_in_progress = False
            self.signal_dispatch()

    def _apply_backoff(self, pending: PendingWorkflow) -> None:
        """Apply exponential backoff after a failed dispatch attempt."""
        # Double the delay for next retry, capped at MAX_RETRY_DELAY
        pending.next_retry_delay = min(
            pending.next_retry_delay * self.BACKOFF_MULTIPLIER,
            self.MAX_RETRY_DELAY,
        )
        # Clear ready state - will be re-signaled after backoff
        pending.clear_ready()

    async def _send_dispatch_plans(
        self,
        pending: PendingWorkflow,
        dispatch_plans: list[tuple[str, int, TrackingToken, WorkflowDispatch]],
    ) -> list[tuple[str, int, TrackingToken, bool]]:
        """Send workflow dispatch plans with bounded concurrency."""
        if not dispatch_plans:
            return []

        semaphore = asyncio.Semaphore(self._max_concurrent_dispatches)

        async def send_one(
            worker_id: str,
            worker_cores: int,
            sub_token: TrackingToken,
            dispatch: WorkflowDispatch,
        ) -> tuple[str, int, TrackingToken, bool]:
            async with semaphore:
                if self._shutting_down or pending.job_id in self._cancelling_jobs:
                    return worker_id, worker_cores, sub_token, False

                try:
                    success = await self._send_dispatch(worker_id, dispatch)
                    return worker_id, worker_cores, sub_token, success
                except asyncio.CancelledError as dispatch_error:
                    if self._shutting_down or pending.job_id in self._cancelling_jobs:
                        raise

                    await self._log_warning(
                        "Dispatch was cancelled by worker-side transport failure "
                        f"for worker {worker_id}: {dispatch_error}",
                        job_id=pending.job_id,
                        workflow_id=pending.workflow_id,
                    )
                    return worker_id, worker_cores, sub_token, False
                except Exception as dispatch_error:
                    await self._log_warning(
                        f"Exception dispatching to worker {worker_id} for "
                        f"workflow {pending.workflow_id}: {dispatch_error}",
                        job_id=pending.job_id,
                        workflow_id=pending.workflow_id,
                    )
                    return worker_id, worker_cores, sub_token, False

        return await asyncio.gather(
            *(send_one(*dispatch_plan) for dispatch_plan in dispatch_plans)
        )

    async def _record_failed_dispatch_plan(
        self,
        *,
        worker_id: str,
        worker_cores: int,
        sub_token: TrackingToken,
        failed_dispatches: list[tuple[str, int]],
    ) -> None:
        await self._worker_pool.release_cores(worker_id, worker_cores)
        await self._job_manager.remove_unstarted_sub_workflow(str(sub_token))
        failed_dispatches.append((worker_id, worker_cores))

    # =========================================================================
    # Event-Driven Dispatch Loop
    # =========================================================================

    async def start_job_dispatch(self, job_id: str, submission: JobSubmission) -> None:
        """
        Start the event-driven dispatch loop for a job.

        This launches a background task that:
        1. Waits for workflows to become ready (dependencies satisfied)
        2. Waits for cores to become available
        3. Dispatches ready workflows as resources allow

        The loop continues until all workflows are dispatched or the job completes.
        """
        if job_id in self._job_dispatch_tasks:
            return  # Already running

        self._job_submissions[job_id] = submission
        task = asyncio.create_task(self._job_dispatch_loop(job_id, submission))
        self._job_dispatch_tasks[job_id] = task

    async def stop_job_dispatch(self, job_id: str) -> None:
        """
        Stop the dispatch loop for a job.

        Called when a job completes or is cancelled.
        """
        task = self._job_dispatch_tasks.pop(job_id, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._job_submissions.pop(job_id, None)

    async def _job_dispatch_loop(self, job_id: str, submission: JobSubmission) -> None:
        """
        Event-driven dispatch loop for a single job.

        Waits on:
        1. Workflow ready events (dependencies satisfied)
        2. Core availability events from WorkerPool
        3. Dispatch trigger events (external signals)

        Exits when:
        - All workflows dispatched
        - Job cancelled/completed
        - Shutdown signaled
        """
        try:
            while not self._shutting_down:
                # Get all workflows still owned by this dispatch queue. A
                # workflow with dispatch_in_progress=True is still active
                # queue state even though it is not currently eligible for
                # another send attempt.
                async with self._pending_lock:
                    job_workflows = [
                        p
                        for p in self._pending.values()
                        if p.job_id == job_id
                    ]
                    job_pending = [p for p in job_workflows if not p.dispatched]

                if not job_workflows:
                    # No more workflows tracked for this job.
                    break

                if not job_pending:
                    # Every tracked workflow has been accepted by a worker.
                    break

                now = time.monotonic()
                allocatable_pending = [
                    p
                    for p in job_pending
                    if (
                        not p.dispatch_in_progress
                        and p.dispatch_attempts < p.max_dispatch_attempts
                        and p.dependencies <= p.completed_dependencies
                        and (
                            p.dispatch_attempts == 0
                            or now - p.last_dispatch_attempt >= p.next_retry_delay
                        )
                    )
                ]
                backoff_delays = [
                    p.next_retry_delay - (now - p.last_dispatch_attempt)
                    for p in job_pending
                    if (
                        not p.dispatch_in_progress
                        and p.dispatch_attempts > 0
                        and p.dispatch_attempts < p.max_dispatch_attempts
                        and p.dependencies <= p.completed_dependencies
                        and now - p.last_dispatch_attempt < p.next_retry_delay
                    )
                ]

                # Build list of events to wait on. Core availability is
                # relevant only when at least one workflow can allocate
                # immediately; otherwise a healthy idle worker would make
                # wait_for_cores() return in a tight loop while the real
                # state is in-flight dispatch or retry backoff.
                ready_events = [
                    p.ready_event.wait()
                    for p in job_pending
                    if not p.dispatch_in_progress
                ]
                wait_coroutines = [*ready_events, self._wait_dispatch_trigger()]
                if allocatable_pending:
                    wait_coroutines.append(
                        self._worker_pool.wait_for_cores(timeout=5.0)
                    )

                wait_timeout = 5.0
                positive_backoff_delays = [
                    delay for delay in backoff_delays if delay > 0.0
                ]
                if positive_backoff_delays:
                    wait_timeout = min(wait_timeout, min(positive_backoff_delays))

                # Wait for any event with a timeout for periodic checks
                tasks = [
                    asyncio.create_task(coro)
                    for coro in wait_coroutines
                ]
                try:
                    done, pending = await asyncio.wait(
                        tasks,
                        timeout=wait_timeout,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    # Cancel pending tasks and suppress CancelledError
                    for task in pending:
                        task.cancel()
                    # Await cancelled tasks to ensure cleanup completes
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)

                    # Retrieve results from done tasks to avoid warnings
                    # (we don't need the results, just need to retrieve them)
                    for task in done:
                        try:
                            task.result()
                        except Exception:
                            pass  # Ignore any exceptions from done tasks

                except asyncio.CancelledError:
                    # On cancellation, clean up all tasks
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise

                # Attempt dispatch regardless of which event fired
                # This handles the case where multiple events fire together
                await self.try_dispatch(job_id, submission)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log_error(
                f"Dispatch loop error for job {job_id}: {e}",
                job_id=job_id,
            )
        finally:
            # Clean up
            self._job_dispatch_tasks.pop(job_id, None)

    async def _wait_dispatch_trigger(self) -> None:
        """Wait for the dispatch trigger event."""
        await self._dispatch_trigger.wait()
        self._dispatch_trigger.clear()

    def signal_dispatch(self) -> None:
        """
        Signal that dispatch should be attempted.

        Called when:
        - Cores become available
        - A workflow completes (dependencies satisfied)
        - Retry timer expires
        """
        self._dispatch_trigger.set()

    def signal_cores_available(self) -> None:
        """
        Signal that cores have become available.

        This triggers dispatch attempts for workflows waiting on resources.
        """
        # Signal all pending workflows to re-check readiness
        for pending in self._pending.values():
            if not pending.dispatched:
                pending.check_and_signal_ready()

        # Also trigger the global dispatch event
        self._dispatch_trigger.set()

    async def shutdown(self) -> None:
        """
        Shutdown all dispatch loops gracefully.

        Cancels all active dispatch tasks and waits for them to complete.
        """
        self._shutting_down = True
        self._dispatch_trigger.set()  # Wake up any waiting loops

        # Cancel all job dispatch tasks
        tasks = list(self._job_dispatch_tasks.values())
        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._job_dispatch_tasks.clear()
        self._job_submissions.clear()

    # =========================================================================
    # Timeout and Eviction
    # =========================================================================

    async def check_timeouts(self) -> list[tuple[str, str, str]]:
        """
        Check for timed-out workflows and workflows that exceeded max retries.

        Should be called periodically (e.g., every 30 seconds) by the manager.

        Returns list of (job_id, workflow_id, reason) for evicted/failed workflows.
        """
        now = time.monotonic()
        evicted: list[tuple[str, str, str]] = []
        failed: list[tuple[str, str, str]] = []

        async with self._pending_lock:
            keys_to_remove = []

            for key, pending in self._pending.items():
                # Check for timeout based on registration time
                # This catches workflows stuck waiting for dependencies or cores
                age = now - pending.registered_at
                if age > pending.timeout_seconds:
                    if pending.dispatched:
                        reason = f"Dispatched workflow timed out after {age:.1f}s"
                    else:
                        reason = f"Pending workflow timed out after {age:.1f}s"
                    keys_to_remove.append(
                        (key, pending.job_id, pending.workflow_id, reason, "evicted")
                    )
                    continue

                # Check for exceeded max retries
                if (
                    pending.dispatch_attempts >= pending.max_dispatch_attempts
                    and not pending.dispatched
                ):
                    reason = (
                        f"Dispatch failed after {pending.dispatch_attempts} attempts"
                    )
                    keys_to_remove.append(
                        (key, pending.job_id, pending.workflow_id, reason, "failed")
                    )

            # Remove workflows
            for key, job_id, workflow_id, reason, failure_type in keys_to_remove:
                self._pending.pop(key, None)
                if failure_type == "evicted":
                    evicted.append((job_id, workflow_id, reason))
                else:
                    failed.append((job_id, workflow_id, reason))

        # Call eviction callback outside the lock
        if self._on_workflow_evicted:
            for job_id, workflow_id, reason in evicted:
                try:
                    await self._on_workflow_evicted(job_id, workflow_id, reason)
                except Exception as e:
                    await self._log_error(
                        f"Exception in eviction callback for workflow {workflow_id}: {e}",
                        job_id=job_id,
                        workflow_id=workflow_id,
                    )

        # Call dispatch failed callback outside the lock
        if self._on_dispatch_failed:
            for job_id, workflow_id, reason in failed:
                try:
                    await self._on_dispatch_failed(job_id, workflow_id, reason)
                except Exception as e:
                    await self._log_error(
                        f"Exception in dispatch_failed callback for workflow {workflow_id}: {e}",
                        job_id=job_id,
                        workflow_id=workflow_id,
                    )

        return evicted + failed

    def get_pending_count(self, job_id: str | None = None) -> int:
        """Get count of pending workflows (optionally filtered by job_id)."""
        if job_id is None:
            return len(self._pending)
        return sum(1 for p in self._pending.values() if p.job_id == job_id)

    def get_dispatched_count(self, job_id: str | None = None) -> int:
        """Get count of dispatched workflows (optionally filtered by job_id)."""
        if job_id is None:
            return sum(1 for p in self._pending.values() if p.dispatched)
        return sum(
            1 for p in self._pending.values() if p.job_id == job_id and p.dispatched
        )

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def cleanup_job(self, job_id: str) -> None:
        """
        Remove all pending workflows for a job and stop its dispatch loop.

        Properly cleans up:
        - Stops the dispatch loop task for this job
        - Clears all pending workflow entries
        - Clears ready_events to unblock any waiters
        - Clears retry budget state (AD-44)
        """
        await self.stop_job_dispatch(job_id)

        await self._retry_budget_manager.cleanup(job_id)

        async with self._pending_lock:
            keys_to_remove = [
                key for key in self._pending if key.startswith(f"{job_id}:")
            ]
            for key in keys_to_remove:
                pending = self._pending.pop(key, None)
                if pending:
                    pending.ready_event.set()

        self._cancelling_jobs.discard(job_id)

    async def cancel_pending_workflows(self, job_id: str) -> list[str]:
        """
        Cancel all pending workflows for a job (AD-20 job cancellation).

        Removes workflows from the pending queue before they can be dispatched.
        This is critical for robust job cancellation - pending workflows must
        be removed BEFORE cancelling running workflows to prevent race conditions
        where a pending workflow gets dispatched during cancellation.

        Args:
            job_id: The job ID whose pending workflows should be cancelled

        Returns:
            List of workflow IDs that were cancelled from the pending queue
        """
        self._cancelling_jobs.add(job_id)
        cancelled_workflow_ids: list[str] = []

        async with self._pending_lock:
            # Find all pending workflows for this job
            keys_to_remove = [
                key for key in self._pending if key.startswith(f"{job_id}:")
            ]

            # Remove each pending workflow
            for key in keys_to_remove:
                pending = self._pending.pop(key, None)
                if pending:
                    # Extract workflow_id from key (format: "job_id:workflow_id")
                    workflow_id = key.split(":", 1)[1]
                    cancelled_workflow_ids.append(workflow_id)

                    # Set ready event to unblock any waiters
                    pending.ready_event.set()

            if cancelled_workflow_ids:
                await self._log_info(
                    f"Cancelled {len(cancelled_workflow_ids)} pending workflows for job cancellation",
                    job_id=job_id,
                )

        return cancelled_workflow_ids

    async def cancel_pending_workflows_by_ids(
        self, job_id: str, workflow_ids: list[str]
    ) -> list[str]:
        """
        Cancel specific pending workflows by their IDs (for single workflow cancellation).

        Used when cancelling a workflow and its dependents - only removes
        workflows from the pending queue if they are in the provided list.

        Args:
            job_id: The job ID
            workflow_ids: List of specific workflow IDs to cancel

        Returns:
            List of workflow IDs that were actually cancelled from the pending queue
        """
        cancelled_workflow_ids: list[str] = []

        async with self._pending_lock:
            # Find pending workflows matching the provided IDs
            for workflow_id in workflow_ids:
                key = f"{job_id}:{workflow_id}"
                pending = self._pending.pop(key, None)

                if pending:
                    cancelled_workflow_ids.append(workflow_id)

                    # Set ready event to unblock any waiters
                    pending.ready_event.set()

            if cancelled_workflow_ids:
                await self._log_info(
                    f"Cancelled {len(cancelled_workflow_ids)} specific pending workflows",
                    job_id=job_id,
                )

        return cancelled_workflow_ids

    async def get_job_dependency_graph(self, job_id: str) -> dict[str, set[str]]:
        """
        Get the dependency graph for all workflows in a job.

        Returns a dict mapping workflow_id -> set of dependency workflow_ids.
        This is needed by the Manager's failure handler to find dependents
        when rescheduling workflows after worker failure (AD-33).

        Args:
            job_id: The job ID

        Returns:
            Dict mapping workflow_id to its set of dependencies.
            Empty dict if job not found or no workflows.
        """
        dependency_graph: dict[str, set[str]] = {}

        async with self._pending_lock:
            # Extract dependencies from all pending workflows for this job
            for key, pending in self._pending.items():
                if pending.job_id == job_id:
                    # Copy the set to avoid external mutation
                    dependency_graph[pending.workflow_id] = pending.dependencies.copy()

        return dependency_graph

    async def add_pending_workflow(
        self,
        job_id: str,
        workflow_id: str,
        workflow_name: str,
        workflow: Workflow,
        vus: int,
        priority: StagePriority,
        is_test: bool,
        dependencies: set[str],
        timeout_seconds: float,
    ) -> None:
        """
        Add a workflow back to the pending queue (AD-33 retry mechanism).

        Used during failure recovery to re-queue failed workflows in dependency order.
        The workflow will be dispatched when its dependencies are satisfied and cores
        are available.

        Args:
            job_id: The job ID
            workflow_id: The workflow ID
            workflow_name: Human-readable workflow name
            workflow: The workflow instance to dispatch
            vus: Virtual users for this workflow
            priority: Dispatch priority
            is_test: Whether this is a test workflow
            dependencies: Set of workflow IDs this workflow depends on
            timeout_seconds: Timeout for this workflow
        """
        now = time.monotonic()
        key = f"{job_id}:{workflow_id}"

        async with self._pending_lock:
            # Check if already pending (idempotent)
            if key in self._pending:
                await self._log_debug(
                    f"Workflow {workflow_id} already pending, skipping add",
                    job_id=job_id,
                    workflow_id=workflow_id,
                )
                return

            # Create new pending workflow entry
            pending = PendingWorkflow(
                job_id=job_id,
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                workflow=workflow,
                vus=vus,
                priority=priority,
                is_test=is_test,
                dependencies=dependencies,
                registered_at=now,
                timeout_seconds=timeout_seconds,
                next_retry_delay=self.INITIAL_RETRY_DELAY,
                max_dispatch_attempts=self._max_dispatch_attempts,
            )

            self._pending[key] = pending

            # Check if ready for immediate dispatch
            pending.check_and_signal_ready()

            await self._log_info(
                f"Added workflow {workflow_id} back to pending queue for retry",
                job_id=job_id,
                workflow_id=workflow_id,
            )

        self.signal_dispatch()

    async def requeue_workflow(
        self,
        sub_workflow_token: str,
        excluded_worker_id: str | None = None,
    ) -> bool:
        try:
            token = TrackingToken.parse(sub_workflow_token)
        except ValueError:
            return False

        if token.workflow_id is None:
            return False

        job_id = token.job_id
        workflow_id = token.workflow_id
        key = f"{job_id}:{workflow_id}"

        async with self._pending_lock:
            pending = self._pending.get(key)
            if pending is None:
                return False
            worker_id_to_exclude = excluded_worker_id or token.worker_id
            if worker_id_to_exclude:
                pending.excluded_worker_ids.add(worker_id_to_exclude)
            pending.dispatched = False
            pending.dispatch_in_progress = False
            pending.dispatched_at = 0.0
            pending.dispatch_attempts = 0
            pending.next_retry_delay = self.INITIAL_RETRY_DELAY
            pending.check_and_signal_ready()
            self.signal_dispatch()

        # The job's dispatch loop exits once every workflow has been
        # dispatched once — there's no consumer for the trigger we
        # just signalled if the loop has already returned. Worker-
        # death reassignment can land long after that point, so we
        # have to restart the loop ourselves. ``start_job_dispatch``
        # is idempotent (no-op if a task is already tracked), which
        # keeps this safe under racing requeues. Done outside the
        # pending lock because ``start_job_dispatch`` creates a task
        # that takes the same lock.
        submission = self._job_submissions.get(job_id)
        if submission is not None:
            existing_task = self._job_dispatch_tasks.get(job_id)
            if existing_task is None or existing_task.done():
                await self.start_job_dispatch(job_id, submission)
        return True

    async def unassign_workflow(self, job_id: str, workflow_id: str) -> bool:
        key = f"{job_id}:{workflow_id}"
        async with self._pending_lock:
            if pending := self._pending.get(key):
                pending.dispatched = False
                pending.dispatch_in_progress = False
                pending.dispatched_at = 0.0
                pending.clear_ready()
                return True
            return False

    async def mark_workflow_assigned(self, job_id: str, workflow_id: str) -> bool:
        key = f"{job_id}:{workflow_id}"
        async with self._pending_lock:
            if pending := self._pending.get(key):
                pending.dispatched = True
                pending.dispatch_in_progress = False
                pending.dispatched_at = time.monotonic()
                pending.clear_ready()
                return True
            return False

    # =========================================================================
    # Logging Helpers
    # =========================================================================

    def _get_log_context(self, job_id: str = "", workflow_id: str = "") -> dict:
        """Get common context fields for logging."""
        return {
            "manager_id": self._manager_id,
            "datacenter": self._datacenter,
            "job_id": job_id,
            "workflow_id": workflow_id,
            "pending_count": len(self._pending),
            "dispatched_count": sum(1 for p in self._pending.values() if p.dispatched),
        }

    async def _log_trace(
        self, message: str, job_id: str = "", workflow_id: str = ""
    ) -> None:
        """Log a trace-level message."""
        await self._logger.log(
            DispatcherTrace(
                message=message, **self._get_log_context(job_id, workflow_id)
            )
        )

    async def _log_debug(
        self, message: str, job_id: str = "", workflow_id: str = ""
    ) -> None:
        """Log a debug-level message."""
        await self._logger.log(
            DispatcherDebug(
                message=message, **self._get_log_context(job_id, workflow_id)
            )
        )

    async def _log_info(
        self, message: str, job_id: str = "", workflow_id: str = ""
    ) -> None:
        """Log an info-level message."""
        await self._logger.log(
            DispatcherInfo(
                message=message, **self._get_log_context(job_id, workflow_id)
            )
        )

    async def _log_warning(
        self, message: str, job_id: str = "", workflow_id: str = ""
    ) -> None:
        """Log a warning-level message."""
        await self._logger.log(
            DispatcherWarning(
                message=message, **self._get_log_context(job_id, workflow_id)
            )
        )

    async def _log_error(
        self, message: str, job_id: str = "", workflow_id: str = ""
    ) -> None:
        """Log an error-level message."""
        await self._logger.log(
            DispatcherError(
                message=message, **self._get_log_context(job_id, workflow_id)
            )
        )

    async def _log_critical(
        self, message: str, job_id: str = "", workflow_id: str = ""
    ) -> None:
        """Log a critical-level message."""
        await self._logger.log(
            DispatcherCritical(
                message=message, **self._get_log_context(job_id, workflow_id)
            )
        )
