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
from hyperscale.distributed_rewrite.jobs.job_manager import JobManager
from hyperscale.distributed_rewrite.models import TrackingToken
from hyperscale.distributed_rewrite.jobs.worker_pool import WorkerPool
from hyperscale.distributed_rewrite.jobs.logging_models import (
    DispatcherTrace,
    DispatcherDebug,
    DispatcherInfo,
    DispatcherWarning,
    DispatcherError,
    DispatcherCritical,
)
from hyperscale.logging import Logger


class WorkflowDispatcher:
    """
    Manages workflow dispatch to workers.

    Coordinates with JobManager for state tracking and WorkerPool
    for resource allocation. Handles dependency-based eager dispatch.
    """

    # Exponential backoff constants
    INITIAL_RETRY_DELAY = 1.0  # seconds
    MAX_RETRY_DELAY = 60.0     # seconds
    BACKOFF_MULTIPLIER = 2.0   # double delay each retry

    def __init__(
        self,
        job_manager: JobManager,
        worker_pool: WorkerPool,
        send_dispatch: Callable[[str, WorkflowDispatch], Coroutine[Any, Any, bool]],
        datacenter: str,
        manager_id: str,
        default_timeout_seconds: float = 300.0,
        max_dispatch_attempts: int = 5,
        on_workflow_evicted: Callable[[str, str, str], Coroutine[Any, Any, None]] | None = None,
        on_dispatch_failed: Callable[[str, str, str], Coroutine[Any, Any, None]] | None = None,
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
        self._logger = Logger()

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
                # Handle DependentWorkflow specially to preserve name and get dependencies
                dependencies: list[str] = []
                if isinstance(wf, DependentWorkflow):
                    dependencies = wf.dependencies
                    name = wf.dependent_workflow.__name__
                    instance = wf.dependent_workflow()
                else:
                    name = wf.__name__
                    instance = wf()

                # Generate workflow ID
                workflow_id = f"wf-{i:04d}"
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
        priority = getattr(workflow, 'priority', None)
        if isinstance(priority, StagePriority):
            return priority
        return StagePriority.AUTO

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
        2. AUTO priority workflows split remaining cores equally (minimum 1 core each)
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
                    share = pending.vus / total_vus if total_vus > 0 else 1 / len(explicit)
                    cores = max(1, int(total_cores * share))
                    cores = min(cores, remaining_cores)

                allocations.append((pending, cores))
                remaining_cores -= cores

        # Step 2: Split remaining cores equally among AUTO workflows (min 1 core each)
        if auto and remaining_cores > 0:
            # Each AUTO workflow needs minimum 1 core
            # Only allocate as many workflows as we have cores for
            num_auto_to_allocate = min(len(auto), remaining_cores)
            cores_per_auto = remaining_cores // num_auto_to_allocate
            leftover = remaining_cores - (cores_per_auto * num_auto_to_allocate)

            for i, pending in enumerate(auto):
                if i >= num_auto_to_allocate:
                    # No more cores - remaining AUTO workflows stay pending
                    break

                # Give one extra core to first workflows if there's leftover
                cores = cores_per_auto + (1 if i < leftover else 0)

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
        # Mark dispatch in progress (atomic check-and-set would be better but
        # this runs under dispatch_lock so we're safe)
        if pending.dispatch_in_progress:
            return False  # Another dispatch is already in progress
        pending.dispatch_in_progress = True

        try:
            # Track this dispatch attempt
            pending.dispatch_attempts += 1
            pending.last_dispatch_attempt = time.monotonic()

            # Allocate cores from worker pool
            allocations = await self._worker_pool.allocate_cores(
                cores_needed,
                timeout=min(submission.timeout_seconds, 30.0),  # Don't wait too long for allocation
            )

            if not allocations:
                # No cores available - apply backoff and allow retry
                self._apply_backoff(pending)
                return False

            # Allocation succeeded - NOW mark as dispatched
            pending.dispatched = True
            pending.dispatched_at = time.monotonic()
            pending.cores_allocated = cores_needed

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

            # Dispatch to each worker, tracking success/failure for cleanup
            successful_dispatches: list[tuple[str, int]] = []  # (worker_id, cores)
            failed_dispatches: list[tuple[str, int]] = []      # (worker_id, cores)

            for worker_id, worker_cores in allocations:
                # Calculate VUs for this worker
                worker_vus = max(1, int(pending.vus * (worker_cores / total_allocated)))

                # Create sub-workflow token
                sub_token = workflow_token.to_sub_workflow_token(worker_id)

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

                # Send dispatch FIRST, only register sub-workflow on success
                try:
                    success = await self._send_dispatch(worker_id, dispatch)
                    if success:
                        # Register sub-workflow AFTER successful dispatch
                        # This prevents orphaned sub-workflow registrations
                        await self._job_manager.register_sub_workflow(
                            job_id=pending.job_id,
                            workflow_id=pending.workflow_id,
                            worker_id=worker_id,
                            cores_allocated=worker_cores,
                        )
                        await self._worker_pool.confirm_allocation(worker_id, worker_cores)
                        successful_dispatches.append((worker_id, worker_cores))
                    else:
                        await self._worker_pool.release_cores(worker_id, worker_cores)
                        failed_dispatches.append((worker_id, worker_cores))
                except Exception as e:
                    await self._log_warning(
                        f"Exception dispatching to worker {worker_id} for workflow {pending.workflow_id}: {e}",
                        job_id=pending.job_id,
                        workflow_id=pending.workflow_id,
                    )
                    await self._worker_pool.release_cores(worker_id, worker_cores)
                    failed_dispatches.append((worker_id, worker_cores))

            # Determine outcome based on dispatch results
            if len(successful_dispatches) == 0:
                # ALL dispatches failed - revert dispatch status and apply backoff
                pending.dispatched = False
                pending.dispatched_at = 0.0
                self._apply_backoff(pending)
                return False

            if len(failed_dispatches) > 0:
                # PARTIAL success - some dispatches succeeded, some failed
                # This is still considered a success, but we log the partial failure
                # The workflow will complete with reduced parallelism
                await self._log_warning(
                    f"Partial dispatch for workflow {pending.workflow_id}: "
                    f"{len(successful_dispatches)}/{len(successful_dispatches) + len(failed_dispatches)} workers succeeded",
                    job_id=pending.job_id,
                    workflow_id=pending.workflow_id,
                )

            return True

        finally:
            # Always clear the in-progress flag
            pending.dispatch_in_progress = False

    def _apply_backoff(self, pending: PendingWorkflow) -> None:
        """Apply exponential backoff after a failed dispatch attempt."""
        # Double the delay for next retry, capped at MAX_RETRY_DELAY
        pending.next_retry_delay = min(
            pending.next_retry_delay * self.BACKOFF_MULTIPLIER,
            self.MAX_RETRY_DELAY,
        )
        # Clear ready state - will be re-signaled after backoff
        pending.clear_ready()

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
                # Get all pending workflows for this job
                async with self._pending_lock:
                    job_pending = [
                        p for p in self._pending.values()
                        if p.job_id == job_id and not p.dispatched
                    ]

                if not job_pending:
                    # No more pending workflows for this job
                    break

                # Build list of events to wait on
                # We wait on ANY workflow becoming ready OR cores becoming available
                ready_events = [p.ready_event.wait() for p in job_pending if not p.dispatched]
                cores_event = self._worker_pool.wait_for_cores(timeout=5.0)
                trigger_event = self._wait_dispatch_trigger()

                if not ready_events:
                    # All workflows either dispatched or failed
                    break

                # Wait for any event with a timeout for periodic checks
                tasks = [asyncio.create_task(coro) for coro in [*ready_events, cores_event, trigger_event]]
                try:
                    done, pending = await asyncio.wait(
                        tasks,
                        timeout=5.0,
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
                    keys_to_remove.append((key, pending.job_id, pending.workflow_id, reason, "evicted"))
                    continue

                # Check for exceeded max retries
                if pending.dispatch_attempts >= pending.max_dispatch_attempts and not pending.dispatched:
                    reason = f"Dispatch failed after {pending.dispatch_attempts} attempts"
                    keys_to_remove.append((key, pending.job_id, pending.workflow_id, reason, "failed"))

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
        return sum(1 for p in self._pending.values() if p.job_id == job_id and p.dispatched)

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
        """
        # Stop the dispatch loop first
        await self.stop_job_dispatch(job_id)

        # Clear pending workflows
        async with self._pending_lock:
            keys_to_remove = [
                key for key in self._pending
                if key.startswith(f"{job_id}:")
            ]
            for key in keys_to_remove:
                pending = self._pending.pop(key, None)
                if pending:
                    # Set the ready event to unblock any waiters, then clear
                    pending.ready_event.set()

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

    async def _log_trace(self, message: str, job_id: str = "", workflow_id: str = "") -> None:
        """Log a trace-level message."""
        await self._logger.log(DispatcherTrace(message=message, **self._get_log_context(job_id, workflow_id)))

    async def _log_debug(self, message: str, job_id: str = "", workflow_id: str = "") -> None:
        """Log a debug-level message."""
        await self._logger.log(DispatcherDebug(message=message, **self._get_log_context(job_id, workflow_id)))

    async def _log_info(self, message: str, job_id: str = "", workflow_id: str = "") -> None:
        """Log an info-level message."""
        await self._logger.log(DispatcherInfo(message=message, **self._get_log_context(job_id, workflow_id)))

    async def _log_warning(self, message: str, job_id: str = "", workflow_id: str = "") -> None:
        """Log a warning-level message."""
        await self._logger.log(DispatcherWarning(message=message, **self._get_log_context(job_id, workflow_id)))

    async def _log_error(self, message: str, job_id: str = "", workflow_id: str = "") -> None:
        """Log an error-level message."""
        await self._logger.log(DispatcherError(message=message, **self._get_log_context(job_id, workflow_id)))

    async def _log_critical(self, message: str, job_id: str = "", workflow_id: str = "") -> None:
        """Log a critical-level message."""
        await self._logger.log(DispatcherCritical(message=message, **self._get_log_context(job_id, workflow_id)))
