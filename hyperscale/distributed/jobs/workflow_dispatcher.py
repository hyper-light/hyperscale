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
import inspect
import traceback
from types import MappingProxyType
from typing import Awaitable, Callable, Mapping

import cloudpickle
import networkx

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import Hook, HookType
from hyperscale.distributed.jobs.dispatch_outcome import (
    BUDGETED_DISPATCH_OUTCOMES,
    DispatchOutcome,
)
from hyperscale.distributed.jobs.workflow_dependencies import workflow_name
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
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.distributed.workflow import WorkflowState
from hyperscale.logging import Logger

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


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
        send_dispatch: Callable[
            [str, WorkflowDispatch],
            Awaitable[tuple[DispatchOutcome, str]],
        ],
        datacenter: str,
        manager_id: str,
        task_runner: TaskRunner,
        on_dispatch_exhausted: Callable[[str, str, str], Awaitable[None]],
        stop_dispatched_plans: Callable[
            [str, list[tuple[str, str]]], Awaitable[None]
        ],
        on_dispatch_state_registered: Callable[[str], Awaitable[bool]]
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
                          Takes (worker_node_id, dispatch) and returns how the
                          worker answered, with the answer's detail
            datacenter: Datacenter identifier
            manager_id: This manager's node ID
            task_runner: Runs a workflow's terminal dispatch failure outside
                         the dispatch pass that decided it
            on_dispatch_exhausted: Fails for good a workflow whose dispatch
                                   failed until its retry budget was spent;
                                   takes (job_id, workflow_id, reason)
            stop_dispatched_plans: Stops sub-workflows workers took after their
                                   workflow was cancelled during the dispatch;
                                   takes (job_id, [(sub_workflow_token, worker_id)])
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
        self._task_runner = task_runner
        self._on_dispatch_exhausted = on_dispatch_exhausted
        self._stop_dispatched_plans = stop_dispatched_plans
        self._on_dispatch_state_registered = on_dispatch_state_registered
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
            total=submission.retry_budget,
            per_workflow=submission.retry_budget_per_workflow,
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
                name = workflow_name(instance)
                vus = (
                    instance.vus
                    if instance.vus and instance.vus > 0
                    else submission.vus
                )

                # Store for graph building
                workflow_by_id[workflow_id] = (name, instance, vus)
                priorities[workflow_id] = self._get_workflow_priority(instance)
                is_test[workflow_id] = self._is_test_workflow(instance)

                graph.add_node(workflow_id)

            except Exception as e:
                # Registration failed - job should be marked failed by caller
                await self._log_error(
                    f"Failed to register workflow {workflow_id} for job {job_id}: {e}",
                    job_id=job_id,
                    workflow_id=workflow_id,
                )
                return False

        # Dependencies link once every workflow is registered, so the
        # order the client listed them in does not matter. The manager
        # validated the job's dependencies before creating it; a name
        # that still does not resolve is refused, never dropped.
        workflow_id_by_name = {
            name: workflow_id for workflow_id, (name, _, _) in workflow_by_id.items()
        }
        for workflow_id, dependencies, _ in workflows:
            for dependency_name in dependencies:
                if (dependency_id := workflow_id_by_name.get(dependency_name)) is None:
                    await self._log_error(
                        f"Workflow {workflow_id} of job {job_id} depends on "
                        f"{dependency_name!r}, which the job does not contain",
                        job_id=job_id,
                        workflow_id=workflow_id,
                    )
                    return False

                graph.add_edge(dependency_id, workflow_id)

        # Register with JobManager once the graph is linked, each workflow
        # with the dependencies it waits on: the job's own record of the
        # graph, which a failure cascades along.
        for workflow_id, (name, instance, _vus) in workflow_by_id.items():
            try:
                await self._job_manager.register_workflow(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    name=name,
                    workflow=instance,
                    dependency_workflow_ids=frozenset(graph.predecessors(workflow_id)),
                    is_test=is_test[workflow_id],
                )
            except Exception as e:
                # Registration failed - job should be marked failed by caller
                await self._log_error(
                    f"Failed to register workflow {workflow_id} for job {job_id}: {e}",
                    job_id=job_id,
                    workflow_id=workflow_id,
                )
                return False

        # Register pending workflows
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
                    next_retry_delay=self.INITIAL_RETRY_DELAY,
                )
                self._pending[key] = pending

                # Signal workflows with no dependencies as ready immediately
                pending.check_and_signal_ready()

        return True

    def _get_workflow_priority(self, workflow: Workflow) -> StagePriority:
        """Determine dispatch priority for a workflow."""
        priority = getattr(workflow, "priority", None)
        if isinstance(priority, StagePriority):
            return priority
        return StagePriority.AUTO

    def _is_test_workflow(self, workflow: Workflow) -> bool:
        """A test workflow drives load: one of its hooks is a TEST hook --
        the rule core's runners apply. Its name is no evidence (a
        "LatestMetrics" workflow is not a test; a "Checkout" one may be)."""
        return any(
            hook.hook_type == HookType.TEST
            for _, hook in inspect.getmembers(
                workflow,
                predicate=lambda member: isinstance(member, Hook),
            )
        )

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

    async def remove_pending_workflows(
        self,
        job_id: str,
        workflow_ids: list[str],
    ) -> None:
        """
        Drop workflows that failed for good -- a failed workflow and the
        dependents its failure cascaded to -- from the dispatch queue, so
        none of them is ever dispatched and the job's dispatch loop exits
        once nothing it holds can still run. A removed entry's ready event
        is set, as ``cleanup_job`` does, so the loop wakes and re-checks.
        """
        async with self._pending_lock:
            for workflow_id in workflow_ids:
                if (pending := self._pending.pop(f"{job_id}:{workflow_id}", None)) is not None:
                    pending.ready_event.set()

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

        Passes hold no lock: the ready set, the capacity and the split are
        read in one synchronous step, the pool reserves cores atomically,
        and a workflow is claimed (PENDING -> DISPATCHED) exactly once, so a
        concurrent pass skips it or gives back what it reserved. A lock held
        across a pass held every job's dispatch behind the slowest answer
        any one pass was waiting on.
        """
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
        """Get workflows ready for dispatch: PENDING, dependencies satisfied,
        backoff elapsed."""
        now = _DEFAULT_CLOCK.monotonic()
        lifecycle = self._job_manager.workflow_lifecycle
        ready = []
        for key, pending in self._pending.items():
            if pending.job_id != job_id:
                continue
            if lifecycle.get_state(job_id, pending.workflow_id) != WorkflowState.PENDING:
                continue

            # Retry backoff via the ONE deadline contract (sub-epsilon
            # remainders count as expired — see PendingWorkflow's
            # backoff helpers and protocol.time_quantum).
            if not pending.is_retry_backoff_expired(now):
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
        Dispatch one PENDING workflow to workers (AD-54, AD-44).

        Waiting for capacity is not a failed attempt. When the pool has no
        cores to allocate, or no worker offered takes the workflow because
        each refused for readiness, is no longer registered, or was never
        sent to (shutdown, the job being cancelled), the workflow returns to
        PENDING and dispatches on a later capacity event -- spending no
        retry budget and no backoff; the job's deadline bounds the wait
        (AD-34).

        An attempt no worker took because the dispatch could not be
        delivered (UNREACHABLE) or because workers refused the workflow
        itself (REJECTED) failed: it spends one unit of the workflow's
        retry budget and backs off exponentially. Once the budget is spent
        the workflow fails for good, with the attempt's cause, and its
        dependents fail with it.

        The workflow is claimed (PENDING -> DISPATCHED) once cores are
        allocated and before anything is sent, so a cancellation racing the
        send sees it dispatched; an attempt no worker took sends it back to
        PENDING through the retry chain. It turns RUNNING on its first
        progress report or result. A plan whose answer was lost but whose
        worker already reported progress or a result for it counts as
        taken: the evidence outranks the answer.

        Returns True when at least one worker took the workflow.
        """
        lifecycle = self._job_manager.workflow_lifecycle
        if lifecycle.get_state(pending.job_id, pending.workflow_id) != WorkflowState.PENDING:
            return False

        attempt_started_at = _DEFAULT_CLOCK.monotonic()

        # One attempt, never a wait: the job's dispatch loop waits for
        # capacity, between passes -- a wait here stalled every pass queued
        # behind this one. An attempt that got no cores changed nothing and
        # signals nothing.
        workflow_token = TrackingToken.for_workflow(
            self._datacenter,
            self._manager_id,
            pending.job_id,
            pending.workflow_id,
        )
        # Each worker's share is reserved under the dispatch it will be
        # sent as, until a report from the worker shows that dispatch.
        allocations = await self._worker_pool.allocate_cores(
            cores_needed,
            excluded_worker_ids=pending.excluded_worker_ids,
            job_id=pending.job_id,
            dispatch_token_for=lambda worker_id: str(workflow_token.to_sub_workflow_token(worker_id)),
        )
        if not allocations:
            return False

        try:
            if not await self._job_manager.claim_workflow_for_dispatch(
                pending.job_id, pending.workflow_id
            ):
                for worker_id, _worker_cores in allocations:
                    await self._worker_pool.release_cores(
                        worker_id, str(workflow_token.to_sub_workflow_token(worker_id))
                    )
                return False

            taken_plans: list[tuple[str, str]] = []
            delivery_failures: list[str] = []
            dispatch_plans: list[
                tuple[str, int, TrackingToken, WorkflowDispatch]
            ] = []
            released_worker_ids: set[str] = set()
            try:
                total_allocated = sum(cores for _, cores in allocations)

                workflow_bytes = cloudpickle.dumps(pending.workflow)

                stored_context = await self._job_manager.get_stored_dispatched_context(
                    pending.job_id,
                    pending.workflow_id,
                )
                if stored_context is not None:
                    context_bytes, layer_version = stored_context
                else:
                    context_bytes = _serialize_context(
                        await self._job_manager.get_job_context(pending.job_id)
                    )
                    layer_version = await self._job_manager.get_layer_version(
                        pending.job_id
                    )

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
                        released_worker_ids.add(worker_id)
                        await self._worker_pool.release_cores(worker_id, str(sub_token))
                        continue

                    dispatch_plans.append((worker_id, worker_cores, sub_token, dispatch))
                    await self._job_manager.set_sub_workflow_dispatched_context(
                        sub_workflow_token=str(sub_token),
                        context_bytes=context_bytes,
                        layer_version=layer_version,
                    )

            except Exception as preparation_error:
                # Preparing the dispatch raised -- the workflow or its context
                # would not serialize, say. Nothing was sent: everything the
                # attempt holds is given back, and it failed with the error
                # as its cause (it surfaces with the workflow's failure).
                for worker_id, _worker_cores in allocations:
                    if worker_id not in released_worker_ids:
                        await self._worker_pool.release_cores(
                            worker_id, str(workflow_token.to_sub_workflow_token(worker_id))
                        )
                for _worker_id, _worker_cores, sub_token, _dispatch in dispatch_plans:
                    await self._job_manager.remove_unstarted_sub_workflow(str(sub_token))
                await self._log_error(
                    f"Preparing the dispatch of workflow {pending.workflow_id} raised "
                    f"{type(preparation_error).__name__}: {preparation_error}",
                    job_id=pending.job_id,
                    workflow_id=pending.workflow_id,
                )
                delivery_failures.append(
                    f"preparing its dispatch raised {type(preparation_error).__name__}: {preparation_error}"
                )

            else:
                if (
                    dispatch_plans
                    and self._on_dispatch_state_registered is not None
                    and not await self._on_dispatch_state_registered(pending.job_id)
                ):
                    # Nothing is sent: the manager tier could not replicate the
                    # dispatch, which is no fault of the workflow's -- it backs
                    # off without spending retry budget.
                    for worker_id, _worker_cores, sub_token, _dispatch in dispatch_plans:
                        await self._worker_pool.release_cores(worker_id, str(sub_token))
                        await self._job_manager.remove_unstarted_sub_workflow(str(sub_token))
                    self._apply_backoff(pending, attempt_started_at)
                    # Back in the queue, it needs the job's dispatch loop, which
                    # may have exited seeing it claimed. A job already dropped
                    # (teardown removes it from the JobManager first) is not
                    # requeued, so a finished job's loop is never restarted.
                    if await self._job_manager.return_workflow_to_pending(
                        pending.job_id,
                        pending.workflow_id,
                        "its dispatch state did not replicate to a quorum of managers",
                    ) and (
                        (loop_task := self._job_dispatch_tasks.get(pending.job_id)) is None
                        or loop_task.done()
                    ):
                        await self.start_job_dispatch(pending.job_id, submission)
                    return False

                for worker_id, worker_cores, sub_token, outcome, detail in await self._send_dispatch_plans(
                    pending,
                    dispatch_plans,
                ):
                    if (
                        outcome == DispatchOutcome.ACCEPTED
                        or not await self._job_manager.remove_unstarted_sub_workflow(str(sub_token))
                    ):
                        # Taken: its cores stay reserved until the worker's
                        # next report shows the dispatch.
                        taken_plans.append((str(sub_token), worker_id))
                        continue

                    await self._worker_pool.release_cores(worker_id, str(sub_token))
                    if outcome in BUDGETED_DISPATCH_OUTCOMES:
                        delivery_failures.append(f"worker {worker_id} {outcome.value}: {detail}")

            if taken_plans:
                if self._job_manager.workflow_lifecycle.get_state(
                    pending.job_id, pending.workflow_id
                ) not in (WorkflowState.DISPATCHED, WorkflowState.RUNNING):
                    # It was cancelled (or failed for good) while it was being
                    # sent: what the workers took is stopped -- otherwise it
                    # would run on, unseen by the cancellation.
                    self._task_runner.run(
                        self._stop_dispatched_plans, pending.job_id, taken_plans
                    )
                elif len(taken_plans) < len(allocations):
                    # PARTIAL success: the workflow runs with reduced
                    # parallelism on the workers that took it.
                    await self._log_warning(
                        f"Partial dispatch for workflow {pending.workflow_id}: "
                        f"{len(taken_plans)}/{len(allocations)} workers took it",
                        job_id=pending.job_id,
                        workflow_id=pending.workflow_id,
                    )
                return True

            if not delivery_failures:
                # Every worker offered refused for capacity (or was gone, or
                # never sent to): the workflow waits for capacity.
                if await self._job_manager.return_workflow_to_pending(
                    pending.job_id,
                    pending.workflow_id,
                    "no worker offered had capacity for it",
                ) and (
                    (loop_task := self._job_dispatch_tasks.get(pending.job_id)) is None
                    or loop_task.done()
                ):
                    await self.start_job_dispatch(pending.job_id, submission)
                return False

            failure_cause = "; ".join(delivery_failures)
            retry_allowed, budget_state = await self._retry_budget_manager.check_and_consume(
                pending.job_id, pending.workflow_id
            )
            if retry_allowed:
                self._apply_backoff(pending, attempt_started_at)
                if await self._job_manager.return_workflow_to_pending(
                    pending.job_id, pending.workflow_id, failure_cause
                ) and (
                    (loop_task := self._job_dispatch_tasks.get(pending.job_id)) is None
                    or loop_task.done()
                ):
                    await self.start_job_dispatch(pending.job_id, submission)
                return False

            # The retry budget is spent: the workflow fails for good, with the
            # cause of its last attempt. It leaves the queue now; failing it
            # announces its terminal, whose handling can complete the job and
            # stop this very dispatch loop, so it runs outside this pass.
            async with self._pending_lock:
                self._pending.pop(f"{pending.job_id}:{pending.workflow_id}", None)
            pending.ready_event.set()
            await self._log_warning(
                f"Dispatch of workflow {pending.workflow_id} failed for good, "
                f"retry budget spent ({budget_state}): {failure_cause}",
                job_id=pending.job_id,
                workflow_id=pending.workflow_id,
            )
            self._task_runner.run(
                self._on_dispatch_exhausted,
                pending.job_id,
                pending.workflow_id,
                f"dispatch failed and its retry budget is spent ({budget_state}): {failure_cause}",
            )
            return False

        finally:
            # Cores were allocated, so the attempt changed something (took
            # the workflow, gave cores back, requeued it) another pass may
            # act on. Only here: signalled after an attempt that allocated
            # nothing, the next pass would start at once and allocate nothing
            # again -- a spin.
            self.signal_dispatch()

    def _apply_backoff(self, pending: PendingWorkflow, attempt_started_at: float) -> None:
        """Back off after an attempt that failed for a cause other than
        capacity: the next attempt waits ``next_retry_delay`` from this
        one's start, and the delay doubles, capped, for the one after."""
        pending.failed_dispatch_attempts += 1
        pending.last_dispatch_attempt = attempt_started_at
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
    ) -> list[tuple[str, int, TrackingToken, DispatchOutcome, str]]:
        """Send workflow dispatch plans with bounded concurrency: each
        plan's worker, cores and sub-workflow token, with how the worker
        answered and the answer's detail."""
        if not dispatch_plans:
            return []

        semaphore = asyncio.Semaphore(self._max_concurrent_dispatches)
        lifecycle = self._job_manager.workflow_lifecycle

        async def send_one(
            worker_id: str,
            worker_cores: int,
            sub_token: TrackingToken,
            dispatch: WorkflowDispatch,
        ) -> tuple[str, int, TrackingToken, DispatchOutcome, str]:
            async with semaphore:
                # Nothing more is sent for a workflow that left the dispatch
                # (cancelled, failed for good) while its plans were queued.
                if self._shutting_down or lifecycle.get_state(
                    pending.job_id, pending.workflow_id
                ) not in (WorkflowState.DISPATCHED, WorkflowState.RUNNING):
                    return (
                        worker_id,
                        worker_cores,
                        sub_token,
                        DispatchOutcome.WITHHELD,
                        "withheld: the dispatcher is shutting down or the workflow left the dispatch",
                    )

                try:
                    outcome, detail = await self._send_dispatch(worker_id, dispatch)
                    return worker_id, worker_cores, sub_token, outcome, detail
                except asyncio.CancelledError as dispatch_error:
                    if self._shutting_down or lifecycle.get_state(
                        pending.job_id, pending.workflow_id
                    ) not in (WorkflowState.DISPATCHED, WorkflowState.RUNNING):
                        raise

                    await self._log_warning(
                        "Dispatch was cancelled by worker-side transport failure "
                        f"for worker {worker_id}: {dispatch_error}",
                        job_id=pending.job_id,
                        workflow_id=pending.workflow_id,
                    )
                    return (
                        worker_id,
                        worker_cores,
                        sub_token,
                        DispatchOutcome.UNREACHABLE,
                        f"cancelled by a transport failure: {dispatch_error}",
                    )
                except Exception as dispatch_error:
                    await self._log_warning(
                        f"Exception dispatching to worker {worker_id} for "
                        f"workflow {pending.workflow_id}: {dispatch_error}",
                        job_id=pending.job_id,
                        workflow_id=pending.workflow_id,
                    )
                    return (
                        worker_id,
                        worker_cores,
                        sub_token,
                        DispatchOutcome.UNREACHABLE,
                        f"{type(dispatch_error).__name__}: {dispatch_error}",
                    )

        return await asyncio.gather(
            *(send_one(*dispatch_plan) for dispatch_plan in dispatch_plans)
        )

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
        # Phase 6b: explicit ``loop.create_task`` so the dispatch task
        # binds to the loop ``start_job_dispatch`` was called from
        # rather than implicitly going through ``get_running_loop`` at
        # task-creation time.
        task = asyncio.get_running_loop().create_task(
            self._job_dispatch_loop(job_id, submission)
        )
        self._job_dispatch_tasks[job_id] = task

    async def stop_job_dispatch(self, job_id: str) -> None:
        """
        Stop the dispatch loop for a job.

        Called when a job completes or is cancelled.
        """
        task = self._job_dispatch_tasks.pop(job_id, None)
        if task and not task.done():
            task.cancel()
            # Waited for, not awaited: awaiting raises the loop's own
            # cancellation here, and catching that swallowed a
            # cancellation of this caller too.
            await asyncio.wait((task,))

        self._job_submissions.pop(job_id, None)

    async def _job_dispatch_loop(self, job_id: str, submission: JobSubmission) -> None:
        """
        Event-driven dispatch loop for a single job.

        Each pass dispatches what it can, then waits on:
        1. Workflow ready events (dependencies satisfied)
        2. A change in the WorkerPool's capacity since the pass
        3. Dispatch trigger events (external signals)

        Exits when:
        - No workflow of the job is PENDING (each was taken by a worker or
          finished; a workflow sent back to PENDING restarts the loop)
        - Job cancelled/completed
        - Shutdown signaled
        """
        lifecycle = self._job_manager.workflow_lifecycle
        try:
            while not self._shutting_down:
                # Read before the pass: a capacity change landing during or
                # after it ends the capacity wait below, so none is lost.
                capacity_generation = self._worker_pool.capacity_generation
                await self.try_dispatch(job_id, submission)

                async with self._pending_lock:
                    job_pending = [
                        p
                        for p in self._pending.values()
                        if p.job_id == job_id
                        and lifecycle.get_state(job_id, p.workflow_id) == WorkflowState.PENDING
                    ]

                if not job_pending:
                    # Nothing this job holds waits for dispatch.
                    break

                # Backoff expiry and remaining-time both route through
                # the ``PendingWorkflow`` helpers — ONE deadline
                # contract (sub-epsilon remainders are expiry, see
                # ``protocol.time_quantum``), so the eligibility filter
                # here, ``_get_ready_workflows``' scan, and the wait
                # computed below can never disagree about a boundary
                # instant (the disagreement was the frozen-instant
                # livelock).
                now = _DEFAULT_CLOCK.monotonic()
                allocatable_pending = [
                    p
                    for p in job_pending
                    if (
                        p.dependencies <= p.completed_dependencies
                        and p.is_retry_backoff_expired(now)
                    )
                ]
                backoff_delays = [
                    p.remaining_retry_backoff_seconds(now)
                    for p in job_pending
                    if (
                        p.failed_dispatch_attempts > 0
                        and p.dependencies <= p.completed_dependencies
                        and not p.is_retry_backoff_expired(now)
                    )
                ]

                # Build list of events to wait on. Capacity is relevant only
                # when at least one workflow can allocate immediately, and is
                # waited for as a CHANGE since the pass above, never as cores
                # existing: cores that pass could not use (an excluded or
                # unselectable worker's) would otherwise end every wait at
                # once -- a frozen-instant livelock under SIM, a 100%-CPU
                # spin on a real host.
                ready_events = [self._consume_ready_signal(p) for p in job_pending]
                wait_coroutines = [*ready_events, self._wait_dispatch_trigger()]
                if allocatable_pending:
                    wait_coroutines.append(
                        self._worker_pool.wait_for_capacity_change(
                            capacity_generation,
                            timeout=5.0,
                        )
                    )

                wait_timeout = 5.0
                positive_backoff_delays = [
                    delay for delay in backoff_delays if delay > 0.0
                ]
                if positive_backoff_delays:
                    wait_timeout = min(wait_timeout, min(positive_backoff_delays))
                # Progress floor — defense in depth for the whole
                # sub-quantum-wait class. The PRIMARY fix is the
                # epsilon-expiry contract in the ``PendingWorkflow``
                # backoff helpers (a remainder the clock cannot honor
                # now counts as expiry, so it never reaches this wait);
                # the floor keeps any OTHER collapsed timeout source —
                # present or future — from arming a same-instant timer
                # in a loop (a dispatcher livelock under SIM's
                # quantized clock, a 100%-CPU micro-spin on a real
                # host). Real backoff waits (>= 1s initial delay) are
                # unaffected.
                wait_timeout = max(wait_timeout, 0.001)

                # Wait for any event with a timeout for periodic checks.
                # Phase 6b: explicit ``loop.create_task`` so each wait
                # task binds to the loop the dispatcher is running on
                # rather than implicitly going through
                # ``get_running_loop`` at task-creation time.
                _loop = asyncio.get_running_loop()
                tasks = [
                    _loop.create_task(coro)
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

                    # A finished wait is a wakeup; one that raised is a fault,
                    # and is reported rather than taken for a wakeup.
                    for task in done:
                        if not task.cancelled() and (wait_error := task.exception()) is not None:
                            await self._log_error(
                                f"Dispatch loop wait for job {job_id} raised "
                                f"{type(wait_error).__name__}: {wait_error}",
                                job_id=job_id,
                            )

                except asyncio.CancelledError:
                    # On cancellation, clean up all tasks
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise

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

    async def _consume_ready_signal(self, pending: PendingWorkflow) -> None:
        """Wait for a pending workflow's ready signal and CONSUME it.

        The ready event is a wakeup edge, not readiness state —
        readiness is re-derived from dependency/backoff state by
        ``try_dispatch`` on every pass. Left set (the old behavior), a
        pending workflow whose dispatch cannot proceed spins the
        dispatch loop at a single instant: every ``wait()`` on the
        still-set event completes immediately, ``try_dispatch`` no-ops
        (no capacity — e.g. every worker just died), and the loop goes
        around again without ever reaching its timeout. On a real
        manager that is a silent 100%-CPU busy-spin; under SIM the
        frozen virtual clock trips the runaway guard. Same
        consume-on-wake contract as ``_wait_dispatch_trigger``; the
        cleanup/cancellation paths that ``set()`` to unblock waiters
        are unaffected (the loop wakes, consumes, and re-checks state).
        """
        await pending.ready_event.wait()
        pending.ready_event.clear()

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
        lifecycle = self._job_manager.workflow_lifecycle
        for pending in self._pending.values():
            if lifecycle.get_state(pending.job_id, pending.workflow_id) == WorkflowState.PENDING:
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

    def abort(self) -> None:
        """Cancel every dispatch loop at once, without waiting for them to
        end: the counterpart of ``shutdown`` for an owner aborting
        synchronously."""
        self._shutting_down = True
        for task in self._job_dispatch_tasks.values():
            task.cancel()

        self._job_dispatch_tasks.clear()
        self._job_submissions.clear()

    # =========================================================================
    # Queue Views
    # =========================================================================

    def get_pending_count(self, job_id: str | None = None) -> int:
        """Get count of pending workflows (optionally filtered by job_id)."""
        if job_id is None:
            return len(self._pending)
        return sum(1 for p in self._pending.values() if p.job_id == job_id)

    def get_pending_workflows(self) -> Mapping[str, PendingWorkflow]:
        """Read-only view of every tracked workflow (AD-43 capacity input)."""
        return MappingProxyType(self._pending)

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
            # Only a workflow the lifecycle sent back to PENDING (the retry
            # chain) is dispatched again; anything else -- finished, or
            # still running elsewhere -- must never be.
            if (
                pending is None
                or self._job_manager.workflow_lifecycle.get_state(job_id, workflow_id)
                != WorkflowState.PENDING
            ):
                return False
            worker_id_to_exclude = excluded_worker_id or token.worker_id
            if worker_id_to_exclude:
                pending.excluded_worker_ids.add(worker_id_to_exclude)
            pending.failed_dispatch_attempts = 0
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

    # =========================================================================
    # Logging Helpers
    # =========================================================================

    def _get_log_context(self, job_id: str = "", workflow_id: str = "") -> dict:
        """Get common context fields for logging: the queue's workflows
        still PENDING, and the rest it holds (taken by workers, or done)."""
        lifecycle = self._job_manager.workflow_lifecycle
        awaiting_count = sum(
            1
            for pending in self._pending.values()
            if lifecycle.get_state(pending.job_id, pending.workflow_id) == WorkflowState.PENDING
        )
        return {
            "manager_id": self._manager_id,
            "datacenter": self._datacenter,
            "job_id": job_id,
            "workflow_id": workflow_id,
            "pending_count": awaiting_count,
            "dispatched_count": len(self._pending) - awaiting_count,
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
