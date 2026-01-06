import asyncio
import inspect
import time
from collections import defaultdict, deque
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Tuple,
)

import networkx

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import Hook, HookType
from hyperscale.core.jobs.models import (
    CancellationUpdate,
    InstanceRoleType,
    PendingWorkflowRun,
    WorkflowCancellationStatus,
    WorkflowCancellationUpdate,
    WorkflowResults,
    WorkflowStatusUpdate,
)
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.jobs.models.env import Env
from hyperscale.core.jobs.workers import Provisioner, StagePriority
from hyperscale.core.state import (
    Context,
    ContextHook,
    StateAction,
)
from hyperscale.logging import Entry, Logger, LogLevel
from hyperscale.logging.hyperscale_logging_models import (
    GraphDebug,
    RemoteManagerInfo,
    WorkflowDebug,
    WorkflowError,
    WorkflowFatal,
    WorkflowInfo,
    WorkflowTrace,
)
from hyperscale.reporting.common.results_types import (
    RunResults,
    WorkflowContextResult,
    WorkflowResultsSet,
    WorkflowStats,
)
from hyperscale.reporting.custom import CustomReporter
from hyperscale.reporting.reporter import Reporter, ReporterConfig
from hyperscale.reporting.results import Results
from hyperscale.ui import InterfaceUpdatesController
from hyperscale.ui.actions import (
    update_active_workflow_message,
    update_workflow_execution_stats,
    update_workflow_executions_counter,
    update_workflow_executions_rates,
    update_workflow_executions_total_rate,
    update_workflow_progress_seconds,
    update_workflow_run_timer,
)

from .remote_graph_controller_rewrite import RemoteGraphController
from hyperscale.core.jobs.models import WorkflowCompletionState

NodeResults = Tuple[
    WorkflowResultsSet,
    Context,
]


ProvisionedBatch = List[
    List[
        Tuple[
            str,
            StagePriority,
            int,
        ]
    ]
]

WorkflowVUs = Dict[str, List[int]]


class RemoteGraphManager:
    def __init__(
        self,
        updates: InterfaceUpdatesController,
        workers: int,
    ) -> None:
        self._updates = updates
        self._workers: List[Tuple[str, int]] | None = None

        self._workflows: Dict[str, Workflow] = {}
        self._workflow_timers: Dict[str, float] = {}
        self._workflow_completion_rates: Dict[str, List[Tuple[float, int]]] = (
            defaultdict(list)
        )
        self._workflow_last_elapsed: Dict[str, float] = {}

        self._threads = workers
        self._controller: RemoteGraphController | None = None
        self._role = InstanceRoleType.PROVISIONER
        self._provisioner: Provisioner | None = None
        self._graph_updates: dict[int, dict[str, asyncio.Queue[WorkflowStatusUpdate]]] = defaultdict(lambda: defaultdict(asyncio.Queue))
        self._workflow_statuses: dict[int, dict[str, Deque[WorkflowStatusUpdate]]] = defaultdict(lambda: defaultdict(deque))
        self._available_cores_updates: asyncio.Queue[tuple[int, int, int]] | None = None
        self._cancellation_updates: dict[int, dict[str, asyncio.Queue[CancellationUpdate]]] = defaultdict(lambda: defaultdict(asyncio.Queue))

        self._step_traversal_orders: Dict[
            str,
            List[
                Dict[
                    str,
                    Hook,
                ]
            ],
        ] = {}

        self._workflow_traversal_order: List[
            Dict[
                str,
                Hook,
            ]
        ] = []

        self._workflow_configs: Dict[str, Dict[str, Any]] = {}
        self._loop = asyncio.get_event_loop()
        self._logger = Logger()
        self._status_lock: asyncio.Lock | None = None

        # Dependency tracking: workflow_name -> set of dependency workflow names
        self._workflow_dependencies: Dict[str, set[str]] = {}
        # Track completed workflows per run_id
        self._completed_workflows: Dict[int, set[str]] = {}
        # Track failed workflows per run_id
        self._failed_workflows: Dict[int, set[str]] = {}

    async def start(
        self,
        host: str,
        port: int,
        env: Env,
        cert_path: str | None = None,
        key_path: str | None = None,
    ):
        async with self._logger.context(
            name="remote_graph_manager",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            await ctx.log(
                RemoteManagerInfo(
                    message=f"Remote Graph Manager starting leader on port {host}:{port}",
                    host=host,
                    port=port,
                    with_ssl=cert_path is not None and key_path is not None,
                )
            )

            if self._available_cores_updates is None:
                self._available_cores_updates = asyncio.Queue()

            if self._controller is None:
                self._controller = RemoteGraphController(
                    None,
                    host,
                    port,
                    env,
                )

            if self._provisioner is None:
                self._provisioner = Provisioner()

            if self._status_lock is None:
                self._status_lock = asyncio.Lock()

            await self._controller.start_server(
                cert_path=cert_path,
                key_path=key_path,
            )

    async def connect_to_workers(
        self,
        workers: List[Tuple[str, int]],
        timeout: int | float | str | None = None,
    ):
        async with self._logger.context(
            name="remote_graph_manager",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            await ctx.log(
                Entry(
                    message=f"Remote Graph Manager connecting to {workers} workers with timeout of {timeout} seconds",
                    level=LogLevel.DEBUG,
                )
            )

            if isinstance(timeout, str):
                timeout = TimeParser(timeout).time

            elif timeout is None:
                timeout = self._controller._request_timeout

            self._workers = workers

            workers_ready = await self._controller.wait_for_workers(
                self._threads,
                timeout=timeout,
            )

            if not workers_ready:
                raise TimeoutError(
                    f"Timed out waiting for {self._threads} workers to start"
                )

            await asyncio.gather(
                *[self._controller.connect_client(address) for address in workers]
            )

            self._provisioner.setup(max_workers=len(self._controller.nodes))

            await ctx.log(
                Entry(
                    message=f"Remote Graph Manager successfully connected to {workers} workers",
                    level=LogLevel.DEBUG,
                )
            )

    async def run_forever(self):
        await self._controller.run_forever()

    async def execute_graph(
        self,
        test_name: str,
        workflows: List[
            tuple[list[str], Workflow],
        ],
    ) -> RunResults:
        """
        Execute a graph of workflows with eager dispatch.

        Workflows are dispatched as soon as their dependencies complete,
        rather than waiting for entire BFS layers. This maximizes
        parallelism and reduces total execution time.
        """
        graph_slug = test_name.lower()

        self._logger.configure(
            name=f"{graph_slug}_logger",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "debug": (
                    GraphDebug,
                    {
                        "workflows": [workflow.name for _, workflow in workflows],
                        "workers": self._workers,
                        "graph": test_name,
                    },
                ),
            },
        )

        run_id = self._controller.id_generator.generate()

        # Initialize tracking for this run
        self._completed_workflows[run_id] = set()
        self._failed_workflows[run_id] = set()

        async with self._logger.context(name=f"{graph_slug}_logger") as ctx:
            await ctx.log_prepared(
                message=f"Graph {test_name} assigned run id {run_id}", name="debug"
            )

            self._controller.create_run_contexts(run_id)

            # Build pending workflows with provisioning
            pending_workflows = self._create_pending_workflows(workflows)

            await ctx.log_prepared(
                message=f"Graph {test_name} created {len(pending_workflows)} pending workflows",
                name="debug",
            )

            # Run the eager dispatch loop
            workflow_results, timeouts, skipped = await self._dispatch_loop(
                run_id,
                test_name,
                pending_workflows,
            )

            await ctx.log_prepared(
                message=f"Graph {test_name} completed execution", name="debug"
            )

            # Cleanup tracking data for this run
            self._completed_workflows.pop(run_id, None)
            self._failed_workflows.pop(run_id, None)

            return {
                "test": test_name,
                "results": workflow_results,
                "timeouts": timeouts,
                "skipped": skipped,
            }

    def _create_pending_workflows(
        self,
        workflows: List[tuple[list[str], Workflow]],
    ) -> Dict[str, PendingWorkflowRun]:
        """
        Create PendingWorkflowRun for each workflow.

        Builds the dependency graph and creates tracking objects.
        Core allocation happens dynamically at dispatch time, not upfront.
        Workflows with no dependencies have their ready_event set immediately.
        """
        # Clear previous run's state
        self._workflows.clear()
        self._workflow_dependencies.clear()

        # Build graph and collect workflow info
        workflow_graph = networkx.DiGraph()

        for dependencies, workflow in workflows:
            self._workflows[workflow.name] = workflow
            workflow_graph.add_node(workflow.name)

            if len(dependencies) > 0:
                self._workflow_dependencies[workflow.name] = set(dependencies)

        # Add edges for dependencies
        for dependent, deps in self._workflow_dependencies.items():
            for dependency in deps:
                workflow_graph.add_edge(dependency, dependent)

        # Determine which workflows are test workflows
        workflow_is_test = self._determine_test_workflows(self._workflows)

        # Create PendingWorkflowRun for each workflow (no core allocation yet)
        pending_workflows: Dict[str, PendingWorkflowRun] = {}

        for workflow_name, workflow in self._workflows.items():
            dependencies = self._workflow_dependencies.get(workflow_name, set())
            priority = getattr(workflow, 'priority', StagePriority.AUTO)
            if not isinstance(priority, StagePriority):
                priority = StagePriority.AUTO

            pending = PendingWorkflowRun(
                workflow_name=workflow_name,
                workflow=workflow,
                dependencies=set(dependencies),
                completed_dependencies=set(),
                vus=workflow.vus,
                priority=priority,
                is_test=workflow_is_test[workflow_name],
                ready_event=asyncio.Event(),
                dispatched=False,
                completed=False,
                failed=False,
            )

            # Workflows with no dependencies are immediately ready
            if len(dependencies) == 0:
                pending.ready_event.set()

            pending_workflows[workflow_name] = pending

        return pending_workflows

    def _determine_test_workflows(
        self,
        workflows: Dict[str, Workflow],
    ) -> Dict[str, bool]:
        """Determine which workflows are test workflows based on their hooks."""
        workflow_hooks: Dict[str, Dict[str, Hook]] = {
            workflow_name: {
                name: hook
                for name, hook in inspect.getmembers(
                    workflow,
                    predicate=lambda member: isinstance(member, Hook),
                )
            }
            for workflow_name, workflow in workflows.items()
        }

        return {
            workflow_name: (
                len([hook for hook in hooks.values() if hook.hook_type == HookType.TEST]) > 0
            )
            for workflow_name, hooks in workflow_hooks.items()
        }

    async def _dispatch_loop(
        self,
        run_id: int,
        test_name: str,
        pending_workflows: Dict[str, PendingWorkflowRun],
    ) -> Tuple[Dict[str, List[WorkflowResultsSet]], Dict[str, Exception], Dict[str, str]]:
        """
        Event-driven dispatch loop for eager execution.

        Dispatches workflows as soon as their dependencies complete.
        Core allocation happens dynamically at dispatch time using
        partion_by_priority on the currently ready workflows.
        Uses asyncio.wait with FIRST_COMPLETED to react immediately
        to workflow completions.
        """
        workflow_results: Dict[str, List[WorkflowResultsSet]] = defaultdict(list)
        timeouts: Dict[str, Exception] = {}
        skipped: Dict[str, str] = {}

        # Track running tasks: task -> workflow_name
        running_tasks: Dict[asyncio.Task, str] = {}

        # Track cores currently in use by running workflows
        cores_in_use = 0
        total_cores = self._provisioner.max_workers

        graph_slug = test_name.lower()

        async with self._logger.context(name=f"{graph_slug}_logger") as ctx:
            while True:
                # Check if all workflows are done
                all_done = all(
                    pending.completed or pending.failed
                    for pending in pending_workflows.values()
                )
                if all_done:
                    break

                # Get ready workflows (dependencies satisfied, not dispatched)
                ready_workflows = [
                    pending for pending in pending_workflows.values()
                    if pending.is_ready()
                ]

                if ready_workflows:
                    # Calculate available cores
                    available_cores = total_cores - cores_in_use

                    # Dynamically allocate cores for ready workflows
                    allocations = self._allocate_cores_for_ready_workflows(
                        ready_workflows, available_cores
                    )

                    for pending, cores in allocations:
                        if cores == 0:
                            # No cores allocated - skip this workflow for now
                            # It will be retried next iteration when cores free up
                            continue

                        pending.dispatched = True
                        pending.ready_event.clear()
                        pending.allocated_cores = cores

                        # Track cores in use
                        cores_in_use += cores

                        # Calculate VUs per worker
                        pending.allocated_vus = self._calculate_vus_per_worker(
                            pending.vus, cores
                        )

                        await ctx.log(
                            GraphDebug(
                                message=f"Graph {test_name} dispatching workflow {pending.workflow_name}",
                                workflows=[pending.workflow_name],
                                workers=cores,
                                graph=test_name,
                                level=LogLevel.DEBUG,
                            )
                        )

                        self._updates.update_active_workflows([
                            pending.workflow_name.lower()
                        ])

                        # Create task for workflow execution
                        task = asyncio.create_task(
                            self._run_workflow(
                                run_id,
                                pending.workflow,
                                cores,
                                pending.allocated_vus,
                            )
                        )
                        running_tasks[task] = pending.workflow_name

                # If no tasks running, check if we can make progress
                if not running_tasks:
                    # Check if any workflows are ready but waiting for cores
                    workflows_waiting_for_cores = [
                        pending for pending in pending_workflows.values()
                        if pending.is_ready() and not pending.dispatched
                    ]

                    if workflows_waiting_for_cores:
                        # This shouldn't happen - if no tasks running, all cores are free
                        # Log error and try to recover by retrying allocation
                        await ctx.log(
                            GraphDebug(
                                message=f"Graph {test_name} has {len(workflows_waiting_for_cores)} workflows waiting for cores but no tasks running (available_cores={available_cores}, cores_in_use={cores_in_use})",
                                workflows=[p.workflow_name for p in workflows_waiting_for_cores],
                                workers=total_cores,
                                graph=test_name,
                                level=LogLevel.DEBUG,
                            )
                        )
                        # Reset cores_in_use since nothing is running
                        cores_in_use = 0
                        continue

                    # No tasks running and no ready workflows - we're stuck
                    # (circular dependency or all remaining workflows have failed deps)
                    for pending in pending_workflows.values():
                        if not pending.dispatched and not pending.failed:
                            pending.failed = True
                            failed_deps = pending.dependencies - pending.completed_dependencies
                            skip_reason = f"Dependencies not satisfied: {', '.join(sorted(failed_deps))}"
                            skipped[pending.workflow_name] = skip_reason
                            self._failed_workflows[run_id].add(pending.workflow_name)
                    break

                # Wait for any task to complete
                done, _ = await asyncio.wait(
                    running_tasks.keys(),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Process completed tasks
                for task in done:
                    workflow_name = running_tasks.pop(task)
                    pending = pending_workflows[workflow_name]

                    # Release cores used by this workflow
                    cores_in_use -= pending.allocated_cores

                    try:
                        result = task.result()
                        name, workflow_result, context, timeout_error = result

                        if timeout_error is None:
                            # Workflow completed successfully
                            workflow_results[workflow_name] = workflow_result
                            pending.completed = True
                            self._completed_workflows[run_id].add(workflow_name)

                            await ctx.log(
                                GraphDebug(
                                    message=f"Graph {test_name} workflow {workflow_name} completed successfully",
                                    workflows=[workflow_name],
                                    workers=pending.allocated_cores,
                                    graph=test_name,
                                    level=LogLevel.DEBUG,
                                )
                            )

                            # Signal dependents
                            self._mark_workflow_completed(
                                workflow_name,
                                pending_workflows,
                            )

                        else:
                            # Workflow failed (timeout)
                            timeouts[workflow_name] = timeout_error
                            pending.failed = True
                            self._failed_workflows[run_id].add(workflow_name)

                            await ctx.log(
                                GraphDebug(
                                    message=f"Graph {test_name} workflow {workflow_name} timed out",
                                    workflows=[workflow_name],
                                    workers=pending.allocated_cores,
                                    graph=test_name,
                                    level=LogLevel.DEBUG,
                                )
                            )

                            # Propagate failure to dependents
                            failed_dependents = self._mark_workflow_failed(
                                run_id,
                                workflow_name,
                                pending_workflows,
                            )

                            for dep_name in failed_dependents:
                                skipped[dep_name] = f"Dependency failed: {workflow_name}"

                    except Exception as err:
                        # Workflow raised an exception
                        pending.failed = True
                        self._failed_workflows[run_id].add(workflow_name)
                        timeouts[workflow_name] = err

                        await ctx.log(
                            GraphDebug(
                                message=f"Graph {test_name} workflow {workflow_name} failed with error: {err}",
                                workflows=[workflow_name],
                                workers=pending.allocated_cores,
                                graph=test_name,
                                level=LogLevel.DEBUG,
                            )
                        )

                        # Propagate failure to dependents
                        failed_dependents = self._mark_workflow_failed(
                            run_id,
                            workflow_name,
                            pending_workflows,
                        )

                        for dep_name in failed_dependents:
                            skipped[dep_name] = f"Dependency failed: {workflow_name}"

        return workflow_results, timeouts, skipped

    def _allocate_cores_for_ready_workflows(
        self,
        ready_workflows: List[PendingWorkflowRun],
        available_cores: int,
    ) -> List[Tuple[PendingWorkflowRun, int]]:
        """
        Dynamically allocate cores for ready workflows.

        Uses partion_by_priority to allocate cores based on priority and VUs,
        constrained by the number of cores currently available.

        Args:
            ready_workflows: List of workflows ready for dispatch
            available_cores: Number of cores not currently in use

        Returns list of (pending_workflow, allocated_cores) tuples.
        """
        # Build configs for the provisioner
        configs = [
            {
                "workflow_name": pending.workflow_name,
                "priority": pending.priority,
                "is_test": pending.is_test,
                "vus": pending.vus,
            }
            for pending in ready_workflows
        ]

        # Get allocations from provisioner, constrained by available cores
        batches = self._provisioner.partion_by_priority(configs, available_cores)

        # Build lookup from workflow_name -> cores
        allocation_lookup: Dict[str, int] = {}
        for batch in batches:
            for workflow_name, _, cores in batch:
                allocation_lookup[workflow_name] = cores

        # Return allocations paired with pending workflows
        return [
            (pending, allocation_lookup.get(pending.workflow_name, 0))
            for pending in ready_workflows
        ]

    def _calculate_vus_per_worker(
        self,
        total_vus: int,
        cores: int,
    ) -> List[int]:
        """Calculate VUs distribution across workers."""
        if cores <= 0:
            return []

        vus_per_core = total_vus // cores
        remainder = total_vus % cores

        # Distribute VUs evenly, with remainder going to first workers
        vus_list = [vus_per_core for _ in range(cores)]
        for index in range(remainder):
            vus_list[index] += 1

        return vus_list

    def _mark_workflow_completed(
        self,
        workflow_name: str,
        pending_workflows: Dict[str, PendingWorkflowRun],
    ) -> None:
        """
        Mark a workflow as completed and signal dependents.

        Updates all pending workflows that depend on this one.
        If a dependent's dependencies are now all satisfied,
        signals its ready_event.
        """
        for pending in pending_workflows.values():
            if workflow_name in pending.dependencies:
                pending.completed_dependencies.add(workflow_name)
                pending.check_and_signal_ready()

    def _mark_workflow_failed(
        self,
        run_id: int,
        workflow_name: str,
        pending_workflows: Dict[str, PendingWorkflowRun],
    ) -> List[str]:
        """
        Mark a workflow as failed and propagate failure to dependents.

        Transitively fails all workflows that depend on this one
        (directly or indirectly).

        Returns list of workflow names that were failed.
        """
        failed_workflows: List[str] = []

        # BFS to find all transitive dependents
        queue = [workflow_name]
        visited = {workflow_name}

        while queue:
            current = queue.pop(0)

            for pending in pending_workflows.values():
                if pending.workflow_name in visited:
                    continue
                if current in pending.dependencies:
                    visited.add(pending.workflow_name)
                    queue.append(pending.workflow_name)

                    if not pending.dispatched and not pending.failed:
                        pending.failed = True
                        pending.ready_event.clear()
                        self._failed_workflows[run_id].add(pending.workflow_name)
                        failed_workflows.append(pending.workflow_name)

        return failed_workflows

    async def execute_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        workflow_context: Dict[str, Any],
        vus: int,
        threads: int,
    ):
        await self._append_workflow_run_status(run_id, workflow.name, WorkflowStatus.QUEUED)

        self._controller.create_context_from_external_store(
            workflow.name,
            run_id,
            workflow_context,
        )

        default_config = {
            "workflow": workflow.name,
            "run_id": run_id,
            "workers": threads,
            "workflow_vus": vus,
            "duration": workflow.duration,
        }

        workflow_slug = workflow.name.lower()

        self._logger.configure(
            name=f"{workflow_slug}_logger",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (WorkflowTrace, default_config),
                "debug": (
                    WorkflowDebug,
                    default_config,
                ),
                "info": (
                    WorkflowInfo,
                    default_config,
                ),
                "error": (
                    WorkflowError,
                    default_config,
                ),
                "fatal": (
                    WorkflowFatal,
                    default_config,
                ),
            },
        )


        async with self._logger.context(
            name=f"{workflow_slug}_logger",
            nested=True,
        ) as ctx:
            await ctx.log_prepared(
                message=f"Received workflow {workflow.name} with {workflow.vus} on {self._threads} workers for {workflow.duration}",
                name="info",
            )

            self._controller.create_run_contexts(run_id)

            _, workflow_vus = self._provision({
                workflow.name: workflow,
            }, threads=threads)

            await self._append_workflow_run_status(run_id, workflow.name, WorkflowStatus.RUNNING)

            results = await self._run_workflow(
                run_id,
                workflow,
                threads,
                workflow_vus[workflow.name],
                skip_reporting=True,
            )
            workflow_name, results, context, error = results

            status = WorkflowStatus.FAILED if error else WorkflowStatus.COMPLETED
            await self._append_workflow_run_status(run_id, workflow.name, status)

            return (
                workflow_name,
                results,
                context,
                error,
                status,
            )

    async def _append_workflow_run_status(
        self,
        run_id: int,
        workflow: str,
        status: WorkflowStatus,
    ):
        if self._status_lock:
            await self._status_lock.acquire()
            self._workflow_statuses[run_id][workflow].append(status)
            self._status_lock.release()

    async def _run_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        threads: int,
        workflow_vus: List[int],
        skip_reporting: bool = False,
    ) -> Tuple[str, WorkflowStats | dict[int, WorkflowResults], Context, Exception | None]:
        workflow_slug = workflow.name.lower()

        try:

            async with self._logger.context(
                name=f"{workflow_slug}_logger",
                nested=True,
            ) as ctx:
                await ctx.log_prepared(
                    message=f"Running workflow {workflow.name} with {workflow.vus} on {self._threads} workers for {workflow.duration}",
                    name="info",
                )

                hooks: Dict[str, Hook] = {
                    name: hook
                    for name, hook in inspect.getmembers(
                        workflow,
                        predicate=lambda member: isinstance(member, Hook),
                    )
                }

                hook_names = ", ".join(hooks.keys())

                await ctx.log_prepared(
                    message=f"Found actions {hook_names} on Workflow {workflow.name}",
                    name="debug",
                )

                is_test_workflow = (
                    len(
                        [
                            hook
                            for hook in hooks.values()
                            if hook.hook_type == HookType.TEST
                        ]
                    )
                    > 0
                )

                await ctx.log_prepared(
                    message=f"Found test actions on Workflow {workflow.name}"
                    if is_test_workflow
                    else f"No test actions found on Workflow {workflow.name}",
                    name="trace",
                )

                state_actions = self._setup_state_actions(workflow)

                if len(state_actions) > 0:
                    state_action_names = ", ".join(state_actions.keys())

                    await ctx.log_prepared(
                        message=f"Found state actions {state_action_names} on Workflow {workflow.name}",
                        name="debug",
                    )

                await ctx.log_prepared(
                    message=f"Assigning context to workflow {workflow.name}",
                    name="trace",
                )

                context = self._controller.assign_context(
                    run_id,
                    workflow.name,
                    threads,
                )

                loaded_context = await self._use_context(
                    workflow.name,
                    state_actions,
                    context,
                )

                # ## Send batched requests

                workflow_slug = workflow.name.lower()

                await asyncio.gather(
                    *[
                        update_active_workflow_message(
                            workflow_slug, f"Starting - {workflow.name}"
                        ),
                        update_workflow_run_timer(workflow_slug, True),
                    ]
                )

                await ctx.log_prepared(
                    message=f"Submitting Workflow {workflow.name} with run id {run_id}",
                    name="trace",
                )

                self._workflow_timers[workflow.name] = time.monotonic()

                # Register for event-driven completion tracking
                completion_state = self._controller.register_workflow_completion(
                    run_id,
                    workflow.name,
                    threads,
                )

                # Submit workflow to workers (no callbacks needed)
                await self._controller.submit_workflow_to_workers(
                    run_id,
                    workflow,
                    loaded_context,
                    threads,
                    workflow_vus,
                )

                await ctx.log_prepared(
                    message=f"Submitted Workflow {workflow.name} with run id {run_id}",
                    name="trace",
                )

                await ctx.log_prepared(
                    message=f"Workflow {workflow.name} run {run_id} waiting for {threads} workers to signal completion",
                    name="info",
                )

                workflow_timeout = int(
                    TimeParser(workflow.duration).time
                    + TimeParser(workflow.timeout).time,
                )

                # Event-driven wait for completion with status update processing
                timeout_error = await self._wait_for_workflow_completion(
                    run_id,
                    workflow.name,
                    workflow_timeout,
                    completion_state,
                    threads,
                )

                # Get results from controller
                results, run_context = self._controller.get_workflow_results(
                    run_id,
                    workflow.name,
                )

                # Cleanup completion state
                self._controller.cleanup_workflow_completion(run_id, workflow.name)

                if timeout_error:
                    await ctx.log_prepared(
                        message=f"Workflow {workflow.name} exceeded timeout of {workflow_timeout} seconds",
                        name="fatal",
                    )

                    await update_active_workflow_message(
                        workflow_slug, f"Timeout - {workflow.name}"
                    )

                await ctx.log_prepared(
                    message=f"Workflow {workflow.name} run {run_id} completed run",
                    name="info",
                )

                await update_workflow_run_timer(workflow_slug, False)
                await update_active_workflow_message(
                    workflow_slug, f"Processing results - {workflow.name}"
                )

                await update_workflow_executions_total_rate(workflow_slug, None, False)

                await ctx.log_prepared(
                    message=f"Processing {len(results)} results sets for Workflow {workflow.name} run {run_id}",
                    name="debug",
                )

                results = [result_set for _, result_set in results.values() if result_set is not None]

                print(len(results), threads)

                if is_test_workflow and len(results) > 1:
                    await ctx.log_prepared(
                        message=f"Merging {len(results)} test results sets for Workflow {workflow.name} run {run_id}",
                        name="trace",
                    )

                    workflow_results = Results(hooks)
                    execution_result = workflow_results.merge_results(
                        results,
                        run_id=run_id,
                    )

                elif is_test_workflow is False and len(results) > 1:
                    _, execution_result = list(
                        sorted(
                            list(enumerate(results)),
                            key=lambda result: result[0],
                            reverse=True,
                        )
                    ).pop()

                elif len(results) > 0:
                    execution_result = results.pop()

                else:
                    await ctx.log_prepared(
                        message=f'No results returned for Workflow {workflow.name} - workers likely encountered a fatal error during execution',
                        name='fatal',
                    )

                    raise Exception('No results returned')

                await ctx.log_prepared(
                    message=f"Updating context for {workflow.name} run {run_id}",
                    name="trace",
                )

                updated_context = await self._provide_context(
                    workflow.name,
                    state_actions,
                    run_context,
                    execution_result,
                )

                await self._controller.update_context(
                    run_id,
                    updated_context,
                )

                if skip_reporting:
                    return (
                        workflow.name,
                        results,
                        updated_context,
                        timeout_error,
                    )

                await ctx.log_prepared(
                    message=f"Submitting results to reporters for Workflow {workflow.name} run {run_id}",
                    name="trace",
                )

                reporting = workflow.reporting

                options: list[ReporterConfig] = []

                if inspect.isawaitable(reporting) or inspect.iscoroutinefunction(
                    reporting
                ):
                    options = await reporting()

                elif inspect.isfunction(reporting):
                    options = await self._loop.run_in_executor(
                        None,
                        reporting,
                    )

                else:
                    options = reporting

                if isinstance(options, list) is False:
                    options = [options]

                custom_reporters = [
                    option
                    for option in options
                    if isinstance(option, CustomReporter)
                ]

                configs = [
                    option
                    for option in options
                    if not isinstance(option, CustomReporter)
                ]

                reporters = [Reporter(config) for config in configs]
                if len(custom_reporters) > 0:
                    for custom_reporter in custom_reporters:
                        custom_reporter_name = custom_reporter.__class__.__name__

                        assert hasattr(custom_reporter, 'connect') and callable(getattr(custom_reporter, 'connect')), f"Custom reporter {custom_reporter_name} missing connect() method"
                        assert hasattr(custom_reporter, 'submit_workflow_results') and callable(getattr(custom_reporter, 'submit_workflow_results')), f"Custom reporter {custom_reporter_name} missing submit_workflow_results() method"

                        submit_workflow_results_method = getattr(custom_reporter, 'submit_workflow_results')
                        assert len(inspect.getargs(submit_workflow_results_method).args) == 1, f"Custom reporter {custom_reporter_name} submit_workflow_results() requires exactly one positional argument for Workflow metrics"

                        assert hasattr(custom_reporter, 'submit_step_results') and callable(getattr(custom_reporter, 'submit_step_results')), f"Custom reporter {custom_reporter_name} missing submit_step_results() method"

                        submit_step_results_method = getattr(custom_reporter, 'submit_step_results')
                        assert len(inspect.getargs(submit_step_results_method).args) == 1, f"Custom reporter {custom_reporter_name} submit_step_results() requires exactly one positional argument for Workflow action metrics"

                        assert hasattr(custom_reporter, 'close') and callable(getattr(custom_reporter, 'close')), f"Custom reporter {custom_reporter_name} missing close() method"

                    reporters.extend(custom_reporters)

                await asyncio.sleep(1)

                selected_reporters = ", ".join(
                    [config.reporter_type.name for config in configs]
                )

                await ctx.log_prepared(
                    message=f"Submitting results to reporters {selected_reporters} for Workflow {workflow.name} run {run_id}",
                    name="info",
                )

                await update_active_workflow_message(
                    workflow_slug, f"Submitting results via - {selected_reporters}"
                )

                try:
                    await asyncio.gather(
                        *[reporter.connect() for reporter in reporters]
                    )

                    await asyncio.gather(
                        *[
                            reporter.submit_workflow_results(execution_result)
                            for reporter in reporters
                        ]
                    )
                    await asyncio.gather(
                        *[
                            reporter.submit_step_results(execution_result)
                            for reporter in reporters
                        ]
                    )

                    await asyncio.gather(*[reporter.close() for reporter in reporters])

                except Exception:
                    await asyncio.gather(
                        *[reporter.close() for reporter in reporters],
                        return_exceptions=True,
                    )

                await asyncio.sleep(1)

                await update_active_workflow_message(
                    workflow_slug, f"Complete - {workflow.name}"
                )

                await asyncio.sleep(1)

                await ctx.log_prepared(
                    message=f"Workflow {workflow.name} run {run_id} complete",
                    name="debug",
                )

                return (workflow.name, execution_result, updated_context, timeout_error)

        except (
            KeyboardInterrupt,
            BrokenPipeError,
            asyncio.CancelledError,
        ) as err:
            import traceback
            print(traceback.format_exc())
            await update_active_workflow_message(workflow_slug, "Aborted")

            raise err

    async def _wait_for_workflow_completion(
        self,
        run_id: int,
        workflow_name: str,
        timeout: int,
        completion_state: WorkflowCompletionState,
        threads: int,
    ) -> Exception | None:
        """
        Wait for workflow completion while processing status updates.

        Uses event-driven completion signaling from the controller.
        Processes status updates from the queue to update UI.
        """
        workflow_slug = workflow_name.lower()
        timeout_error: Exception | None = None
        start_time = time.monotonic()

        while not completion_state.completion_event.is_set():
            remaining_timeout = timeout - (time.monotonic() - start_time)
            if remaining_timeout <= 0:
                timeout_error = asyncio.TimeoutError(
                    f"Workflow {workflow_name} exceeded timeout of {timeout} seconds"
                )
                break

            # Wait for either completion or a status update (with short timeout for responsiveness)
            try:
                await asyncio.wait_for(
                    completion_state.completion_event.wait(),
                    timeout=min(0.1, remaining_timeout),
                )
            except asyncio.TimeoutError:
                pass  # Expected - just check for status updates

            # Process any pending status updates
            await self._process_status_updates(
                run_id,
                workflow_name,
                completion_state,
                threads,
            )

        # Process any final status updates
        await self._process_status_updates(
            run_id,
            workflow_name,
            completion_state,
            threads,
        )

        return timeout_error

    async def _process_status_updates(
        self,
        run_id: int,
        workflow_name: str,
        completion_state: WorkflowCompletionState,
        threads: int,
    ) -> None:
        """
        Process status updates from the completion state queue.

        Updates UI with execution progress.
        """
        workflow_slug = workflow_name.lower()

        # Process any pending cores updates
        while True:
            try:
                assigned, completed = completion_state.cores_update_queue.get_nowait()
                self._update_available_cores(assigned, completed)
            except asyncio.QueueEmpty:
                break

        # Drain the status update queue and process all available updates
        while True:
            try:
                update = completion_state.status_update_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            # Update UI with stats
            elapsed = time.monotonic() - self._workflow_timers.get(workflow_name, time.monotonic())
            completed_count = update.completed_count

            await asyncio.gather(
                *[
                    update_workflow_executions_counter(
                        workflow_slug,
                        completed_count,
                    ),
                    update_workflow_executions_total_rate(
                        workflow_slug, completed_count, True
                    ),
                    update_workflow_progress_seconds(workflow_slug, elapsed),
                ]
            )

            if self._workflow_last_elapsed.get(workflow_name) is None:
                self._workflow_last_elapsed[workflow_name] = time.monotonic()

            last_sampled = (
                time.monotonic() - self._workflow_last_elapsed[workflow_name]
            )

            if last_sampled > 1:
                self._workflow_completion_rates[workflow_name].append(
                    (int(elapsed), int(completed_count / elapsed) if elapsed > 0 else 0)
                )

                await update_workflow_executions_rates(
                    workflow_slug, self._workflow_completion_rates[workflow_name]
                )

                await update_workflow_execution_stats(
                    workflow_slug, update.step_stats
                )

                self._workflow_last_elapsed[workflow_name] = time.monotonic()

            # Store update for external consumers
            self._graph_updates[run_id][workflow_name].put_nowait(update)

    def _setup_state_actions(self, workflow: Workflow) -> Dict[str, ContextHook]:
        state_actions: Dict[str, ContextHook] = {
            name: hook
            for name, hook in inspect.getmembers(
                workflow,
                predicate=lambda member: isinstance(member, ContextHook),
            )
        }

        for action in state_actions.values():
            action._call = action._call.__get__(workflow, workflow.__class__)
            setattr(workflow, action.name, action._call)

        return state_actions

    async def _use_context(
        self,
        workflow: str,
        state_actions: Dict[str, ContextHook],
        context: Context,
    ):
        use_actions = [
            action
            for action in state_actions.values()
            if action.action_type == StateAction.USE
        ]

        if len(use_actions) < 1:
            return context[workflow]

        for hook in use_actions:
            hook.context_args = {
                name: value
                for provider in hook.workflows
                for name, value in context[provider].items()
            }

        resolved = await asyncio.gather(
            *[hook.call(**hook.context_args) for hook in use_actions]
        )

        await asyncio.gather(
            *[context[workflow].set(hook_name, value) for hook_name, value in resolved]
        )

        return context[workflow]

    def get_last_workflow_status(self, run_id: int, workflow: str) -> WorkflowStatus:
        statuses = self._workflow_statuses[run_id][workflow]

        if len(statuses) > 1:
            return statuses.pop()

        elif len(statuses) > 0:
            return statuses[0]

        return WorkflowStatus.UNKNOWN

    def start_server_cleanup(self):
        self._controller.start_controller_cleanup()

    async def cancel_workflow(
        self,
        run_id: int,
        workflow: str,
        timeout: str = "1m",
        update_rate: str = "0.25s",
    ):

        (
            cancellation_status_counts,
            expected_nodes,
        ) = await self._controller.submit_workflow_cancellation(
            run_id,
            workflow,
            self._update_cancellation,
            timeout=timeout,
            rate=update_rate,
        )

        return CancellationUpdate(
            run_id=run_id,
            workflow_name=workflow,
            cancellation_status_counts=cancellation_status_counts,
            expected_cancellations=expected_nodes,
        )

    async def get_cancelation_update(
        self,
        run_id: int,
        workflow: str,
    ):
        if self._cancellation_updates[run_id][workflow].empty():
            return CancellationUpdate(
                run_id=run_id,
                workflow_name=workflow,
                cancellation_status_counts=defaultdict(lambda: 0),
                expected_cancellations=0,
            )

        return await self._cancellation_updates[run_id][workflow].get()


    async def get_workflow_update(self, run_id: int, workflow: str) -> WorkflowStatusUpdate | None:
        workflow_status_update: WorkflowStatusUpdate | None = None
        if self._graph_updates[run_id][workflow].empty() is False:
            workflow_status_update = await self._graph_updates[run_id][workflow].get()

        if self._status_lock and workflow_status_update:
            await self._status_lock.acquire()
            self._workflow_statuses[run_id][workflow].append(workflow_status_update.status)
            self._status_lock.release()

        return workflow_status_update

    async def get_availability(self):
        if self._available_cores_updates:
            return await self._available_cores_updates.get()

        return 0

    def _update_available_cores(
        self,
        assigned: int,
        completed: int,
    ):
        # Availablity is the total pool minus the difference between assigned and completd
        self._available_cores_updates.put_nowait((
            assigned,
            completed,
            self._threads - max(assigned - completed, 0),
        ))

    def _update_cancellation(
        self,
        run_id: int,
        workflow_name: str,
        cancellation_status_counts: dict[WorkflowCancellationStatus, list[WorkflowCancellationUpdate]],
        expected_cancellations: int,
    ):
        self._cancellation_updates[run_id][workflow_name].put_nowait(CancellationUpdate(
            run_id=run_id,
            workflow_name=workflow_name,
            cancellation_status_counts=cancellation_status_counts,
            expected_cancellations=expected_cancellations,
        ))

    def _provision(
        self,
        workflows: Dict[str, Workflow],
        threads: int | None = None,
    ) -> Tuple[ProvisionedBatch, WorkflowVUs]:
        if threads is None:
            threads = self._threads


        configs = {
            workflow_name: {
                "threads": threads,
                "vus": 1000,
            }
            for workflow_name in workflows
        }

        for workflow_name, config in configs.items():
            config.update(
                {
                    name: value
                    for name, value in inspect.getmembers(
                        workflows[workflow_name],
                    )
                    if config.get(name)
                }
            )

            config["threads"] = min(config["threads"], threads)

        workflow_hooks: Dict[str, Dict[str, Hook]] = {
            workflow_name: {
                name: hook
                for name, hook in inspect.getmembers(
                    workflow,
                    predicate=lambda member: isinstance(member, Hook),
                )
            }
            for workflow_name, workflow in workflows.items()
        }

        test_workflows = {
            workflow_name: (
                len(
                    [hook for hook in hooks.values() if hook.hook_type == HookType.TEST]
                )
                > 0
            )
            for workflow_name, hooks in workflow_hooks.items()
        }

        provisioned_workers = self._provisioner.partion_by_priority(
            [
                {
                    "workflow_name": workflow_name,
                    "priority": config.get("priority", StagePriority.AUTO),
                    "is_test": test_workflows[workflow_name],
                    "vus": config.get("vus", 1000),
                }
                for workflow_name, config in configs.items()
            ]
        )

        workflow_vus: Dict[str, List[int]] = defaultdict(list)

        for batch in provisioned_workers:
            for workflow_name, _, batch_threads in batch:
                workflow_config = configs[workflow_name]

                batch_threads = max(batch_threads, 1)

                vus = int(workflow_config["vus"] / batch_threads)
                remainder_vus = workflow_config["vus"] % batch_threads

                workflow_vus[workflow_name].extend([vus for _ in range(batch_threads)])

                workflow = workflows.get(workflow_name)

                if hasattr(workflow, "threads"):
                    setattr(workflow, "threads", threads)

                workflow_vus[workflow_name][-1] += remainder_vus

        return (provisioned_workers, workflow_vus)

    async def _provide_context(
        self,
        workflow: str,
        state_actions: Dict[str, ContextHook],
        context: Context,
        results: Dict[str, Any],
    ):
        workflow_slug = workflow.lower()
        async with self._logger.context(
            name=f"{workflow_slug}_logger",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Workflow {workflow} updating context",
                name="debug",
            )

            provide_actions = [
                action
                for action in state_actions.values()
                if action.action_type == StateAction.PROVIDE
            ]

            if len(provide_actions) < 1:
                return context

            hook_targets: Dict[str, Hook] = {}
            for hook in provide_actions:
                hook.context_args = {
                    name: value for name, value in context[workflow].items()
                }

                hook.context_args.update(results)

                hook_targets[hook.name] = hook.workflows

            context_results = await asyncio.gather(
                *[hook.call(**hook.context_args) for hook in provide_actions]
            )

            await asyncio.gather(
                *[
                    context[target].set(hook_name, result)
                    for hook_name, result in context_results
                    for target in hook_targets[hook_name]
                ]
            )

            return context

    async def shutdown_workers(self):
        async with self._logger.context(
            name="remote_graph_manager",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            await ctx.log(
                Entry(
                    message=f"Receivied shutdown request - stopping {self._threads} workers",
                    level=LogLevel.INFO,
                )
            )

            await self._controller.submit_stop_request()

    async def close(self):
        self._controller.stop()
        await self._controller.close()

    def abort(self):
        try:
            self._logger.abort()
            self._controller.abort()

        except Exception:
            pass
