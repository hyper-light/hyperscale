import asyncio
import inspect
import time
from collections import defaultdict, deque
from typing import (
    Any,
    Dict,
    List,
    Tuple,
    Deque,
)

import networkx

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import Hook, HookType
from hyperscale.core.jobs.models import InstanceRoleType, WorkflowStatusUpdate
from hyperscale.core.jobs.models import (
    CancellationUpdate,
    WorkflowResults,
    WorkflowCancellationStatus,
    WorkflowCancellationUpdate,
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

from .remote_graph_controller import RemoteGraphController

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

            await self._controller.poll_for_start(self._threads)

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
        Execute a graph of workflows respecting dependencies.

        Uses an iterative approach where we repeatedly find workflows whose
        ALL dependencies have completed successfully, execute them in parallel,
        and repeat until no more workflows can be executed.

        A workflow only executes if ALL its dependencies have completed
        successfully. If any dependency failed, the dependent workflow is
        skipped (failure propagates transitively).

        This mirrors worker execution semantics where dependent workflows
        cannot execute until all dependencies have successfully completed.
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

            # Build the workflow graph - returns layers in dependency order
            workflow_traversal_order = self._create_workflow_graph(workflows)

            workflow_results: Dict[str, List[WorkflowResultsSet]] = defaultdict(list)
            timeouts: dict[str, Exception] = {}
            skipped: dict[str, str] = {}  # workflow_name -> reason for skipping

            # Execute workflows layer by layer (BFS order ensures dependencies run first)
            for workflow_set in workflow_traversal_order:
                # Filter out workflows whose dependencies failed
                eligible_workflows: Dict[str, Workflow] = {}
                for workflow_name, workflow in workflow_set.items():
                    dependencies = self._workflow_dependencies.get(workflow_name, [])

                    # Check if any dependencies failed
                    failed_deps = [
                        dep for dep in dependencies
                        if dep in self._failed_workflows[run_id]
                    ]
                    if failed_deps:
                        # Skip this workflow - one or more dependencies failed
                        failed_dep_names = ", ".join(sorted(failed_deps))
                        skip_reason = f"Dependencies failed: {failed_dep_names}"
                        skipped[workflow_name] = skip_reason
                        self._failed_workflows[run_id].add(workflow_name)

                        await ctx.log(
                            GraphDebug(
                                message=f"Skipping workflow {workflow_name}: {skip_reason}",
                                workflows=[workflow_name],
                                workers=self._threads,
                                graph=test_name,
                                level=LogLevel.DEBUG,
                            )
                        )
                        continue

                    eligible_workflows[workflow_name] = workflow

                if not eligible_workflows:
                    # All workflows in this layer were skipped
                    continue

                provisioned_batch, workflow_vus = self._provision(eligible_workflows)

                batch_workflows = [
                    workflow_name
                    for group in provisioned_batch
                    for workflow_name, _, _ in group
                ]

                print(batch_workflows)

                workflow_names = ", ".join(batch_workflows)

                await ctx.log(
                    GraphDebug(
                        message=f"Graph {test_name} executing workflows {workflow_names}",
                        workflows=batch_workflows,
                        workers=self._threads,
                        graph=test_name,
                        level=LogLevel.DEBUG,
                    )
                )

                self._updates.update_active_workflows([
                    workflow_name.lower() for workflow_name in batch_workflows
                ])

                results = await asyncio.gather(
                    *[
                        self._run_workflow(
                            run_id,
                            eligible_workflows[workflow_name],
                            threads,
                            workflow_vus[workflow_name],
                        )
                        for group in provisioned_batch
                        for workflow_name, _, threads in group
                    ]
                )


                await ctx.log(
                    GraphDebug(
                        message=f"Graph {test_name} completed workflows {workflow_names}",
                        workflows=batch_workflows,
                        workers=self._threads,
                        graph=test_name,
                        level=LogLevel.DEBUG,
                    )
                )

                # Process results and track completion/failure status
                for workflow_name, workflow_result, _, timeout_error in results:
                    if timeout_error is None:
                        # Workflow completed successfully
                        workflow_results[workflow_name] = workflow_result
                        self._completed_workflows[run_id].add(workflow_name)
                    else:
                        # Workflow failed (timeout or error)
                        timeouts[workflow_name] = timeout_error
                        self._failed_workflows[run_id].add(workflow_name)

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
            
            results =  await self._run_workflow(
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

    def _create_workflow_graph(self, workflows: List[
        tuple[list[str], Workflow]
    ]):
        """
        Create workflow dependency graph and return traversal order.

        Builds a directed acyclic graph (DAG) where edges represent dependencies.
        Returns workflows grouped by BFS layer - all workflows in a layer can
        execute in parallel once their dependencies are satisfied.

        Also populates self._workflow_dependencies for runtime dependency checking.
        """
        # Clear previous run's workflows
        self._workflows.clear()
        self._workflow_dependencies.clear()

        workflow_graph = networkx.DiGraph()

        sources = []

        workflow_traversal_order: List[
            Dict[
                str,
                Workflow,
            ]
        ] = []
        
        for dependencies, workflow in workflows:
            if (
                len(dependencies) > 0
            ):
                self._workflows[workflow.name] = workflow
                self._workflow_dependencies[workflow.name] = dependencies
                workflow_graph.add_node(workflow.name)

            else:
                sources.append(workflow.name)
                self._workflows[workflow.name] = workflow
                workflow_graph.add_node(workflow.name)

        for dependent, deps in self._workflow_dependencies.items():
            for dependency in deps:
                workflow_graph.add_edge(dependency, dependent)

                

        for traversal_layer in networkx.bfs_layers(workflow_graph, sources):
            workflow_traversal_order.append(
                {
                    workflow_name: self._workflows.get(workflow_name)
                    for workflow_name in traversal_layer
                }
            )

        return workflow_traversal_order

    async def _run_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        threads: int,
        workflow_vus: List[int],
        skip_reporting: bool = False,
    ) -> Tuple[str, WorkflowStats | dict[int, WorkflowResults], Context, Exception | None]:
        import sys
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

                if is_test_workflow is False:
                    threads = self._threads # We do this to ensure *every* local worker node gets the update
                    workflow_vus = [workflow.vus for _ in range(threads)]
                    await ctx.log_prepared(
                        message=f"Non-test Workflow {workflow.name} now using 1 workers",
                        name="trace",
                    )

                await ctx.log_prepared(
                    message=f"Workflow {workflow.name} waiting for {threads} workers to be available",
                    name="trace",
                )

                await self._provisioner.acquire(threads)

                await ctx.log_prepared(
                    message=f"Workflow {workflow.name} successfully assigned {threads} workers",
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


                await self._controller.submit_workflow_to_workers(
                    run_id,
                    workflow,
                    loaded_context,
                    threads,
                    workflow_vus,
                    self._update,
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

                worker_results = await self._controller.poll_for_workflow_complete(
                    run_id,
                    workflow.name,
                    workflow_timeout,
                    self._update_available_cores,
                )

                results, run_context, timeout_error = worker_results

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
                    self._provisioner.release(threads)

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
                    message=f"Workflow {workflow.name} run {run_id} complete - releasing workers from pool",
                    name="debug",
                )

                self._provisioner.release(threads)

                return (workflow.name, execution_result, updated_context, timeout_error)

        except (
            KeyboardInterrupt,
            BrokenPipeError,
            asyncio.CancelledError,
        ) as err:
            self._provisioner.release(threads)
            await update_active_workflow_message(workflow_slug, "Aborted")

            raise err

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

    async def _update(
        self,
        run_id: int,
        update: WorkflowStatusUpdate,
    ):
        if update:
            workflow_slug = update.workflow.lower()

            async with self._logger.context(
                name=f"{workflow_slug}_logger",
            ) as ctx:
                await ctx.log_prepared(
                    message=f"Workflow {update.workflow} submitting stats update",
                    name="trace",
                )

                elapsed = time.monotonic() - self._workflow_timers[update.workflow]
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

                if self._workflow_last_elapsed.get(update.workflow) is None:
                    self._workflow_last_elapsed[update.workflow] = time.monotonic()

                last_sampled = (
                    time.monotonic() - self._workflow_last_elapsed[update.workflow]
                )

                if last_sampled > 1:
                    self._workflow_completion_rates[update.workflow].append(
                        (int(elapsed), int(completed_count / elapsed))
                    )

                    await update_workflow_executions_rates(
                        workflow_slug, self._workflow_completion_rates[update.workflow]
                    )

                    await update_workflow_execution_stats(
                        workflow_slug, update.step_stats
                    )

                    self._workflow_last_elapsed[update.workflow] = time.monotonic()

                self._graph_updates[run_id][update.workflow].put_nowait(update)

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
