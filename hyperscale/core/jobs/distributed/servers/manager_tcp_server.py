import asyncio
import inspect
import os
import psutil
import networkx
import socket
import time
from collections import defaultdict
from hyperscale.core.graph.dependent_workflow import DependentWorkflow
from hyperscale.core.hooks import Hook, HookType
from hyperscale.core.jobs.protocols import TCPProtocol
from hyperscale.core.jobs.models import (
    JobContext,
    ReceivedReceipt,
    Response,
    WorkflowJob,
    WorkflowResults,
    WorkflowStatusUpdate,
    Env
)
from hyperscale.core.jobs.graphs import WorkflowRunner
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.snowflake import Snowflake
from hyperscale.core.state import Context
from hyperscale.logging import Logger, Entry, LogLevel
from hyperscale.logging.hyperscale_logging_models import (
    RunTrace,
    RunDebug,
    RunInfo,
    RunError,
    RunFatal,
    StatusUpdate
)
from hyperscale.core.jobs.hooks import (
    receive,
    send,
    task,
)
from hyperscale.core.jobs.distributed.models import (
    Test,
    TestResults,
    TestReceivedReceipt,
    WorkerStatsUpdateRequest,
    WorkerStatsUpdate,
    WorkflowRun,
)
from hyperscale.core.jobs.distributed.concurrency import BatchedSemaphore
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.core.jobs.runner.local_server_pool import LocalServerPool
from hyperscale.core.jobs.workers import Provisioner, StagePriority
from hyperscale.core.state import (
    Context,
    ContextHook,
    StateAction,
)
from hyperscale.reporting.common.results_types import (
    RunResults,
    WorkflowResultsSet,
    WorkflowContextResult,
    WorkflowStats,
)
from hyperscale.reporting.results import Results
from hyperscale.reporting.reporter import Reporter, ReporterConfig
from hyperscale.reporting.reporter import ReporterConfig
from hyperscale.ui import HyperscaleInterface, InterfaceUpdatesController
from typing import Any, Tuple, TypeVar, Dict, Literal, Set, List

T = TypeVar("T")

WorkflowResult = Tuple[
    int,
    WorkflowStats | Dict[str, Any | Exception],
]


NodeContextSet = Dict[int, Context]

NodeData = Dict[
    int,
    Dict[
        str,
        Dict[int, T],
    ],
]

StepStatsType = Literal[
    "total",
    "ok",
    "err",
]


StepStatsUpdate = Dict[str, Dict[StepStatsType, int]]

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


class ManagerTCPServer(TCPProtocol[JobContext[Any], JobContext[Any]]):
    
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
    ):
        super().__init__(host, port, env)
        self._node_pool_availability: dict[int, int] = {}
        self._node_run_results: dict[
            int,
            dict[
                int,
                RunResults
            ]
        ] = {}

        self._logfile = 'hyperscale.distributed.manager.log.json'

        self._results: NodeData[WorkflowResult] = defaultdict(lambda: defaultdict(dict))
        self._errors: NodeData[Exception] = defaultdict(lambda: defaultdict(dict))

        self._node_context: NodeContextSet = defaultdict(lambda: Context())
        self._statuses: NodeData[WorkflowStatus] = defaultdict(
            lambda: defaultdict(dict)
        )

        self._run_workflow_expected_nodes: Dict[int, Dict[str, int]] = defaultdict(dict)

        self._completions: Dict[int, Dict[str, Set[int]]] = defaultdict(
            lambda: defaultdict(set),
        )

        self._completed_counts: Dict[int, Dict[str, Dict[int, int]]] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: 0),
            )
        )

        self._failed_counts: Dict[int, Dict[str, Dict[int, int]]] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: 0),
            )
        )

        self._step_stats: Dict[int, Dict[str, Dict[int, StepStatsUpdate]]] = (
            defaultdict(
                lambda: defaultdict(
                    lambda: defaultdict(
                        lambda: defaultdict(lambda: {"total": 0, "ok": 0, "err": 0})
                    )
                )
            )
        )

        self._cpu_usage_stats: Dict[int, Dict[str, Dict[int, float]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: 0))
        )

        self._memory_usage_stats: Dict[int, Dict[str, Dict[int, float]]] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: 0),
            )
        )

        self._context_poll_rate = TimeParser(env.MERCURY_SYNC_CONTEXT_POLL_RATE).time
        self._completion_write_lock: Dict[int, Dict[str, Dict[int, asyncio.Lock]]] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(asyncio.Lock)))
        )

        self._leader_lock: asyncio.Lock | None = None
        self._run_workflows: Dict[int, Dict[str, Workflow]] = defaultdict(dict)
        self._provisioner = Provisioner()

    async def start_server(
        self,
        cert_path: str | None = None,
        key_path: str | None = None,
        worker_socket: socket.socket | None = None,
        worker_server: asyncio.Server | None = None,
    ) -> None:
        if self._leader_lock is None:
            self._leader_lock = asyncio.Lock()

        await super().start_server(
            self._logfile,
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=worker_socket,
            worker_server=worker_server,
        )

    def create_run_contexts(self, run_id: int):
        self._node_context[run_id] = Context()

    def assign_context(
        self,
        run_id: int,
        workflow_name: str,
        threads: int,
    ):
        self._run_workflow_expected_nodes[run_id][workflow_name] = threads

        return self._node_context[run_id]
    
    def create_workflow_graph(
        self, 
        run_id: int,
        workflows: List[Workflow | DependentWorkflow],
    ):
        workflow_graph = networkx.DiGraph()

        workflow_dependencies: Dict[str, List[str]] = {}

        sources = []

        workflow_traversal_order: List[
            Dict[
                str,
                Workflow,
            ]
        ] = []

        for workflow in workflows:
            if (
                isinstance(workflow, DependentWorkflow)
                and len(workflow.dependencies) > 0
            ):
                dependent_workflow = workflow.dependent_workflow
                workflow_dependencies[dependent_workflow.name] = workflow.dependencies

                self._run_workflows[run_id][dependent_workflow.name] = dependent_workflow

                workflow_graph.add_node(dependent_workflow.name)

            else:
                self._run_workflows[run_id][workflow.name] = workflow
                sources.append(workflow.name)

                workflow_graph.add_node(workflow.name)

        for workflow_name, dependencies in workflow_dependencies.items():
            for dependency in dependencies:
                workflow_graph.add_edge(dependency, workflow_name)

        for traversal_layer in networkx.bfs_layers(workflow_graph, sources):
            workflow_traversal_order.append(
                {
                    workflow_name: self._run_workflows[run_id].get(workflow_name)
                    for workflow_name in traversal_layer
                }
            )

        return workflow_traversal_order
    
    def provision(
        self,
        registered_threads: int,
        workflows: Dict[str, Workflow],
    ) -> Tuple[ProvisionedBatch, WorkflowVUs]:
        configs = {
            workflow_name: {
                "threads": registered_threads,
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

            config["threads"] = min(config["threads"], registered_threads)

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
                    "threads": config.get(
                        "threads",
                    )
                    if config.get("threads")
                    else registered_threads
                    if test_workflows[workflow_name]
                    else 0,
                }
                for workflow_name, config in configs.items()
            ]
        )

        workflow_vus: Dict[str, List[int]] = defaultdict(list)

        for batch in provisioned_workers:
            for workflow_name, _, threads in batch:
                workflow_config = configs[workflow_name]

                vus = int(workflow_config["vus"] / threads)
                remainder_vus = workflow_config["vus"] % threads

                workflow_vus[workflow_name].extend([vus for _ in range(threads)])

                workflow = workflows.get(workflow_name)

                if hasattr(workflow, "threads"):
                    setattr(workflow, "threads", threads)

                workflow_vus[workflow_name][-1] += remainder_vus

        return (provisioned_workers, workflow_vus)
    
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

    async def update_context(
        self,
        run_id: int,
        context: Context,
    ):
        await self._node_context[run_id].copy(context)

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

    async def _provide_context(
        self,
        workflow: str,
        state_actions: Dict[str, ContextHook],
        context: Context,
        results: Dict[str, Any],
    ):
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
    
    async def _report_results(
        self,
        workflow: Workflow,
        execution_result: WorkflowStats | Dict[str, Any | Exception],
    ):

        reporting = workflow.reporting 

        configs: list[ReporterConfig] = []

        if inspect.isawaitable(reporting) or inspect.iscoroutinefunction(reporting):
            configs = await reporting()

        elif inspect.isfunction(reporting):
            configs = await self._loop.run_in_executor(
                None,
                reporting,
            )

        else:
            configs = reporting

        if isinstance(configs, list) is False:
            configs = [configs]
        
        reporters = [
            Reporter(config) for config in configs
        ]

        try:

            await asyncio.gather(*[
                reporter.connect() for reporter in reporters
            ])

            await asyncio.gather(*[
                reporter.submit_workflow_results(execution_result) for reporter in reporters
            ])
            await asyncio.gather(*[
                reporter.submit_step_results(execution_result) for reporter in reporters
            ])

            await asyncio.gather(*[
                reporter.close() for reporter in reporters
            ])

        except Exception:
            await asyncio.gather(*[
                reporter.close() for reporter in reporters
            ], return_exceptions=True)

    async def run_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        threads: int,
        workflow_vus: List[int],
        skip_reporting: bool = False,
    ) -> Tuple[str, WorkflowStats | WorkflowContextResult, Exception | None]:

        hooks: Dict[str, Hook] = {
            name: hook
            for name, hook in inspect.getmembers(
                workflow,
                predicate=lambda member: isinstance(member, Hook),
            )
        }

        is_test_workflow = (
            len([hook for hook in hooks.values() if hook.hook_type == HookType.TEST])
            > 0
        )

        state_actions = self._setup_state_actions(workflow)
        context = self.assign_context(
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

        await self._controller.submit_workflow_to_workers(
            run_id,
            workflow,
            loaded_context,
            threads,
            workflow_vus,
        )

        workflow_timeout = int(
            TimeParser(workflow.duration).time + TimeParser(workflow.timeout).time,
        )

        worker_results = await self.poll_for_workflow_complete(
            run_id,
            workflow.name, 
            workflow_timeout,
        )

        results, run_context, timeout_error = worker_results

        await self._provisioner.acquire(threads)

        if is_test_workflow and len(results) > 1:

            workflow_results = Results(hooks)
            execution_result = workflow_results.merge_results(
                [
                    result_set
                    for _, result_set in results.values()
                    if result_set is not None
                ],
                run_id=run_id,
            )

        elif is_test_workflow is False and len(results) > 1:     
            _, execution_result = list(
                sorted(
                    results.values(),
                    key=lambda result: result[0],
                    reverse=True,
                )
            ).pop()

        else:
            _, execution_result = list(results.values()).pop()

        updated_context = await self._provide_context(
            workflow.name,
            state_actions,
            run_context,
            execution_result,
        )

        await self.update_context(
            run_id,
            updated_context,
        )

        if skip_reporting is False:
            await self._report_results(
                workflow,
                execution_result,
            )

        self._provisioner.release(threads)

        return (workflow.name, execution_result, timeout_error)
    
    @task()
    async def run_test(
        self,
        run_id: int,
        test: Test,
    ):
        run_id = self.id_generator.generate()
        self.create_run_contexts(run_id)

        workflow_traversal_order = self.create_workflow_graph(
            run_id,
            test.workflows,
        )

        workflow_results: Dict[str, List[WorkflowResultsSet]] = defaultdict(list)

        timeouts: dict[str, Exception] = {}


        self._provisioner.setup(max_workers=self._total_threads)

        for workflow_set in workflow_traversal_order:
            provisioned_batch, workflow_vus = self.provision(
                self._total_threads,
                workflow_set
            )

            results = await asyncio.gather(
                *[
                    self.run_workflow(
                        run_id,
                        workflow_set[workflow_name],
                        threads,
                        workflow_vus[workflow_name],
                    )
                    for group in provisioned_batch
                    for workflow_name, _, threads in group
                ],
                return_exceptions=True
            )

            workflow_results.update(
                {workflow_name: results for workflow_name, results, timeout_error in results if timeout_error is None}
            )

            for workflow_name, _, timeout_error in results:
                timeouts[workflow_name] = timeout_error

        return {
            "test": test.name, 
            "results": workflow_results, 
            'timeouts': timeouts,
        }

    @send()
    async def submit_workflow(
        self,
        run_id: int,
        target_worker: tuple[str, int],
        workflow: Workflow,
        threads: int,
        vus: int,
    ) -> Response[JobContext[ReceivedReceipt]]:
        return await self.send(
            'receive_new_workflow',
            WorkflowRun(
                workflow,
                threads,
                vus,
            ),
            run_id=run_id,
            target_address=target_worker,
        )
        

    @send()
    async def update_worker_availability(
        self,
        worker_address: tuple[str, int],
    ) -> JobContext[WorkerStatsUpdate]:
        response = await self.send(
            'process_availability_request',
            JobContext(
                WorkerStatsUpdateRequest(self._node_id_base)
            ),
            target_address=worker_address,
        )

        _, ctx = response

        status_update: WorkerStatsUpdate = ctx.data

        self._node_pool_availability[status_update.node_id] = max(
            0,
            status_update.available_threads,
        )

        return (
            worker_address,
            status_update
        )
    

    @receive()
    async def receive_test_results(
        self,
        shard_id: int,
        workflow_results: JobContext[WorkflowResults],
    ):
        
        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance
        timestamp = snowflake.timestamp

        run_id = workflow_results.run_id
        workflow_name = workflow_results.data.workflow

        results = workflow_results.data.results
        workflow_context = workflow_results.data.context
        error = workflow_results.data.error
        status = workflow_results.data.status

        await self._leader_lock.acquire()
        await asyncio.gather(
            *[
                self._node_context[run_id].update(
                    workflow_name,
                    key,
                    value,
                    timestamp=timestamp,
                )
                for _ in self.nodes
                for key, value in workflow_context.items()
            ]
        )

        self._results[run_id][workflow_name][node_id] = (
            timestamp,
            results,
        )
        self._statuses[run_id][workflow_name][node_id] = status
        self._errors[run_id][workflow_name][node_id] = Exception(error)

        self._completions[run_id][workflow_name].add(node_id)

        if self._leader_lock.locked():
            self._leader_lock.release()

        return JobContext(
            ReceivedReceipt(
                workflow_name,
                node_id,
            ),
            run_id=run_id,
        )

