import asyncio
import inspect
import time

from collections import defaultdict
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Tuple,
)

import networkx

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph.dependent_workflow import DependentWorkflow
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import Hook, HookType
from hyperscale.core.jobs.models import InstanceRoleType, WorkflowStatusUpdate
from hyperscale.core.jobs.models.env import Env
from hyperscale.core.jobs.workers import Provisioner, StagePriority
from hyperscale.core.results.workflow_results import WorkflowResults
from hyperscale.core.state import (
    Context,
    ContextHook,
    StateAction,
)
from hyperscale.ui import InterfaceUpdatesController
from hyperscale.ui.actions import (
    update_workflow_run_timer,
    update_active_workflow_message,
    update_workflow_executions_counter,
    update_workflow_executions_total_rate,
    update_workflow_progress_seconds,
    update_workflow_executions_rates,
    update_workflow_execution_stats,
)
from hyperscale.reporting.results_types import (
    RunResults,
    WorkflowResultsSet,
    WorkflowContextResult,
    WorkflowStats,
)
from hyperscale.reporting.reporter import Reporter
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
        self._graph_updates: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

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

    async def start(
        self,
        host: str,
        port: int,
        env: Env,
        cert_path: str | None = None,
        key_path: str | None = None,
    ):
        if self._controller is None:
            self._controller = RemoteGraphController(
                host,
                port,
                env,
            )

        if self._provisioner is None:
            self._provisioner = Provisioner()

        await self._controller.start_server(
            cert_path=cert_path,
            key_path=key_path,
        )

    async def connect_to_workers(
        self,
        workers: List[Tuple[str, int]],
        timeout: int | float | str | None = None,
    ):
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

    async def run_forever(self):
        await self._controller.run_forever()

    async def execute_graph(
        self,
        test_name: str,
        workflows: List[Workflow | DependentWorkflow],
    ) -> RunResults:
        run_id = self._controller.id_generator.generate()

        self._controller.create_run_contexts(run_id)

        workflow_traversal_order = self._create_workflow_graph(workflows)

        workflow_results: Dict[str, List[WorkflowResultsSet]] = defaultdict(list)

        for workflow_set in workflow_traversal_order:
            provisioned_batch, workflow_vus = self._provision(workflow_set)

            self._updates.update_active_workflows(
                [
                    workflow_name.lower()
                    for group in provisioned_batch
                    for workflow_name, _, _ in group
                ]
            )

            results = await asyncio.gather(
                *[
                    self._run_workflow(
                        run_id,
                        workflow_set[workflow_name],
                        threads,
                        workflow_vus[workflow_name],
                    )
                    for group in provisioned_batch
                    for workflow_name, _, threads in group
                ]
            )

            workflow_results.update(
                {workflow_name: results for workflow_name, results in results}
            )

        return {"test": test_name, "results": workflow_results}

    def _create_workflow_graph(self, workflows: List[Workflow | DependentWorkflow]):
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

                self._workflows[dependent_workflow.name] = dependent_workflow

                workflow_graph.add_node(dependent_workflow.name)

            else:
                self._workflows[workflow.name] = workflow
                sources.append(workflow.name)

                workflow_graph.add_node(workflow.name)

        for workflow_name, dependencies in workflow_dependencies.items():
            for dependency in dependencies:
                workflow_graph.add_edge(dependency, workflow_name)

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
    ) -> Tuple[str, WorkflowStats | WorkflowContextResult]:
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

        if is_test_workflow is False:
            threads = len(self._controller.nodes)

        await self._provisioner.acquire(threads)

        state_actions = self._setup_state_actions(workflow)

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

        self._workflow_timers[workflow.name] = time.monotonic()

        await self._controller.submit_workflow_to_workers(
            run_id,
            workflow,
            loaded_context,
            threads,
            workflow_vus,
            self._update,
        )

        worker_results = await self._controller.poll_for_workflow_complete(
            run_id, workflow.name, int(TimeParser(workflow.duration).time * 1.5)
        )

        await update_workflow_run_timer(workflow_slug, False)
        await update_active_workflow_message(
            workflow_slug, f"Processing results - {workflow.name}"
        )

        await update_workflow_executions_total_rate(workflow_slug, None, False)

        results, run_context = worker_results

        if is_test_workflow and len(results) > 1:
            workflow_results = WorkflowResults()
            execution_result = workflow_results.merge_results(
                [result_set for _, result_set in results.values()],
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

        await self._controller.update_context(
            run_id,
            updated_context,
        )

        reporter = Reporter(workflow.reporting_config)

        await reporter.connect()

        await reporter.submit_workflow_results(execution_result)
        await reporter.submit_step_results(execution_result)

        await reporter.close()

        await asyncio.sleep(1)

        await update_active_workflow_message(
            workflow_slug, f"Complete - {workflow.name}"
        )

        await asyncio.sleep(1)

        self._provisioner.release(threads)

        return (workflow.name, execution_result)

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

    async def get_workflow_update(self, workflow: str) -> WorkflowStatusUpdate | None:
        if self._graph_updates[workflow].empty() is False:
            return await self._graph_updates[workflow].get()

    async def _update(self, update: WorkflowStatusUpdate):
        if update:
            workflow_slug = update.workflow.lower()

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

                await update_workflow_execution_stats(workflow_slug, update.step_stats)

                self._workflow_last_elapsed[update.workflow] = time.monotonic()

            self._graph_updates[update.workflow].put_nowait(update)

    def _provision(
        self,
        workflows: Dict[str, Workflow],
    ) -> Tuple[ProvisionedBatch, WorkflowVUs]:
        configs = {
            workflow_name: {
                "threads": self._threads,
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

            config["threads"] = min(config["threads"], self._threads)

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
                    else self._threads
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

    async def shutdown_workers(self):
        await self._controller.submit_stop_request()

    async def close(self):
        await self._controller.close()

    def abort(self):
        try:
            self._controller.abort()

        except Exception:
            pass
