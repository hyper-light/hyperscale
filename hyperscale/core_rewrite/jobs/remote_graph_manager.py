import asyncio
import inspect
from collections import defaultdict
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Tuple,
)

import networkx
import psutil

from hyperscale.core_rewrite.engines.client.time_parser import TimeParser
from hyperscale.core_rewrite.graph.dependent_workflow import DependentWorkflow
from hyperscale.core_rewrite.graph.workflow import Workflow
from hyperscale.core_rewrite.hooks import Hook, HookType
from hyperscale.core_rewrite.jobs.models.env import Env
from hyperscale.core_rewrite.jobs.workers import Provisioner, StagePriority
from hyperscale.core_rewrite.results.workflow_results import WorkflowResults
from hyperscale.core_rewrite.results.workflow_types import (
    WorkflowContextResult,
    WorkflowStats,
)
from hyperscale.core_rewrite.state import (
    Context,
    ContextHook,
    StateAction,
)

from .graphs.workflow_runner import cancel_pending
from .models import InstanceRoleType
from .remote_graph_controller import RemoteGraphController

WorkflowResultsSet = WorkflowStats | WorkflowContextResult
NodeResults = Tuple[
    WorkflowResultsSet,
    Context,
]

RunResults = Dict[
    Literal[
        "workflow",
        "results",
    ],
    str
    | Dict[
        str,
        WorkflowStats | WorkflowContextResult,
    ],
]


class RemoteGraphManager:
    def __init__(
        self,
        workers: List[Tuple[str, int]] | None = None,
    ) -> None:
        self._workers = workers
        self._workflows: Dict[str, Workflow] = {}
        self._threads = psutil.cpu_count(logical=False)
        self._controller: RemoteGraphController | None = None
        self._role = InstanceRoleType.PROVISIONER
        self._provisioner: Provisioner | None = None

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
        cert_path: str | None = None,
        key_path: str | None = None,
        timeout: int | float | str | None = None,
    ):
        if isinstance(timeout, str):
            timeout = TimeParser(timeout).time

        elif timeout is None:
            timeout = self._controller._request_timeout

        completed, pending = await asyncio.wait(
            [
                asyncio.create_task(
                    asyncio.wait_for(
                        self._controller.connect_client(
                            address,
                            cert_path=cert_path,
                            key_path=key_path,
                        ),
                        timeout=timeout,
                    )
                )
                for address in self._workers
            ],
            timeout=timeout,
        )

        await asyncio.gather(*completed)

        await asyncio.gather(*[cancel_pending(pend) for pend in pending])

        self._provisioner.setup(max_workers=len(self._controller.nodes))

        pending_count = len(pending)
        if pending_count > 0:
            raise Exception(
                f"Err. - failed to {pending_count} nodes on initial connect."
            )

    async def run_forever(self):
        await self._controller.run_forever()

    async def execute_graph(
        self,
        workflow: str,
        workflows: List[Workflow | DependentWorkflow],
    ) -> RunResults:
        run_id = self._controller.id_generator.generate()

        self._controller.create_run_contexts(run_id)

        workflow_traversal_order = self._create_workflow_graph(workflows)

        workflow_results: Dict[str, List[WorkflowResultsSet]] = defaultdict(list)

        for workflow_set in workflow_traversal_order:
            results = await asyncio.gather(
                *[
                    self._run_workflow(
                        run_id,
                        workflow_set[workflow_name],
                        threads,
                    )
                    for group in self._provision(workflow_set)
                    for workflow_name, _, threads in group
                ]
            )

            workflow_results.update(
                {workflow_name: results for workflow_name, results in results}
            )

        return {"workflow": workflow, "results": workflow_results}

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

        await self._controller.submit_workflow_to_workers(
            run_id,
            workflow,
            loaded_context,
            threads,
        )

        worker_results = await self._controller.poll_for_workflow_complete(
            run_id,
            workflow.name,
        )

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

    def _provision(
        self,
        workflows: Dict[str, Workflow],
    ) -> List[
        List[
            Tuple[
                str,
                StagePriority,
                int,
            ]
        ]
    ]:
        configs = {
            workflow_name: {
                "threads": self._threads,
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

        config["threads"] = min(config["threads"], len(self._controller.nodes))

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
                        self._threads if test_workflows[workflow_name] else 0,
                    ),
                }
                for workflow_name, config in configs.items()
            ]
        )

        return provisioned_workers

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

    async def close(self):
        await self._controller.close()
