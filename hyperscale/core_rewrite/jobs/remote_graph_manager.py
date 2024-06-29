import asyncio
import inspect
from collections import defaultdict
from typing import (
    Any,
    Dict,
    List,
    Tuple,
)

import psutil

from hyperscale.core_rewrite.engines.client.time_parser import TimeParser
from hyperscale.core_rewrite.graph.workflow import Workflow
from hyperscale.core_rewrite.hooks import Hook, HookType
from hyperscale.core_rewrite.jobs.models.env import Env
from hyperscale.core_rewrite.jobs.workers import Provisioner, StagePriority
from hyperscale.core_rewrite.results.workflow_types import WorkflowStats
from hyperscale.core_rewrite.state import (
    Context,
    ContextHook,
    StateAction,
)

from .graphs import WorkflowManager
from .graphs.workflow_runner import cancel_pending
from .models import InstanceRoleType
from .remote_graph_controller import RemoteGraphController

WorkflowResults = WorkflowStats | Dict[str, Any | Exception]
NodeResults = Tuple[
    WorkflowResults,
    Context,
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
            self._provisioner = Provisioner(max_workers=self._threads)
            self._provisioner.setup()

        await self._controller.start_server(
            cert_path=cert_path,
            key_path=key_path,
        )

        self.node_id = self._controller.node_id

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

        self._provisioner.max_workers = len(self._controller.nodes)

        pending_count = len(pending)
        if pending_count > 0:
            raise Exception(
                f"Err. - failed to {pending_count} nodes on initial connect."
            )

    async def run_forever(self):
        await self._controller.run_forever()

    async def execute_graph(
        self,
        graph: str,
        workflows: List[Workflow],
    ):
        manager = WorkflowManager(graph, workflows)

        run_id = self._controller.id_generator.generate()

        self._controller.create_run_contexts(run_id)

        manager.create_workflow_graph()

        graph_results: Dict[str, List[WorkflowResults]] = defaultdict(list)

        for workflow_set in manager.workflow_traversal_order:
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

            for workflow_name, worklow_results in results:
                graph_results[workflow_name] = [
                    node_results for node_results, _ in worklow_results
                ]

        return graph_results

    async def _run_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        threads: int,
    ) -> Tuple[str, List[NodeResults]]:
        await self._provisioner.acquire(threads)

        state_actions = self._setup_state_actions(workflow)

        contexts = self._controller.assign_contexts(
            run_id,
            workflow.name,
            threads,
        )

        loaded_contexts = await asyncio.gather(
            *[
                self._use_context(
                    workflow.name,
                    state_actions,
                    thread_context,
                )
                for thread_context in contexts
            ]
        )

        # ## Send batched requests

        await self._controller.submit_workflow_to_workers(
            run_id,
            workflow,
            loaded_contexts,
        )

        worker_results = await self._controller.poll_for_workflow_complete(
            run_id,
            workflow.name,
        )

        updated_contexts = await asyncio.gather(
            *[
                self._provide_context(
                    workflow.name,
                    state_actions,
                    thread_context,
                    results,
                )
                for results, thread_context in worker_results
            ]
        )

        await self._controller.update_contexts(
            run_id,
            updated_contexts,
        )

        self._provisioner.release(threads)

        return (workflow.name, worker_results)

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
            return context

        for hook in use_actions:
            hook.context_args = {
                name: value
                for provider in hook.workflows
                for name, value in context[provider].items()
            }

        results = await asyncio.gather(
            *[hook.call(**hook.context_args) for hook in use_actions]
        )

        await asyncio.gather(
            *[context[workflow].set(hook_name, result) for hook_name, result in results]
        )

        return context

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
