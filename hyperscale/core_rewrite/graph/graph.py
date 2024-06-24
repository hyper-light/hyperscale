import asyncio
import inspect
import math
import os
import time
import warnings
from collections import defaultdict
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    List,
    Tuple,
)

import networkx
import psutil

from hyperscale.core_rewrite.engines.client import TimeParser
from hyperscale.core_rewrite.engines.client.setup_clients import setup_client
from hyperscale.core_rewrite.hooks import Hook, HookType
from hyperscale.core_rewrite.results import WorkflowResults

from .dependent_workflow import DependentWorkflow
from .workflow import Workflow

warnings.simplefilter("ignore")


async def cancel_pending(pend: asyncio.Task):
    try:
        if pend.done():
            pend.exception()

            return pend

        pend.cancel()
        await asyncio.sleep(0)
        if not pend.cancelled():
            await pend

        return pend

    except asyncio.CancelledError as cancelled_error:
        return cancelled_error

    except asyncio.TimeoutError as timeout_error:
        return timeout_error

    except asyncio.InvalidStateError as invalid_state:
        return invalid_state


def _guard_result(result: asyncio.Task):
    try:
        return result.result()

    except Exception as err:
        return err


class Graph:
    def __init__(
        self,
        workflows: List[Workflow | DependentWorkflow],
    ) -> None:
        self.graph = __file__
        self.workflows = workflows
        self.max_active = 0
        self.active = 0

        self._active_waiters: Dict[str, asyncio.Future | None] = {}

        self._workflows_by_name: Dict[str, Workflow] = {}
        self._threads = os.cpu_count()

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

        self._workflow_test_status: Dict[str, bool] = {}
        self._pending: Dict[str, List[asyncio.Task]] = defaultdict(list)
        self._workflows: Dict[str, Workflow] = {}

    def create_workflow_graph(self):
        workflow_graph = networkx.DiGraph()

        workflow_dependencies: Dict[str, List[str]] = {}

        sources = []

        for workflow in self.workflows:
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
            self._workflow_traversal_order.append(
                {
                    workflow_name: self._workflows.get(workflow_name)
                    for workflow_name in traversal_layer
                }
            )

    async def run(self):
        for workflow_set in self._workflow_traversal_order:
            await asyncio.gather(
                *[self._run_workflow(workflow) for workflow in workflow_set.values()]
            )

    async def _run_workflow(self, workflow: Workflow):
        workflow = await self._setup(workflow)

        traversal_order = self._step_traversal_orders[workflow.name]

        is_test_workflow = (
            len(
                [
                    hook
                    for hook in workflow.hooks.values()
                    if hook.hook_type == HookType.TEST
                ]
            )
            > 0
        )

        if is_test_workflow:
            return await self._execute_test_workflow(workflow, traversal_order)

        return await self._execute_non_test_workflow(workflow, traversal_order)

    async def _execute_test_workflow(
        self,
        workflow: Workflow,
        traversal_order: List[Dict[str, Hook]],
    ):
        loop = asyncio.get_event_loop()

        workflow_name = workflow.name

        config = self._workflow_configs[workflow_name]

        completed, pending = await asyncio.wait(
            [
                loop.create_task(
                    self._spawn_vu(
                        workflow_name,
                        traversal_order,
                        remaining,
                    ),
                    name=workflow.name,
                )
                async for remaining in self._generate(
                    workflow_name,
                    config,
                )
            ],
            timeout=1,
        )

        completed_results = await asyncio.gather(*completed)

        await asyncio.gather(
            *[
                asyncio.create_task(cancel_pending(pend))
                for pend in self._pending[workflow_name]
            ]
        )

        await asyncio.gather(
            *[asyncio.create_task(cancel_pending(pend)) for pend in pending]
        )

        workflow_results_set: Dict[str, List[Any]] = {
            hook_name: [] for hook_name in workflow.hooks
        }

        [
            workflow_results_set[result.get_name()].append(
                _guard_result(result),
            )
            for complete in completed
            for result in complete.result()
            if _guard_result(result) is not None
        ]

        workflow_results = WorkflowResults(workflow.hooks)

        processed_results = workflow_results.process(
            workflow_name,
            workflow_results_set,
        )

        return processed_results

    async def _execute_non_test_workflow(
        self,
        workflow: Workflow,
        traversal_order: List[Dict[str, Hook]],
    ):
        workflow_name = workflow.name
        config = self._workflow_configs[workflow_name]

        execution_results: List[asyncio.Task] = await self._spawn_vu(
            workflow_name,
            traversal_order,
            config.get(
                "workflow_timeout",
                TimeParser("5m").time,
            ),
        )

        await asyncio.gather(*execution_results)

        await asyncio.gather(
            *[
                asyncio.create_task(cancel_pending(pend))
                for pend in self._pending[workflow_name]
            ]
        )

        return {
            result.get_name(): _guard_result(result) for result in execution_results
        }

    async def _setup(self, workflow: Workflow) -> Workflow:
        self._workflows_by_name[workflow.name] = workflow

        config = {
            "vus": 1000,
            "duration": "1m",
            "threads": self._threads,
            "connect_retries": 3,
            "workflow_timeout": "5m",
        }

        config.update(
            {
                name: value
                for name, value in inspect.getmembers(workflow)
                if config.get(name)
            }
        )

        config["workflow_timeout"] = TimeParser(config["workflow_timeout"]).time
        config["duration"] = TimeParser(config["duration"]).time

        self._workflow_configs[workflow.name] = config

        vus = config.get("vus")
        threads = config.get("threads")

        self.max_active = math.ceil(
            vus * (psutil.cpu_count(logical=False) ** 2) / threads
        )

        for client in workflow.client:
            setup_client(
                client,
                config.get("vus"),
                pages=config.get("pages", 1),
                cert_path=config.get("cert_path"),
                key_path=config.get("key_path"),
                reset_connections=config.get("reset_connections"),
            )

        hooks: Dict[str, Hook] = {
            name: hook
            for name, hook in inspect.getmembers(
                workflow,
                predicate=lambda member: isinstance(member, Hook),
            )
        }

        self._workflow_test_status[workflow.name] = (
            len([hook for hook in hooks.values() if hook.is_test]) > 0
        )

        step_graph = networkx.DiGraph()

        for hook in hooks.values():
            step_graph.add_node(hook.name)

            hook.call = hook.call.__get__(workflow, workflow.__class__)
            setattr(workflow, hook.name, hook.call)

        sources = []

        for hook in hooks.values():
            if len(hook.optimized_args) > 0 and hook.hook_type == HookType.TEST:
                await asyncio.gather(
                    *[
                        arg.optimize(hook.engine_type)
                        for arg in hook.optimized_args.values()
                    ]
                )

                await asyncio.gather(
                    *[
                        workflow.client[hook.engine_type]._optimize(arg)
                        for arg in hook.optimized_args.values()
                    ]
                )

            if len(hook.dependencies) == 0:
                sources.append(hook.name)

            for dependency in hook.dependencies:
                step_graph.add_edge(dependency, hook.name)

        traversal_order: List[Dict[str, Hook]] = []

        for traversal_layer in networkx.bfs_layers(step_graph, sources):
            traversal_order.append(
                {hook_name: hooks.get(hook_name) for hook_name in traversal_layer}
            )

        self._step_traversal_orders[workflow.name] = traversal_order

        workflow.hooks = hooks

        self._active_waiters[workflow.name] = None

        return workflow

    async def _generate(
        self,
        workflow_name: str,
        config: Dict[str, Any],
    ) -> AsyncGenerator[Any, float]:
        duration = config.get("duration")

        elapsed = 0

        start = time.monotonic()
        while elapsed < duration:
            remaining = duration - elapsed

            yield remaining

            await asyncio.sleep(0)

            if (
                self.active > self.max_active
                and self._active_waiters[workflow_name] is None
            ):
                self._active_waiters[workflow_name] = (
                    asyncio.get_event_loop().create_future()
                )

                try:
                    await asyncio.wait_for(
                        self._active_waiters[workflow_name],
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    pass

            elapsed = time.monotonic() - start

    async def _spawn_vu(
        self,
        workflow_name: str,
        traversal_order: List[Dict[str, Hook]],
        remaining: float,
    ):
        try:
            results: List[asyncio.Task] = []

            context: Dict[str, Any] = {}

            for hook_set in traversal_order:
                set_count = len(hook_set)
                self.active += set_count

                for hook in hook_set.values():
                    hook.context_args.update(
                        {
                            key: context[key]
                            for key in context
                            if key in hook.kwarg_names
                        }
                    )

                tasks: Tuple[
                    List[asyncio.Task],
                    List[asyncio.Task],
                ] = await asyncio.wait(
                    [
                        asyncio.create_task(
                            hook.call(**hook.context_args),
                            name=hook_name,
                        )
                        for hook_name, hook in hook_set.items()
                    ],
                    timeout=remaining,
                )

                completed, pending = tasks
                results.extend(completed)

                for complete in completed:
                    try:
                        result = complete.result()

                    except Exception as err:
                        result = err

                    context[complete.get_name()] = result

                self._pending[workflow_name].extend(pending)

                self.active -= set_count

                if (
                    self.active <= self.max_active
                    and self._active_waiters[workflow_name]
                ):
                    try:
                        self._active_waiters[workflow_name].set_result(None)
                        self._active_waiters[workflow_name] = None

                    except asyncio.InvalidStateError:
                        self._active_waiters[workflow_name] = None

        except Exception:
            pass

        return results
