import asyncio
import inspect
import math
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
from hyperscale.core_rewrite.graph.workflow import Workflow
from hyperscale.core_rewrite.hooks import Hook, HookType
from hyperscale.core_rewrite.jobs.models.env import Env
from hyperscale.core_rewrite.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core_rewrite.results.workflow_results import WorkflowResults
from hyperscale.core_rewrite.results.workflow_types import WorkflowStats
from hyperscale.core_rewrite.state import Context, ContextHook, StateAction
from hyperscale.core_rewrite.state.workflow_context import WorkflowContext
from hyperscale.core_rewrite.testing.models.base import OptimizedArg

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


def guard_result(result: asyncio.Task):
    try:
        return result.result()

    except Exception as err:
        return err


class WorkflowRunner:
    def __init__(self, env: Env) -> None:
        self.run_statuses: Dict[int, Dict[str, WorkflowStatus]] = defaultdict(dict)

        self._active: Dict[int, Dict[str, int]] = defaultdict(dict)
        self._active_waiters: Dict[int, Dict[str, asyncio.Future]] = defaultdict(dict)
        self._max_active: Dict[int, Dict[str, int]] = defaultdict(dict)
        self._pending: Dict[int, Dict[str, List[asyncio.Task]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._threads = psutil.cpu_count(logical=False)
        self._workflows_sem: asyncio.Semaphore | None = None
        self._max_running_workflows = env.MERCURY_SYNC_MAX_RUNNING_WORKFLOWS
        self._max_pending_workflows = env.MERCURY_SYNC_MAX_PENDING_WORKFLOWS
        self._run_check_lock: asyncio.Lock | None = None

    def setup(self):
        if self._workflows_sem is None:
            self._workflows_sem = asyncio.Semaphore(self._max_running_workflows)

        if self._run_check_lock is None:
            self._run_check_lock = asyncio.Lock()

    @property
    def pending(self):
        return len(
            [
                status
                for workflow_statuses in self.run_statuses.values()
                for status in workflow_statuses.values()
                if status == WorkflowStatus.PENDING
            ]
        )

    def get_workflow_status(
        self,
        run_id: int,
        workflow: str,
    ):
        return self.run_statuses.get(
            run_id,
            {},
        ).get(workflow, WorkflowStatus.UNKNOWN)

    async def run(
        self,
        run_id: int,
        workflow: Workflow,
        workflow_context: Dict[str, Any],
    ) -> Tuple[
        int,
        WorkflowStats
        | Dict[
            str,
            Any | Exception,
        ]
        | None,
        WorkflowContext | None,
        Exception | None,
        WorkflowStatus,
    ]:
        await self._run_check_lock.acquire()
        already_running = (
            self.run_statuses[run_id].get(workflow.name) == WorkflowStatus.RUNNING
        )

        if self.pending >= self._max_pending_workflows:
            return (
                run_id,
                None,
                None,
                Exception("Err. - Run rejected. Too many pending workflows."),
                WorkflowStatus.REJECTED,
            )

        elif already_running:
            return (
                run_id,
                None,
                None,
                Exception("Err. - Run rejected. Already running."),
                WorkflowStatus.REJECTED,
            )

        self._run_check_lock.release()

        self.run_statuses[run_id][workflow.name] = WorkflowStatus.PENDING

        async with self._workflows_sem:
            workflow_name = workflow.name

            self.run_statuses[run_id][workflow_name] = WorkflowStatus.CREATED

            context = Context()

            await asyncio.gather(
                *[
                    context.update(workflow_name, hook_name, value)
                    for hook_name, value in workflow_context.items()
                ]
            )

            self.run_statuses[run_id][workflow.name] = WorkflowStatus.RUNNING

            try:
                (results, updated_context) = await self._run_workflow(
                    run_id,
                    workflow,
                    context,
                )

                await asyncio.gather(
                    *[
                        context.update(workflow_name, hook_name, value)
                        for workflow_name, hook_context in updated_context.iter_workflow_contexts()
                        for hook_name, value in hook_context.items()
                    ]
                )

                workflow_name = workflow.name

                del self._active[run_id][workflow_name]
                del self._active_waiters[run_id][workflow_name]
                del self._max_active[run_id][workflow_name]
                del self._pending[run_id][workflow_name]

                self.run_statuses[run_id][workflow_name] = WorkflowStatus.COMPLETED

                return (
                    run_id,
                    results,
                    updated_context[workflow_name],
                    None,
                    WorkflowStatus.COMPLETED,
                )

            except Exception as err:
                self.run_statuses[run_id][workflow.name] = WorkflowStatus.FAILED
                return (
                    run_id,
                    None,
                    context[workflow_name],
                    err,
                    WorkflowStatus.FAILED,
                )

    async def _run_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        context: Context,
    ) -> Tuple[
        WorkflowStats
        | Dict[
            str,
            Any | Exception,
        ],
        Context,
    ]:
        state_actions = self._setup_state_actions(workflow)
        context = await self._use_context(
            workflow.name,
            state_actions,
            context,
        )

        (
            workflow,
            hooks,
            traversal_order,
            config,
        ) = await self._setup(run_id, workflow, context)

        is_test_workflow = (
            len([hook for hook in hooks.values() if hook.hook_type == HookType.TEST])
            > 0
        )

        if is_test_workflow:
            results = await self._execute_test_workflow(
                run_id,
                workflow,
                traversal_order,
                hooks,
                context,
                config,
            )

        else:
            results = await self._execute_non_test_workflow(
                run_id,
                workflow,
                traversal_order,
                context,
                config,
            )

        context = await self._provide_context(
            workflow.name,
            state_actions,
            context,
            results,
        )

        return (results, context)

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

        resolved = await asyncio.gather(
            *[hook.call(**hook.context_args) for hook in use_actions]
        )

        await asyncio.gather(
            *[context[workflow].set(hook_name, value) for hook_name, value in resolved]
        )

        return context

    async def _setup(
        self,
        run_id: int,
        workflow: Workflow,
        context: Context,
    ) -> Tuple[
        Workflow,
        Dict[str, Hook],
        List[
            Dict[
                str,
                Hook,
            ]
        ],
        Dict[str, Any],
    ]:
        self._active[run_id][workflow.name] = 0

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

        vus = config.get("vus")
        threads = config.get("threads")

        self._max_active[run_id][workflow.name] = math.ceil(
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

        step_graph = networkx.DiGraph()

        for hook in hooks.values():
            step_graph.add_node(hook.name)

            hook.call = hook.call.__get__(workflow, workflow.__class__)
            setattr(workflow, hook.name, hook.call)

        sources = []

        workflow_context = context[workflow.name]
        optimized_context_args = {
            name: value
            for name, value in workflow_context.items()
            if isinstance(value, OptimizedArg)
        }

        for hook in hooks.values():
            if hook.hook_type == HookType.TEST:
                hook.optimized_args.update(
                    {
                        name: value
                        for name, value in optimized_context_args.items()
                        if name in hook.kwarg_names
                    }
                )

        for hook in hooks.values():
            if len(hook.optimized_args) > 0 and hook.hook_type == HookType.TEST:
                for arg in hook.optimized_args.values():
                    arg.call_name = hook.name

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

        self._active_waiters[run_id][workflow.name] = None

        return (
            workflow,
            hooks,
            traversal_order,
            config,
        )

    async def _execute_test_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        traversal_order: List[Dict[str, Hook]],
        hooks: Dict[str, Hook],
        context: Context,
        config: Dict[str, Any],
    ):
        loop = asyncio.get_event_loop()

        workflow_name = workflow.name

        workflow_context = context[workflow_name].dict()

        start = time.monotonic()

        completed, pending = await asyncio.wait(
            [
                loop.create_task(
                    self._spawn_vu(
                        run_id,
                        workflow_name,
                        traversal_order,
                        remaining,
                        workflow_context,
                    ),
                    name=workflow.name,
                )
                async for remaining in self._generate(
                    run_id,
                    workflow_name,
                    config,
                )
            ],
            timeout=1,
        )

        elapsed = time.monotonic() - start

        await asyncio.gather(*completed)

        await asyncio.gather(
            *[
                asyncio.create_task(cancel_pending(pend))
                for pend in self._pending[run_id][workflow.name]
            ]
        )

        await asyncio.gather(
            *[asyncio.create_task(cancel_pending(pend)) for pend in pending]
        )

        workflow_results_set: Dict[str, List[Any]] = {
            hook_name: [] for hook_name in hooks
        }

        [
            workflow_results_set[result.get_name()].append(
                guard_result(result),
            )
            for complete in completed
            for result in complete.result()
            if guard_result(result) is not None
        ]

        workflow_results = WorkflowResults(hooks)

        processed_results = workflow_results.process(
            workflow_name,
            workflow_results_set,
            elapsed,
        )

        return processed_results

    async def _execute_non_test_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        traversal_order: List[Dict[str, Hook]],
        context: Context,
        config: Dict[str, Any],
    ) -> Dict[str, Any | Exception]:
        workflow_name = workflow.name
        workflow_context = context[workflow_name].dict()

        execution_results: List[asyncio.Task] = await self._spawn_vu(
            run_id,
            workflow_name,
            traversal_order,
            config.get(
                "workflow_timeout",
                TimeParser("5m").time,
            ),
            workflow_context,
        )

        await asyncio.gather(*execution_results)

        await asyncio.gather(
            *[
                asyncio.create_task(cancel_pending(pend))
                for pend in self._pending[run_id][workflow_name]
            ]
        )

        return {result.get_name(): guard_result(result) for result in execution_results}

    async def _spawn_vu(
        self,
        run_id: int,
        workflow_name: str,
        traversal_order: List[Dict[str, Hook]],
        remaining: float,
        context: Dict[str, Any],
    ):
        results: List[asyncio.Task] = []
        context: Dict[str, Any] = dict(context)

        for hook_set in traversal_order:
            set_count = len(hook_set)
            self._active[run_id][workflow_name] += set_count

            for hook in hook_set.values():
                hook.context_args.update(
                    {key: context[key] for key in context if key in hook.kwarg_names}
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

            self._pending[run_id][workflow_name].extend(pending)

            self._active[run_id][workflow_name] -= set_count

            if (
                self._active[run_id][workflow_name]
                <= self._max_active[run_id][workflow_name]
                and self._active_waiters[run_id][workflow_name]
            ):
                try:
                    self._active_waiters[run_id][workflow_name].set_result(None)
                    self._active_waiters[run_id][workflow_name] = None

                except asyncio.InvalidStateError:
                    self._active_waiters[run_id][workflow_name] = None

        return results

    async def _generate(
        self,
        run_id: int,
        workflow_name: str,
        config: Dict[str, Any],
    ) -> AsyncGenerator[float, None]:
        duration = config.get("duration")

        elapsed = 0

        start = time.monotonic()
        while elapsed < duration:
            remaining = duration - elapsed

            yield remaining

            await asyncio.sleep(0)

            if (
                self._active[run_id][workflow_name]
                > self._max_active[run_id][workflow_name]
                and self._active_waiters[run_id][workflow_name] is None
            ):
                self._active_waiters[run_id][workflow_name] = (
                    asyncio.get_event_loop().create_future()
                )

                try:
                    await asyncio.wait_for(
                        self._active_waiters[run_id][workflow_name],
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    pass

            elapsed = time.monotonic() - start

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

        for hook in provide_actions:
            hook.context_args = {
                name: value for name, value in context[workflow].items()
            }

            hook.context_args.update(results)

        await asyncio.gather(
            *[hook.call(**hook.context_args) for hook in provide_actions]
        )

        await asyncio.gather(
            *[
                context[target].set(hook.name, hook.result)
                for hook in provide_actions
                for target in hook.workflows
            ]
        )

        return context
