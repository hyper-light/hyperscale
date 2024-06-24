import asyncio
import inspect
import math
import os
import time
import warnings
from typing import (
    Any,
    Dict,
    List,
    Tuple,
)

import networkx
import psutil

from .engines.client import TimeParser
from .engines.client.setup_clients import setup_client
from .hooks import Hook, HookType
from .results import WorkflowResults
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
        workflows: List[Workflow],
    ) -> None:
        self.graph = __file__
        self.workflows = workflows
        self.max_active = 0
        self.active = 0

        self._active_waiter: asyncio.Future | None = None
        self._workflows_by_name: Dict[str, Workflow] = {}
        self._threads = os.cpu_count()
        self._config: Dict[str, Any] = {}
        self._traversal_orders: Dict[
            str,
            List[
                Dict[
                    str,
                    Hook,
                ]
            ],
        ] = {}

        self._workflow_test_status: Dict[str, bool] = {}
        self._pending: List[asyncio.Task] = []
        self._results: Dict[Tuple[str, HookType], List[Any]] = {}

    async def run(self):
        for workflow in self.workflows:
            await self._run(workflow)

    async def setup(self):
        call_ids: List[str] = []
        hooks_by_call_id: Dict[str, Hook] = {}

        for workflow in self.workflows:
            self._workflows_by_name[workflow.name] = workflow

            clients = [
                workflow.client.graphql,
                workflow.client.graphqlh2,
                workflow.client.grpc,
                workflow.client.http,
                workflow.client.http2,
                workflow.client.http3,
                workflow.client.playwright,
                workflow.client.udp,
                workflow.client.websocket,
            ]

            config = {
                "vus": 1000,
                "duration": "1m",
                "threads": self._threads,
                "connect_retries": 3,
            }

            config.update(
                {
                    name: value
                    for name, value in inspect.getmembers(workflow)
                    if config.get(name)
                }
            )

            config["duration"] = TimeParser(config["duration"]).time

            self._config = config
            vus = self._config.get("vus")
            threads = self._config.get("threads")

            self.max_active = math.ceil(
                vus * (psutil.cpu_count(logical=False) ** 2) / threads
            )

            for client in clients:
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

            workflow_graph = networkx.DiGraph()

            for hook in hooks.values():
                workflow_graph.add_node(hook.name)

                hook.call = hook.call.__get__(workflow, workflow.__class__)
                setattr(workflow, hook.name, hook.call)

                self._results[hook.name] = []

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
                    workflow_graph.add_edge(dependency, hook.name)

            traversal_order: List[Dict[str, Hook]] = []

            for traversal_layer in networkx.bfs_layers(workflow_graph, sources):
                traversal_order.append(
                    {hook_name: hooks.get(hook_name) for hook_name in traversal_layer}
                )

            self._traversal_orders[workflow.name] = traversal_order

            for hook in hooks.values():
                hooks_by_call_id.update({hook.call_id: hook})

                call_ids.append(hook.call_id)

            workflow.hooks = hooks

    async def _run(self, workflow: Workflow):
        loop = asyncio.get_event_loop()

        traversal_order = self._traversal_orders[workflow.name]

        completed, pending = await asyncio.wait(
            [
                loop.create_task(
                    self._spawn_vu(traversal_order, remaining),
                    name=workflow.name,
                )
                async for remaining in self._generate()
            ],
            timeout=1,
        )

        await asyncio.gather(*completed)

        await asyncio.gather(
            *[asyncio.create_task(cancel_pending(pend)) for pend in self._pending]
        )

        await asyncio.gather(
            *[asyncio.create_task(cancel_pending(pend)) for pend in pending]
        )

        [
            self._results[result.get_name()].append(
                _guard_result(result),
            )
            for complete in completed
            for result in complete.result()
            if _guard_result(result) is not None
        ]

        results = WorkflowResults(
            workflow.hooks,
        )

        processed_results = results.process(workflow.name, self._results)

        return processed_results

    async def _generate(self):
        duration = self._config.get("duration")

        elapsed = 0

        start = time.monotonic()
        while elapsed < duration:
            remaining = duration - elapsed

            yield remaining

            await asyncio.sleep(0)

            if self.active > self.max_active and self._active_waiter is None:
                self._active_waiter = asyncio.get_event_loop().create_future()

                try:
                    await asyncio.wait_for(
                        self._active_waiter,
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    pass

            elapsed = time.monotonic() - start

    async def _spawn_vu(
        self,
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

                self._pending.extend(pending)

                self.active -= set_count

                if self.active <= self.max_active and self._active_waiter:
                    try:
                        self._active_waiter.set_result(None)
                        self._active_waiter = None

                    except asyncio.InvalidStateError:
                        self._active_waiter = None

        except Exception:
            pass

        return results
