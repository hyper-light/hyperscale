import asyncio
import inspect
import math
import os
import time
from typing import Any, Callable, Dict, List

import networkx
import psutil

from .engines.client import TimeParser
from .engines.client.setup_clients import setup_client
from .hooks import Hook
from .workflow import Workflow


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


class Graph:
    def __init__(
        self,
        workflows: List[Workflow],
        context: Dict[str, Callable[..., Any] | object] = {},
    ) -> None:
        self.graph = __file__
        self.workflows = workflows
        self.max_active = 0
        self.active = 0

        self.context: Dict[str, Callable[..., Any] | object] = context

        self._active_waiter: asyncio.Future | None = None
        self._workflows_by_name: Dict[str, Workflow] = {}
        self._threads = os.cpu_count()
        self._config: Dict[str, Any] = {}
        self._traversal_orders: Dict[str, List[List[Hook]]] = {}

        self._workflow_test_status: Dict[str, bool] = {}
        self._pending: List[asyncio.Task] = []

    async def run(self):
        for workflow in self.workflows:
            await self._run(workflow)

    async def setup(self):
        call_ids: List[str] = []
        hooks_by_call_id: Dict[str, Hook] = {}

        for workflow in self.workflows:
            self._workflows_by_name[workflow.name] = workflow

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
                hook.call = hook.call.__get__(workflow, workflow.__class__)
                setattr(workflow, hook.name, hook.call)

            for hook_name, hook in hooks.items():
                workflow_graph.add_node(hook_name, hook=hook)

            sources = []

            for hook in hooks.values():
                if len(hook.dependencies) == 0:
                    sources.append(hook.name)

                for dependency in hook.dependencies:
                    workflow_graph.add_edge(dependency, hook.name)

            traversal_order: List[List[Hook]] = []

            for traversal_layer in networkx.bfs_layers(workflow_graph, sources):
                traversal_order.append(
                    [hooks.get(hook_name) for hook_name in traversal_layer]
                )

            self._traversal_orders[workflow.name] = traversal_order

            for hook in hooks.values():
                hooks_by_call_id.update({hook.call_id: hook})

                call_ids.append(hook.call_id)

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

    async def _run(self, workflow: Workflow):
        loop = asyncio.get_event_loop()

        traversal_order = self._traversal_orders[workflow.name]

        completed, pending = await asyncio.wait(
            [
                loop.create_task(
                    self._spawn_vu(traversal_order, remaining),
                )
                async for remaining in self._generate()
            ],
            timeout=1,
        )

        results: List[List[Any]] = await asyncio.gather(*completed)

        all_completed: List[Any] = []
        for results_set in results:
            all_completed.extend(results_set)

        print(len(all_completed))

        await asyncio.gather(
            *[asyncio.create_task(cancel_pending(pend)) for pend in self._pending]
        )

        await asyncio.gather(
            *[asyncio.create_task(cancel_pending(pend)) for pend in pending]
        )

        return all_completed

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
        traversal_order: List[List[Hook]],
        remaining: float,
    ):
        try:
            results: List[Any] = []

            for hook_set in traversal_order:
                set_count = len(hook_set)
                self.active += set_count

                completed, pending = await asyncio.wait(
                    [
                        asyncio.create_task(
                            hook.call(),
                        )
                        for hook in hook_set
                    ],
                    timeout=remaining,
                )

                results.extend(completed)

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
