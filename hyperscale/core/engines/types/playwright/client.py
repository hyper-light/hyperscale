import asyncio
import uuid
from typing import Any, Coroutine, Dict, List, Union

from hyperscale.core.engines.types.common import Timeouts
from hyperscale.core.engines.types.common.base_engine import BaseEngine
from hyperscale.core.engines.types.common.types import RequestTypes

from .command import PlaywrightCommand
from .context_config import ContextConfig
from .context_group import ContextGroup
from .pool import ContextPool
from .result import PlaywrightResult


class MercuryPlaywrightClient(BaseEngine[PlaywrightCommand, PlaywrightResult]):
    __slots__ = (
        "session_id",
        "pool",
        "timeouts",
        "registered",
        "closed",
        "config",
        "sem",
        "active",
        "waiter",
        "_discarded_context_groups",
        "_discarded_contexts",
        "_pending_context_groups",
        "_playwright_setup",
    )

    def __init__(
        self,
        concurrency: int = 500,
        group_size: int = 50,
        timeouts: Timeouts = Timeouts(),
    ) -> None:
        super(MercuryPlaywrightClient, self).__init__()

        self.session_id = str(uuid.uuid4())

        self.pool = ContextPool(concurrency, group_size)
        self.timeouts = timeouts
        self.registered: Dict[str, PlaywrightCommand] = {}
        self.closed = False
        self.config: Union[ContextConfig, None] = None

        self.sem = asyncio.Semaphore(value=concurrency)
        self.active = 0
        self.waiter = None

        self._discarded_context_groups: List[ContextGroup] = []
        self._discarded_contexts = []
        self._pending_context_groups: List[ContextGroup] = []
        self._playwright_setup = False

    def config_to_dict(self):
        return {
            "concurrency": self.pool.size,
            "group_size": self.pool.group_size,
            "timeouts": self.timeouts,
            "context_config": {**self.config.data, "options": self.config.options},
        }

    async def set_pool(self, concurrency: int):
        self.sem = asyncio.Semaphore(value=concurrency)
        self.pool = ContextPool(
            concurrency, reset_connections=self.pool.reset_connections
        )

    async def setup(self, config: ContextConfig = None):
        if config is None and self.config:
            config = self.config

        if self._playwright_setup is False:
            self.config = config
            self.pool.create_pool(self.config)
            for context_group in self.pool:
                await context_group.create()

            self._playwright_setup = True

    async def prepare(self, command: PlaywrightCommand) -> Coroutine[Any, Any, None]:
        command.options.extra = {
            **command.options.extra,
            "timeout": self.timeouts.total_timeout * 1000,
        }

        self.registered[command.name] = command

    def extend_pool(self, increased_capacity: int):
        self.pool.size += increased_capacity
        for _ in range(increased_capacity):
            context_group = ContextGroup(
                **self.config, concurrency=int(self.pool.size / self.pool.group_size)
            )

            self._pending_context_groups.append(context_group)

            self.pool.contexts.append(context_group)

        self.sem = asyncio.Semaphore(self.pool.size)

    def shrink_pool(self, decrease_capacity: int):
        self.pool.size -= decrease_capacity

        for context_group in self.pool.contexts[self.pool.size :]:
            self._discarded_context_groups.append(context_group)

        self.pool.contexts = self.pool.contexts[: self.pool.size]

        for context_group in self.pool.contexts:
            group_size = int(self.pool.size / self.pool.group_size)

            for context in context_group.contexts[group_size:]:
                self._discarded_contexts.append(context)

            context_group.contexts = context_group.contexts[:group_size]
            context_group.librarians = context_group.librarians[:group_size]

        self.sem = asyncio.Semaphore(self.pool.size)

    async def execute_prepared_command(
        self, command: PlaywrightCommand
    ) -> Coroutine[Any, Any, PlaywrightResult]:
        for pending_context in self._pending_context_groups:
            await pending_context.create()

        result = PlaywrightResult(command, type=RequestTypes.PLAYWRIGHT)
        self.active += 1

        async with self.sem:
            context = self.pool.contexts.pop()
            try:
                if command.hooks.listen:
                    event = asyncio.Event()
                    command.hooks.channel_events.append(event)
                    await event.wait()

                if command.hooks.before:
                    command = await self.execute_before(command)

                result = await context.execute(command)

                if command.hooks.after:
                    result = await self.execute_after(command, result)

                if command.hooks.notify:
                    await asyncio.gather(
                        *[
                            asyncio.create_task(
                                channel.call(result, command.hooks.listeners)
                            )
                            for channel in command.hooks.channels
                        ]
                    )

                    for listener in command.hooks.listeners:
                        if len(listener.hooks.channel_events) > 0:
                            event = listener.hooks.channel_events.pop()
                            if not event.is_set():
                                event.set()

                self.pool.contexts.append(context)

            except Exception as e:
                result.error = e
                self.pool.contexts.append(context)

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:
                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None

            return result

    async def close(self):
        if self.closed is False:
            for context_group in self._discarded_context_groups:
                await context_group.close()

            for context in self._discarded_contexts:
                await context.close()

            self.closed = True
