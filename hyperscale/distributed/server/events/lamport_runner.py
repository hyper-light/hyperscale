from __future__ import annotations
import asyncio
from typing import TypeVar
from .lamport_clock import LamportClock
from .lamport_message import LamportMessage


T = TypeVar("T", bound=LamportMessage)

DEFAULT_QUEUE_MAX_SIZE = 10_000


class LamportRunner:
    def __init__(
        self,
        name: str,
        max_queue_size: int = DEFAULT_QUEUE_MAX_SIZE,
    ):
        self.name = name
        self.clock = LamportClock()
        self._max_queue_size = max_queue_size
        self.registered: dict[str, asyncio.Queue[LamportMessage]] = {}
        self.waiter: asyncio.Queue[LamportMessage] = asyncio.Queue(
            maxsize=max_queue_size,
        )

        self.registered[self.name] = self.waiter

        self._running: bool = True
        self._run_task: asyncio.Future | None = None
        self.processed = 0
        self._dropped_messages = 0

    def subscribe(self, runner: LamportRunner):
        self.registered[runner.name] = runner.waiter

    def _try_put_message(
        self,
        waiter: asyncio.Queue[LamportMessage],
        message: LamportMessage,
    ) -> bool:
        try:
            waiter.put_nowait(message)
            return True
        except asyncio.QueueFull:
            self._dropped_messages += 1
            return False

    async def update(self):
        next_time = await self.clock.increment()
        self.processed = next_time

        for node, waiter in self.registered.items():
            if node != self.name:
                self._try_put_message(
                    waiter,
                    LamportMessage(
                        timestamp=next_time,
                        sender=self.name,
                        receiver=node,
                    ),
                )

    async def ack(self, time: int):
        await self.clock.ack(time)

    def run(self):
        self._running = True
        self._run_task = asyncio.ensure_future(self._run())

    async def _run(self):
        while self._running:
            result = await self.waiter.get()
            incoming_time = result.timestamp

            message_type = result.message_type

            match message_type:
                case "ack":
                    await self.clock.ack(incoming_time)

                case "update":
                    await self.clock.update(incoming_time)
                    next_time = await self.clock.update(incoming_time)
                    self.processed = next_time - 1

                    for node, waiter in self.registered.items():
                        if node != self.name:
                            self._try_put_message(
                                waiter,
                                LamportMessage(
                                    message_type="ack",
                                    timestamp=next_time,
                                    sender=self.name,
                                    receiver=node,
                                ),
                            )

    async def stop(self):
        self._running = False

        if self._run_task is None:
            return

        try:
            self._run_task.cancel()
            await self._run_task

        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass
