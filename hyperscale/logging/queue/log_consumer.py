import asyncio
from typing import (
    AsyncGenerator,
    Callable,
    TypeVar,
)

from hyperscale.logging.models import Log

from .consumer_status import ConsumerStatus

T = TypeVar('T')


class LogConsumer:

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Log] = asyncio.Queue()
        self._wait_task: asyncio.Task | None = None
        self._loop = asyncio.get_event_loop()
        self._pending_waiter: asyncio.Future | None = None
        self._yield_lock = asyncio.Lock()
        self.status = ConsumerStatus.READY

    @property
    def pending(self):
        return self._queue.qsize() > 0

    async def wait_for_pending(self):
        if self.status == ConsumerStatus.CLOSING:
            self._pending_waiter = asyncio.Future()
            await self._pending_waiter

    async def iter_logs(
        self,
        filter: Callable[[T], bool] | None = None,
    ) -> AsyncGenerator[Log, None]:

        if self.status == ConsumerStatus.READY:
            self.status =  ConsumerStatus.RUNNING

        try:
            
            while self.status == ConsumerStatus.RUNNING:
                self._wait_task = asyncio.create_task(self._queue.get())

                log: Log = await self._wait_task

                if filter and filter(log.entry):
                    yield log

                elif filter is None:
                    yield log

                else:
                    self._queue.put_nowait(log)

        except (
            asyncio.CancelledError,
            asyncio.InvalidStateError
        ):
            pass

        remaining = self._queue.qsize()

        if self.status == ConsumerStatus.CLOSING:
            for _ in range(remaining):
                self._wait_task = asyncio.create_task(self._queue.get())
                log: Log = await self._wait_task

                if filter and filter(log.entry):
                    yield log

                elif filter is None:
                    yield log

        if self._pending_waiter and not self._pending_waiter.done():
            self._pending_waiter.set_result(None)

        self.status = ConsumerStatus.CLOSED

    async def put(self, log: Log):
        await self._queue.put(log)

    def abort(self):
        self.status = ConsumerStatus.ABORTING
        if self._wait_task:
            
            try:
                self._wait_task.cancel()

            except asyncio.CancelledError:
                pass

            except asyncio.InvalidStateError:
                pass

        remaining = self._queue.qsize()
        for _ in range(remaining):
            self._queue.get_nowait()

        self.status = ConsumerStatus.CLOSED
        
   
    def stop(self):
        self.status = ConsumerStatus.CLOSING

        if self._queue.qsize() < 1 and self._wait_task:
            try:
                self._wait_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
            ):
                pass
