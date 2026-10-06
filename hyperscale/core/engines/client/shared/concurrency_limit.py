import asyncio
from collections import deque
from typing import Deque, Optional, Type


class ConcurrencyLimit:
    """
    Admits at most ``limit`` holders at once, the rest in FIFO order: what
    ``asyncio.Semaphore`` gives an engine's requests, with an uncontended
    acquire that is one method call -- ``async with`` on a Semaphore costs
    three coroutines and a generator on every request.

        if not limit.try_acquire():
            await limit.acquire()

        try:
            ...

        finally:
            limit.release()

    ``async with limit:`` does the same, for callers off the hot path.

    Invariant: a holder waits only while no slot is free -- ``release()``
    hands a slot to the longest waiter before freeing it -- so a free slot
    means nobody is waiting, and ``try_acquire()`` never jumps the queue.
    """

    __slots__ = (
        "_available",
        "_waiters",
    )

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError(f"A concurrency limit admits at least one holder, not {limit}")

        self._available = limit
        self._waiters: Deque[asyncio.Future[None]] = deque()

    def try_acquire(self) -> bool:
        """Takes a free slot, if there is one, without waiting."""
        if self._available:
            self._available -= 1
            return True

        return False

    async def acquire(self) -> None:
        """Takes a slot, waiting in FIFO order while none is free."""
        if self._available:
            self._available -= 1
            return

        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)

        try:
            await waiter

        except BaseException:
            if waiter.done() and not waiter.cancelled():
                # release() handed this waiter a slot, then a cancellation
                # (or the coroutine's closing) arrived before it resumed: the
                # slot goes to the next one.
                self.release()

            elif waiter in self._waiters:
                # Still queued: release() has not reached it.
                self._waiters.remove(waiter)

            raise

    def release(self) -> None:
        """Returns a slot: to the longest live waiter, or else to the free count."""
        waiters = self._waiters
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

        self._available += 1

    async def __aenter__(self) -> None:
        if not self.try_acquire():
            await self.acquire()

    async def __aexit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception: Optional[BaseException],
        traceback: object,
    ) -> None:
        self.release()
