"""``ReentrantAsyncLock`` -- pickled under the namespace
``hyperscale.distributed.server.context.context`` (see that module)."""

import asyncio


class ReentrantAsyncLock:
    """Task-aware reentrant lock for asyncio.

    ``asyncio.Lock`` is non-reentrant: the same task awaiting
    re-acquisition deadlocks itself. ``Context``'s API intentionally
    nests locks (``with_value(key)`` opens a critical section, and
    ``write(key, ...)`` / ``update(key, ...)`` re-enter the same key
    lock from inside that section), so the underlying primitive must
    be reentrant for the API to be safe to use.

    The reentrance check uses ``asyncio.current_task()`` as identity —
    every coroutine that holds the lock continues to hold it across
    awaits within the same task; a different task awaiting the same
    lock blocks normally.
    """

    __slots__ = ("_lock", "_owner", "_depth")

    def __init__(self) -> None:
        self._lock: asyncio.Lock = asyncio.Lock()
        self._owner: asyncio.Task | None = None
        self._depth: int = 0

    async def acquire(self) -> bool:
        current = asyncio.current_task()
        if self._owner is current and current is not None:
            self._depth += 1
            return True
        await self._lock.acquire()
        self._owner = current
        self._depth = 1
        return True

    def release(self) -> None:
        if self._depth == 0 or self._owner is not asyncio.current_task():
            raise RuntimeError(
                "ReentrantAsyncLock release without matching acquire "
                "from the same task"
            )
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            self._lock.release()

    def locked(self) -> bool:
        return self._lock.locked()

    async def __aenter__(self) -> "ReentrantAsyncLock":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.release()
