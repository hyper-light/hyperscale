import asyncio
from typing import TypeVar, Generic, Any, Callable


Update = Callable[[Any], Any]


T = TypeVar("T", bound=dict[str, Any])
U = TypeVar("U", bound=Update)
V = TypeVar("V")


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


class Context(Generic[T]):
    def __init__(self, init_context: T | None = None):
        self._store: T = init_context or {}
        self._value_locks: dict[str, ReentrantAsyncLock] = {}
        self._value_locks_creation_lock = asyncio.Lock()
        self._store_lock = asyncio.Lock()

    async def get_value_lock(self, key: str) -> ReentrantAsyncLock:
        async with self._value_locks_creation_lock:
            if key not in self._value_locks:
                self._value_locks[key] = ReentrantAsyncLock()
            return self._value_locks[key]

    async def with_value(self, key: str) -> ReentrantAsyncLock:
        async with self._value_locks_creation_lock:
            if key not in self._value_locks:
                self._value_locks[key] = ReentrantAsyncLock()
            return self._value_locks[key]

    async def read(self, key: str, default: V | None = None):
        async with self._store_lock:
            return self._store.get(key, default)

    async def update(self, key: str, update: U):
        lock = await self.get_value_lock(key)
        async with lock:
            self._store[key] = update(self._store.get(key))
            return self._store[key]

    async def write(self, key: str, value: V):
        lock = await self.get_value_lock(key)
        async with lock:
            self._store[key] = value
            return self._store[key]

    async def delete(self, key: str):
        async with self._store_lock:
            del self._store[key]

    async def merge(self, update: T):
        async with self._store_lock:
            self._store.update(update)
