import asyncio
from typing import TypeVar, Generic, Any, Callable


Update = Callable[[Any], Any]


T = TypeVar("T", bound=dict[str, Any])
U = TypeVar("U", bound=Update)
V = TypeVar("V")


class Context(Generic[T]):
    def __init__(self, init_context: T | None = None):
        self._store: T = init_context or {}
        self._value_locks: dict[str, asyncio.Lock] = {}
        self._value_locks_creation_lock = asyncio.Lock()
        self._store_lock = asyncio.Lock()

    async def get_value_lock(self, key: str) -> asyncio.Lock:
        async with self._value_locks_creation_lock:
            return self._value_locks.setdefault(key, asyncio.Lock())

    def with_value(self, key: str) -> asyncio.Lock:
        return self._value_locks.setdefault(key, asyncio.Lock())

    async def read_with_lock(self, key: str):
        async with self._store_lock:
            return self._store.get(key)

    def read(self, key: str, default: V | None = None):
        return self._store.get(key, default)

    async def update_with_lock(self, key: str, update: U):
        lock = await self.get_value_lock(key)
        async with lock:
            self._store[key] = update(
                self._store.get(key),
            )

            return self._store[key]

    def update(self, key: str, update: U):
        self._store[key] = update(self._store.get(key))

        return self._store[key]

    async def write_with_lock(self, key: str, value: V):
        lock = await self.get_value_lock(key)
        async with lock:
            self._store[key] = value

            return self._store[key]

    def write(self, key: str, value: V):
        self._store[key] = value
        return self._store[key]

    async def delete_with_lock(self, key: str):
        async with self._store_lock:
            del self._store[key]

    def delete(self, key: str):
        del self._store[key]

    async def merge_with_lock(self, update: T):
        async with self._store_lock:
            self._store.update(update)

    def merge(self, update: T):
        self._store.update(update)
