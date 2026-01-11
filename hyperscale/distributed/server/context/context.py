import asyncio
from collections import defaultdict
from typing import TypeVar, Generic, Any, Callable


Update = Callable[[Any], Any]


T = TypeVar('T', bound=dict[str, Any])
U = TypeVar('U', bound=Update)
V = TypeVar('V')



class Context(Generic[T]):

    def __init__(
        self,
        init_context: T | None = None
    ):
        self._store: T = init_context or {}
        self._value_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._store_lock = asyncio.Lock()

    def with_value(self, key: str):
        return self._value_locks[key]

        # Perform asynchronous cleanup here,

    async def read_with_lock(self, key: str):
        async with self._lock:
            return self._store.get(key)


    def read(self, key: str, default: V | None = None):
        return self._store.get(key, default)
    
    async def update_with_lock(self, key: str, update: U):
        async with self._value_locks[key]:
            self._store[key] = update(
                self._store.get(key),
            )

            return self._store[key]

    def update(self, key: str, update: V):
        self._store[key] = update(
            self._store.get(key)
        )

        return self._store[key]

    async def write_with_lock(self, key: str, value: V):
        async with self._value_locks[key]:
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

