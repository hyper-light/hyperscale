"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from collections.abc import Hashable
from typing import TypeVar, Generic, Callable

from .context_value_lease import ContextValueLease
from .reentrant_async_lock import ReentrantAsyncLock

V = TypeVar("V")

# Maps a key's current value (``None`` when unset) to its new value.
Update = Callable[[V | None], V]

T = TypeVar("T", bound=dict[str, object])


class Context(Generic[T]):
    def __init__(self, init_context: T | None = None):
        self._store: T = init_context or {}
        # Per-key locks, each kept only while a lease holds or awaits it
        # (``ContextValueLease``). All on the event loop, and nothing here
        # awaits between looking a key up and creating it.
        self._value_locks: dict[Hashable, ReentrantAsyncLock] = {}
        self._value_lock_users: dict[Hashable, int] = {}
        self._store_lock = asyncio.Lock()

    async def with_value(self, key: Hashable) -> ContextValueLease:
        """A critical section on ``key`` (``async with``), reentrant within
        a task: ``write``/``update`` on the same key nest inside it."""
        return ContextValueLease(self._value_locks, self._value_lock_users, key)

    async def read(self, key: str, default: V | None = None):
        async with self._store_lock:
            return self._store.get(key, default)

    async def update(self, key: str, update: Update[V]):
        async with ContextValueLease(self._value_locks, self._value_lock_users, key):
            self._store[key] = update(self._store.get(key))
            return self._store[key]

    async def write(self, key: str, value: V):
        async with ContextValueLease(self._value_locks, self._value_lock_users, key):
            self._store[key] = value
            return self._store[key]

    async def delete(self, key: str):
        async with self._store_lock:
            del self._store[key]

    async def merge(self, update: T):
        async with self._store_lock:
            self._store.update(update)

_REHOMED = (
    ContextValueLease,
    ReentrantAsyncLock,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
