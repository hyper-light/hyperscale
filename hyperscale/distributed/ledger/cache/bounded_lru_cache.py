from __future__ import annotations

from collections import OrderedDict
from typing import Generic, TypeVar

KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")


class BoundedLRUCache(Generic[KeyT, ValueT]):
    __slots__ = ("_max_size", "_cache")

    def __init__(self, max_size: int) -> None:
        if max_size < 1:
            raise ValueError("max_size must be at least 1")

        self._max_size = max_size
        self._cache: OrderedDict[KeyT, ValueT] = OrderedDict()

    def get(self, key: KeyT) -> ValueT | None:
        if key not in self._cache:
            return None

        self._cache.move_to_end(key)
        return self._cache[key]

    def put(self, key: KeyT, value: ValueT) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)
            self._cache[key] = value
            return

        if len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)

        self._cache[key] = value

    def remove(self, key: KeyT) -> ValueT | None:
        return self._cache.pop(key, None)

    def contains(self, key: KeyT) -> bool:
        return key in self._cache

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, key: KeyT) -> bool:
        return key in self._cache

    @property
    def max_size(self) -> int:
        return self._max_size
