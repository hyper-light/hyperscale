from typing import TypeVar, Generic


K = TypeVar("K")
V = TypeVar("V")


class Context(Generic[K, V]):
    def __init__(self):
        self._context: dict[K, V] = {}

    def __setitem__(self, key: K, value: V):
        self._context[key] = value

    def __getitem__(self, key: K):
        return self._context.get(key)
