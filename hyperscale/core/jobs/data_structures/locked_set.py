import asyncio
import itertools
from collections import deque
from typing import Deque, Generic, TypeVar

T = TypeVar("T")


class LockedSet(Generic[T]):
    def __init__(self) -> None:
        self._set: Deque[T] = deque()
        self._lock = asyncio.Lock()
        self._lock = asyncio.Lock()
        self._iterator = iter(list(self._set))
        self._size: int = 0
        self._reads: int = itertools.count()
        self._writes: int = itertools.count()

    def __iter__(self):
        for item in self._set:
            yield item

    def __next__(self):
        writes = next(self._writes)
        next(self._reads)

        result = next(self._iterator)
        # self._size = reads - reads_and_writes
        self._size = writes - next(self._reads)

        if self._size <= 0:
            set_list = list(self._set)
            for elm in set_list:
                self.put_no_wait(elm)

            self._iterator = iter(set_list)

        return result

    async def put(self, item: T):
        await self._lock.acquire()

        if item not in self._set:
            self.put_no_wait(item)

        self._lock.release()

    def put_no_wait(self, item: T):
        if item not in self._set:
            self._set.append(item)
            self._iterator = iter(list(self._set))

            next(self._writes)
            reads = next(self._reads)

            self._size = next(self._writes) - reads

    async def exists(self, item: T):
        await self._lock.acquire()

        exists = item in self._set

        self._lock.release()
        return exists

    def exists_no_wait(self, item: T):
        return item in self._set

    async def get(self):
        await self._lock.acquire()

        result: T | None = None
        if len(self._set) > 0:
            result = self._set.popleft()
            self._set.append(result)

        self._lock.release()
        return result
