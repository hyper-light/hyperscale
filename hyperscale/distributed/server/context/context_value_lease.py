"""``ContextValueLease`` -- pickled under the namespace
``hyperscale.distributed.server.context.context`` (see that module)."""

from collections.abc import Hashable

from .reentrant_async_lock import ReentrantAsyncLock


class ContextValueLease:
    """One critical section on one key of a ``Context``.

    A key's lock exists only while some lease holds or awaits it: the last
    lease out removes it. Keys come and go with the peers they name, so a
    lock kept per key ever seen would grow without bound.
    """

    __slots__ = ("_value_locks", "_value_lock_users", "_key", "_lock")

    def __init__(
        self,
        value_locks: dict[Hashable, ReentrantAsyncLock],
        value_lock_users: dict[Hashable, int],
        key: Hashable,
    ) -> None:
        self._value_locks = value_locks
        self._value_lock_users = value_lock_users
        self._key = key
        self._lock: ReentrantAsyncLock | None = None

    async def __aenter__(self) -> ReentrantAsyncLock:
        if (lock := self._value_locks.get(self._key)) is None:
            lock = self._value_locks[self._key] = ReentrantAsyncLock()
        self._value_lock_users[self._key] = self._value_lock_users.get(self._key, 0) + 1
        try:
            await lock.acquire()
        except BaseException:
            # A lease cancelled while it waited gives its claim back.
            self._give_back_claim()
            raise
        self._lock = lock
        return lock

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self._lock.release()
        self._lock = None
        self._give_back_claim()

    def _give_back_claim(self) -> None:
        """The last lease out removes the key's lock."""
        if (users := self._value_lock_users[self._key] - 1) == 0:
            del self._value_lock_users[self._key]
            del self._value_locks[self._key]
        else:
            self._value_lock_users[self._key] = users
