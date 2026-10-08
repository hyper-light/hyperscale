from collections.abc import Awaitable, Callable

from hyperscale.distributed.raft.store.models import (
    KeyedStateRecord,
    KeyedStateReleasedRecord,
    RecoveredRaftGroup,
)
from hyperscale.distributed.raft.store.raft_store_codec import RaftStoreRecord


class ScriptedDurableRaftStorage:
    """A durable ``RaftStorage`` stand-in whose ``write`` runs the script a
    test hands it -- one that never returns, one that raises -- so a test
    drives a real ``RaftNode`` through a write that is cancelled or fails.
    Each batch handed to ``write`` is kept in ``write_attempts``."""

    __slots__ = ("_write_script", "_participation", "write_attempts")

    def __init__(self, write_script: Callable[[list[RaftStoreRecord]], Awaitable[None]]) -> None:
        self._write_script = write_script
        self._participation = 0
        self.write_attempts: list[list[RaftStoreRecord]] = []

    @property
    def durable(self) -> bool:
        return True

    @property
    def participation(self) -> int:
        return self._participation

    async def write(self, records: list[RaftStoreRecord]) -> None:
        self.write_attempts.append(records)
        await self._write_script(records)

    async def advance_participation(self) -> int:
        self._participation += 1
        return self._participation

    def take_recovered_groups(self, belongs: Callable[[str], bool]) -> dict[str, RecoveredRaftGroup]:
        return {}

    def take_recovered_states(self, namespace: str) -> dict[str, KeyedStateRecord | KeyedStateReleasedRecord]:
        return {}
