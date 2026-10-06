from collections.abc import Callable

from .models import RecoveredRaftGroup
from .raft_store_codec import RaftStoreRecord


class VolatileRaftStorage:
    """No disk (D1 P7): nothing a group writes is kept, so a restart is a
    new identity -- a new member of every group -- as before D1. Its
    participation lives as long as the process."""

    __slots__ = ("_participation",)

    def __init__(self) -> None:
        self._participation = 0

    @property
    def durable(self) -> bool:
        return False

    @property
    def participation(self) -> int:
        return self._participation

    async def write(self, records: list[RaftStoreRecord]) -> None:
        return None

    async def advance_participation(self) -> int:
        self._participation += 1
        return self._participation

    def take_recovered_groups(self, belongs: Callable[[str], bool]) -> dict[str, RecoveredRaftGroup]:
        return {}
