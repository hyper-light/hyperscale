from collections.abc import Callable
from typing import Protocol

from .models import RecoveredRaftGroup
from .raft_store_codec import RaftStoreRecord


class RaftStorage(Protocol):
    """Where a node's Raft groups keep their persistent state (D1): the
    node's ``RaftStore``, or ``VolatileRaftStorage`` when the node has no
    disk it can use. ``durable`` says whether ``write`` keeps anything.

    ``participation`` is how many times this node has left a membership
    group for good; ``advance_participation`` makes the next one durable
    before the node takes part again. ``take_recovered_groups`` hands each
    group the disk held to the coordinator that resumes it, once."""

    @property
    def durable(self) -> bool: ...

    @property
    def participation(self) -> int: ...

    async def write(self, records: list[RaftStoreRecord]) -> None: ...

    async def advance_participation(self) -> int: ...

    def take_recovered_groups(self, belongs: Callable[[str], bool]) -> dict[str, RecoveredRaftGroup]: ...
