"""``JobGroupConsensus`` -- pickled under the namespace
``hyperscale.distributed.raft.ledger_replicator`` (see that module)."""

from typing import Protocol

from .models.ledger_append_command import LedgerAppendCommand
from .raft_node import RaftNode


class JobGroupConsensus(Protocol):
    """The per-job group operations replication needs (manager and gate
    consensus coordinators both provide them)."""

    def get_node(self, job_id: str) -> RaftNode | None: ...

    async def propose_command(self, job_id: str, command: LedgerAppendCommand) -> tuple[bool, int]: ...

    def member_address(self, node_id: str) -> tuple[str, int] | None: ...
