"""``RaftVolatileState`` -- pickled under the namespace
``hyperscale.distributed.raft.models.raft_state`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class RaftVolatileState:
    """
    Volatile state on all servers (rebuilt from log on restart).

    Attributes:
        commit_index: Highest log entry known to be committed.
        last_applied: Highest log entry applied to state machine.
    """

    commit_index: int = 0
    last_applied: int = 0
