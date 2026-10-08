"""``RaftLeaderVolatileState`` -- pickled under the namespace
``hyperscale.distributed.raft.models.raft_state`` (see that module)."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class RaftLeaderVolatileState:
    """
    Volatile state on leaders only (reinitialized after election).

    Attributes:
        next_index: For each follower, index of next entry to send.
        match_index: For each follower, highest entry known to be replicated.
    """

    next_index: dict[str, int] = field(default_factory=dict)
    match_index: dict[str, int] = field(default_factory=dict)
