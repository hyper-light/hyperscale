from dataclasses import dataclass

from .raft_identity import RaftIdentity
from .recovered_raft_group import RecoveredRaftGroup


@dataclass(slots=True, frozen=True)
class RaftStoreRecovery:
    """What opening a node's Raft store found: the identity to run under,
    each unreleased group's persistent state, whether the identity resumed
    from disk, and -- when the disk was set aside -- why."""

    identity: RaftIdentity
    groups: dict[str, RecoveredRaftGroup]
    resumed: bool
    set_aside_reason: str | None
