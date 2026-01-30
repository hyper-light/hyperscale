"""
Raft state models per the Raft paper.

Separates persistent state (survives crashes), volatile state
(rebuilt on restart), and leader-only volatile state.
"""

from dataclasses import dataclass, field

from .log_entry import RaftLogEntry


@dataclass(slots=True)
class RaftPersistentState:
    """
    State that must survive crashes (Section 5.2).

    In-memory until Phase 6 adds WAL persistence.

    Attributes:
        current_term: Latest term this node has seen.
        voted_for: Candidate this node voted for in current term, or None.
        log: The Raft log entries (managed by RaftLog in practice).
    """

    current_term: int = 0
    voted_for: str | None = None
    log: list[RaftLogEntry] = field(default_factory=list)


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
