"""``RaftPersistentState`` -- pickled under the namespace
``hyperscale.distributed.raft.models.raft_state`` (see that module)."""

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
