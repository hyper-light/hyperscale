from dataclasses import dataclass, field

from hyperscale.distributed.raft.models import RaftLogEntry

from .snapshot_record import SnapshotRecord


@dataclass(slots=True)
class RecoveredRaftGroup:
    """One group's persistent Raft state as its store recovered it: the
    member this node took part as, the voters it was created with, term, vote, the latest snapshot, and the
    log entries after it."""

    member_id: str = ""
    initial_voters: list[str] = field(default_factory=list)
    term: int = 0
    voted_for: str | None = None
    snapshot: SnapshotRecord | None = None
    entries: list[RaftLogEntry] = field(default_factory=list)

    @property
    def base_index(self) -> int:
        """The index the log's entries follow (the snapshot's, or 0)."""
        return 0 if self.snapshot is None else self.snapshot.last_index

    @property
    def last_index(self) -> int:
        return self.entries[-1].index if self.entries else self.base_index
