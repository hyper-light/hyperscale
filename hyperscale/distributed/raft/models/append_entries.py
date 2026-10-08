"""``AppendEntries`` -- pickled under the namespace
``hyperscale.distributed.raft.models.messages`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.distributed.models.message import Message

from .log_entry import RaftLogEntry


@dataclass(slots=True)
class AppendEntries(Message):
    """
    AppendEntries RPC (Section 5.3).

    Sent by leaders for log replication and heartbeats.
    Empty entries list means heartbeat.
    """

    job_id: str = ""
    term: int = 0
    leader_id: str = ""
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: list[RaftLogEntry] = field(default_factory=list)
    leader_commit: int = 0
    # The leader's ReadIndex round when it sent this (Raft thesis 6.4): an
    # answer echoing it proves the leader still led after a read began.
    read_round: int = 0
