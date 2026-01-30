"""
Raft RPC message models.

Defines the four core Raft RPC messages: RequestVote, RequestVoteResponse,
AppendEntries, and AppendEntriesResponse. All extend Message for
consistent serialization via cloudpickle.
"""

from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message

from .log_entry import RaftLogEntry


@dataclass(slots=True)
class RequestVote(Message):
    """
    RequestVote RPC (Section 5.2).

    Sent by candidates to gather votes during election.
    """

    job_id: str = ""
    term: int = 0
    candidate_id: str = ""
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass(slots=True)
class RequestVoteResponse(Message):
    """
    Response to RequestVote RPC.

    Grants or denies vote based on term and log freshness.
    """

    job_id: str = ""
    term: int = 0
    vote_granted: bool = False
    voter_id: str = ""


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


@dataclass(slots=True)
class AppendEntriesResponse(Message):
    """
    Response to AppendEntries RPC.

    Includes conflict info for efficient log reconciliation
    (Section 5.3 optimization).
    """

    job_id: str = ""
    term: int = 0
    success: bool = False
    follower_id: str = ""
    match_index: int = 0
    conflict_term: int | None = None
    conflict_index: int | None = None
