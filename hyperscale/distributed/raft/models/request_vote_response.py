"""``RequestVoteResponse`` -- pickled under the namespace
``hyperscale.distributed.raft.models.messages`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from hyperscale.distributed.models.message import Message

if TYPE_CHECKING:
    from .request_vote import RequestVote


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
    pre_vote: bool = False
