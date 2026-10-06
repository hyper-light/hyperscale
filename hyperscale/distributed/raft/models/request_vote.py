"""``RequestVote`` -- pickled under the namespace
``hyperscale.distributed.raft.models.messages`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models.message import Message


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
    # PreVote (Raft thesis 9.6): ``term`` is the term the sender WOULD
    # campaign in; receivers answer without changing any state.
    pre_vote: bool = False
