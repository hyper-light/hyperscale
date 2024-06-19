from typing import List, Optional, Tuple

from pydantic import StrictInt, StrictStr

from hyperscale.distributed.models.base.message import Message

from .healthcheck import HealthStatus
from .logs import Entry, NodeState
from .vote_result import VoteResult


class RaftMessage(Message):
    source_host: StrictStr
    source_port: StrictInt
    elected_leader: Optional[Tuple[StrictStr, StrictInt]]
    failed_node: Optional[Tuple[StrictStr, StrictInt]]
    vote_result: Optional[VoteResult]
    raft_node_status: NodeState
    status: HealthStatus
    entries: Optional[List[Entry]]
    term_number: StrictInt
    received_timestamp: Optional[StrictInt]
