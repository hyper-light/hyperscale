"""Wire model ``JobLeaderTransfer`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobLeaderTransfer(Message):
    """
    Manager → Gate: Notify gate of leader change (AD-34 multi-DC coordination).

    Sent by new leader after taking over job leadership. Gate updates its
    tracking to send future timeout decisions to the new leader.

    Includes incremented fence token to prevent stale operations.
    """

    job_id: str
    datacenter: str
    new_leader_id: str
    new_leader_host: str
    new_leader_port: int
    fence_token: int  # New leader's fence token
