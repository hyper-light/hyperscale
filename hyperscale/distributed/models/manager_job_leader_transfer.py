"""Wire model ``ManagerJobLeaderTransfer`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class ManagerJobLeaderTransfer(Message):
    """
    Notification to client that manager job leadership has transferred (Section 9.2.2).

    Typically forwarded by gate to client when a manager job leader changes.
    """

    job_id: str
    new_manager_id: str
    new_manager_addr: tuple[str, int]
    fence_token: int
    datacenter_id: str
    old_manager_id: str | None = None
    old_manager_addr: tuple[str, int] | None = None
