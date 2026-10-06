"""Wire model ``JobLeaderGateTransferAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobLeaderGateTransferAck(Message):
    """
    Acknowledgment of job leader gate transfer.
    """

    job_id: str  # Job being acknowledged
    manager_id: str  # Node ID of responding manager
    accepted: bool = True  # Whether transfer was applied
