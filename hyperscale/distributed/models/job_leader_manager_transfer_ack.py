"""Wire model ``JobLeaderManagerTransferAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobLeaderManagerTransferAck(Message):
    """
    Acknowledgment of job leader manager transfer.
    """

    job_id: str  # Job being acknowledged
    gate_id: str  # Node ID of responding gate
    accepted: bool = True  # Whether transfer was applied
