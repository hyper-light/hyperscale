"""Wire model ``GateJobLeaderTransferAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class GateJobLeaderTransferAck(Message):
    """
    Acknowledgment of gate job leader transfer notification.
    """

    job_id: str
    client_id: str
    accepted: bool = True
    rejection_reason: str = ""
