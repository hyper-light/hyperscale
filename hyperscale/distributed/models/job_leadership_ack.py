"""Wire model ``JobLeadershipAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobLeadershipAck(Message):
    """
    Acknowledgment of job leadership announcement.
    """

    job_id: str  # Job being acknowledged
    accepted: bool  # Whether announcement was accepted
    responder_id: str  # Node ID of responder
    error: str | None = None  # Error message if not accepted
