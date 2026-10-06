"""Wire model ``JobStateSyncAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobStateSyncAck(Message):
    """
    Acknowledgment of job state sync.
    """

    job_id: str  # Job being acknowledged
    responder_id: str  # Node ID of responder
    accepted: bool = True  # Whether sync was applied
