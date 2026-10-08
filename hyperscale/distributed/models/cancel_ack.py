"""Wire model ``CancelAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class CancelAck(Message):
    """
    Acknowledgment of cancellation.
    """

    job_id: str  # Job identifier
    cancelled: bool  # Whether successfully cancelled
    workflows_cancelled: int = 0  # Number of workflows stopped
    error: str | None = None  # Error if cancellation failed
