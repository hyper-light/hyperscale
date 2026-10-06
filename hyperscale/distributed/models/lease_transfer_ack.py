"""Wire model ``LeaseTransferAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class LeaseTransferAck(Message):
    """
    Acknowledgment of a lease transfer.
    """

    job_id: str  # Job identifier
    accepted: bool  # Whether transfer was accepted
    new_fence_token: int = 0  # New fencing token if accepted
    error: str | None = None  # Error message if rejected
