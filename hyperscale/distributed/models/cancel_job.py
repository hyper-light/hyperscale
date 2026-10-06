"""Wire model ``CancelJob`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class CancelJob(Message):
    """
    Request to cancel a job.

    Flows: client -> gate -> manager -> worker
           or: client -> manager -> worker
    """

    job_id: str  # Job to cancel
    reason: str = ""  # Cancellation reason
    fence_token: int = 0  # Fencing token for validation
