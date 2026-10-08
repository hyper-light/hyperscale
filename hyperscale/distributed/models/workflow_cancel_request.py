"""Wire model ``WorkflowCancelRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class WorkflowCancelRequest(Message):
    """
    Request to cancel a specific workflow on a worker (AD-20).

    Sent from Manager -> Worker for individual workflow cancellation.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Specific workflow to cancel
    requester_id: str = ""  # Who requested cancellation
    timestamp: float = 0.0  # When cancellation was requested
    reason: str = ""  # Optional cancellation reason
