"""Wire model ``WorkflowCancelResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class WorkflowCancelResponse(Message):
    """
    Response to a workflow cancellation request (AD-20).

    Returned by Worker -> Manager after attempting cancellation.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Workflow that was cancelled
    success: bool  # Whether cancellation succeeded
    was_running: bool = False  # True if workflow was actively running
    already_completed: bool = False  # True if already finished
    error: str | None = None  # Error message if failed
