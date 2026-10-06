"""Wire model ``WorkflowCancellationComplete`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message


@dataclass(slots=True)
class WorkflowCancellationComplete(Message):
    """
    Push notification from Worker -> Manager when workflow cancellation completes.

    Sent after _cancel_workflow() finishes (success or failure) to notify the
    manager that the workflow has been fully cancelled and cleanup is done.
    This enables the manager to:
    1. Update workflow status to CANCELLED
    2. Aggregate errors across all workers
    3. Push completion notification to origin gate/client
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Workflow that was cancelled
    success: bool  # True if cancellation succeeded without errors
    errors: list[str] = field(default_factory=list)  # Any errors during cancellation
    cancelled_at: float = 0.0  # Timestamp when cancellation completed
    node_id: str = ""  # Worker node ID that performed cancellation
