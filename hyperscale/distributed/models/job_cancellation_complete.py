"""Wire model ``JobCancellationComplete`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message


@dataclass(slots=True)
class JobCancellationComplete(Message):
    """
    Push notification from Manager -> Gate/Client when job cancellation completes.

    Sent after all workflows for a job have been cancelled. Aggregates results
    from all workers and includes any errors encountered during cancellation.
    This enables the client to:
    1. Know when cancellation is fully complete (not just acknowledged)
    2. See any errors that occurred during cancellation
    3. Clean up local job state
    """

    job_id: str  # Job that was cancelled
    success: bool  # True if all workflows cancelled without errors
    cancelled_workflow_count: int = 0  # Number of workflows that were cancelled
    total_workflow_count: int = 0  # Total workflows that needed cancellation
    errors: list[str] = field(
        default_factory=list
    )  # Aggregated errors from all workers
    cancelled_at: float = 0.0  # Timestamp when cancellation completed
