"""Wire model ``WorkflowCancellationStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class WorkflowCancellationStatus(str, Enum):
    """Status result for workflow cancellation request."""

    CANCELLED = "cancelled"  # Successfully cancelled
    PENDING_CANCELLED = "pending_cancelled"  # Was pending, now cancelled
    ALREADY_CANCELLED = "already_cancelled"  # Was already cancelled
    ALREADY_COMPLETED = "already_completed"  # Already finished, can't cancel
    NOT_FOUND = "not_found"  # Workflow not found
    CANCELLING = "cancelling"  # Cancellation in progress
