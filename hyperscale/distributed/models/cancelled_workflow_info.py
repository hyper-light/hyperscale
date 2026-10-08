"""Wire model ``CancelledWorkflowInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class CancelledWorkflowInfo:
    """
    Tracking info for a cancelled workflow (Section 6).

    Stored in manager's _cancelled_workflows bucket to prevent
    resurrection of cancelled workflows.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Cancelled workflow ID
    cancelled_at: float  # When cancelled
    request_id: str = ""  # Original request ID
    reason: str = ""  # Free-text cancellation reason
    dependents: list[str] = field(default_factory=list)  # Cancelled dependents
