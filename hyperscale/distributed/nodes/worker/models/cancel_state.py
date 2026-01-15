"""
Cancellation state for worker workflows.

Tracks cancellation events and completion status for workflows
being cancelled on this worker.
"""

from dataclasses import dataclass


@dataclass(slots=True)
class CancelState:
    """
    Cancellation state for a workflow.

    Tracks the cancellation request and completion status.
    """

    workflow_id: str
    job_id: str
    cancel_requested_at: float
    cancel_reason: str
    cancel_completed: bool = False
    cancel_success: bool = False
    cancel_error: str | None = None
