"""
TCP handlers for job cancellation (AD-20).

Handles:
- CancelJob: Cancel all workflows for a job
- JobCancellationComplete: Manager notification of cancellation completion
- SingleWorkflowCancelRequest: Cancel a specific workflow

Dependencies:
- Job manager
- Leadership tracker
- Manager dispatcher
- Cancellation tracking state

TODO: Extract from gate.py:
- receive_cancel_job() (lines 5618-5763)
- receive_job_cancellation_complete() (lines 5764-5847)
- receive_cancel_single_workflow() (lines 5848-5988)
"""

from typing import Protocol


class CancellationDependencies(Protocol):
    """Protocol defining dependencies for cancellation handlers."""

    def is_job_leader(self, job_id: str) -> bool:
        """Check if this gate is the leader for the job."""
        ...

    def forward_cancellation_to_managers(
        self, job_id: str, datacenters: list[str]
    ) -> None:
        """Forward cancellation request to DC managers."""
        ...

    def initialize_cancellation_tracking(self, job_id: str) -> None:
        """Initialize tracking for cancellation completion."""
        ...

    def complete_cancellation(
        self, job_id: str, success: bool, errors: list[str]
    ) -> None:
        """Complete cancellation and notify client."""
        ...


# Placeholder for full handler implementation
# The handlers will be extracted when the composition root is refactored

__all__ = ["CancellationDependencies"]
