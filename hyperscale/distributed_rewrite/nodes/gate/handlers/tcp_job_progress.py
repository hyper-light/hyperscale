"""
TCP handlers for job progress and status.

Handles:
- JobStatusRequest: Query job status
- JobProgress: Progress updates from managers
- WorkflowResultPush: Workflow completion results

Dependencies:
- Job manager
- Leadership tracker
- Load shedder (AD-22)
- Windowed stats collector
- Forwarding tracker

TODO: Extract from gate.py:
- receive_job_status_request() (lines 5395-5433)
- receive_job_progress() (lines 5434-5617)
- workflow_result_push() (lines 7177-7250)
"""

from typing import Protocol


class JobProgressDependencies(Protocol):
    """Protocol defining dependencies for job progress handlers."""

    def get_job_status(self, job_id: str):
        """Get current job status."""
        ...

    def is_job_leader(self, job_id: str) -> bool:
        """Check if this gate is the leader for the job."""
        ...

    def forward_to_job_leader(self, job_id: str, message_type: str, data: bytes) -> None:
        """Forward message to the job's leader gate."""
        ...

    def should_shed_handler(self, handler_name: str) -> bool:
        """Check if handler request should be shed."""
        ...


# Placeholder for full handler implementation
# The handlers will be extracted when the composition root is refactored

__all__ = ["JobProgressDependencies"]
