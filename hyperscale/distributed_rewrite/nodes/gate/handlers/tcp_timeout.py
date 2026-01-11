"""
TCP handlers for job timeout coordination (AD-34).

Handles:
- JobProgressReport: Progress report from manager timeout strategy
- JobTimeoutReport: Manager reporting local timeout
- JobLeaderTransfer: Leader transfer for timeout coordination
- JobFinalStatus: Final job status from manager

Dependencies:
- Job timeout tracker
- Leadership tracker
- Job manager

TODO: Extract from gate.py:
- receive_job_progress_report() (lines 6081-6102)
- receive_job_timeout_report() (lines 6103-6124)
- receive_job_leader_transfer() (lines 6125-6146)
- receive_job_final_status() (lines 6147-6172)
"""

from typing import Protocol


class TimeoutDependencies(Protocol):
    """Protocol defining dependencies for timeout handlers."""

    def update_job_progress(
        self, job_id: str, datacenter_id: str, manager_addr: tuple[str, int]
    ) -> None:
        """Update job progress timestamp for timeout tracking."""
        ...

    def record_dc_timeout(self, job_id: str, datacenter_id: str, reason: str) -> None:
        """Record that a DC timed out for a job."""
        ...

    def check_global_timeout(self, job_id: str) -> bool:
        """Check if job should be declared globally timed out."""
        ...


# Placeholder for full handler implementation
# The handlers will be extracted when the composition root is refactored

__all__ = ["TimeoutDependencies"]
