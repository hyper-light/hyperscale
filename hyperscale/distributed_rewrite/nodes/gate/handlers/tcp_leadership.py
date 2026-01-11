"""
TCP handlers for job leadership and lease management.

Handles:
- LeaseTransfer: Transfer datacenter lease between gates
- JobLeadershipAnnouncement: Gate announcing job leadership
- JobLeaderManagerTransfer: Manager leadership transfer notification
- DCLeaderAnnouncement: Datacenter leader announcements

Dependencies:
- Leadership tracker
- Lease manager
- Job manager
- Fence token validation

TODO: Extract from gate.py:
- receive_lease_transfer() (lines 5989-6042)
- job_leadership_announcement() (lines 7367-7436)
- job_leader_manager_transfer() (lines 7538-7649)
- dc_leader_announcement() (lines 7491-7537)
"""

from typing import Protocol


class LeadershipDependencies(Protocol):
    """Protocol defining dependencies for leadership handlers."""

    def validate_fence_token(self, job_id: str, token: int) -> bool:
        """Validate fence token for leadership operation."""
        ...

    def transfer_leadership(
        self, job_id: str, new_leader_id: str, new_leader_addr: tuple[str, int]
    ) -> bool:
        """Transfer job leadership to another gate."""
        ...

    def accept_leadership(self, job_id: str, metadata: int) -> None:
        """Accept leadership for a job."""
        ...


# Placeholder for full handler implementation
# The handlers will be extracted when the composition root is refactored

__all__ = ["LeadershipDependencies"]
