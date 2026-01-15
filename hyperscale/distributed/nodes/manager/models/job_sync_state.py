"""
Job sync state tracking.

Tracks state for synchronizing jobs during leader election
and recovery scenarios.
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class JobSyncState:
    """
    State for tracking job state synchronization.

    Used during leader election and recovery to rebuild job metadata
    from peer managers (retry counts, context versions, etc.).
    """

    job_id: str
    leader_node_id: str | None = None
    fencing_token: int = 0
    layer_version: int = 0
    workflow_count: int = 0
    completed_count: int = 0
    failed_count: int = 0
    sync_source: str | None = None
    sync_timestamp: float = 0.0

    @property
    def is_complete(self) -> bool:
        """Check if job has completed (all workflows finished)."""
        return (self.completed_count + self.failed_count) >= self.workflow_count
