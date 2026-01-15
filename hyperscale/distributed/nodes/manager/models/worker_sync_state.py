"""
Worker sync state tracking.

Tracks state for synchronizing with workers during leader election
and recovery scenarios.
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class WorkerSyncState:
    """
    State for tracking worker state synchronization.

    Used during leader election and recovery to rebuild workflow state
    from workers (workers are source of truth for active workflows).
    """

    worker_id: str
    tcp_host: str
    tcp_port: int
    sync_requested_at: float = 0.0
    sync_completed_at: float | None = None
    sync_success: bool = False
    sync_attempts: int = 0
    last_error: str | None = None

    @property
    def tcp_addr(self) -> tuple[str, int]:
        """TCP address tuple."""
        return (self.tcp_host, self.tcp_port)

    @property
    def is_synced(self) -> bool:
        """Check if sync has completed successfully."""
        return self.sync_success and self.sync_completed_at is not None
