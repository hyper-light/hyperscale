"""
Worker state synchronization module.

Handles state snapshot generation and sync request handling
for manager synchronization.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.models import WorkflowProgress


class WorkerStateSync:
    """
    Handles state synchronization for worker.

    Generates state snapshots for manager sync requests and
    handles sync protocol messages.
    """

    def __init__(self) -> None:
        """Initialize state sync manager."""
        self._state_version: int = 0

    def increment_version(self) -> int:
        """Increment and return state version."""
        self._state_version += 1
        return self._state_version

    @property
    def state_version(self) -> int:
        """Get current state version."""
        return self._state_version

    def generate_snapshot(
        self,
        active_workflows: dict[str, "WorkflowProgress"],
        allocated_cores: dict[str, list[int]],
        available_cores: int,
        total_cores: int,
        workflow_job_leaders: dict[str, tuple[str, int]],
    ) -> dict[str, Any]:
        """
        Generate a state snapshot for manager sync requests.

        Args:
            active_workflows: Map of workflow_id to WorkflowProgress
            allocated_cores: Map of workflow_id to allocated core indices
            available_cores: Number of currently available cores
            total_cores: Total number of cores
            workflow_job_leaders: Map of workflow_id to job leader address

        Returns:
            Dictionary containing worker state snapshot
        """
        workflow_snapshots = {}
        for workflow_id, progress in active_workflows.items():
            workflow_snapshots[workflow_id] = {
                "job_id": progress.job_id,
                "status": progress.status,
                "completed_count": progress.completed_count,
                "failed_count": progress.failed_count,
                "assigned_cores": list(progress.assigned_cores) if progress.assigned_cores else [],
                "job_leader": workflow_job_leaders.get(workflow_id),
            }

        return {
            "state_version": self._state_version,
            "total_cores": total_cores,
            "available_cores": available_cores,
            "active_workflow_count": len(active_workflows),
            "workflows": workflow_snapshots,
        }

    def apply_snapshot(self, snapshot: dict[str, Any]) -> bool:
        """
        Apply a state snapshot (for future use in state recovery).

        Args:
            snapshot: State snapshot dictionary

        Returns:
            True if applied successfully
        """
        # Workers typically don't apply snapshots from managers
        # This is a placeholder for potential future use
        return True
