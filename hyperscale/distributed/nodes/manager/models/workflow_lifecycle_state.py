"""
Workflow lifecycle state tracking.

Tracks local manager state for workflows managed by this manager.
This is distinct from the AD-33 WorkflowStateMachine which handles
state transitions - this tracks manager-local metadata.
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class WorkflowLifecycleState:
    """
    Manager-local workflow lifecycle state.

    Tracks manager-specific metadata for workflows including retry
    attempts, dispatch history, and completion tracking.
    """

    workflow_id: str
    job_id: str
    worker_id: str | None = None
    fence_token: int = 0
    retry_count: int = 0
    max_retries: int = 3
    dispatch_timestamp: float = 0.0
    last_progress_timestamp: float = 0.0
    failed_workers: frozenset[str] = field(default_factory=frozenset)

    def record_failure(self, worker_id: str) -> "WorkflowLifecycleState":
        """
        Record a worker failure for this workflow.

        Returns a new state with the updated failed workers set.
        """
        return WorkflowLifecycleState(
            workflow_id=self.workflow_id,
            job_id=self.job_id,
            worker_id=None,
            fence_token=self.fence_token,
            retry_count=self.retry_count + 1,
            max_retries=self.max_retries,
            dispatch_timestamp=self.dispatch_timestamp,
            last_progress_timestamp=self.last_progress_timestamp,
            failed_workers=self.failed_workers | {worker_id},
        )

    @property
    def can_retry(self) -> bool:
        """Check if workflow can be retried."""
        return self.retry_count < self.max_retries
