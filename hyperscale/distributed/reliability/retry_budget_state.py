"""
Retry budget state tracking (AD-44).
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class RetryBudgetState:
    """
    Tracks retry budget consumption for a job.

    Enforced at manager level since managers handle dispatch.
    """

    job_id: str
    total_budget: int
    per_workflow_max: int
    consumed: int = 0
    per_workflow_consumed: dict[str, int] = field(default_factory=dict)

    def can_retry(self, workflow_id: str) -> tuple[bool, str]:
        """
        Check if workflow can retry.

        Returns:
            (allowed, reason) - reason explains denial if not allowed.
        """
        if self.consumed >= self.total_budget:
            return False, f"job_budget_exhausted ({self.consumed}/{self.total_budget})"

        workflow_consumed = self.per_workflow_consumed.get(workflow_id, 0)
        if workflow_consumed >= self.per_workflow_max:
            return (
                False,
                f"workflow_budget_exhausted ({workflow_consumed}/{self.per_workflow_max})",
            )

        return True, "allowed"

    def consume_retry(self, workflow_id: str) -> None:
        """Record a retry attempt."""
        self.consumed += 1
        self.per_workflow_consumed[workflow_id] = (
            self.per_workflow_consumed.get(workflow_id, 0) + 1
        )

    def get_remaining(self) -> int:
        """Get remaining job-level retries."""
        return max(0, self.total_budget - self.consumed)

    def get_workflow_remaining(self, workflow_id: str):
        """Get remaining retries for specific workflow."""
        workflow_consumed = self.per_workflow_consumed.get(workflow_id, 0)
        return max(0, self.per_workflow_max - workflow_consumed)
