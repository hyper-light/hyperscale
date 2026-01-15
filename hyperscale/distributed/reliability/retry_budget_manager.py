"""
Retry budget manager for distributed workflow dispatch (AD-44).
"""

import asyncio

from hyperscale.distributed.env import Env

from .reliability_config import ReliabilityConfig, create_reliability_config_from_env
from .retry_budget_state import RetryBudgetState


class RetryBudgetManager:
    """
    Manages retry budgets for jobs and workflows.

    Uses an asyncio lock to protect shared budget state.
    """

    __slots__ = ("_budgets", "_config", "_lock")

    def __init__(self, config: ReliabilityConfig | None = None) -> None:
        env_config = config or create_reliability_config_from_env(Env())
        self._config = env_config
        self._budgets: dict[str, RetryBudgetState] = {}
        self._lock = asyncio.Lock()

    async def create_budget(self, job_id: str, total: int, per_workflow: int):
        """Create and store retry budget state for a job."""
        total_budget = self._resolve_total_budget(total)
        per_workflow_max = self._resolve_per_workflow_budget(per_workflow, total_budget)
        budget = RetryBudgetState(
            job_id=job_id,
            total_budget=total_budget,
            per_workflow_max=per_workflow_max,
        )
        async with self._lock:
            self._budgets[job_id] = budget
        return budget

    async def check_and_consume(self, job_id: str, workflow_id: str):
        """
        Check retry budget and consume on approval.

        Returns:
            (allowed, reason)
        """
        async with self._lock:
            budget = self._budgets.get(job_id)
            if budget is None:
                return False, "retry_budget_missing"

            can_retry, reason = budget.can_retry(workflow_id)
            if can_retry:
                budget.consume_retry(workflow_id)

            return can_retry, reason

    async def cleanup(self, job_id: str):
        """Remove retry budget state for a completed job."""
        async with self._lock:
            self._budgets.pop(job_id, None)

    def _resolve_total_budget(self, total: int):
        requested = total if total > 0 else self._config.retry_budget_default
        return min(max(0, requested), self._config.retry_budget_max)

    def _resolve_per_workflow_budget(self, per_workflow: int, total_budget: int):
        requested = (
            per_workflow
            if per_workflow > 0
            else self._config.retry_budget_per_workflow_default
        )
        return min(
            min(max(0, requested), self._config.retry_budget_per_workflow_max),
            total_budget,
        )
