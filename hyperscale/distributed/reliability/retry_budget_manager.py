"""
Retry budget manager for distributed workflow dispatch (AD-44).
"""

import asyncio

from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import RetryBudgetExhausted

from .reliability_config import ReliabilityConfig
from .retry_budget_state import RetryBudgetState


class RetryBudgetManager:
    """
    Manages retry budgets for jobs and workflows.

    Uses an asyncio lock to protect shared budget state. Every refused
    retry is logged (``RetryBudgetExhausted``) and counted; the per-job
    consumed and refused counts are the AD-44 ``retry_budget_consumed_total``
    and ``retry_budget_exhausted_total`` metrics, held while the job's
    budget is.
    """

    __slots__ = ("_budgets", "_config", "_lock", "_logger", "_node_id", "_datacenter")

    def __init__(
        self,
        config: ReliabilityConfig,
        logger: Logger,
        node_id: str,
        datacenter: str,
    ) -> None:
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._datacenter = datacenter
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
        Check retry budget and consume on approval; a refusal for a spent
        budget is counted and logged.

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
                return True, reason

            scope, consumed, total = budget.record_refusal(workflow_id)

        await self._logger.log(
            RetryBudgetExhausted(
                message=f"Retry of workflow {workflow_id} of job {job_id} refused: {reason}",
                node_id=self._node_id,
                datacenter=self._datacenter,
                job_id=job_id,
                workflow_id=workflow_id,
                scope=scope,
                consumed=consumed,
                budget=total,
            )
        )
        return False, reason

    async def cleanup(self, job_id: str):
        """Remove retry budget state for a completed job."""
        async with self._lock:
            self._budgets.pop(job_id, None)

    def consumed_by_job(self) -> dict[str, int]:
        """Retries each job with a budget held here consumed (AD-44
        ``retry_budget_consumed_total{job_id}``)."""
        return {job_id: budget.consumed for job_id, budget in self._budgets.items()}

    def exhausted_by_job(self) -> dict[str, int]:
        """Retries each job with a budget held here was refused for a spent
        budget (AD-44 ``retry_budget_exhausted_total{job_id}``)."""
        return {job_id: budget.refused for job_id, budget in self._budgets.items()}

    def refused_retries(self, job_id: str) -> int:
        """Retries of a job refused for a spent budget so far; 0 for a job
        with no budget held here (D-67 reads it as the job's noise)."""
        budget = self._budgets.get(job_id)
        return budget.refused if budget is not None else 0

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
