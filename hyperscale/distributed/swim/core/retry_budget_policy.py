"""``RetryBudgetPolicy`` -- the policy shape the retry steps read."""

from typing import Protocol


class RetryBudgetPolicy(Protocol):
    """The ``RetryPolicy`` fields the retry steps in ``retry_steps`` read."""

    max_attempts: int
    budget_seconds: float | None
