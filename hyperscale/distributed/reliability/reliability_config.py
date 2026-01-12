"""
Reliability configuration for retry budgets and best-effort completion (AD-44).
"""

from dataclasses import dataclass

from hyperscale.distributed.env import Env


@dataclass(slots=True)
class ReliabilityConfig:
    """Configuration values for retry budgets and best-effort handling."""

    retry_budget_max: int
    retry_budget_per_workflow_max: int
    retry_budget_default: int
    retry_budget_per_workflow_default: int
    best_effort_deadline_max: float
    best_effort_deadline_default: float
    best_effort_min_dcs_default: int
    best_effort_deadline_check_interval: float


def create_reliability_config_from_env(env: Env):
    """Create reliability configuration from environment settings."""
    return ReliabilityConfig(
        retry_budget_max=env.RETRY_BUDGET_MAX,
        retry_budget_per_workflow_max=env.RETRY_BUDGET_PER_WORKFLOW_MAX,
        retry_budget_default=env.RETRY_BUDGET_DEFAULT,
        retry_budget_per_workflow_default=env.RETRY_BUDGET_PER_WORKFLOW_DEFAULT,
        best_effort_deadline_max=env.BEST_EFFORT_DEADLINE_MAX,
        best_effort_deadline_default=env.BEST_EFFORT_DEADLINE_DEFAULT,
        best_effort_min_dcs_default=env.BEST_EFFORT_MIN_DCS_DEFAULT,
        best_effort_deadline_check_interval=env.BEST_EFFORT_DEADLINE_CHECK_INTERVAL,
    )
