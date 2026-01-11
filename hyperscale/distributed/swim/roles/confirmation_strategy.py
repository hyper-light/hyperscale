"""
Role-based confirmation strategy configuration (AD-35 Task 12.5.1-12.5.2).

Defines how long to wait and whether to proactively confirm unconfirmed peers
based on their role (Gate/Manager/Worker).
"""

from dataclasses import dataclass

from hyperscale.distributed.models.distributed import NodeRole


@dataclass(slots=True)
class RoleBasedConfirmationStrategy:
    """
    Confirmation strategy for a specific role (AD-35 Task 12.5.1).

    Defines timeout and confirmation behavior for unconfirmed peers.
    """

    role: NodeRole
    passive_timeout_seconds: float  # Base timeout before action
    enable_proactive_confirmation: bool  # Whether to actively probe
    confirmation_attempts: int  # Number of retries (if proactive)
    attempt_interval_seconds: float  # Delay between retries
    latency_aware: bool  # Use Vivaldi for timeout adjustment
    use_vivaldi: bool  # Enable Vivaldi coordinate tracking
    load_multiplier_max: float  # Max timeout multiplier under load


# Role-specific strategy constants (AD-35 Task 12.5.2)

GATE_STRATEGY = RoleBasedConfirmationStrategy(
    role=NodeRole.GATE,
    passive_timeout_seconds=120.0,  # 2 minutes base timeout
    enable_proactive_confirmation=True,  # Actively probe gates
    confirmation_attempts=5,  # 5 retries for cross-DC gates
    attempt_interval_seconds=5.0,  # 5 seconds between attempts
    latency_aware=True,  # Use Vivaldi RTT for timeout
    use_vivaldi=True,  # Enable coordinate system
    load_multiplier_max=3.0,  # Max 3x under load
)

MANAGER_STRATEGY = RoleBasedConfirmationStrategy(
    role=NodeRole.MANAGER,
    passive_timeout_seconds=90.0,  # 90 seconds base timeout
    enable_proactive_confirmation=True,  # Actively probe managers
    confirmation_attempts=3,  # 3 retries
    attempt_interval_seconds=5.0,  # 5 seconds between attempts
    latency_aware=True,  # Use Vivaldi RTT for timeout
    use_vivaldi=True,  # Enable coordinate system
    load_multiplier_max=5.0,  # Max 5x under load
)

WORKER_STRATEGY = RoleBasedConfirmationStrategy(
    role=NodeRole.WORKER,
    passive_timeout_seconds=180.0,  # 3 minutes base timeout (workers are busy)
    enable_proactive_confirmation=False,  # NEVER probe workers
    confirmation_attempts=0,  # No retries
    attempt_interval_seconds=0.0,  # N/A
    latency_aware=False,  # Workers are same-DC, no Vivaldi needed
    use_vivaldi=False,  # Disable coordinate system for workers
    load_multiplier_max=10.0,  # Max 10x under extreme load
)


def get_strategy_for_role(role: NodeRole) -> RoleBasedConfirmationStrategy:
    """
    Get confirmation strategy for a node role.

    Args:
        role: The node's role (Gate/Manager/Worker)

    Returns:
        Appropriate confirmation strategy for that role
    """
    strategies = {
        NodeRole.GATE: GATE_STRATEGY,
        NodeRole.MANAGER: MANAGER_STRATEGY,
        NodeRole.WORKER: WORKER_STRATEGY,
    }
    return strategies.get(role, WORKER_STRATEGY)  # Default to worker (most conservative)
