"""``ProgressState`` -- pickled under the namespace
``hyperscale.distributed.health.worker_health`` (see that module)."""

from enum import Enum


class ProgressState(Enum):
    """Progress signal states."""

    IDLE = "idle"  # No work assigned
    NORMAL = "normal"  # Completing at expected rate
    SLOW = "slow"  # Below expected rate but making progress
    DEGRADED = "degraded"  # Significantly below expected rate
    STUCK = "stuck"  # No completions despite having work
