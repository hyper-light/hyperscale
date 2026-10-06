"""``WorkerHealthManagerConfig`` -- pickled under the namespace
``hyperscale.distributed.health.worker_health_manager`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class WorkerHealthManagerConfig:
    """
    Configuration for WorkerHealthManager.

    Attributes:
        base_deadline: Base deadline in seconds for extensions.
        min_grant: Minimum extension grant in seconds.
        max_extensions: Maximum extensions per worker per cycle.
        eviction_threshold: Number of failed extensions before eviction.
        warning_threshold: Remaining extensions to trigger warning notification.
        grace_period: Seconds of grace after exhaustion before kill.
    """

    base_deadline: float = 30.0
    min_grant: float = 1.0
    max_extensions: int = 5
    eviction_threshold: int = 3
    warning_threshold: int = 1
    grace_period: float = 10.0
