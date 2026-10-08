"""``WorkerHealthManagerConfig`` -- pickled under the namespace
``hyperscale.distributed.health.worker_health_manager`` (see that module)."""

from dataclasses import dataclass

from hyperscale.distributed.env.env import Env


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

    @classmethod
    def from_env(cls, env: Env) -> "WorkerHealthManagerConfig":
        """
        Worker health manager configuration (AD-26) from ``env``.

        Controls deadline extension tracking for workers; extensions use
        logarithmic decay to prevent indefinite extensions.
        """
        return cls(
            base_deadline=env.EXTENSION_BASE_DEADLINE,
            min_grant=env.EXTENSION_MIN_GRANT,
            max_extensions=env.EXTENSION_MAX_EXTENSIONS,
            eviction_threshold=env.EXTENSION_EVICTION_THRESHOLD,
            warning_threshold=env.EXTENSION_EXHAUSTION_WARNING_THRESHOLD,
            grace_period=env.EXTENSION_EXHAUSTION_GRACE_PERIOD,
        )
