"""``GateHealthConfig`` -- pickled under the namespace
``hyperscale.distributed.health.gate_health`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class GateHealthConfig:
    """Configuration for gate health thresholds."""

    # Liveness thresholds
    liveness_timeout_seconds: float = 30.0
    max_consecutive_liveness_failures: int = 3

    # Progress rate thresholds (as fraction of expected)
    normal_rate_threshold: float = 0.8  # >= 80% of expected = normal
    slow_rate_threshold: float = 0.3  # >= 30% of expected = slow
    # Below slow threshold = degraded
    # Zero forwards with jobs = stuck

    # Overload states that indicate not ready
    overload_not_ready_states: tuple[str, ...] = ("stressed", "overloaded")
