"""``CircuitBreakerConfig`` -- pickled under the namespace
``hyperscale.distributed.health.circuit_breaker_manager`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class CircuitBreakerConfig:
    """Configuration for circuit breakers."""

    max_errors: int = 5
    window_seconds: float = 60.0
    half_open_after: float = 30.0
