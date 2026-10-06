"""Per-category circuit breaker overrides consumed by ``ErrorHandler``."""

from typing import TypedDict


class CircuitBreakerSettings(TypedDict, total=False):
    """Keyword arguments forwarded to ``ErrorStats`` for one error category."""

    window_seconds: float
    max_errors: int
    half_open_after: float
    max_timestamps: int
    error_threshold: int | None
    error_rate_threshold: float
