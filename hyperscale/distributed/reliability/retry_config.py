"""``RetryConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.retry`` (see that module)."""

from dataclasses import dataclass, field
from typing import Callable

from .jitter_strategy import JitterStrategy


@dataclass(slots=True)
class RetryConfig:
    """Configuration for retry behavior."""

    # None: bounded only by the deadline ``execute`` is given.
    max_attempts: int | None = 3
    base_delay: float = 0.5  # seconds
    max_delay: float = 30.0  # cap
    jitter: JitterStrategy = JitterStrategy.FULL

    # Exceptions that should trigger a retry
    retryable_exceptions: tuple[type[Exception], ...] = field(
        default_factory=lambda: (
            ConnectionError,
            TimeoutError,
            OSError,
        )
    )

    # Optional: function to determine if an exception is retryable
    # Takes exception, returns bool
    is_retryable: Callable[[Exception], bool] | None = None
