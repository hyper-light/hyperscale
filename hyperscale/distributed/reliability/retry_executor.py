"""``RetryExecutor`` -- pickled under the namespace
``hyperscale.distributed.reliability.retry`` (see that module)."""

from itertools import count
from typing import Awaitable, Callable, TypeVar
from hyperscale.distributed.runtime import Clock, Random, RealClock

from .retry_shared import _DEFAULT_RANDOM
from .jitter_strategy import JitterStrategy
from .retry_config import RetryConfig

T = TypeVar("T")

_DEFAULT_CLOCK: Clock = RealClock()


class RetryExecutor:
    """
    Unified retry execution with jitter.

    Example usage:
        executor = RetryExecutor(RetryConfig(max_attempts=3))

        result = await executor.execute(
            lambda: client.send_request(data),
            operation_name="send_request"
        )
    """

    def __init__(
        self,
        config: RetryConfig | None = None,
        *,
        clock: Clock | None = None,
        random_source: Random | None = None,
    ):
        self._config = config or RetryConfig()
        self._previous_delay: float = self._config.base_delay
        self._clock: Clock = clock if clock is not None else _DEFAULT_CLOCK
        self._random: Random = (
            random_source if random_source is not None else _DEFAULT_RANDOM
        )

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay with jitter for given attempt.

        Args:
            attempt: Zero-based attempt number (0 = first retry after initial failure)

        Returns:
            Delay in seconds before next retry
        """
        base = self._config.base_delay
        cap = self._config.max_delay
        jitter = self._config.jitter

        if jitter == JitterStrategy.FULL:
            # Full jitter: random(0, calculated_delay)
            temp = min(cap, base * (2**attempt))
            return self._random.uniform(0, temp)

        elif jitter == JitterStrategy.EQUAL:
            # Equal jitter: half deterministic, half random
            temp = min(cap, base * (2**attempt))
            return temp / 2 + self._random.uniform(0, temp / 2)

        elif jitter == JitterStrategy.DECORRELATED:
            # Decorrelated: each delay depends on previous
            delay = self._random.uniform(base, self._previous_delay * 3)
            delay = min(cap, delay)
            self._previous_delay = delay
            return delay

        else:  # NONE
            # Pure exponential backoff, no jitter
            return min(cap, base * (2**attempt))

    def reset(self) -> None:
        """Reset state for decorrelated jitter."""
        self._previous_delay = self._config.base_delay

    def _is_retryable(self, exc: Exception) -> bool:
        """Check if exception should trigger a retry."""
        # Check custom function first
        if self._config.is_retryable is not None:
            return self._config.is_retryable(exc)

        # Check against retryable exception types
        return isinstance(exc, self._config.retryable_exceptions)

    async def execute(
        self,
        operation: Callable[[], Awaitable[T]],
        operation_name: str = "operation",
        deadline_at: float | None = None,
    ) -> T:
        """
        Execute operation with retry and jitter.

        Args:
            operation: Async callable to execute
            operation_name: Name for error messages
            deadline_at: Clock instant after which no retry starts; the
                last wait is cut short to end at it. None: bounded by
                ``max_attempts`` alone.

        Returns:
            Result of successful operation

        Raises:
            Last exception if all retries exhausted
        """
        max_attempts = self._config.max_attempts
        if max_attempts is None and deadline_at is None:
            raise ValueError(f"{operation_name}: retries need max_attempts or a deadline")
        self.reset()  # Reset decorrelated jitter state

        for attempt in count():
            try:
                return await operation()
            except Exception as exc:
                # Check if we should retry
                if not self._is_retryable(exc):
                    raise

                # Check if we have more attempts
                if max_attempts is not None and attempt >= max_attempts - 1:
                    raise

                # Calculate and apply delay, never past the deadline
                delay = self.calculate_delay(attempt)
                if deadline_at is not None:
                    if (remaining := deadline_at - self._clock.monotonic()) <= 0:
                        raise
                    delay = min(delay, remaining)
                await self._clock.sleep(delay)

    async def execute_with_fallback(
        self,
        operation: Callable[[], Awaitable[T]],
        fallback: Callable[[], Awaitable[T]],
        operation_name: str = "operation",
    ) -> T:
        """
        Execute operation with retry, falling back to alternate on exhaustion.

        Args:
            operation: Primary async callable to execute
            fallback: Fallback async callable if primary exhausts retries
            operation_name: Name for error messages

        Returns:
            Result of successful operation (primary or fallback)
        """
        try:
            return await self.execute(operation, operation_name)
        except Exception:
            return await fallback()
