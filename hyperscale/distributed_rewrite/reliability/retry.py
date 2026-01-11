"""
Unified Retry Framework with Jitter (AD-21).

Provides a consistent retry mechanism with exponential backoff and jitter
for all network operations. Different jitter strategies suit different scenarios.

Jitter prevents thundering herd when multiple clients retry simultaneously.
"""

import asyncio
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


class JitterStrategy(Enum):
    """
    Jitter strategies for retry delays.

    FULL: Maximum spread, best for independent clients
        delay = random(0, min(cap, base * 2^attempt))

    EQUAL: Guarantees minimum delay while spreading
        temp = min(cap, base * 2^attempt)
        delay = temp/2 + random(0, temp/2)

    DECORRELATED: Each retry depends on previous, good bounded growth
        delay = random(base, previous_delay * 3)

    NONE: No jitter, pure exponential backoff
        delay = min(cap, base * 2^attempt)
    """

    FULL = "full"
    EQUAL = "equal"
    DECORRELATED = "decorrelated"
    NONE = "none"


@dataclass(slots=True)
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
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

    def __init__(self, config: RetryConfig | None = None):
        self._config = config or RetryConfig()
        self._previous_delay: float = self._config.base_delay

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
            return random.uniform(0, temp)

        elif jitter == JitterStrategy.EQUAL:
            # Equal jitter: half deterministic, half random
            temp = min(cap, base * (2**attempt))
            return temp / 2 + random.uniform(0, temp / 2)

        elif jitter == JitterStrategy.DECORRELATED:
            # Decorrelated: each delay depends on previous
            delay = random.uniform(base, self._previous_delay * 3)
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
    ) -> T:
        """
        Execute operation with retry and jitter.

        Args:
            operation: Async callable to execute
            operation_name: Name for error messages

        Returns:
            Result of successful operation

        Raises:
            Last exception if all retries exhausted
        """
        self.reset()  # Reset decorrelated jitter state
        last_exception: Exception | None = None

        for attempt in range(self._config.max_attempts):
            try:
                return await operation()
            except Exception as exc:
                last_exception = exc

                # Check if we should retry
                if not self._is_retryable(exc):
                    raise

                # Check if we have more attempts
                if attempt >= self._config.max_attempts - 1:
                    raise

                # Calculate and apply delay
                delay = self.calculate_delay(attempt)
                await asyncio.sleep(delay)

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        raise RuntimeError(f"{operation_name} failed without exception")

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


def calculate_jittered_delay(
    attempt: int,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    jitter: JitterStrategy = JitterStrategy.FULL,
) -> float:
    """
    Standalone function to calculate a jittered delay.

    Useful when you need jitter calculation without the full executor.

    Args:
        attempt: Zero-based attempt number
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap in seconds
        jitter: Jitter strategy to use

    Returns:
        Delay in seconds
    """
    if jitter == JitterStrategy.FULL:
        temp = min(max_delay, base_delay * (2**attempt))
        return random.uniform(0, temp)

    elif jitter == JitterStrategy.EQUAL:
        temp = min(max_delay, base_delay * (2**attempt))
        return temp / 2 + random.uniform(0, temp / 2)

    elif jitter == JitterStrategy.DECORRELATED:
        # For standalone use, treat as full jitter since we don't track state
        temp = min(max_delay, base_delay * (2**attempt))
        return random.uniform(0, temp)

    else:  # NONE
        return min(max_delay, base_delay * (2**attempt))


def add_jitter(
    interval: float,
    jitter_factor: float = 0.1,
) -> float:
    """
    Add jitter to a fixed interval.

    Useful for heartbeats, health checks, and other periodic operations
    where you want some variation to prevent synchronization.

    Args:
        interval: Base interval in seconds
        jitter_factor: Maximum jitter as fraction of interval (default 10%)

    Returns:
        Interval with random jitter applied

    Example:
        # 30 second heartbeat with 10% jitter (27-33 seconds)
        delay = add_jitter(30.0, jitter_factor=0.1)
    """
    jitter_amount = interval * jitter_factor
    return interval + random.uniform(-jitter_amount, jitter_amount)
