"""
Retry utilities with exponential backoff.

Provides robust retry logic for distributed systems with:
- Exponential backoff with configurable base and max delay
- Jitter to prevent thundering herd on recovery
- Retry budgets to limit total retry time
- Category-specific retry policies
"""

import asyncio
import random
from dataclasses import dataclass, field
from typing import TypeVar, Callable, Awaitable, Any
from enum import Enum, auto

from hyperscale.distributed.swim.core import SwimError, ErrorCategory, ErrorSeverity, NetworkError


T = TypeVar('T')


class RetryDecision(Enum):
    """Decision for whether to retry an operation."""
    RETRY = auto()       # Retry after delay
    ABORT = auto()       # Don't retry, give up
    IMMEDIATE = auto()   # Retry immediately (no delay)


@dataclass(slots=True)
class RetryPolicy:
    """
    Configuration for retry behavior.
    
    Example:
        # Aggressive retry for probes
        probe_policy = RetryPolicy(
            max_attempts=3,
            base_delay=0.1,
            max_delay=2.0,
            jitter=0.2,
        )
        
        # Conservative retry for elections
        election_policy = RetryPolicy(
            max_attempts=2,
            base_delay=1.0,
            max_delay=5.0,
            budget_seconds=10.0,
        )
    """
    
    max_attempts: int = 3
    """Maximum number of attempts (including first try)."""
    
    base_delay: float = 0.1
    """Initial delay in seconds."""
    
    max_delay: float = 5.0
    """Maximum delay in seconds (caps exponential growth)."""
    
    exponential_base: float = 2.0
    """Base for exponential backoff (delay = base_delay * base^attempt)."""
    
    jitter: float = 0.1
    """Jitter factor (0-1). Delay varies by ±jitter*delay."""
    
    budget_seconds: float | None = None
    """Total time budget for all retries. None = unlimited."""
    
    retryable_categories: set[ErrorCategory] = field(
        default_factory=lambda: {
            ErrorCategory.NETWORK,
            ErrorCategory.RESOURCE,
        }
    )
    """Error categories that should be retried."""
    
    retryable_severities: set[ErrorSeverity] = field(
        default_factory=lambda: {
            ErrorSeverity.TRANSIENT,
            ErrorSeverity.DEGRADED,
        }
    )
    """Error severities that should be retried."""
    
    def should_retry(self, error: SwimError | Exception) -> RetryDecision:
        """Determine if an error should trigger a retry."""
        if isinstance(error, SwimError):
            if error.category not in self.retryable_categories:
                return RetryDecision.ABORT
            if error.severity not in self.retryable_severities:
                return RetryDecision.ABORT
            return RetryDecision.RETRY
        
        # Standard exceptions
        if isinstance(error, (asyncio.TimeoutError, ConnectionError, OSError)):
            return RetryDecision.RETRY
        if isinstance(error, (ValueError, TypeError, AttributeError)):
            return RetryDecision.ABORT  # Likely a bug, don't retry
        
        return RetryDecision.RETRY  # Default to retry for unknown
    
    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt number.
        
        Uses exponential backoff with jitter.
        """
        # Exponential backoff
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay,
        )
        
        # Add jitter to prevent thundering herd
        if self.jitter > 0:
            jitter_range = delay * self.jitter
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)


# Pre-defined policies for common use cases
PROBE_RETRY_POLICY = RetryPolicy(
    max_attempts=3,
    base_delay=0.1,
    max_delay=2.0,
    jitter=0.15,
    retryable_categories={ErrorCategory.NETWORK},
)

ELECTION_RETRY_POLICY = RetryPolicy(
    max_attempts=2,
    base_delay=0.5,
    max_delay=3.0,
    jitter=0.2,
    budget_seconds=10.0,
    retryable_categories={ErrorCategory.NETWORK, ErrorCategory.ELECTION},
)

GOSSIP_RETRY_POLICY = RetryPolicy(
    max_attempts=2,
    base_delay=0.05,
    max_delay=0.5,
    jitter=0.3,
    retryable_categories={ErrorCategory.NETWORK},
)


@dataclass(slots=True)
class RetryResult:
    """Result of a retry operation."""
    
    success: bool
    """Whether the operation eventually succeeded."""
    
    value: Any = None
    """Return value if successful."""
    
    attempts: int = 0
    """Number of attempts made."""
    
    total_time: float = 0.0
    """Total time spent including delays."""
    
    last_error: Exception | None = None
    """Last error encountered (if failed)."""
    
    errors: list[Exception] = field(default_factory=list)
    """All errors encountered."""


async def retry_with_backoff(
    fn: Callable[[], Awaitable[T]],
    policy: RetryPolicy | None = None,
    on_retry: Callable[[int, Exception, float], Awaitable[None] | None] | None = None,
    on_success: Callable[[int], Awaitable[None] | None] | None = None,
) -> T:
    """
    Retry an async function with exponential backoff.
    
    Args:
        fn: Async function to retry
        policy: Retry policy (defaults to PROBE_RETRY_POLICY)
        on_retry: Callback before each retry (attempt, error, delay)
        on_success: Callback on success (attempts)
    
    Returns:
        Result of successful function call
    
    Raises:
        Last exception if all retries exhausted
    
    Example:
        async def probe_node(target):
            # ... probe logic ...
        
        result = await retry_with_backoff(
            lambda: probe_node(target),
            policy=PROBE_RETRY_POLICY,
            on_retry=lambda a, e, d: print(f"Retry {a}: {e}, waiting {d:.2f}s"),
        )
    """
    if policy is None:
        policy = PROBE_RETRY_POLICY
    
    import time
    start_time = time.monotonic()
    last_error: Exception | None = None
    
    for attempt in range(policy.max_attempts):
        try:
            result = await fn()
            
            if on_success:
                callback_result = on_success(attempt + 1)
                if asyncio.iscoroutine(callback_result):
                    await callback_result
            
            return result
            
        except Exception as e:
            last_error = e
            
            # Check if we should retry
            decision = policy.should_retry(e)
            if decision == RetryDecision.ABORT:
                raise
            
            # Check if we're on last attempt
            if attempt == policy.max_attempts - 1:
                raise
            
            # Check budget
            if policy.budget_seconds is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= policy.budget_seconds:
                    raise
            
            # Calculate delay
            delay = policy.get_delay(attempt)
            
            # Check budget again with delay
            if policy.budget_seconds is not None:
                remaining = policy.budget_seconds - (time.monotonic() - start_time)
                if delay > remaining:
                    delay = max(0, remaining)
            
            # Callback before retry
            if on_retry:
                callback_result = on_retry(attempt + 1, e, delay)
                if asyncio.iscoroutine(callback_result):
                    await callback_result
            
            # Wait before retry
            if delay > 0 and decision != RetryDecision.IMMEDIATE:
                await asyncio.sleep(delay)
    
    # Should not reach here, but just in case
    if last_error:
        raise last_error
    raise RuntimeError("Retry loop exited unexpectedly")


async def retry_with_result(
    fn: Callable[[], Awaitable[T]],
    policy: RetryPolicy | None = None,
    on_retry: Callable[[int, Exception, float], Awaitable[None] | None] | None = None,
) -> RetryResult:
    """
    Retry an async function, returning detailed result.
    
    Unlike retry_with_backoff, this never raises - it returns
    a RetryResult indicating success or failure with details.
    
    Example:
        result = await retry_with_result(
            lambda: probe_node(target),
            policy=PROBE_RETRY_POLICY,
        )
        
        if result.success:
            print(f"Succeeded after {result.attempts} attempts")
        else:
            print(f"Failed after {result.attempts} attempts: {result.last_error}")
    """
    if policy is None:
        policy = PROBE_RETRY_POLICY
    
    import time
    start_time = time.monotonic()
    errors: list[Exception] = []
    
    for attempt in range(policy.max_attempts):
        try:
            value = await fn()
            return RetryResult(
                success=True,
                value=value,
                attempts=attempt + 1,
                total_time=time.monotonic() - start_time,
                errors=errors,
            )
            
        except Exception as e:
            errors.append(e)
            
            # Check if we should retry
            decision = policy.should_retry(e)
            if decision == RetryDecision.ABORT:
                break
            
            # Check if we're on last attempt
            if attempt == policy.max_attempts - 1:
                break
            
            # Check budget
            if policy.budget_seconds is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= policy.budget_seconds:
                    break
            
            # Calculate delay
            delay = policy.get_delay(attempt)
            
            # Callback before retry
            if on_retry:
                callback_result = on_retry(attempt + 1, e, delay)
                if asyncio.iscoroutine(callback_result):
                    await callback_result
            
            # Wait before retry
            if delay > 0 and decision != RetryDecision.IMMEDIATE:
                await asyncio.sleep(delay)
    
    return RetryResult(
        success=False,
        attempts=len(errors),
        total_time=time.monotonic() - start_time,
        last_error=errors[-1] if errors else None,
        errors=errors,
    )


def with_retry(
    policy: RetryPolicy | None = None,
    on_retry: Callable[[int, Exception, float], None] | None = None,
):
    """
    Decorator to add retry behavior to an async function.
    
    Example:
        @with_retry(policy=PROBE_RETRY_POLICY)
        async def send_probe(target: tuple[str, int]) -> bytes:
            # ... probe logic ...
    """
    def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await retry_with_backoff(
                lambda: fn(*args, **kwargs),
                policy=policy,
                on_retry=on_retry,
            )
        return wrapper
    return decorator

