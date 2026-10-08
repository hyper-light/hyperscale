"""
Retry utilities with exponential backoff.

Provides robust retry logic for distributed systems with:
- Exponential backoff with configurable base and max delay
- Jitter to prevent thundering herd on recovery
- Retry budgets to limit total retry time
- Category-specific retry policies

Phase 5 DI: same shape as ``swim/retry.py`` — ``Clock`` / ``Random``
threaded through the standalone helpers and the policy's delay
calculation, defaults bound to the module-level ``RealClock`` /
``RealRandom``.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from typing import TypeVar, Callable, Awaitable, ClassVar, ParamSpec
from enum import Enum, auto
from hyperscale.distributed.runtime import Clock, Random, RealClock, RealRandom

from .errors import SwimError, ErrorCategory, ErrorSeverity, NetworkError
from .retry_steps import (
    clamp_delay_to_budget,
    invoke_retry_callback,
    last_error_or_none,
    raise_exhausted,
    record_bounded_error,
    resolve_default,
    should_stop_retrying,
    sleep_before_retry,
)
from .retry_shared import _DEFAULT_RANDOM
from .retry_decision import RetryDecision
from .retry_policy import RetryPolicy
from .retry_result import RetryResult

T = TypeVar('T')
P = ParamSpec('P')

_DEFAULT_CLOCK: Clock = RealClock()

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


async def retry_with_backoff(
    fn: Callable[[], Awaitable[T]],
    policy: RetryPolicy | None = None,
    on_retry: Callable[[int, Exception, float], Awaitable[None] | None] | None = None,
    on_success: Callable[[int], Awaitable[None] | None] | None = None,
    *,
    clock: Clock | None = None,
    random_source: Random | None = None,
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
            on_retry=lambda a, e, d: logger.debug(f"Retry {a}: {e}, waiting {d:.2f}s"),
        )
    """
    policy = resolve_default(policy, PROBE_RETRY_POLICY)

    active_clock = resolve_default(clock, _DEFAULT_CLOCK)
    active_random = resolve_default(random_source, _DEFAULT_RANDOM)

    start_time = active_clock.monotonic()
    last_error: Exception | None = None

    for attempt in range(policy.max_attempts):
        try:
            result = await fn()

            await invoke_retry_callback(on_success, attempt + 1)

            return result

        except Exception as e:
            last_error = e

            # Check if we should retry, if we're on the last attempt, and the budget
            decision = policy.should_retry(e)
            if should_stop_retrying(policy, decision, RetryDecision.ABORT, attempt, active_clock, start_time):
                raise

            # Calculate delay, then check budget again with delay
            delay = clamp_delay_to_budget(
                policy,
                policy.get_delay(attempt, random_source=active_random),
                active_clock,
                start_time,
            )

            # Callback before retry
            await invoke_retry_callback(on_retry, attempt + 1, e, delay)

            # Wait before retry
            await sleep_before_retry(active_clock, delay, decision, RetryDecision.IMMEDIATE)

    raise_exhausted(last_error)


async def retry_with_result(
    fn: Callable[[], Awaitable[T]],
    policy: RetryPolicy | None = None,
    on_retry: Callable[[int, Exception, float], Awaitable[None] | None] | None = None,
    *,
    clock: Clock | None = None,
    random_source: Random | None = None,
) -> RetryResult[T]:
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
            # Handle success - result.value contains the return value
            pass
        else:
            # Handle failure - result.last_error contains the exception
            pass
    """
    policy = resolve_default(policy, PROBE_RETRY_POLICY)

    active_clock = resolve_default(clock, _DEFAULT_CLOCK)
    active_random = resolve_default(random_source, _DEFAULT_RANDOM)

    start_time = active_clock.monotonic()
    errors: list[Exception] = []
    max_stored_errors = RetryResult.MAX_STORED_ERRORS

    for attempt in range(policy.max_attempts):
        try:
            value = await fn()
            return RetryResult(
                success=True,
                value=value,
                attempts=attempt + 1,
                total_time=active_clock.monotonic() - start_time,
                errors=errors,
            )

        except Exception as e:
            # Bound stored errors to prevent memory growth
            record_bounded_error(errors, e, max_stored_errors)

            # Check if we should retry, if we're on the last attempt, and the budget
            decision = policy.should_retry(e)
            if should_stop_retrying(policy, decision, RetryDecision.ABORT, attempt, active_clock, start_time):
                break

            # Calculate delay
            delay = policy.get_delay(attempt, random_source=active_random)

            # Callback before retry
            await invoke_retry_callback(on_retry, attempt + 1, e, delay)

            # Wait before retry
            await sleep_before_retry(active_clock, delay, decision, RetryDecision.IMMEDIATE)

    return RetryResult(
        success=False,
        attempts=len(errors),
        total_time=active_clock.monotonic() - start_time,
        last_error=last_error_or_none(errors),
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
    def decorator(fn: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return await retry_with_backoff(
                lambda: fn(*args, **kwargs),
                policy=policy,
                on_retry=on_retry,
            )
        return wrapper
    return decorator

_REHOMED = (
    RetryDecision,
    RetryPolicy,
    RetryResult,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
