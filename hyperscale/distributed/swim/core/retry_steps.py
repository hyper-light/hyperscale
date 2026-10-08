"""
Sequential steps of the SWIM retry loops.

``swim/retry.py`` and ``swim/core/retry.py`` both run the same
exponential-backoff loop over their own ``RetryPolicy`` / ``RetryDecision``
types; these steps take the decision members they compare against as
arguments so both modules share one implementation.
"""

import asyncio
from enum import Enum
from typing import Awaitable, Callable, TypeVar

from hyperscale.distributed.runtime import Clock

from .retry_budget_policy import RetryBudgetPolicy

T = TypeVar("T")


def resolve_default(value: T | None, default: T) -> T:
    """Return ``value`` unless it is ``None``, in which case ``default``."""
    return value if value is not None else default


def budget_exhausted(policy: RetryBudgetPolicy, clock: Clock, start_time: float) -> bool:
    """True when the policy has a time budget and it has been spent."""
    return policy.budget_seconds is not None and clock.monotonic() - start_time >= policy.budget_seconds


def should_stop_retrying(
    policy: RetryBudgetPolicy,
    decision: Enum,
    abort_decision: Enum,
    attempt: int,
    clock: Clock,
    start_time: float,
) -> bool:
    """End the loop on an ABORT decision, on the last attempt, or once the budget is spent."""
    return (
        decision == abort_decision
        or attempt == policy.max_attempts - 1
        or budget_exhausted(policy, clock, start_time)
    )


def clamp_delay_to_budget(policy: RetryBudgetPolicy, delay: float, clock: Clock, start_time: float) -> float:
    """Shorten the backoff delay so it never outlasts the remaining budget."""
    if policy.budget_seconds is None:
        return delay
    remaining = policy.budget_seconds - (clock.monotonic() - start_time)
    return max(0, remaining) if delay > remaining else delay


async def invoke_retry_callback(
    callback: Callable[..., Awaitable[None] | None] | None,
    *callback_arguments: object,
) -> None:
    """Call an optional retry/success callback, awaiting it when it returns a coroutine."""
    if callback:
        callback_result = callback(*callback_arguments)
        if asyncio.iscoroutine(callback_result):
            await callback_result


async def sleep_before_retry(clock: Clock, delay: float, decision: Enum, immediate_decision: Enum) -> None:
    """Wait out the backoff delay unless it is zero or the decision asks for an immediate retry."""
    if delay > 0 and decision != immediate_decision:
        await clock.sleep(delay)


def record_bounded_error(errors: list[Exception], error: Exception, max_stored_errors: int) -> None:
    """Append ``error``, first dropping the oldest when the list is full (bounds memory growth)."""
    # Bound stored errors to prevent memory growth
    if len(errors) >= max_stored_errors:
        # Keep most recent errors by removing oldest
        errors.pop(0)
    errors.append(error)


def last_error_or_none(errors: list[Exception]) -> Exception | None:
    """The most recent stored error, or ``None`` when no attempt failed."""
    return errors[-1] if errors else None


def raise_exhausted(last_error: Exception | None) -> None:
    """Raise the last error when the loop ends without returning, or a RuntimeError when none was seen."""
    # Should not reach here, but just in case
    if last_error:
        raise last_error
    raise RuntimeError("Retry loop exited unexpectedly")
