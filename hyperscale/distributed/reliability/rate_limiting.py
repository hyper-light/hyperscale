"""
Rate Limiting (AD-24).

Provides adaptive rate limiting that integrates with the HybridOverloadDetector
to avoid false positives during legitimate traffic bursts.

Components:
- SlidingWindowCounter: Deterministic counting without time-division edge cases
- AdaptiveRateLimiter: Health-gated limiting that only activates under stress
- ServerRateLimiter: Per-client rate limiting using adaptive approach
- CooperativeRateLimiter: Client-side rate limit tracking

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Callable
from hyperscale.distributed.reliability.overload import HybridOverloadDetector, OverloadConfig, OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.distributed.runtime import Clock, RealClock

from .rate_limiting_shared import _DEFAULT_CLOCK
from .server_rate_limiter import HANDLER_RATE_LIMIT_OPERATIONS
from .adaptive_rate_limit_config import AdaptiveRateLimitConfig
from .adaptive_rate_limiter import AdaptiveRateLimiter
from .cooperative_rate_limiter import CooperativeRateLimiter
from .rate_limit_config import RateLimitConfig
from .rate_limit_result import RateLimitResult
from .rate_limit_retry_config import RateLimitRetryConfig
from .rate_limit_retry_result import RateLimitRetryResult
from .server_rate_limiter import ServerRateLimiter
from .sliding_window_counter import SlidingWindowCounter


def is_rate_limit_response(data: bytes) -> bool:
    """
    Check if response data is a RateLimitResponse.

    This is a lightweight check before attempting full deserialization.
    Uses the msgspec message type marker to identify RateLimitResponse.

    Args:
        data: Raw response bytes from TCP handler

    Returns:
        True if this appears to be a RateLimitResponse
    """
    # RateLimitResponse has 'operation' and 'retry_after_seconds' fields
    # Check for common patterns in msgspec serialization
    # This is a heuristic - the full check requires deserialization
    if len(data) < 10:
        return False

    # RateLimitResponse will contain 'operation' field name in the struct
    # For msgspec Struct serialization, look for the field marker
    return b"operation" in data and b"retry_after_seconds" in data


async def handle_rate_limit_response(
    limiter: CooperativeRateLimiter,
    operation: str,
    retry_after_seconds: float,
    wait: bool = True,
) -> float:
    """
    Handle a rate limit response from the server.

    Registers the rate limit with the cooperative limiter and optionally
    waits before returning.

    Args:
        limiter: The CooperativeRateLimiter instance
        operation: The operation that was rate limited
        retry_after_seconds: How long to wait before retrying
        wait: If True, wait for the retry_after period before returning

    Returns:
        Time waited in seconds (0 if wait=False)

    Example:
        # In client code after receiving response
        response_data = await send_tcp(addr, "job_submit", request.dump())
        if is_rate_limit_response(response_data):
            rate_limit = RateLimitResponse.load(response_data)
            await handle_rate_limit_response(
                my_limiter,
                rate_limit.operation,
                rate_limit.retry_after_seconds,
            )
            # Retry the request
            response_data = await send_tcp(addr, "job_submit", request.dump())
    """
    limiter.handle_rate_limit(operation, retry_after_seconds)

    if wait:
        return await limiter.wait_if_needed(operation)

    return 0.0


async def execute_with_rate_limit_retry(
    operation_func,
    operation_name: str,
    limiter: CooperativeRateLimiter,
    config: RateLimitRetryConfig | None = None,
    response_parser=None,
) -> RateLimitRetryResult:
    """
    Execute an operation with automatic retry on rate limiting.

    This function wraps any async operation and automatically handles
    rate limit responses by waiting the specified retry_after time
    and retrying up to max_retries times.

    Args:
        operation_func: Async function that performs the operation and returns bytes
        operation_name: Name of the operation for rate limiting (e.g., "job_submit")
        limiter: CooperativeRateLimiter to track rate limit state
        config: Retry configuration (defaults to RateLimitRetryConfig())
        response_parser: Optional function to parse response and check if it's
                         a RateLimitResponse. If None, uses is_rate_limit_response.

    Returns:
        RateLimitRetryResult with success status, response, retry count, and wait time

    Example:
        async def submit_job():
            return await send_tcp(gate_addr, "job_submit", submission.dump())

        result = await execute_with_rate_limit_retry(
            submit_job,
            "job_submit",
            my_limiter,
        )

        if result.success:
            job_ack = JobAck.load(result.response)
        else:
            print(f"Failed after {result.retries} retries: {result.final_error}")
    """
    if config is None:
        config = RateLimitRetryConfig()

    if response_parser is None:
        response_parser = is_rate_limit_response

    total_wait_time = 0.0
    retries = 0
    start_time = _DEFAULT_CLOCK.monotonic()

    # Check if we're already blocked for this operation
    if limiter.is_blocked(operation_name):
        initial_wait = await limiter.wait_if_needed(operation_name)
        total_wait_time += initial_wait

    while retries <= config.max_retries:
        # Check if we've exceeded max total wait time
        elapsed = _DEFAULT_CLOCK.monotonic() - start_time
        if elapsed >= config.max_total_wait:
            return RateLimitRetryResult(
                success=False,
                response=None,
                retries=retries,
                total_wait_time=total_wait_time,
                final_error=f"Exceeded max total wait time ({config.max_total_wait}s)",
            )

        try:
            # Execute the operation
            response = await operation_func()

            # Check if response is a rate limit response
            if response and response_parser(response):
                # Parse the rate limit response to get retry_after
                # Import here to avoid circular dependency
                from hyperscale.distributed.models import RateLimitResponse

                try:
                    rate_limit = RateLimitResponse.load(response)
                    retry_after = rate_limit.retry_after_seconds

                    # Apply backoff multiplier for subsequent retries
                    if retries > 0:
                        retry_after *= config.backoff_multiplier**retries

                    # Check if waiting would exceed our limits
                    if total_wait_time + retry_after > config.max_total_wait:
                        return RateLimitRetryResult(
                            success=False,
                            response=response,
                            retries=retries,
                            total_wait_time=total_wait_time,
                            final_error=f"Rate limited, retry_after ({retry_after}s) would exceed max wait",
                        )

                    # Wait and retry
                    limiter.handle_rate_limit(operation_name, retry_after)
                    await _DEFAULT_CLOCK.sleep(retry_after)
                    total_wait_time += retry_after
                    retries += 1
                    continue

                except Exception:
                    # Couldn't parse rate limit response, treat as failure
                    return RateLimitRetryResult(
                        success=False,
                        response=response,
                        retries=retries,
                        total_wait_time=total_wait_time,
                        final_error="Failed to parse rate limit response",
                    )

            # Success - not a rate limit response
            return RateLimitRetryResult(
                success=True,
                response=response,
                retries=retries,
                total_wait_time=total_wait_time,
            )

        except Exception as e:
            # Operation failed with exception
            return RateLimitRetryResult(
                success=False,
                response=None,
                retries=retries,
                total_wait_time=total_wait_time,
                final_error=str(e),
            )

    # Exhausted retries
    return RateLimitRetryResult(
        success=False,
        response=None,
        retries=retries,
        total_wait_time=total_wait_time,
        final_error=f"Exhausted max retries ({config.max_retries})",
    )

_REHOMED = (
    SlidingWindowCounter,
    AdaptiveRateLimitConfig,
    AdaptiveRateLimiter,
    RateLimitConfig,
    RateLimitResult,
    ServerRateLimiter,
    CooperativeRateLimiter,
    RateLimitRetryConfig,
    RateLimitRetryResult,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
