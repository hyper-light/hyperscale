"""
Rate Limiting (AD-24).

Provides token bucket-based rate limiting for both client and server side.

Components:
- TokenBucket: Classic token bucket algorithm with configurable refill
- RateLimitConfig: Per-operation rate limits
- ServerRateLimiter: Per-client token buckets with cleanup
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class TokenBucket:
    """
    Classic token bucket algorithm for rate limiting.

    Tokens are added at a constant rate up to a maximum bucket size.
    Each operation consumes tokens, and operations are rejected when
    the bucket is empty.

    Thread-safety note: Synchronous methods (acquire, try_acquire) are safe
    for use in asyncio as they run atomically within a single event loop
    iteration. The async method (acquire_async) uses an asyncio.Lock to
    prevent race conditions across await points.

    Example usage:
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        # Check if operation is allowed
        if bucket.acquire():
            # Process request
            pass
        else:
            # Rate limited
            return 429
    """

    bucket_size: int
    refill_rate: float  # Tokens per second

    # Internal state
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _async_lock: asyncio.Lock = field(init=False)

    def __post_init__(self) -> None:
        self._tokens = float(self.bucket_size)
        self._last_refill = time.monotonic()
        self._async_lock = asyncio.Lock()

    def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            True if tokens were acquired, False if rate limited
        """
        self._refill()

        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def try_acquire(self, tokens: int = 1) -> tuple[bool, float]:
        """
        Try to acquire tokens and return wait time if not available.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            Tuple of (acquired, wait_seconds). If not acquired,
            wait_seconds indicates how long to wait for tokens.
        """
        self._refill()

        if self._tokens >= tokens:
            self._tokens -= tokens
            return True, 0.0

        # Calculate wait time for tokens to be available
        tokens_needed = tokens - self._tokens
        wait_seconds = tokens_needed / self.refill_rate
        return False, wait_seconds

    async def acquire_async(self, tokens: int = 1, max_wait: float = 10.0) -> bool:
        """
        Async version that waits for tokens if necessary.

        Uses asyncio.Lock to prevent race conditions where multiple coroutines
        wait for tokens and all try to acquire after the wait completes.

        Args:
            tokens: Number of tokens to acquire
            max_wait: Maximum time to wait for tokens

        Returns:
            True if tokens were acquired, False if timed out
        """
        async with self._async_lock:
            acquired, wait_time = self.try_acquire(tokens)
            if acquired:
                return True

            if wait_time > max_wait:
                return False

            # Wait while holding lock - prevents race where multiple waiters
            # all succeed after the wait
            await asyncio.sleep(wait_time)
            return self.acquire(tokens)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.refill_rate
        self._tokens = min(self.bucket_size, self._tokens + tokens_to_add)
        self._last_refill = now

    @property
    def available_tokens(self) -> float:
        """Get current number of available tokens."""
        self._refill()
        return self._tokens

    def reset(self) -> None:
        """Reset bucket to full capacity."""
        self._tokens = float(self.bucket_size)
        self._last_refill = time.monotonic()


@dataclass
class RateLimitConfig:
    """
    Configuration for rate limits per operation type.

    Each operation type has its own bucket configuration.
    """

    # Default limits for unknown operations
    default_bucket_size: int = 100
    default_refill_rate: float = 10.0  # per second

    # Per-operation limits: operation_name -> (bucket_size, refill_rate)
    operation_limits: dict[str, tuple[int, float]] = field(
        default_factory=lambda: {
            # High-frequency operations get larger buckets
            "stats_update": (500, 50.0),
            "heartbeat": (200, 20.0),
            "progress_update": (300, 30.0),
            # Standard operations
            "job_submit": (50, 5.0),
            "job_status": (100, 10.0),
            "workflow_dispatch": (100, 10.0),
            # Infrequent operations
            "cancel": (20, 2.0),
            "reconnect": (10, 1.0),
        }
    )

    def get_limits(self, operation: str) -> tuple[int, float]:
        """Get bucket size and refill rate for an operation."""
        return self.operation_limits.get(
            operation,
            (self.default_bucket_size, self.default_refill_rate),
        )


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    retry_after_seconds: float = 0.0
    tokens_remaining: float = 0.0


class ServerRateLimiter:
    """
    Server-side rate limiter with per-client token buckets.

    Maintains separate token buckets for each client, with automatic
    cleanup of inactive clients to prevent memory leaks.

    Example usage:
        limiter = ServerRateLimiter()

        # Check rate limit
        result = limiter.check_rate_limit("client-123", "job_submit")
        if not result.allowed:
            return Response(429, headers={"Retry-After": str(result.retry_after_seconds)})

        # Process request
        ...
    """

    def __init__(
        self,
        config: RateLimitConfig | None = None,
        inactive_cleanup_seconds: float = 300.0,  # 5 minutes
    ):
        self._config = config or RateLimitConfig()
        self._inactive_cleanup_seconds = inactive_cleanup_seconds

        # Per-client buckets: client_id -> {operation -> TokenBucket}
        self._client_buckets: dict[str, dict[str, TokenBucket]] = {}

        # Track last activity per client for cleanup
        self._client_last_activity: dict[str, float] = {}

        # Metrics
        self._total_requests: int = 0
        self._rate_limited_requests: int = 0
        self._clients_cleaned: int = 0

    def check(
        self,
        addr: tuple[str, int],
        raise_on_limit: bool = False,
    ) -> bool:
        """
        Compatibility method matching the simple RateLimiter.check() API.

        This allows ServerRateLimiter to be used as a drop-in replacement
        for the simple RateLimiter in base server code.

        Args:
            addr: Source address tuple (host, port)
            raise_on_limit: If True, raise RateLimitExceeded instead of returning False

        Returns:
            True if request is allowed, False if rate limited

        Raises:
            RateLimitExceeded: If raise_on_limit is True and rate is exceeded
        """
        # Convert address tuple to client_id string
        client_id = f"{addr[0]}:{addr[1]}"

        # Use "default" operation for simple rate limiting
        result = self.check_rate_limit(client_id, "default")

        if not result.allowed and raise_on_limit:
            from hyperscale.core.jobs.protocols.rate_limiter import RateLimitExceeded
            raise RateLimitExceeded(f"Rate limit exceeded for {addr[0]}:{addr[1]}")

        return result.allowed

    def check_rate_limit(
        self,
        client_id: str,
        operation: str,
        tokens: int = 1,
    ) -> RateLimitResult:
        """
        Check if a request is within rate limits.

        Args:
            client_id: Identifier for the client
            operation: Type of operation being performed
            tokens: Number of tokens to consume

        Returns:
            RateLimitResult indicating if allowed and retry info
        """
        self._total_requests += 1
        self._client_last_activity[client_id] = time.monotonic()

        bucket = self._get_or_create_bucket(client_id, operation)
        allowed, wait_time = bucket.try_acquire(tokens)

        if not allowed:
            self._rate_limited_requests += 1

        return RateLimitResult(
            allowed=allowed,
            retry_after_seconds=wait_time,
            tokens_remaining=bucket.available_tokens,
        )

    async def check_rate_limit_async(
        self,
        client_id: str,
        operation: str,
        tokens: int = 1,
        max_wait: float = 0.0,
    ) -> RateLimitResult:
        """
        Check rate limit with optional wait for tokens.

        Uses the TokenBucket's async acquire method which has proper locking
        to prevent race conditions when multiple coroutines wait for tokens.

        Args:
            client_id: Identifier for the client
            operation: Type of operation being performed
            tokens: Number of tokens to consume
            max_wait: Maximum time to wait for tokens (0 = no wait)

        Returns:
            RateLimitResult indicating if allowed
        """
        self._total_requests += 1
        self._client_last_activity[client_id] = time.monotonic()

        bucket = self._get_or_create_bucket(client_id, operation)

        if max_wait <= 0:
            # No wait - use synchronous check
            allowed, wait_time = bucket.try_acquire(tokens)
            if not allowed:
                self._rate_limited_requests += 1
            return RateLimitResult(
                allowed=allowed,
                retry_after_seconds=wait_time,
                tokens_remaining=bucket.available_tokens,
            )

        # Use async acquire with lock protection
        allowed = await bucket.acquire_async(tokens, max_wait)
        if not allowed:
            self._rate_limited_requests += 1

        return RateLimitResult(
            allowed=allowed,
            retry_after_seconds=0.0 if allowed else max_wait,
            tokens_remaining=bucket.available_tokens,
        )

    def _get_or_create_bucket(
        self,
        client_id: str,
        operation: str,
    ) -> TokenBucket:
        """Get existing bucket or create new one for client/operation."""
        if client_id not in self._client_buckets:
            self._client_buckets[client_id] = {}

        buckets = self._client_buckets[client_id]
        if operation not in buckets:
            bucket_size, refill_rate = self._config.get_limits(operation)
            buckets[operation] = TokenBucket(
                bucket_size=bucket_size,
                refill_rate=refill_rate,
            )

        return buckets[operation]

    def cleanup_inactive_clients(self) -> int:
        """
        Remove buckets for clients that have been inactive.

        Returns:
            Number of clients cleaned up
        """
        now = time.monotonic()
        cutoff = now - self._inactive_cleanup_seconds

        inactive_clients = [
            client_id
            for client_id, last_activity in self._client_last_activity.items()
            if last_activity < cutoff
        ]

        for client_id in inactive_clients:
            self._client_buckets.pop(client_id, None)
            self._client_last_activity.pop(client_id, None)
            self._clients_cleaned += 1

        return len(inactive_clients)

    def reset_client(self, client_id: str) -> None:
        """Reset all buckets for a client."""
        if client_id in self._client_buckets:
            for bucket in self._client_buckets[client_id].values():
                bucket.reset()

    def get_client_stats(self, client_id: str) -> dict[str, float]:
        """Get token counts for all operations for a client."""
        if client_id not in self._client_buckets:
            return {}

        return {
            operation: bucket.available_tokens
            for operation, bucket in self._client_buckets[client_id].items()
        }

    def get_metrics(self) -> dict:
        """Get rate limiting metrics."""
        rate_limited_rate = (
            self._rate_limited_requests / self._total_requests
            if self._total_requests > 0
            else 0.0
        )

        return {
            "total_requests": self._total_requests,
            "rate_limited_requests": self._rate_limited_requests,
            "rate_limited_rate": rate_limited_rate,
            "active_clients": len(self._client_buckets),
            "clients_cleaned": self._clients_cleaned,
        }

    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self._total_requests = 0
        self._rate_limited_requests = 0
        self._clients_cleaned = 0


class CooperativeRateLimiter:
    """
    Client-side cooperative rate limiter.

    Respects rate limit signals from the server and adjusts
    request rate accordingly.

    Example usage:
        limiter = CooperativeRateLimiter()

        # Before sending request
        await limiter.wait_if_needed("job_submit")

        # After receiving response
        if response.status == 429:
            retry_after = float(response.headers.get("Retry-After", 1.0))
            limiter.handle_rate_limit("job_submit", retry_after)
    """

    def __init__(self, default_backoff: float = 1.0):
        self._default_backoff = default_backoff

        # Per-operation state
        self._blocked_until: dict[str, float] = {}  # operation -> monotonic time

        # Metrics
        self._total_waits: int = 0
        self._total_wait_time: float = 0.0

    async def wait_if_needed(self, operation: str) -> float:
        """
        Wait if operation is currently rate limited.

        Args:
            operation: Type of operation

        Returns:
            Time waited in seconds
        """
        blocked_until = self._blocked_until.get(operation, 0.0)
        now = time.monotonic()

        if blocked_until <= now:
            return 0.0

        wait_time = blocked_until - now
        self._total_waits += 1
        self._total_wait_time += wait_time

        await asyncio.sleep(wait_time)
        return wait_time

    def handle_rate_limit(
        self,
        operation: str,
        retry_after: float | None = None,
    ) -> None:
        """
        Handle rate limit response from server.

        Args:
            operation: Type of operation that was rate limited
            retry_after: Suggested retry time from server
        """
        delay = retry_after if retry_after is not None else self._default_backoff
        self._blocked_until[operation] = time.monotonic() + delay

    def is_blocked(self, operation: str) -> bool:
        """Check if operation is currently blocked."""
        blocked_until = self._blocked_until.get(operation, 0.0)
        return time.monotonic() < blocked_until

    def get_retry_after(self, operation: str) -> float:
        """Get remaining time until operation is unblocked."""
        blocked_until = self._blocked_until.get(operation, 0.0)
        remaining = blocked_until - time.monotonic()
        return max(0.0, remaining)

    def clear(self, operation: str | None = None) -> None:
        """Clear rate limit state for operation (or all if None)."""
        if operation is None:
            self._blocked_until.clear()
        else:
            self._blocked_until.pop(operation, None)

    def get_metrics(self) -> dict:
        """Get cooperative rate limiting metrics."""
        return {
            "total_waits": self._total_waits,
            "total_wait_time": self._total_wait_time,
            "active_blocks": len(self._blocked_until),
        }


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


class RateLimitRetryConfig:
    """Configuration for rate limit retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        max_total_wait: float = 60.0,
        backoff_multiplier: float = 1.5,
    ):
        """
        Initialize retry configuration.

        Args:
            max_retries: Maximum number of retry attempts after rate limiting
            max_total_wait: Maximum total time to spend waiting/retrying (seconds)
            backoff_multiplier: Multiplier applied to retry_after on each retry
        """
        self.max_retries = max_retries
        self.max_total_wait = max_total_wait
        self.backoff_multiplier = backoff_multiplier


class RateLimitRetryResult:
    """Result of a rate-limit-aware operation."""

    def __init__(
        self,
        success: bool,
        response: bytes | None,
        retries: int,
        total_wait_time: float,
        final_error: str | None = None,
    ):
        self.success = success
        self.response = response
        self.retries = retries
        self.total_wait_time = total_wait_time
        self.final_error = final_error


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
    start_time = time.monotonic()

    # Check if we're already blocked for this operation
    if limiter.is_blocked(operation_name):
        initial_wait = await limiter.wait_if_needed(operation_name)
        total_wait_time += initial_wait

    while retries <= config.max_retries:
        # Check if we've exceeded max total wait time
        elapsed = time.monotonic() - start_time
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
                from hyperscale.distributed_rewrite.models import RateLimitResponse

                try:
                    rate_limit = RateLimitResponse.load(response)
                    retry_after = rate_limit.retry_after_seconds

                    # Apply backoff multiplier for subsequent retries
                    if retries > 0:
                        retry_after *= config.backoff_multiplier ** retries

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
                    await asyncio.sleep(retry_after)
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
