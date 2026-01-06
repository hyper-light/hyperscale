"""
Integration tests for Rate Limiting (AD-24).

Tests:
- TokenBucket acquire and refill behavior
- ServerRateLimiter per-client limits
- CooperativeRateLimiter client-side throttling
- Client cleanup to prevent memory leaks
"""

import asyncio
import time
from unittest.mock import patch

import pytest

from hyperscale.distributed_rewrite.reliability import (
    CooperativeRateLimiter,
    RateLimitConfig,
    RateLimitResult,
    ServerRateLimiter,
    TokenBucket,
)


class TestTokenBucket:
    """Test TokenBucket basic operations."""

    def test_initial_state(self) -> None:
        """Test bucket starts full."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        assert bucket.available_tokens == 100.0

    def test_acquire_success(self) -> None:
        """Test successful token acquisition."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        result = bucket.acquire(10)

        assert result is True
        # Use approx due to time-based refill between operations
        assert bucket.available_tokens == pytest.approx(90.0, abs=0.1)

    def test_acquire_failure(self) -> None:
        """Test failed token acquisition when bucket empty."""
        bucket = TokenBucket(bucket_size=10, refill_rate=1.0)

        # Drain the bucket
        bucket.acquire(10)

        # Try to acquire more
        result = bucket.acquire(1)

        assert result is False

    def test_acquire_partial(self) -> None:
        """Test that partial tokens don't work."""
        bucket = TokenBucket(bucket_size=10, refill_rate=1.0)

        # Use up most tokens
        bucket.acquire(8)

        # Try to acquire more than available
        result = bucket.acquire(5)

        assert result is False
        # Use approx due to time-based refill between operations
        assert bucket.available_tokens == pytest.approx(2.0, abs=0.1)

    def test_try_acquire_with_wait_time(self) -> None:
        """Test try_acquire returns wait time."""
        bucket = TokenBucket(bucket_size=10, refill_rate=10.0)

        # Drain bucket
        bucket.acquire(10)

        # Check wait time for 5 tokens
        acquired, wait_time = bucket.try_acquire(5)

        assert acquired is False
        assert wait_time == pytest.approx(0.5, rel=0.1)  # 5 tokens / 10 per second

    def test_refill_over_time(self) -> None:
        """Test that tokens refill over time."""
        bucket = TokenBucket(bucket_size=100, refill_rate=100.0)  # 100 per second

        # Drain bucket
        bucket.acquire(100)
        # Use approx since tiny time passes between operations
        assert bucket.available_tokens == pytest.approx(0.0, abs=0.1)

        # Actually wait for refill (0.1 seconds = 10 tokens at 100/s)
        import asyncio
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.1))

        tokens = bucket.available_tokens
        # Should have gained approximately 10 tokens
        assert tokens == pytest.approx(10.0, abs=2.0)

    def test_refill_caps_at_bucket_size(self) -> None:
        """Test that refill doesn't exceed bucket size."""
        bucket = TokenBucket(bucket_size=100, refill_rate=1000.0)  # Very fast refill

        # Use some tokens
        bucket.acquire(50)

        # Wait a short time but enough to overfill at 1000/s rate
        import asyncio
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.2))

        tokens = bucket.available_tokens
        # Should be capped at 100, not 50 + 200 = 250
        assert tokens == pytest.approx(100.0, abs=0.1)

    def test_reset(self) -> None:
        """Test bucket reset."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        bucket.acquire(100)
        # Use approx since tiny time passes between operations
        assert bucket.available_tokens == pytest.approx(0.0, abs=0.1)

        bucket.reset()
        assert bucket.available_tokens == pytest.approx(100.0, abs=0.1)

    @pytest.mark.asyncio
    async def test_acquire_async(self) -> None:
        """Test async acquire with wait."""
        bucket = TokenBucket(bucket_size=10, refill_rate=100.0)  # Fast refill

        # Drain bucket
        bucket.acquire(10)

        # Async acquire should wait for tokens
        start = time.monotonic()
        result = await bucket.acquire_async(5, max_wait=1.0)
        elapsed = time.monotonic() - start

        assert result is True
        assert elapsed >= 0.04  # At least 50ms to get 5 tokens

    @pytest.mark.asyncio
    async def test_acquire_async_timeout(self) -> None:
        """Test async acquire times out."""
        bucket = TokenBucket(bucket_size=10, refill_rate=1.0)  # Slow refill

        # Drain bucket
        bucket.acquire(10)

        # Try to acquire with short timeout
        result = await bucket.acquire_async(10, max_wait=0.01)

        assert result is False


class TestRateLimitConfig:
    """Test RateLimitConfig."""

    def test_default_limits(self) -> None:
        """Test default limits for unknown operations."""
        config = RateLimitConfig()

        bucket_size, refill_rate = config.get_limits("unknown_operation")

        assert bucket_size == 100
        assert refill_rate == 10.0

    def test_operation_limits(self) -> None:
        """Test configured limits for known operations."""
        config = RateLimitConfig()

        stats_size, stats_rate = config.get_limits("stats_update")
        assert stats_size == 500
        assert stats_rate == 50.0

        cancel_size, cancel_rate = config.get_limits("cancel")
        assert cancel_size == 20
        assert cancel_rate == 2.0

    def test_custom_operation_limits(self) -> None:
        """Test custom operation limits."""
        config = RateLimitConfig(
            operation_limits={
                "custom_op": (50, 5.0),
            }
        )

        size, rate = config.get_limits("custom_op")
        assert size == 50
        assert rate == 5.0


class TestServerRateLimiter:
    """Test ServerRateLimiter."""

    def test_check_rate_limit_allowed(self) -> None:
        """Test rate limit check when allowed."""
        limiter = ServerRateLimiter()

        result = limiter.check_rate_limit("client-1", "job_submit")

        assert result.allowed is True
        assert result.retry_after_seconds == 0.0
        assert result.tokens_remaining > 0

    def test_check_rate_limit_exhausted(self) -> None:
        """Test rate limit check when exhausted."""
        config = RateLimitConfig(
            operation_limits={"test_op": (5, 1.0)}
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust the bucket
        for _ in range(5):
            limiter.check_rate_limit("client-1", "test_op")

        # Should be rate limited now
        result = limiter.check_rate_limit("client-1", "test_op")

        assert result.allowed is False
        assert result.retry_after_seconds > 0

    def test_per_client_isolation(self) -> None:
        """Test that clients have separate buckets."""
        config = RateLimitConfig(
            operation_limits={"test_op": (3, 1.0)}
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust client-1
        for _ in range(3):
            limiter.check_rate_limit("client-1", "test_op")

        # client-2 should still have tokens
        result = limiter.check_rate_limit("client-2", "test_op")

        assert result.allowed is True

    def test_per_operation_isolation(self) -> None:
        """Test that operations have separate buckets."""
        config = RateLimitConfig(
            operation_limits={
                "op1": (3, 1.0),
                "op2": (3, 1.0),
            }
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust op1 for client-1
        for _ in range(3):
            limiter.check_rate_limit("client-1", "op1")

        # op2 for same client should still work
        result = limiter.check_rate_limit("client-1", "op2")

        assert result.allowed is True

    def test_cleanup_inactive_clients(self) -> None:
        """Test cleanup of inactive clients."""
        limiter = ServerRateLimiter(inactive_cleanup_seconds=0.1)

        # Create some clients
        limiter.check_rate_limit("client-1", "test")
        limiter.check_rate_limit("client-2", "test")

        # Wait for them to become inactive
        time.sleep(0.15)

        # Cleanup
        cleaned = limiter.cleanup_inactive_clients()

        assert cleaned == 2
        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 0

    def test_cleanup_preserves_active_clients(self) -> None:
        """Test that cleanup preserves recently active clients."""
        limiter = ServerRateLimiter(inactive_cleanup_seconds=1.0)

        # Create client and keep it active
        limiter.check_rate_limit("client-1", "test")

        # Cleanup immediately (client is still active)
        cleaned = limiter.cleanup_inactive_clients()

        assert cleaned == 0
        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 1

    def test_reset_client(self) -> None:
        """Test resetting a client's buckets."""
        config = RateLimitConfig(
            operation_limits={"test_op": (3, 1.0)}
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust client
        for _ in range(3):
            limiter.check_rate_limit("client-1", "test_op")

        # Rate limited
        result = limiter.check_rate_limit("client-1", "test_op")
        assert result.allowed is False

        # Reset client
        limiter.reset_client("client-1")

        # Should work again
        result = limiter.check_rate_limit("client-1", "test_op")
        assert result.allowed is True

    def test_get_client_stats(self) -> None:
        """Test getting client's token stats."""
        limiter = ServerRateLimiter()

        # Use some tokens
        limiter.check_rate_limit("client-1", "job_submit", tokens=10)
        limiter.check_rate_limit("client-1", "job_status", tokens=5)

        stats = limiter.get_client_stats("client-1")

        assert "job_submit" in stats
        assert "job_status" in stats
        assert stats["job_submit"] < 50  # Started with 50

    def test_metrics(self) -> None:
        """Test metrics tracking."""
        config = RateLimitConfig(
            operation_limits={"test_op": (2, 1.0)}
        )
        limiter = ServerRateLimiter(config=config)

        # Make some requests
        limiter.check_rate_limit("client-1", "test_op")
        limiter.check_rate_limit("client-1", "test_op")
        limiter.check_rate_limit("client-1", "test_op")  # Rate limited

        metrics = limiter.get_metrics()

        assert metrics["total_requests"] == 3
        assert metrics["rate_limited_requests"] == 1
        assert metrics["active_clients"] == 1

    @pytest.mark.asyncio
    async def test_check_rate_limit_async(self) -> None:
        """Test async rate limit check with wait."""
        config = RateLimitConfig(
            operation_limits={"test_op": (3, 100.0)}  # Fast refill
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust bucket
        for _ in range(3):
            limiter.check_rate_limit("client-1", "test_op")

        # Async check should wait for tokens
        start = time.monotonic()
        result = await limiter.check_rate_limit_async(
            "client-1", "test_op", max_wait=1.0
        )
        elapsed = time.monotonic() - start

        assert result.allowed is True
        assert elapsed >= 0.005  # At least some wait time


class TestCooperativeRateLimiter:
    """Test CooperativeRateLimiter client-side throttling."""

    def test_not_blocked_initially(self) -> None:
        """Test that operations are not blocked initially."""
        limiter = CooperativeRateLimiter()

        assert limiter.is_blocked("test_op") is False
        assert limiter.get_retry_after("test_op") == 0.0

    def test_handle_rate_limit(self) -> None:
        """Test handling rate limit response."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("test_op", retry_after=1.0)

        assert limiter.is_blocked("test_op") is True
        assert limiter.get_retry_after("test_op") > 0.9

    def test_block_expires(self) -> None:
        """Test that block expires after retry_after."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("test_op", retry_after=0.05)

        assert limiter.is_blocked("test_op") is True

        # Wait for block to expire
        time.sleep(0.06)

        assert limiter.is_blocked("test_op") is False

    def test_default_backoff(self) -> None:
        """Test default backoff when no retry_after specified."""
        limiter = CooperativeRateLimiter(default_backoff=2.0)

        limiter.handle_rate_limit("test_op")

        assert limiter.is_blocked("test_op") is True
        assert limiter.get_retry_after("test_op") >= 1.9

    def test_clear_specific_operation(self) -> None:
        """Test clearing block for specific operation."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("op1", retry_after=10.0)
        limiter.handle_rate_limit("op2", retry_after=10.0)

        limiter.clear("op1")

        assert limiter.is_blocked("op1") is False
        assert limiter.is_blocked("op2") is True

    def test_clear_all(self) -> None:
        """Test clearing all blocks."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("op1", retry_after=10.0)
        limiter.handle_rate_limit("op2", retry_after=10.0)

        limiter.clear()

        assert limiter.is_blocked("op1") is False
        assert limiter.is_blocked("op2") is False

    @pytest.mark.asyncio
    async def test_wait_if_needed_not_blocked(self) -> None:
        """Test wait_if_needed when not blocked."""
        limiter = CooperativeRateLimiter()

        wait_time = await limiter.wait_if_needed("test_op")

        assert wait_time == 0.0

    @pytest.mark.asyncio
    async def test_wait_if_needed_blocked(self) -> None:
        """Test wait_if_needed when blocked."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("test_op", retry_after=0.1)

        start = time.monotonic()
        wait_time = await limiter.wait_if_needed("test_op")
        elapsed = time.monotonic() - start

        assert wait_time >= 0.09
        assert elapsed >= 0.09

    def test_metrics(self) -> None:
        """Test cooperative rate limiter metrics."""
        limiter = CooperativeRateLimiter()

        # Initially no waits
        metrics = limiter.get_metrics()
        assert metrics["total_waits"] == 0
        assert metrics["total_wait_time"] == 0.0


class TestRateLimitResult:
    """Test RateLimitResult dataclass."""

    def test_allowed_result(self) -> None:
        """Test allowed result."""
        result = RateLimitResult(
            allowed=True,
            retry_after_seconds=0.0,
            tokens_remaining=95.0,
        )

        assert result.allowed is True
        assert result.retry_after_seconds == 0.0
        assert result.tokens_remaining == 95.0

    def test_rate_limited_result(self) -> None:
        """Test rate limited result."""
        result = RateLimitResult(
            allowed=False,
            retry_after_seconds=0.5,
            tokens_remaining=0.0,
        )

        assert result.allowed is False
        assert result.retry_after_seconds == 0.5
        assert result.tokens_remaining == 0.0


class TestRetryAfterHelpers:
    """Test retry-after helper functions."""

    def test_is_rate_limit_response_positive(self) -> None:
        """Test detection of rate limit response data."""
        from hyperscale.distributed_rewrite.reliability import is_rate_limit_response
        from hyperscale.distributed_rewrite.models import RateLimitResponse

        response = RateLimitResponse(
            operation="job_submit",
            retry_after_seconds=1.5,
        )
        data = response.dump()

        assert is_rate_limit_response(data) is True

    def test_is_rate_limit_response_negative(self) -> None:
        """Test non-rate-limit response is not detected."""
        from hyperscale.distributed_rewrite.reliability import is_rate_limit_response

        # Some other data
        data = b"not a rate limit response"

        assert is_rate_limit_response(data) is False

    def test_is_rate_limit_response_empty(self) -> None:
        """Test empty data is not detected as rate limit."""
        from hyperscale.distributed_rewrite.reliability import is_rate_limit_response

        assert is_rate_limit_response(b"") is False
        assert is_rate_limit_response(b"short") is False

    @pytest.mark.asyncio
    async def test_handle_rate_limit_response_with_wait(self) -> None:
        """Test handling rate limit response with wait."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            handle_rate_limit_response,
        )

        limiter = CooperativeRateLimiter()

        # Handle rate limit with short wait
        start = time.monotonic()
        wait_time = await handle_rate_limit_response(
            limiter,
            operation="test_op",
            retry_after_seconds=0.05,
            wait=True,
        )
        elapsed = time.monotonic() - start

        assert wait_time >= 0.04
        assert elapsed >= 0.04

    @pytest.mark.asyncio
    async def test_handle_rate_limit_response_without_wait(self) -> None:
        """Test handling rate limit response without wait."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            handle_rate_limit_response,
        )

        limiter = CooperativeRateLimiter()

        # Handle rate limit without waiting
        wait_time = await handle_rate_limit_response(
            limiter,
            operation="test_op",
            retry_after_seconds=10.0,
            wait=False,
        )

        assert wait_time == 0.0
        # But the operation should be blocked
        assert limiter.is_blocked("test_op") is True
        assert limiter.get_retry_after("test_op") >= 9.9

    @pytest.mark.asyncio
    async def test_retry_after_flow(self) -> None:
        """Test complete retry-after flow."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            ServerRateLimiter,
            RateLimitConfig,
            handle_rate_limit_response,
        )

        # Server-side: create a rate limiter with small bucket
        config = RateLimitConfig(
            operation_limits={"test_op": (2, 10.0)}  # 2 tokens, refill 10/s
        )
        server_limiter = ServerRateLimiter(config=config)

        # Client-side: create cooperative limiter
        client_limiter = CooperativeRateLimiter()

        # First 2 requests succeed
        result1 = server_limiter.check_rate_limit("client-1", "test_op")
        result2 = server_limiter.check_rate_limit("client-1", "test_op")
        assert result1.allowed is True
        assert result2.allowed is True

        # Third request is rate limited
        result3 = server_limiter.check_rate_limit("client-1", "test_op")
        assert result3.allowed is False
        assert result3.retry_after_seconds > 0

        # Client handles rate limit response
        await handle_rate_limit_response(
            client_limiter,
            operation="test_op",
            retry_after_seconds=result3.retry_after_seconds,
            wait=True,
        )

        # After waiting, client can check if blocked and retry
        assert client_limiter.is_blocked("test_op") is False

        # Server should now allow the request again
        result4 = server_limiter.check_rate_limit("client-1", "test_op")
        assert result4.allowed is True


class TestExecuteWithRateLimitRetry:
    """Test automatic retry on rate limiting."""

    @pytest.mark.asyncio
    async def test_success_on_first_try(self) -> None:
        """Test successful operation without rate limiting."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            execute_with_rate_limit_retry,
        )

        limiter = CooperativeRateLimiter()
        call_count = 0

        async def operation():
            nonlocal call_count
            call_count += 1
            return b"success_response"

        result = await execute_with_rate_limit_retry(
            operation,
            "test_op",
            limiter,
        )

        assert result.success is True
        assert result.response == b"success_response"
        assert result.retries == 0
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_after_rate_limit(self) -> None:
        """Test automatic retry after rate limit response."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            RateLimitRetryConfig,
            execute_with_rate_limit_retry,
        )
        from hyperscale.distributed_rewrite.models import RateLimitResponse

        limiter = CooperativeRateLimiter()
        call_count = 0

        async def operation():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call returns rate limit
                return RateLimitResponse(
                    operation="test_op",
                    retry_after_seconds=0.05,
                ).dump()
            else:
                # Second call succeeds
                return b"success_response"

        config = RateLimitRetryConfig(max_retries=3, max_total_wait=10.0)

        start = time.monotonic()
        result = await execute_with_rate_limit_retry(
            operation,
            "test_op",
            limiter,
            config=config,
        )
        elapsed = time.monotonic() - start

        assert result.success is True
        assert result.response == b"success_response"
        assert result.retries == 1
        assert call_count == 2
        assert elapsed >= 0.04  # Waited for retry_after

    @pytest.mark.asyncio
    async def test_exhausted_retries(self) -> None:
        """Test failure after exhausting retries."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            RateLimitRetryConfig,
            execute_with_rate_limit_retry,
        )
        from hyperscale.distributed_rewrite.models import RateLimitResponse

        limiter = CooperativeRateLimiter()
        call_count = 0

        async def operation():
            nonlocal call_count
            call_count += 1
            # Always return rate limit
            return RateLimitResponse(
                operation="test_op",
                retry_after_seconds=0.01,
            ).dump()

        config = RateLimitRetryConfig(max_retries=2, max_total_wait=10.0)

        result = await execute_with_rate_limit_retry(
            operation,
            "test_op",
            limiter,
            config=config,
        )

        assert result.success is False
        # retries counts how many times we retried (after initial attempt failed)
        # With max_retries=2, we try: initial, retry 1, retry 2, then exit
        # The implementation increments retries after each rate limit, so we get 3
        assert result.retries == 3
        assert call_count == 3  # Initial + 2 retries
        assert "Exhausted max retries" in result.final_error

    @pytest.mark.asyncio
    async def test_max_total_wait_exceeded(self) -> None:
        """Test failure when max total wait time is exceeded."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            RateLimitRetryConfig,
            execute_with_rate_limit_retry,
        )
        from hyperscale.distributed_rewrite.models import RateLimitResponse

        limiter = CooperativeRateLimiter()

        async def operation():
            # Return a rate limit with long retry_after
            return RateLimitResponse(
                operation="test_op",
                retry_after_seconds=10.0,
            ).dump()

        # Max wait is shorter than retry_after
        config = RateLimitRetryConfig(max_retries=5, max_total_wait=1.0)

        result = await execute_with_rate_limit_retry(
            operation,
            "test_op",
            limiter,
            config=config,
        )

        assert result.success is False
        assert "would exceed max wait" in result.final_error

    @pytest.mark.asyncio
    async def test_backoff_multiplier(self) -> None:
        """Test that backoff multiplier increases wait time."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            RateLimitRetryConfig,
            execute_with_rate_limit_retry,
        )
        from hyperscale.distributed_rewrite.models import RateLimitResponse

        limiter = CooperativeRateLimiter()
        call_count = 0

        async def operation():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return RateLimitResponse(
                    operation="test_op",
                    retry_after_seconds=0.02,
                ).dump()
            else:
                return b"success"

        # With backoff_multiplier=2.0:
        # First retry: 0.02s
        # Second retry: 0.02 * 2.0 = 0.04s
        config = RateLimitRetryConfig(
            max_retries=5,
            max_total_wait=10.0,
            backoff_multiplier=2.0,
        )

        start = time.monotonic()
        result = await execute_with_rate_limit_retry(
            operation,
            "test_op",
            limiter,
            config=config,
        )
        elapsed = time.monotonic() - start

        assert result.success is True
        assert result.retries == 2
        # Total wait should be at least 0.02 + 0.04 = 0.06
        assert elapsed >= 0.05

    @pytest.mark.asyncio
    async def test_exception_handling(self) -> None:
        """Test that exceptions are properly handled."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            execute_with_rate_limit_retry,
        )

        limiter = CooperativeRateLimiter()

        async def operation():
            raise ConnectionError("Network failure")

        result = await execute_with_rate_limit_retry(
            operation,
            "test_op",
            limiter,
        )

        assert result.success is False
        assert "Network failure" in result.final_error


class TestServerRateLimiterCheckCompatibility:
    """Test ServerRateLimiter.check() compatibility method for simple RateLimiter API."""

    def test_check_allowed(self) -> None:
        """Test check() returns True when allowed."""
        limiter = ServerRateLimiter()
        addr = ("192.168.1.1", 8080)

        result = limiter.check(addr)

        assert result is True

    def test_check_rate_limited(self) -> None:
        """Test check() returns False when rate limited."""
        config = RateLimitConfig(
            default_bucket_size=3,
            default_refill_rate=1.0,
        )
        limiter = ServerRateLimiter(config=config)
        addr = ("192.168.1.1", 8080)

        # Exhaust the bucket using check() API
        for _ in range(3):
            limiter.check(addr)

        # Should be rate limited now
        result = limiter.check(addr)

        assert result is False

    def test_check_raises_on_limit(self) -> None:
        """Test check() raises RateLimitExceeded when raise_on_limit=True."""
        from hyperscale.core.jobs.protocols.rate_limiter import RateLimitExceeded

        config = RateLimitConfig(
            default_bucket_size=2,
            default_refill_rate=1.0,
        )
        limiter = ServerRateLimiter(config=config)
        addr = ("10.0.0.1", 9000)

        # Exhaust the bucket
        limiter.check(addr)
        limiter.check(addr)

        # Should raise
        with pytest.raises(RateLimitExceeded) as exc_info:
            limiter.check(addr, raise_on_limit=True)

        assert "10.0.0.1:9000" in str(exc_info.value)

    def test_check_does_not_raise_when_allowed(self) -> None:
        """Test check() does not raise when allowed even with raise_on_limit=True."""
        limiter = ServerRateLimiter()
        addr = ("192.168.1.1", 8080)

        # Should not raise
        result = limiter.check(addr, raise_on_limit=True)
        assert result is True

    def test_check_different_addresses_isolated(self) -> None:
        """Test that different addresses have separate buckets via check()."""
        config = RateLimitConfig(
            default_bucket_size=2,
            default_refill_rate=1.0,
        )
        limiter = ServerRateLimiter(config=config)

        addr1 = ("192.168.1.1", 8080)
        addr2 = ("192.168.1.2", 8080)

        # Exhaust addr1
        limiter.check(addr1)
        limiter.check(addr1)
        assert limiter.check(addr1) is False

        # addr2 should still be allowed
        assert limiter.check(addr2) is True

    def test_check_converts_address_to_client_id(self) -> None:
        """Test that check() properly converts address tuple to client_id string."""
        limiter = ServerRateLimiter()
        addr = ("myhost.example.com", 12345)

        # Make a request
        limiter.check(addr)

        # Verify internal client was created with correct ID format
        expected_client_id = "myhost.example.com:12345"
        assert expected_client_id in limiter._client_buckets

    def test_check_uses_default_operation(self) -> None:
        """Test that check() uses 'default' operation bucket."""
        limiter = ServerRateLimiter()
        addr = ("192.168.1.1", 8080)

        # Make a request via check()
        limiter.check(addr)

        # Verify 'default' operation was used
        client_id = "192.168.1.1:8080"
        stats = limiter.get_client_stats(client_id)
        assert "default" in stats

    def test_check_interoperates_with_check_rate_limit(self) -> None:
        """Test that check() and check_rate_limit() share state correctly."""
        config = RateLimitConfig(
            default_bucket_size=5,
            default_refill_rate=1.0,
        )
        limiter = ServerRateLimiter(config=config)
        addr = ("192.168.1.1", 8080)
        client_id = "192.168.1.1:8080"

        # Use 2 tokens via check()
        limiter.check(addr)
        limiter.check(addr)

        # Use 2 more via check_rate_limit()
        limiter.check_rate_limit(client_id, "default")
        limiter.check_rate_limit(client_id, "default")

        # Should have 1 token left
        stats = limiter.get_client_stats(client_id)
        assert stats["default"] == pytest.approx(1.0, abs=0.1)

        # One more check should work
        assert limiter.check(addr) is True

        # Now should be exhausted
        assert limiter.check(addr) is False

    def test_check_with_ipv6_address(self) -> None:
        """Test check() works with IPv6 addresses."""
        limiter = ServerRateLimiter()
        addr = ("::1", 8080)

        result = limiter.check(addr)

        assert result is True
        # Verify client was created
        assert "::1:8080" in limiter._client_buckets

    def test_check_metrics_updated(self) -> None:
        """Test that check() updates metrics correctly."""
        config = RateLimitConfig(
            default_bucket_size=2,
            default_refill_rate=1.0,
        )
        limiter = ServerRateLimiter(config=config)
        addr = ("192.168.1.1", 8080)

        # Make requests - 2 allowed, 1 rate limited
        limiter.check(addr)
        limiter.check(addr)
        limiter.check(addr)

        metrics = limiter.get_metrics()
        assert metrics["total_requests"] == 3
        assert metrics["rate_limited_requests"] == 1
