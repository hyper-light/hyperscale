"""
Failure path tests for Rate Limiting (AD-24).

Tests failure scenarios and edge cases:
- Token bucket edge cases (zero tokens, negative values)
- Server rate limiter cleanup and memory management
- Cooperative rate limiter concurrent operations
- Rate limit retry exhaustion and timeout
- Recovery from rate limiting
- Edge cases in configuration
"""

import asyncio
import pytest
import time

from hyperscale.distributed_rewrite.reliability import (
    CooperativeRateLimiter,
    RateLimitConfig,
    RateLimitResult,
    ServerRateLimiter,
    TokenBucket,
)
from hyperscale.distributed_rewrite.reliability.rate_limiting import (
    RateLimitRetryConfig,
    RateLimitRetryResult,
    execute_with_rate_limit_retry,
    is_rate_limit_response,
)


class TestTokenBucketEdgeCases:
    """Test edge cases in TokenBucket."""

    def test_acquire_zero_tokens(self) -> None:
        """Test acquiring zero tokens."""
        bucket = TokenBucket(bucket_size=10, refill_rate=1.0)

        # Zero tokens should succeed
        result = bucket.acquire(0)
        assert result is True
        # Should not change token count significantly
        assert bucket.available_tokens == pytest.approx(10.0, abs=0.1)

    def test_acquire_more_than_bucket_size(self) -> None:
        """Test acquiring more tokens than bucket size."""
        bucket = TokenBucket(bucket_size=10, refill_rate=1.0)

        # Requesting more than bucket can ever hold
        result = bucket.acquire(100)
        assert result is False

    def test_bucket_with_zero_size(self) -> None:
        """Test bucket with zero size."""
        bucket = TokenBucket(bucket_size=0, refill_rate=1.0)

        # Should start with 0 tokens
        assert bucket.available_tokens == 0.0

        # Any acquire should fail
        result = bucket.acquire(1)
        assert result is False

    def test_bucket_with_zero_refill_rate(self) -> None:
        """Test bucket with zero refill rate."""
        bucket = TokenBucket(bucket_size=10, refill_rate=0.0)

        # Drain bucket
        bucket.acquire(10)

        # Wait a bit
        time.sleep(0.1)

        # Should never refill
        assert bucket.available_tokens == pytest.approx(0.0, abs=0.01)

    def test_bucket_with_very_high_refill_rate(self) -> None:
        """Test bucket with very high refill rate."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10000.0)  # 10k/s

        # Drain bucket
        bucket.acquire(100)

        # Wait tiny bit
        time.sleep(0.01)

        # Should refill to cap
        assert bucket.available_tokens == pytest.approx(100.0, abs=1.0)

    def test_try_acquire_returns_correct_wait_time(self) -> None:
        """Test try_acquire wait time calculation."""
        bucket = TokenBucket(bucket_size=10, refill_rate=10.0)  # 10/s

        # Drain completely
        bucket.acquire(10)

        # Need 10 tokens, refill is 10/s, so 1 second wait
        acquired, wait_time = bucket.try_acquire(10)
        assert acquired is False
        assert wait_time == pytest.approx(1.0, rel=0.1)

    def test_try_acquire_partial_wait_time(self) -> None:
        """Test wait time when partially empty."""
        bucket = TokenBucket(bucket_size=10, refill_rate=10.0)

        # Use 5 tokens
        bucket.acquire(5)

        # Need 8 tokens, have ~5, need 3 more at 10/s = 0.3s
        acquired, wait_time = bucket.try_acquire(8)
        assert acquired is False
        assert wait_time == pytest.approx(0.3, rel=0.2)

    @pytest.mark.asyncio
    async def test_acquire_async_with_zero_wait(self) -> None:
        """Test async acquire with zero max_wait."""
        bucket = TokenBucket(bucket_size=10, refill_rate=1.0)
        bucket.acquire(10)

        # Zero max_wait should fail immediately
        result = await bucket.acquire_async(5, max_wait=0.0)
        assert result is False

    @pytest.mark.asyncio
    async def test_acquire_async_race_condition(self) -> None:
        """Test concurrent async acquire attempts."""
        bucket = TokenBucket(bucket_size=10, refill_rate=100.0)  # Fast refill

        # Drain bucket
        bucket.acquire(10)

        # Try multiple concurrent acquires
        results = await asyncio.gather(*[
            bucket.acquire_async(5, max_wait=1.0) for _ in range(5)
        ])

        # Some should succeed, some may fail depending on timing
        success_count = sum(1 for r in results if r)
        # With 100 tokens/s refill, should have time for at least 2 acquires
        assert success_count >= 2

    def test_reset_during_usage(self) -> None:
        """Test reset during active usage."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        # Use some tokens
        bucket.acquire(50)
        assert bucket.available_tokens == pytest.approx(50.0, abs=1.0)

        # Reset
        bucket.reset()
        assert bucket.available_tokens == pytest.approx(100.0, abs=0.1)


class TestServerRateLimiterFailurePaths:
    """Test failure paths in ServerRateLimiter."""

    def test_unknown_client_creates_bucket(self) -> None:
        """Test that unknown client gets new bucket."""
        limiter = ServerRateLimiter()

        result = limiter.check_rate_limit("unknown-client", "job_submit")

        # Should succeed (new bucket starts full)
        assert result.allowed is True

    def test_many_clients_memory_growth(self) -> None:
        """Test memory behavior with many clients."""
        limiter = ServerRateLimiter(inactive_cleanup_seconds=0.1)

        # Create many clients
        for i in range(1000):
            limiter.check_rate_limit(f"client-{i}", "job_submit")

        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 1000

        # Wait for cleanup threshold
        time.sleep(0.2)

        # Cleanup should remove all
        cleaned = limiter.cleanup_inactive_clients()
        assert cleaned == 1000

        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 0

    def test_cleanup_preserves_active_clients(self) -> None:
        """Test cleanup preserves recently active clients."""
        limiter = ServerRateLimiter(inactive_cleanup_seconds=1.0)

        # Create two clients
        limiter.check_rate_limit("active-client", "job_submit")
        limiter.check_rate_limit("inactive-client", "job_submit")

        # Wait a bit but less than cleanup threshold
        time.sleep(0.5)

        # Touch active client
        limiter.check_rate_limit("active-client", "heartbeat")

        # Wait past threshold for original activity
        time.sleep(0.6)

        # Cleanup
        cleaned = limiter.cleanup_inactive_clients()

        # Only inactive should be cleaned
        assert cleaned == 1
        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 1

    def test_rapid_requests_from_single_client(self) -> None:
        """Test rapid requests exhaust tokens."""
        config = RateLimitConfig(
            operation_limits={"test": (10, 1.0)}  # 10 tokens, 1/s refill
        )
        limiter = ServerRateLimiter(config=config)

        # Rapid requests
        allowed_count = 0
        for _ in range(20):
            result = limiter.check_rate_limit("rapid-client", "test")
            if result.allowed:
                allowed_count += 1

        # Should allow first 10, deny rest
        assert allowed_count == 10

        metrics = limiter.get_metrics()
        assert metrics["rate_limited_requests"] == 10

    def test_reset_client_restores_tokens(self) -> None:
        """Test reset_client restores all buckets."""
        limiter = ServerRateLimiter()

        # Exhaust multiple operations
        for _ in range(100):
            limiter.check_rate_limit("reset-client", "job_submit")
            limiter.check_rate_limit("reset-client", "stats_update")

        # Verify exhausted
        result = limiter.check_rate_limit("reset-client", "job_submit")
        # Most likely rate limited now
        stats = limiter.get_client_stats("reset-client")
        job_tokens_before = stats.get("job_submit", 0)

        # Reset
        limiter.reset_client("reset-client")

        stats = limiter.get_client_stats("reset-client")
        # Should be full now
        assert stats["job_submit"] == pytest.approx(50.0, abs=1.0)  # job_submit bucket size

    def test_reset_nonexistent_client(self) -> None:
        """Test reset for client that doesn't exist."""
        limiter = ServerRateLimiter()

        # Should not raise
        limiter.reset_client("nonexistent")

    def test_get_stats_nonexistent_client(self) -> None:
        """Test getting stats for nonexistent client."""
        limiter = ServerRateLimiter()

        stats = limiter.get_client_stats("nonexistent")
        assert stats == {}

    @pytest.mark.asyncio
    async def test_async_rate_limit_with_wait(self) -> None:
        """Test async rate limit with waiting."""
        config = RateLimitConfig(
            operation_limits={"test": (10, 100.0)}  # Fast refill
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust tokens
        for _ in range(10):
            limiter.check_rate_limit("async-client", "test")

        # Async check with wait
        result = await limiter.check_rate_limit_async(
            "async-client", "test", max_wait=0.2
        )

        # Should succeed after waiting for refill
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_async_rate_limit_timeout(self) -> None:
        """Test async rate limit timing out."""
        config = RateLimitConfig(
            operation_limits={"test": (10, 1.0)}  # Slow refill
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust tokens
        for _ in range(10):
            limiter.check_rate_limit("timeout-client", "test")

        # Async check with short wait
        result = await limiter.check_rate_limit_async(
            "timeout-client", "test", max_wait=0.01
        )

        # Should fail
        assert result.allowed is False


class TestCooperativeRateLimiterFailurePaths:
    """Test failure paths in CooperativeRateLimiter."""

    @pytest.mark.asyncio
    async def test_wait_when_not_blocked(self) -> None:
        """Test wait returns immediately when not blocked."""
        limiter = CooperativeRateLimiter()

        start = time.monotonic()
        waited = await limiter.wait_if_needed("unblocked_op")
        elapsed = time.monotonic() - start

        assert waited == 0.0
        assert elapsed < 0.01

    @pytest.mark.asyncio
    async def test_handle_rate_limit_with_zero(self) -> None:
        """Test handling rate limit with zero retry_after."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("zero_op", retry_after=0.0)

        # Should not be blocked
        assert limiter.is_blocked("zero_op") is False

    @pytest.mark.asyncio
    async def test_handle_rate_limit_with_negative(self) -> None:
        """Test handling rate limit with negative retry_after."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("negative_op", retry_after=-1.0)

        # Should not be blocked (negative time is in past)
        assert limiter.is_blocked("negative_op") is False

    @pytest.mark.asyncio
    async def test_concurrent_wait_same_operation(self) -> None:
        """Test concurrent waits on same operation."""
        limiter = CooperativeRateLimiter()

        # Block operation
        limiter.handle_rate_limit("concurrent_op", retry_after=0.1)

        # Multiple concurrent waits
        start = time.monotonic()
        wait_times = await asyncio.gather(*[
            limiter.wait_if_needed("concurrent_op") for _ in range(5)
        ])
        elapsed = time.monotonic() - start

        # All should have waited, but not serially
        # Total elapsed should be ~0.1s, not 0.5s
        assert elapsed < 0.2
        assert all(w >= 0 for w in wait_times)

    def test_get_retry_after_not_blocked(self) -> None:
        """Test get_retry_after for unblocked operation."""
        limiter = CooperativeRateLimiter()

        remaining = limiter.get_retry_after("not_blocked")
        assert remaining == 0.0

    def test_clear_specific_operation(self) -> None:
        """Test clearing specific operation."""
        limiter = CooperativeRateLimiter()

        # Block multiple operations
        limiter.handle_rate_limit("op1", retry_after=10.0)
        limiter.handle_rate_limit("op2", retry_after=10.0)

        assert limiter.is_blocked("op1") is True
        assert limiter.is_blocked("op2") is True

        # Clear only op1
        limiter.clear("op1")

        assert limiter.is_blocked("op1") is False
        assert limiter.is_blocked("op2") is True

    def test_clear_all_operations(self) -> None:
        """Test clearing all operations."""
        limiter = CooperativeRateLimiter()

        # Block multiple operations
        limiter.handle_rate_limit("op1", retry_after=10.0)
        limiter.handle_rate_limit("op2", retry_after=10.0)
        limiter.handle_rate_limit("op3", retry_after=10.0)

        # Clear all
        limiter.clear()

        assert limiter.is_blocked("op1") is False
        assert limiter.is_blocked("op2") is False
        assert limiter.is_blocked("op3") is False

    def test_handle_none_retry_after_uses_default(self) -> None:
        """Test that None retry_after uses default backoff."""
        limiter = CooperativeRateLimiter(default_backoff=2.5)

        limiter.handle_rate_limit("default_op", retry_after=None)

        # Should be blocked for ~2.5 seconds
        remaining = limiter.get_retry_after("default_op")
        assert remaining == pytest.approx(2.5, rel=0.1)


class TestRateLimitRetryFailurePaths:
    """Test failure paths in rate limit retry mechanism."""

    @pytest.mark.asyncio
    async def test_exhausted_retries(self) -> None:
        """Test behavior when retries are exhausted."""
        limiter = CooperativeRateLimiter()
        config = RateLimitRetryConfig(max_retries=2)

        call_count = 0

        async def always_rate_limited():
            nonlocal call_count
            call_count += 1
            # Return bytes that look like rate limit response
            return b'{"operation": "test", "retry_after_seconds": 0.01}'

        def always_rate_limit_check(data):
            return True

        result = await execute_with_rate_limit_retry(
            always_rate_limited,
            "test_op",
            limiter,
            config,
            response_parser=always_rate_limit_check,
        )

        assert result.success is False
        assert result.retries == 3  # Initial + 2 retries
        assert "Exhausted" in result.final_error or "max retries" in result.final_error.lower()

    @pytest.mark.asyncio
    async def test_max_total_wait_exceeded(self) -> None:
        """Test behavior when max total wait time is exceeded."""
        limiter = CooperativeRateLimiter()
        config = RateLimitRetryConfig(max_retries=10, max_total_wait=0.1)

        async def long_rate_limit():
            # Return rate limit with long retry_after
            return b'{"operation": "test", "retry_after_seconds": 1.0}'

        def rate_limit_check(data):
            return True

        result = await execute_with_rate_limit_retry(
            long_rate_limit,
            "test_op",
            limiter,
            config,
            response_parser=rate_limit_check,
        )

        assert result.success is False
        assert "max" in result.final_error.lower() and "wait" in result.final_error.lower()

    @pytest.mark.asyncio
    async def test_operation_exception(self) -> None:
        """Test handling of operation exception."""
        limiter = CooperativeRateLimiter()

        async def failing_operation():
            raise ConnectionError("Network failure")

        result = await execute_with_rate_limit_retry(
            failing_operation,
            "test_op",
            limiter,
        )

        assert result.success is False
        assert "Network failure" in result.final_error

    @pytest.mark.asyncio
    async def test_successful_operation_no_retries(self) -> None:
        """Test successful operation without rate limiting."""
        limiter = CooperativeRateLimiter()

        async def successful_operation():
            return b'{"status": "ok"}'

        def not_rate_limited(data):
            return False

        result = await execute_with_rate_limit_retry(
            successful_operation,
            "test_op",
            limiter,
            response_parser=not_rate_limited,
        )

        assert result.success is True
        assert result.retries == 0
        assert result.total_wait_time == 0.0

    @pytest.mark.asyncio
    async def test_initially_blocked_operation(self) -> None:
        """Test operation that is initially blocked."""
        limiter = CooperativeRateLimiter()
        limiter.handle_rate_limit("blocked_op", retry_after=0.05)

        async def quick_operation():
            return b'{"status": "ok"}'

        def not_rate_limited(data):
            return False

        start = time.monotonic()
        result = await execute_with_rate_limit_retry(
            quick_operation,
            "blocked_op",
            limiter,
            response_parser=not_rate_limited,
        )
        elapsed = time.monotonic() - start

        assert result.success is True
        assert elapsed >= 0.05  # Should have waited


class TestRateLimitResponseDetection:
    """Test rate limit response detection."""

    def test_is_rate_limit_response_valid(self) -> None:
        """Test detection of valid rate limit response."""
        data = b'{"operation": "test", "retry_after_seconds": 1.0, "allowed": false}'

        result = is_rate_limit_response(data)
        assert result is True

    def test_is_rate_limit_response_too_short(self) -> None:
        """Test rejection of too-short data."""
        data = b'short'

        result = is_rate_limit_response(data)
        assert result is False

    def test_is_rate_limit_response_empty(self) -> None:
        """Test rejection of empty data."""
        data = b''

        result = is_rate_limit_response(data)
        assert result is False

    def test_is_rate_limit_response_non_rate_limit(self) -> None:
        """Test rejection of non-rate-limit response."""
        data = b'{"job_id": "123", "status": "completed", "some_other_field": true}'

        result = is_rate_limit_response(data)
        assert result is False


class TestRateLimitConfigEdgeCases:
    """Test edge cases in RateLimitConfig."""

    def test_custom_default_limits(self) -> None:
        """Test custom default limits."""
        config = RateLimitConfig(
            default_bucket_size=50,
            default_refill_rate=5.0,
        )

        size, rate = config.get_limits("unknown_operation")
        assert size == 50
        assert rate == 5.0

    def test_override_standard_operation(self) -> None:
        """Test overriding standard operation limits."""
        config = RateLimitConfig(
            operation_limits={
                "job_submit": (1000, 100.0),  # Override default
            }
        )

        size, rate = config.get_limits("job_submit")
        assert size == 1000
        assert rate == 100.0

    def test_empty_operation_limits(self) -> None:
        """Test with empty operation limits."""
        config = RateLimitConfig(operation_limits={})

        size, rate = config.get_limits("any_operation")
        assert size == 100  # default
        assert rate == 10.0  # default


class TestRateLimitRecovery:
    """Test recovery scenarios from rate limiting."""

    @pytest.mark.asyncio
    async def test_recovery_after_token_refill(self) -> None:
        """Test recovery after tokens refill."""
        config = RateLimitConfig(
            operation_limits={"test": (10, 100.0)}  # Fast refill
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust tokens
        for _ in range(10):
            limiter.check_rate_limit("recovery-client", "test")

        # Verify exhausted
        result = limiter.check_rate_limit("recovery-client", "test")
        assert result.allowed is False

        # Wait for refill
        await asyncio.sleep(0.15)

        # Should recover
        result = limiter.check_rate_limit("recovery-client", "test")
        assert result.allowed is True

    def test_metrics_reset(self) -> None:
        """Test metrics reset clears counters."""
        limiter = ServerRateLimiter()

        # Generate some activity
        for i in range(100):
            limiter.check_rate_limit(f"client-{i}", "job_submit")

        metrics_before = limiter.get_metrics()
        assert metrics_before["total_requests"] == 100

        limiter.reset_metrics()

        metrics_after = limiter.get_metrics()
        assert metrics_after["total_requests"] == 0
        assert metrics_after["rate_limited_requests"] == 0
        # Note: clients_cleaned is not reset, active_clients persists

    @pytest.mark.asyncio
    async def test_cooperative_limiter_recovery_after_block(self) -> None:
        """Test cooperative limiter unblocks after time."""
        limiter = CooperativeRateLimiter()

        # Block for short time
        limiter.handle_rate_limit("recover_op", retry_after=0.1)

        assert limiter.is_blocked("recover_op") is True

        # Wait
        await asyncio.sleep(0.15)

        assert limiter.is_blocked("recover_op") is False

    @pytest.mark.asyncio
    async def test_multiple_operations_independent(self) -> None:
        """Test that rate limits on different operations are independent."""
        limiter = CooperativeRateLimiter()

        # Block one operation
        limiter.handle_rate_limit("blocked_op", retry_after=10.0)

        # Other operation should not be blocked
        assert limiter.is_blocked("blocked_op") is True
        assert limiter.is_blocked("other_op") is False

        # Wait on other operation should be instant
        waited = await limiter.wait_if_needed("other_op")
        assert waited == 0.0
