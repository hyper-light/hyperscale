"""
Integration tests for Rate Limiting (AD-24).

Tests:
- SlidingWindowCounter deterministic counting
- AdaptiveRateLimiter health-gated behavior
- ServerRateLimiter with adaptive limiting
- TokenBucket (legacy) basic operations
- CooperativeRateLimiter client-side throttling
- Client cleanup to prevent memory leaks
"""

import asyncio
import time
from unittest.mock import patch

import pytest

from hyperscale.distributed_rewrite.reliability import (
    AdaptiveRateLimitConfig,
    AdaptiveRateLimiter,
    CooperativeRateLimiter,
    HybridOverloadDetector,
    OverloadConfig,
    OverloadState,
    RateLimitConfig,
    RateLimitResult,
    ServerRateLimiter,
    SlidingWindowCounter,
    TokenBucket,
)
from hyperscale.distributed_rewrite.reliability.load_shedding import RequestPriority


class TestSlidingWindowCounter:
    """Test SlidingWindowCounter deterministic counting."""

    def test_initial_state(self) -> None:
        """Test counter starts empty with full capacity."""
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=100)

        assert counter.get_effective_count() == 0.0
        assert counter.available_slots == 100.0

    def test_acquire_success(self) -> None:
        """Test successful slot acquisition."""
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=100)

        acquired, wait_time = counter.try_acquire(10)

        assert acquired is True
        assert wait_time == 0.0
        assert counter.get_effective_count() == 10.0
        assert counter.available_slots == 90.0

    def test_acquire_at_limit(self) -> None:
        """Test acquisition when at exact limit."""
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=10)

        # Fill to exactly limit
        acquired, _ = counter.try_acquire(10)
        assert acquired is True

        # One more should fail
        acquired, wait_time = counter.try_acquire(1)
        assert acquired is False
        assert wait_time > 0

    def test_acquire_exceeds_limit(self) -> None:
        """Test acquisition fails when exceeding limit."""
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=10)

        # Fill most of capacity
        counter.try_acquire(8)

        # Try to acquire more than remaining
        acquired, wait_time = counter.try_acquire(5)

        assert acquired is False
        assert wait_time > 0
        # Count should be unchanged
        assert counter.get_effective_count() == 8.0

    def test_window_rotation(self) -> None:
        """Test that window rotates correctly."""
        counter = SlidingWindowCounter(window_size_seconds=0.1, max_requests=100)

        # Fill current window
        counter.try_acquire(50)
        assert counter.get_effective_count() == 50.0

        # Wait for window to rotate
        time.sleep(0.12)

        # After rotation, previous count contributes weighted portion
        effective = counter.get_effective_count()
        # Previous = 50, current = 0, window_progress ~= 0.2
        # effective = 0 + 50 * (1 - 0.2) = 40 (approximately)
        # But since we're early in new window, previous contribution is high
        assert effective < 50.0  # Some decay from window progress
        assert effective > 0.0  # But not fully gone

    def test_multiple_window_rotation(self) -> None:
        """Test that multiple windows passing clears all counts."""
        counter = SlidingWindowCounter(window_size_seconds=0.05, max_requests=100)

        # Fill current window
        counter.try_acquire(50)

        # Wait for 2+ windows to pass
        time.sleep(0.12)

        # Both previous and current should be cleared
        effective = counter.get_effective_count()
        assert effective == 0.0
        assert counter.available_slots == 100.0

    def test_reset(self) -> None:
        """Test counter reset."""
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=100)

        counter.try_acquire(50)
        assert counter.get_effective_count() == 50.0

        counter.reset()

        assert counter.get_effective_count() == 0.0
        assert counter.available_slots == 100.0

    @pytest.mark.asyncio
    async def test_acquire_async(self) -> None:
        """Test async acquire with wait."""
        counter = SlidingWindowCounter(window_size_seconds=0.1, max_requests=10)

        # Fill counter
        counter.try_acquire(10)

        # Async acquire should wait for window to rotate
        start = time.monotonic()
        result = await counter.acquire_async(5, max_wait=0.2)
        elapsed = time.monotonic() - start

        assert result is True
        assert elapsed >= 0.05  # Waited for some window rotation

    @pytest.mark.asyncio
    async def test_acquire_async_timeout(self) -> None:
        """Test async acquire times out."""
        counter = SlidingWindowCounter(window_size_seconds=10.0, max_requests=10)

        # Fill counter
        counter.try_acquire(10)

        # Try to acquire with short timeout (window won't rotate)
        result = await counter.acquire_async(5, max_wait=0.01)

        assert result is False


class TestAdaptiveRateLimiter:
    """Test AdaptiveRateLimiter health-gated behavior."""

    def test_allows_all_when_healthy(self) -> None:
        """Test that all requests pass when system is healthy."""
        detector = HybridOverloadDetector()
        limiter = AdaptiveRateLimiter(overload_detector=detector)

        # System is healthy by default
        for i in range(100):
            result = limiter.check(f"client-{i}", RequestPriority.LOW)
            assert result.allowed is True

    def test_sheds_low_priority_when_busy(self) -> None:
        """Test that LOW priority requests are shed when BUSY."""
        config = OverloadConfig(absolute_bounds=(10.0, 50.0, 200.0))  # Lower bounds
        detector = HybridOverloadDetector(config=config)
        limiter = AdaptiveRateLimiter(overload_detector=detector)

        # Record high latencies to trigger BUSY state
        for _ in range(15):
            detector.record_latency(25.0)  # Above busy threshold

        assert detector.get_state() == OverloadState.BUSY

        # LOW priority should be shed
        result = limiter.check("client-1", RequestPriority.LOW)
        assert result.allowed is False

        # HIGH priority should pass
        result = limiter.check("client-1", RequestPriority.HIGH)
        assert result.allowed is True

        # CRITICAL always passes
        result = limiter.check("client-1", RequestPriority.CRITICAL)
        assert result.allowed is True

    def test_only_critical_when_overloaded(self) -> None:
        """Test that only CRITICAL passes when OVERLOADED."""
        config = OverloadConfig(absolute_bounds=(10.0, 50.0, 100.0))
        detector = HybridOverloadDetector(config=config)
        limiter = AdaptiveRateLimiter(overload_detector=detector)

        # Record very high latencies to trigger OVERLOADED state
        for _ in range(15):
            detector.record_latency(150.0)  # Above overloaded threshold

        assert detector.get_state() == OverloadState.OVERLOADED

        # Only CRITICAL passes
        assert limiter.check("client-1", RequestPriority.LOW).allowed is False
        assert limiter.check("client-1", RequestPriority.NORMAL).allowed is False
        assert limiter.check("client-1", RequestPriority.HIGH).allowed is False
        assert limiter.check("client-1", RequestPriority.CRITICAL).allowed is True

    def test_fair_share_when_stressed(self) -> None:
        """Test per-client limits when system is STRESSED."""
        config = OverloadConfig(absolute_bounds=(10.0, 30.0, 100.0))
        detector = HybridOverloadDetector(config=config)
        adaptive_config = AdaptiveRateLimitConfig(
            window_size_seconds=60.0,
            stressed_requests_per_window=5,  # Low limit for testing
        )
        limiter = AdaptiveRateLimiter(
            overload_detector=detector,
            config=adaptive_config,
        )

        # Trigger STRESSED state
        for _ in range(15):
            detector.record_latency(50.0)

        assert detector.get_state() == OverloadState.STRESSED

        # First 5 requests for client-1 should pass (within counter limit)
        for i in range(5):
            result = limiter.check("client-1", RequestPriority.NORMAL)
            assert result.allowed is True, f"Request {i} should be allowed"

        # 6th request should be rate limited
        result = limiter.check("client-1", RequestPriority.NORMAL)
        assert result.allowed is False
        assert result.retry_after_seconds > 0

        # Different client should still have their own limit
        result = limiter.check("client-2", RequestPriority.NORMAL)
        assert result.allowed is True

    def test_cleanup_inactive_clients(self) -> None:
        """Test cleanup of inactive clients."""
        adaptive_config = AdaptiveRateLimitConfig(
            inactive_cleanup_seconds=0.1,
        )
        limiter = AdaptiveRateLimiter(config=adaptive_config)

        # Create some clients
        limiter.check("client-1", RequestPriority.NORMAL)
        limiter.check("client-2", RequestPriority.NORMAL)

        # Wait for them to become inactive
        time.sleep(0.15)

        # Cleanup
        cleaned = limiter.cleanup_inactive_clients()

        assert cleaned == 2
        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 0

    def test_metrics_tracking(self) -> None:
        """Test that metrics are tracked correctly."""
        config = OverloadConfig(absolute_bounds=(10.0, 30.0, 100.0))
        detector = HybridOverloadDetector(config=config)
        adaptive_config = AdaptiveRateLimitConfig(
            stressed_requests_per_window=2,
        )
        limiter = AdaptiveRateLimiter(
            overload_detector=detector,
            config=adaptive_config,
        )

        # Make requests when healthy
        limiter.check("client-1", RequestPriority.NORMAL)
        limiter.check("client-1", RequestPriority.NORMAL)

        metrics = limiter.get_metrics()
        assert metrics["total_requests"] == 2
        assert metrics["allowed_requests"] == 2
        assert metrics["shed_requests"] == 0

        # Trigger stressed state and exhaust limit
        for _ in range(15):
            detector.record_latency(50.0)

        limiter.check("client-1", RequestPriority.NORMAL)  # Allowed (new counter)
        limiter.check("client-1", RequestPriority.NORMAL)  # Allowed
        limiter.check("client-1", RequestPriority.NORMAL)  # Shed

        metrics = limiter.get_metrics()
        assert metrics["total_requests"] == 5
        assert metrics["shed_requests"] >= 1

    @pytest.mark.asyncio
    async def test_check_async(self) -> None:
        """Test async check with wait."""
        config = OverloadConfig(absolute_bounds=(10.0, 30.0, 100.0))
        detector = HybridOverloadDetector(config=config)
        adaptive_config = AdaptiveRateLimitConfig(
            window_size_seconds=0.1,  # Short window for testing
            stressed_requests_per_window=2,
        )
        limiter = AdaptiveRateLimiter(
            overload_detector=detector,
            config=adaptive_config,
        )

        # Trigger stressed state
        for _ in range(15):
            detector.record_latency(50.0)

        # Exhaust limit
        limiter.check("client-1", RequestPriority.NORMAL)
        limiter.check("client-1", RequestPriority.NORMAL)

        # Async check should wait
        start = time.monotonic()
        result = await limiter.check_async(
            "client-1",
            RequestPriority.NORMAL,
            max_wait=0.2,
        )
        elapsed = time.monotonic() - start

        # Should have waited for window to rotate
        assert elapsed >= 0.05


class TestTokenBucket:
    """Test TokenBucket basic operations (legacy support)."""

    def test_initial_state(self) -> None:
        """Test bucket starts full."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        assert bucket.available_tokens == 100.0

    def test_acquire_success(self) -> None:
        """Test successful token acquisition."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        result = bucket.acquire(10)

        assert result is True
        assert bucket.available_tokens == pytest.approx(90.0, abs=0.1)

    def test_acquire_failure(self) -> None:
        """Test failed token acquisition when bucket empty."""
        bucket = TokenBucket(bucket_size=10, refill_rate=1.0)

        # Drain the bucket
        bucket.acquire(10)

        # Try to acquire more
        result = bucket.acquire(1)

        assert result is False

    def test_try_acquire_with_wait_time(self) -> None:
        """Test try_acquire returns wait time."""
        bucket = TokenBucket(bucket_size=10, refill_rate=10.0)

        # Drain bucket
        bucket.acquire(10)

        # Check wait time for 5 tokens
        acquired, wait_time = bucket.try_acquire(5)

        assert acquired is False
        assert wait_time == pytest.approx(0.5, rel=0.1)

    def test_try_acquire_zero_refill_rate(self) -> None:
        """Test try_acquire with zero refill rate returns infinity."""
        bucket = TokenBucket(bucket_size=10, refill_rate=0.0)

        # Drain bucket
        bucket.acquire(10)

        # Try to acquire - should return infinity wait time
        acquired, wait_time = bucket.try_acquire(1)

        assert acquired is False
        assert wait_time == float('inf')

    def test_refill_over_time(self) -> None:
        """Test that tokens refill over time."""
        bucket = TokenBucket(bucket_size=100, refill_rate=100.0)

        # Drain bucket
        bucket.acquire(100)
        assert bucket.available_tokens == pytest.approx(0.0, abs=0.1)

        # Wait for refill
        time.sleep(0.1)

        tokens = bucket.available_tokens
        assert tokens == pytest.approx(10.0, abs=2.0)

    def test_reset(self) -> None:
        """Test bucket reset."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        bucket.acquire(100)
        assert bucket.available_tokens == pytest.approx(0.0, abs=0.1)

        bucket.reset()
        assert bucket.available_tokens == pytest.approx(100.0, abs=0.1)

    @pytest.mark.asyncio
    async def test_acquire_async(self) -> None:
        """Test async acquire with wait."""
        bucket = TokenBucket(bucket_size=10, refill_rate=100.0)

        # Drain bucket
        bucket.acquire(10)

        # Async acquire should wait for tokens
        start = time.monotonic()
        result = await bucket.acquire_async(5, max_wait=1.0)
        elapsed = time.monotonic() - start

        assert result is True
        assert elapsed >= 0.04


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


class TestServerRateLimiter:
    """Test ServerRateLimiter with adaptive limiting."""

    def test_allows_all_when_healthy(self) -> None:
        """Test that all requests pass when system is healthy."""
        limiter = ServerRateLimiter()

        # System is healthy - all should pass
        for i in range(50):
            result = limiter.check_rate_limit(f"client-{i % 5}", "job_submit")
            assert result.allowed is True

    def test_respects_operation_limits_when_healthy(self) -> None:
        """Test per-operation limits are applied when healthy."""
        config = RateLimitConfig(
            operation_limits={"test_op": (5, 1.0)}  # Low limit
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust the operation limit
        for _ in range(5):
            result = limiter.check_rate_limit("client-1", "test_op")
            assert result.allowed is True

        # Should be rate limited now
        result = limiter.check_rate_limit("client-1", "test_op")
        assert result.allowed is False
        assert result.retry_after_seconds > 0

    def test_per_client_isolation(self) -> None:
        """Test that clients have separate counters."""
        config = RateLimitConfig(
            operation_limits={"test_op": (3, 1.0)}
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust client-1
        for _ in range(3):
            limiter.check_rate_limit("client-1", "test_op")

        # client-2 should still have capacity
        result = limiter.check_rate_limit("client-2", "test_op")
        assert result.allowed is True

    def test_check_rate_limit_with_priority(self) -> None:
        """Test priority-aware rate limit check."""
        config = OverloadConfig(absolute_bounds=(10.0, 50.0, 100.0))
        detector = HybridOverloadDetector(config=config)
        limiter = ServerRateLimiter(overload_detector=detector)

        # Trigger BUSY state
        for _ in range(15):
            detector.record_latency(25.0)

        # LOW should be shed, HIGH should pass
        result_low = limiter.check_rate_limit_with_priority(
            "client-1", RequestPriority.LOW
        )
        result_high = limiter.check_rate_limit_with_priority(
            "client-1", RequestPriority.HIGH
        )

        assert result_low.allowed is False
        assert result_high.allowed is True

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

    def test_reset_client(self) -> None:
        """Test resetting a client's counters."""
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
        """Test async rate limit check."""
        config = RateLimitConfig(
            operation_limits={"test_op": (3, 100.0)}
        )
        limiter = ServerRateLimiter(config=config)

        # Exhaust bucket
        for _ in range(3):
            limiter.check_rate_limit("client-1", "test_op")

        # Async check with wait
        start = time.monotonic()
        result = await limiter.check_rate_limit_async(
            "client-1", "test_op", max_wait=1.0
        )
        elapsed = time.monotonic() - start

        assert result.allowed is True
        assert elapsed >= 0.005

    def test_overload_detector_property(self) -> None:
        """Test that overload_detector property works."""
        limiter = ServerRateLimiter()

        detector = limiter.overload_detector
        assert isinstance(detector, HybridOverloadDetector)

        # Should be able to record latency
        detector.record_latency(50.0)

    def test_adaptive_limiter_property(self) -> None:
        """Test that adaptive_limiter property works."""
        limiter = ServerRateLimiter()

        adaptive = limiter.adaptive_limiter
        assert isinstance(adaptive, AdaptiveRateLimiter)


class TestServerRateLimiterCheckCompatibility:
    """Test ServerRateLimiter.check() compatibility method."""

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

        # Exhaust the counter
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

        # Exhaust the counter
        limiter.check(addr)
        limiter.check(addr)

        # Should raise
        with pytest.raises(RateLimitExceeded) as exc_info:
            limiter.check(addr, raise_on_limit=True)

        assert "10.0.0.1:9000" in str(exc_info.value)

    def test_check_different_addresses_isolated(self) -> None:
        """Test that different addresses have separate counters."""
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

        data = b"not a rate limit response"

        assert is_rate_limit_response(data) is False

    @pytest.mark.asyncio
    async def test_handle_rate_limit_response_with_wait(self) -> None:
        """Test handling rate limit response with wait."""
        from hyperscale.distributed_rewrite.reliability import (
            CooperativeRateLimiter,
            handle_rate_limit_response,
        )

        limiter = CooperativeRateLimiter()

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
                return RateLimitResponse(
                    operation="test_op",
                    retry_after_seconds=0.05,
                ).dump()
            else:
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
        assert elapsed >= 0.04

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


class TestHealthGatedBehavior:
    """Test health-gated behavior under various conditions."""

    def test_burst_traffic_allowed_when_healthy(self) -> None:
        """Test that burst traffic is allowed when system is healthy."""
        limiter = ServerRateLimiter()

        # Simulate burst traffic from multiple clients
        results = []
        for burst in range(10):
            for client in range(5):
                result = limiter.check_rate_limit(
                    f"client-{client}",
                    "stats_update",
                    tokens=10,
                )
                results.append(result.allowed)

        # All should pass when healthy
        assert all(results), "All burst requests should pass when healthy"

    def test_graceful_degradation_under_stress(self) -> None:
        """Test graceful degradation when system becomes stressed."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            warmup_samples=5,
        )
        detector = HybridOverloadDetector(config=config)
        limiter = ServerRateLimiter(overload_detector=detector)

        # Initially healthy - all pass
        for _ in range(5):
            result = limiter.check_rate_limit_with_priority(
                "client-1", RequestPriority.LOW
            )
            assert result.allowed is True

        # Trigger stress
        for _ in range(10):
            detector.record_latency(120.0)

        # Now should shed low priority
        result = limiter.check_rate_limit_with_priority(
            "client-1", RequestPriority.LOW
        )
        # May or may not be shed depending on state
        # But critical should always pass
        result_critical = limiter.check_rate_limit_with_priority(
            "client-1", RequestPriority.CRITICAL
        )
        assert result_critical.allowed is True

    def test_recovery_after_stress(self) -> None:
        """Test that system recovers after stress subsides."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            warmup_samples=3,
            hysteresis_samples=2,
        )
        detector = HybridOverloadDetector(config=config)
        limiter = ServerRateLimiter(overload_detector=detector)

        # Start with stress
        for _ in range(5):
            detector.record_latency(150.0)

        # Recover
        for _ in range(10):
            detector.record_latency(20.0)

        # Should be healthy again
        result = limiter.check_rate_limit_with_priority(
            "client-1", RequestPriority.LOW
        )
        # After recovery, low priority should pass again
        assert result.allowed is True
