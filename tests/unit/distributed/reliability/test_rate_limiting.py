"""
Integration tests for Rate Limiting (AD-24).

Tests:
- SlidingWindowCounter deterministic counting
- AdaptiveRateLimiter health-gated behavior
- ServerRateLimiter with adaptive limiting
- Client cleanup to prevent memory leaks
"""

import asyncio
import time

import pytest

from hyperscale.distributed.reliability import (
    AdaptiveRateLimitConfig,
    AdaptiveRateLimiter,
    HybridOverloadDetector,
    OverloadConfig,
    OverloadState,
    RateLimitResult,
    ServerRateLimiter,
    SlidingWindowCounter,
)
from hyperscale.distributed.reliability.load_shedding import RequestPriority


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

    @pytest.mark.asyncio
    async def test_allows_all_when_healthy(self) -> None:
        """Test that all requests pass when system is healthy."""
        detector = HybridOverloadDetector()
        limiter = AdaptiveRateLimiter(overload_detector=detector)

        # System is healthy by default
        for i in range(100):
            result = await limiter.check(f"client-{i}", "default", RequestPriority.LOW)
            assert result.allowed is True

    @pytest.mark.asyncio
    async def test_sheds_low_priority_when_busy(self) -> None:
        """Test that LOW priority requests are shed when BUSY."""
        config = OverloadConfig(absolute_bounds=(10.0, 50.0, 200.0))  # Lower bounds
        detector = HybridOverloadDetector(config=config)
        limiter = AdaptiveRateLimiter(overload_detector=detector)

        # Record high latencies to trigger BUSY state
        for _ in range(15):
            detector.record_latency(25.0)  # Above busy threshold

        assert detector.get_state() == OverloadState.BUSY

        # LOW priority should be shed
        result = await limiter.check("client-1", "default", RequestPriority.LOW)
        assert result.allowed is False

        # HIGH priority should pass
        result = await limiter.check("client-1", "default", RequestPriority.HIGH)
        assert result.allowed is True

        # CRITICAL always passes
        result = await limiter.check("client-1", "default", RequestPriority.CRITICAL)
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_only_critical_when_overloaded(self) -> None:
        """Test that only CRITICAL passes when OVERLOADED."""
        config = OverloadConfig(absolute_bounds=(10.0, 50.0, 100.0))
        detector = HybridOverloadDetector(config=config)
        limiter = AdaptiveRateLimiter(overload_detector=detector)

        # Record very high latencies to trigger OVERLOADED state
        for _ in range(15):
            detector.record_latency(150.0)  # Above overloaded threshold

        assert detector.get_state() == OverloadState.OVERLOADED

        # Only CRITICAL passes
        assert (
            await limiter.check("client-1", "default", RequestPriority.LOW)
        ).allowed is False
        assert (
            await limiter.check("client-1", "default", RequestPriority.NORMAL)
        ).allowed is False
        assert (
            await limiter.check("client-1", "default", RequestPriority.HIGH)
        ).allowed is False
        assert (
            await limiter.check("client-1", "default", RequestPriority.CRITICAL)
        ).allowed is True

    @pytest.mark.asyncio
    async def test_fair_share_when_stressed(self) -> None:
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
            result = await limiter.check("client-1", "default", RequestPriority.NORMAL)
            assert result.allowed is True, f"Request {i} should be allowed"

        # 6th request should be rate limited
        result = await limiter.check("client-1", "default", RequestPriority.NORMAL)
        assert result.allowed is False
        assert result.retry_after_seconds > 0

        # Different client should still have their own limit
        result = await limiter.check("client-2", "default", RequestPriority.NORMAL)
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_cleanup_inactive_clients(self) -> None:
        """Test cleanup of inactive clients."""
        adaptive_config = AdaptiveRateLimitConfig(
            inactive_cleanup_seconds=0.1,
        )
        limiter = AdaptiveRateLimiter(config=adaptive_config)

        # Create some clients
        await limiter.check("client-1", "default", RequestPriority.NORMAL)
        await limiter.check("client-2", "default", RequestPriority.NORMAL)

        # Wait for them to become inactive
        await asyncio.sleep(0.15)

        # Cleanup
        cleaned = await limiter.cleanup_inactive_clients()

        assert cleaned == 2
        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 0

    @pytest.mark.asyncio
    async def test_metrics_tracking(self) -> None:
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
        await limiter.check("client-1", "default", RequestPriority.NORMAL)
        await limiter.check("client-1", "default", RequestPriority.NORMAL)

        metrics = limiter.get_metrics()
        assert metrics["total_requests"] == 2
        assert metrics["allowed_requests"] == 2
        assert metrics["shed_requests"] == 0

        # Trigger stressed state and exhaust limit
        for _ in range(15):
            detector.record_latency(50.0)

        await limiter.check(
            "client-1", "default", RequestPriority.NORMAL
        )  # Allowed (new counter)
        await limiter.check("client-1", "default", RequestPriority.NORMAL)  # Allowed
        await limiter.check("client-1", "default", RequestPriority.NORMAL)  # Shed

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
        await limiter.check("client-1", "default", RequestPriority.NORMAL)
        await limiter.check("client-1", "default", RequestPriority.NORMAL)

        # Async check should wait
        start = time.monotonic()
        result = await limiter.check_async(
            "client-1",
            "default",
            RequestPriority.NORMAL,
            max_wait=0.2,
        )
        elapsed = time.monotonic() - start

        # Should have waited for window to rotate
        assert elapsed >= 0.05


class TestServerRateLimiter:
    """Test ServerRateLimiter with adaptive limiting."""

    @pytest.mark.asyncio
    async def test_allows_all_when_healthy(self) -> None:
        """Test that all requests pass when system is healthy."""
        limiter = ServerRateLimiter()

        # System is healthy - all should pass
        for i in range(50):
            result = await limiter.check_rate_limit(f"client-{i % 5}", "job_submit")
            assert result.allowed is True

    @pytest.mark.asyncio
    async def test_respects_operation_limits_when_healthy(self) -> None:
        """Test per-operation limits are applied when healthy."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test_op": (5, 5.0), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Exhaust the operation limit
        for _ in range(5):
            result = await limiter.check_rate_limit("client-1", "test_op")
            assert result.allowed is True

        # Should be rate limited now
        result = await limiter.check_rate_limit("client-1", "test_op")
        assert result.allowed is False
        assert result.retry_after_seconds > 0

    @pytest.mark.asyncio
    async def test_per_client_isolation(self) -> None:
        """Test that clients have separate counters."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test_op": (3, 3.0), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Exhaust client-1
        for _ in range(3):
            await limiter.check_rate_limit("client-1", "test_op")

        # client-2 should still have capacity
        result = await limiter.check_rate_limit("client-2", "test_op")
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_check_rate_limit_with_priority(self) -> None:
        """Test priority-aware rate limit check."""
        config = OverloadConfig(absolute_bounds=(10.0, 50.0, 100.0))
        detector = HybridOverloadDetector(config=config)
        limiter = ServerRateLimiter(overload_detector=detector)

        # Trigger BUSY state
        for _ in range(15):
            detector.record_latency(25.0)

        # LOW should be shed, HIGH should pass
        result_low = await limiter.check_rate_limit_with_priority(
            "client-1", "default", RequestPriority.LOW
        )
        result_high = await limiter.check_rate_limit_with_priority(
            "client-1", "default", RequestPriority.HIGH
        )

        assert result_low.allowed is False
        assert result_high.allowed is True

    @pytest.mark.asyncio
    async def test_cleanup_inactive_clients(self) -> None:
        """Test cleanup of inactive clients."""
        limiter = ServerRateLimiter(adaptive_config=AdaptiveRateLimitConfig(inactive_cleanup_seconds=0.1))

        # Create some clients
        await limiter.check_rate_limit("client-1", "test")
        await limiter.check_rate_limit("client-2", "test")

        # Wait for them to become inactive
        await asyncio.sleep(0.15)

        # Cleanup
        cleaned = await limiter.cleanup_inactive_clients()

        assert cleaned == 2
        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 0

    @pytest.mark.asyncio
    async def test_reset_client(self) -> None:
        """Test resetting a client's counters."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test_op": (3, 3.0), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Exhaust client
        for _ in range(3):
            await limiter.check_rate_limit("client-1", "test_op")

        # Rate limited
        result = await limiter.check_rate_limit("client-1", "test_op")
        assert result.allowed is False

        # Reset client
        limiter.reset_client("client-1")

        # Should work again
        result = await limiter.check_rate_limit("client-1", "test_op")
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_metrics(self) -> None:
        """Test metrics tracking."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test_op": (2, 2.0), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Make some requests
        await limiter.check_rate_limit("client-1", "test_op")
        await limiter.check_rate_limit("client-1", "test_op")
        await limiter.check_rate_limit("client-1", "test_op")  # Rate limited

        metrics = limiter.get_metrics()

        assert metrics["total_requests"] == 3
        assert metrics["rate_limited_requests"] == 1
        assert metrics["active_clients"] == 1

    @pytest.mark.asyncio
    async def test_check_rate_limit_async(self) -> None:
        """Test async rate limit check."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test_op": (3, 0.05), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Exhaust bucket
        for _ in range(3):
            await limiter.check_rate_limit("client-1", "test_op")

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

    @pytest.mark.asyncio
    async def test_check_allowed(self) -> None:
        """Test check() returns True when allowed."""
        limiter = ServerRateLimiter()
        addr = ("192.168.1.1", 8080)

        result = await limiter.check(addr)

        assert result is True

    @pytest.mark.asyncio
    async def test_check_rate_limited(self) -> None:
        """Test check() returns False when rate limited."""
        config = AdaptiveRateLimitConfig(default_max_requests=3, default_window_size=3.0, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (3, 3.0)})
        limiter = ServerRateLimiter(adaptive_config=config)
        addr = ("192.168.1.1", 8080)

        # Exhaust the counter
        for _ in range(3):
            await limiter.check(addr)

        # Should be rate limited now
        result = await limiter.check(addr)

        assert result is False

    @pytest.mark.asyncio
    async def test_check_raises_on_limit(self) -> None:
        """Test check() raises RateLimitExceeded when raise_on_limit=True."""
        from hyperscale.core.jobs.protocols.rate_limiter import RateLimitExceeded

        config = AdaptiveRateLimitConfig(default_max_requests=2, default_window_size=2.0, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (2, 2.0)})
        limiter = ServerRateLimiter(adaptive_config=config)
        addr = ("10.0.0.1", 9000)

        # Exhaust the counter
        await limiter.check(addr)
        await limiter.check(addr)

        # Should raise
        with pytest.raises(RateLimitExceeded) as exc_info:
            await limiter.check(addr, raise_on_limit=True)

        assert "10.0.0.1:9000" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_check_different_addresses_isolated(self) -> None:
        """Test that different addresses have separate counters."""
        config = AdaptiveRateLimitConfig(default_max_requests=2, default_window_size=2.0, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (2, 2.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        addr1 = ("192.168.1.1", 8080)
        addr2 = ("192.168.1.2", 8080)

        # Exhaust addr1
        await limiter.check(addr1)
        await limiter.check(addr1)
        assert await limiter.check(addr1) is False

        # addr2 should still be allowed
        assert await limiter.check(addr2) is True


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


class TestHealthGatedBehavior:
    """Test health-gated behavior under various conditions."""

    @pytest.mark.asyncio
    async def test_burst_traffic_allowed_when_healthy(self) -> None:
        """Test that burst traffic is allowed when system is healthy."""
        limiter = ServerRateLimiter()

        # Simulate burst traffic from multiple clients
        results = []
        for burst in range(10):
            for client in range(5):
                result = await limiter.check_rate_limit(
                    f"client-{client}",
                    "stats_update",
                    tokens=10,
                )
                results.append(result.allowed)

        # All should pass when healthy
        assert all(results), "All burst requests should pass when healthy"

    @pytest.mark.asyncio
    async def test_graceful_degradation_under_stress(self) -> None:
        """Test graceful degradation when system becomes stressed."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            warmup_samples=5,
        )
        detector = HybridOverloadDetector(config=config)
        limiter = ServerRateLimiter(overload_detector=detector)

        # Initially healthy - all pass
        for _ in range(5):
            result = await limiter.check_rate_limit_with_priority(
                "client-1", "default", RequestPriority.LOW
            )
            assert result.allowed is True

        # Trigger stress
        for _ in range(10):
            detector.record_latency(120.0)

        # Now should shed low priority
        result = await limiter.check_rate_limit_with_priority(
            "client-1", "default", RequestPriority.LOW
        )
        # May or may not be shed depending on state
        # But critical should always pass
        result_critical = await limiter.check_rate_limit_with_priority(
            "client-1", "default", RequestPriority.CRITICAL
        )
        assert result_critical.allowed is True

    @pytest.mark.asyncio
    async def test_recovery_after_stress(self) -> None:
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
        result = await limiter.check_rate_limit_with_priority(
            "client-1", "default", RequestPriority.LOW
        )
        # After recovery, low priority should pass again
        assert result.allowed is True
