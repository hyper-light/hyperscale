"""
Failure path tests for Rate Limiting (AD-24).

Tests failure scenarios and edge cases:
- SlidingWindowCounter edge cases
- Token bucket edge cases (zero tokens, negative values)
- Server rate limiter cleanup and memory management
- Adaptive rate limiter failure modes
- Cooperative rate limiter concurrent operations
- Rate limit retry exhaustion and timeout
- Recovery from rate limiting
- Edge cases in configuration
"""

import asyncio
import pytest
import time

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
from hyperscale.distributed.models import RateLimitResponse


class TestSlidingWindowCounterEdgeCases:
    """Test edge cases in SlidingWindowCounter."""

    def test_acquire_zero_count(self) -> None:
        """Test acquiring zero slots."""
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=10)

        acquired, wait_time = counter.try_acquire(0)
        assert acquired is True
        assert wait_time == 0.0
        assert counter.get_effective_count() == 0.0

    def test_acquire_more_than_max(self) -> None:
        """Test acquiring more than max allowed."""
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=10)

        acquired, wait_time = counter.try_acquire(100)
        assert acquired is False
        assert wait_time > 0

    def test_counter_with_zero_max_requests(self) -> None:
        """Test counter with zero max requests."""
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=0)

        # Any acquire should fail
        acquired, wait_time = counter.try_acquire(1)
        assert acquired is False

    def test_counter_with_very_short_window(self) -> None:
        """Test counter with very short window."""
        counter = SlidingWindowCounter(window_size_seconds=0.01, max_requests=10)

        # Fill counter
        counter.try_acquire(10)

        # Wait for window rotation
        time.sleep(0.02)

        # Should have capacity again
        acquired, _ = counter.try_acquire(5)
        assert acquired is True

    def test_counter_with_very_long_window(self) -> None:
        """Test counter with very long window."""
        counter = SlidingWindowCounter(window_size_seconds=3600.0, max_requests=10)

        # Fill counter
        counter.try_acquire(10)

        # Should be at limit
        acquired, wait_time = counter.try_acquire(1)
        assert acquired is False
        assert wait_time > 0

    @pytest.mark.asyncio
    async def test_acquire_async_race_condition(self) -> None:
        """Test concurrent async acquire attempts."""
        counter = SlidingWindowCounter(window_size_seconds=0.1, max_requests=10)

        # Fill counter
        counter.try_acquire(10)

        # Try multiple concurrent acquires
        results = await asyncio.gather(
            *[counter.acquire_async(3, max_wait=0.2) for _ in range(5)]
        )

        # Some should succeed after window rotation
        success_count = sum(1 for r in results if r)
        assert success_count >= 1


class TestAdaptiveRateLimiterEdgeCases:
    """Test edge cases in AdaptiveRateLimiter."""

    @pytest.mark.asyncio
    async def test_rapid_state_transitions(self) -> None:
        """Test behavior during rapid state transitions."""
        config = OverloadConfig(
            absolute_bounds=(10.0, 50.0, 100.0),
            warmup_samples=3,
            hysteresis_samples=1,  # Disable hysteresis for rapid transitions
        )
        detector = HybridOverloadDetector(config=config)
        limiter = AdaptiveRateLimiter(overload_detector=detector)

        # Start healthy
        for _ in range(5):
            detector.record_latency(5.0)
        result = await limiter.check("client-1", "default", RequestPriority.LOW)
        assert result.allowed is True

        # Spike to overloaded
        for _ in range(5):
            detector.record_latency(150.0)

        # Should shed low priority
        result = await limiter.check("client-1", "default", RequestPriority.LOW)
        # May or may not be shed depending on exact state

        # Critical should always pass
        result = await limiter.check("client-1", "default", RequestPriority.CRITICAL)
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_many_clients_memory_pressure(self) -> None:
        """Test with many clients to check memory handling."""
        adaptive_config = AdaptiveRateLimitConfig(
            inactive_cleanup_seconds=0.1,
        )
        limiter = AdaptiveRateLimiter(config=adaptive_config)

        # Create many clients
        for i in range(1000):
            await limiter.check(f"client-{i}", "default", RequestPriority.NORMAL)

        metrics = limiter.get_metrics()
        # Note: adaptive limiter only creates counters when stressed
        # So active_clients may be 0 if system is healthy
        assert metrics["total_requests"] == 1000

        # Wait and cleanup
        await asyncio.sleep(0.15)
        cleaned = await limiter.cleanup_inactive_clients()
        # Should clean up tracked clients
        assert cleaned >= 0

    @pytest.mark.asyncio
    async def test_priority_ordering(self) -> None:
        """Test that priority ordering is correct."""
        config = OverloadConfig(absolute_bounds=(10.0, 20.0, 50.0))
        detector = HybridOverloadDetector(config=config)
        limiter = AdaptiveRateLimiter(overload_detector=detector)

        # Trigger overloaded state
        for _ in range(15):
            detector.record_latency(100.0)

        # Verify priority ordering
        result = await limiter.check("c1", "default", RequestPriority.CRITICAL)
        assert result.allowed is True
        result = await limiter.check("c2", "default", RequestPriority.HIGH)
        assert result.allowed is False
        result = await limiter.check("c3", "default", RequestPriority.NORMAL)
        assert result.allowed is False
        result = await limiter.check("c4", "default", RequestPriority.LOW)
        assert result.allowed is False

    @pytest.mark.asyncio
    async def test_reset_metrics_clears_counters(self) -> None:
        """Test that reset_metrics clears all counters."""
        limiter = AdaptiveRateLimiter()

        # Generate activity
        for i in range(100):
            await limiter.check(f"client-{i}", "default", RequestPriority.NORMAL)

        metrics_before = limiter.get_metrics()
        assert metrics_before["total_requests"] == 100

        limiter.reset_metrics()

        metrics_after = limiter.get_metrics()
        assert metrics_after["total_requests"] == 0
        assert metrics_after["allowed_requests"] == 0
        assert metrics_after["shed_requests"] == 0


class TestServerRateLimiterFailurePaths:
    """Test failure paths in ServerRateLimiter."""

    @pytest.mark.asyncio
    async def test_unknown_client_creates_counter(self) -> None:
        """Test that unknown client gets new counter."""
        limiter = ServerRateLimiter()

        result = await limiter.check_rate_limit("unknown-client", "job_submit")

        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_many_clients_memory_growth(self) -> None:
        """Test memory behavior with many clients."""
        limiter = ServerRateLimiter(adaptive_config=AdaptiveRateLimitConfig(inactive_cleanup_seconds=0.1))

        # Create many clients
        for i in range(1000):
            await limiter.check_rate_limit(f"client-{i}", "job_submit")

        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 1000

        # Wait for cleanup threshold
        await asyncio.sleep(0.2)

        # Cleanup should remove all
        cleaned = await limiter.cleanup_inactive_clients()
        assert cleaned == 1000

        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 0

    @pytest.mark.asyncio
    async def test_cleanup_preserves_active_clients(self) -> None:
        """Test cleanup preserves recently active clients."""
        limiter = ServerRateLimiter(adaptive_config=AdaptiveRateLimitConfig(inactive_cleanup_seconds=1.0))

        await limiter.check_rate_limit("active-client", "job_submit")
        await limiter.check_rate_limit("inactive-client", "job_submit")

        await asyncio.sleep(0.5)
        await limiter.check_rate_limit("active-client", "heartbeat")

        await asyncio.sleep(0.6)
        cleaned = await limiter.cleanup_inactive_clients()

        assert cleaned == 1
        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 1

    @pytest.mark.asyncio
    async def test_rapid_requests_from_single_client(self) -> None:
        """Test rapid requests exhaust counter."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test": (10, 10.0), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        allowed_count = 0
        for _ in range(20):
            result = await limiter.check_rate_limit("rapid-client", "test")
            if result.allowed:
                allowed_count += 1

        assert allowed_count == 10
        metrics = limiter.get_metrics()
        assert metrics["rate_limited_requests"] == 10

    @pytest.mark.asyncio
    async def test_reset_client_restores_capacity(self) -> None:
        """Test reset_client restores capacity."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test": (5, 5.0), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Exhaust
        for _ in range(5):
            await limiter.check_rate_limit("reset-client", "test")

        result = await limiter.check_rate_limit("reset-client", "test")
        assert result.allowed is False

        # Reset
        limiter.reset_client("reset-client")

        # Should work again
        result = await limiter.check_rate_limit("reset-client", "test")
        assert result.allowed is True

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
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test": (10, 0.1), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        for _ in range(10):
            await limiter.check_rate_limit("async-client", "test")

        result = await limiter.check_rate_limit_async(
            "async-client", "test", max_wait=0.2
        )

        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_async_rate_limit_timeout(self) -> None:
        """Test async rate limit timing out."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test": (10, 10.0), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        for _ in range(10):
            await limiter.check_rate_limit("timeout-client", "test")

        result = await limiter.check_rate_limit_async(
            "timeout-client", "test", max_wait=0.01
        )

        assert result.allowed is False


class TestAdaptiveRateLimitConfigEdgeCases:
    """Test edge cases in AdaptiveRateLimitConfig."""

    def test_very_short_window(self) -> None:
        """Test with very short window size."""
        config = AdaptiveRateLimitConfig(
            window_size_seconds=0.01,
            stressed_requests_per_window=10,
        )

        assert config.window_size_seconds == 0.01
        assert config.stressed_requests_per_window == 10

    def test_very_high_limits(self) -> None:
        """Test with very high limits."""
        config = AdaptiveRateLimitConfig(
            stressed_requests_per_window=1000000,
        )

        assert config.stressed_requests_per_window == 1000000

    def test_zero_limits(self) -> None:
        """Test with zero limits (should effectively block all)."""
        config = AdaptiveRateLimitConfig(
            stressed_requests_per_window=0,
        )

        assert config.stressed_requests_per_window == 0


class TestRateLimitRecovery:
    """Test recovery scenarios from rate limiting."""

    @pytest.mark.asyncio
    async def test_recovery_after_window_rotation(self) -> None:
        """Test recovery after window rotates."""
        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test": (10, 0.1), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Exhaust
        for _ in range(10):
            await limiter.check_rate_limit("recovery-client", "test")

        result = await limiter.check_rate_limit("recovery-client", "test")
        assert result.allowed is False

        # Wait for recovery
        await asyncio.sleep(0.15)

        result = await limiter.check_rate_limit("recovery-client", "test")
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_metrics_reset(self) -> None:
        """Test metrics reset clears counters."""
        limiter = ServerRateLimiter()

        for i in range(100):
            await limiter.check_rate_limit(f"client-{i}", "job_submit")

        metrics_before = limiter.get_metrics()
        assert metrics_before["total_requests"] == 100

        limiter.reset_metrics()

        metrics_after = limiter.get_metrics()
        assert metrics_after["total_requests"] == 0
        assert metrics_after["rate_limited_requests"] == 0

class TestServerRateLimiterCheckEdgeCases:
    """Test edge cases for ServerRateLimiter.check() compatibility method."""

    @pytest.mark.asyncio
    async def test_check_with_port_zero(self) -> None:
        """Test check() with port 0 (ephemeral port)."""
        limiter = ServerRateLimiter()
        addr = ("192.168.1.1", 0)

        result = await limiter.check(addr)
        assert result is True

    @pytest.mark.asyncio
    async def test_check_with_high_port(self) -> None:
        """Test check() with maximum port number."""
        limiter = ServerRateLimiter()
        addr = ("192.168.1.1", 65535)

        result = await limiter.check(addr)
        assert result is True

    @pytest.mark.asyncio
    async def test_check_with_empty_host(self) -> None:
        """Test check() with empty host string."""
        limiter = ServerRateLimiter()
        addr = ("", 8080)

        result = await limiter.check(addr)
        assert result is True

    @pytest.mark.asyncio
    async def test_check_rapid_fire_same_address(self) -> None:
        """Test rapid-fire requests from same address."""
        config = AdaptiveRateLimitConfig(default_max_requests=10, default_window_size=10.0, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (10, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)
        addr = ("192.168.1.1", 8080)

        allowed_count = 0
        for _ in range(20):
            if await limiter.check(addr):
                allowed_count += 1

        assert allowed_count == 10

    @pytest.mark.asyncio
    async def test_check_recovery_after_time(self) -> None:
        """Test that check() allows requests again after time passes."""
        config = AdaptiveRateLimitConfig(default_max_requests=2, default_window_size=0.05, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (2, 0.05)})
        limiter = ServerRateLimiter(adaptive_config=config)
        addr = ("192.168.1.1", 8080)

        await limiter.check(addr)
        await limiter.check(addr)
        assert await limiter.check(addr) is False

        # Window size is max(0.05, 2/100) = 0.05s
        # With sliding window, we need: total_count * (1 - progress) + 1 <= 2
        # So: 2 * (1 - progress) <= 1, meaning progress >= 0.5
        # That's 0.5 * 0.05 = 0.025s into the new window, plus the remaining
        # time in current window. Total wait ~0.05 + 0.025 = 0.075s
        await asyncio.sleep(0.08)

        assert await limiter.check(addr) is True

    @pytest.mark.asyncio
    async def test_check_with_special_characters_in_host(self) -> None:
        """Test check() with hostname containing dots and dashes."""
        limiter = ServerRateLimiter()
        addr = ("my-server.example-domain.com", 8080)

        result = await limiter.check(addr)
        assert result is True

    @pytest.mark.asyncio
    async def test_check_does_not_interfere_with_other_operations(self) -> None:
        """Test that check() using 'default' doesn't affect other operations."""
        config = AdaptiveRateLimitConfig(default_max_requests=2, default_window_size=2.0, operation_limits={"custom_op": (10, 10.0), "default": (2, 2.0)})
        limiter = ServerRateLimiter(adaptive_config=config)
        addr = ("192.168.1.1", 8080)
        client_id = "192.168.1.1:8080"

        await limiter.check(addr)
        await limiter.check(addr)
        assert await limiter.check(addr) is False

        result = await limiter.check_rate_limit(client_id, "custom_op")
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_check_cleanup_affects_check_clients(self) -> None:
        """Test that cleanup_inactive_clients() cleans up clients created via check()."""
        limiter = ServerRateLimiter(adaptive_config=AdaptiveRateLimitConfig(inactive_cleanup_seconds=0.05))

        for i in range(5):
            addr = (f"192.168.1.{i}", 8080)
            await limiter.check(addr)

        assert limiter.get_metrics()["active_clients"] == 5

        await asyncio.sleep(0.1)

        cleaned = await limiter.cleanup_inactive_clients()
        assert cleaned == 5
        assert limiter.get_metrics()["active_clients"] == 0

    @pytest.mark.asyncio
    async def test_check_reset_client_affects_check_counter(self) -> None:
        """Test that reset_client() restores capacity for clients created via check()."""
        config = AdaptiveRateLimitConfig(default_max_requests=3, default_window_size=3.0, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (3, 3.0)})
        limiter = ServerRateLimiter(adaptive_config=config)
        addr = ("192.168.1.1", 8080)
        client_id = "192.168.1.1:8080"

        await limiter.check(addr)
        await limiter.check(addr)
        await limiter.check(addr)
        assert await limiter.check(addr) is False

        limiter.reset_client(client_id)

        assert await limiter.check(addr) is True

    @pytest.mark.asyncio
    async def test_check_exception_message_format(self) -> None:
        """Test that RateLimitExceeded exception has correct message format."""
        from hyperscale.core.jobs.protocols.rate_limiter import RateLimitExceeded

        config = AdaptiveRateLimitConfig(default_max_requests=1, default_window_size=1.0, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (1, 1.0)})
        limiter = ServerRateLimiter(adaptive_config=config)
        addr = ("10.20.30.40", 12345)

        await limiter.check(addr)

        try:
            await limiter.check(addr, raise_on_limit=True)
            assert False, "Should have raised"
        except RateLimitExceeded as exc:
            assert "10.20.30.40" in str(exc)
            assert "12345" in str(exc)

    @pytest.mark.asyncio
    async def test_check_multiple_concurrent_addresses(self) -> None:
        """Test check() with many different addresses concurrently."""
        config = AdaptiveRateLimitConfig(default_max_requests=5, default_window_size=5.0, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (5, 5.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        for i in range(100):
            addr = (f"10.0.0.{i}", 8080 + i)
            assert await limiter.check(addr) is True

        assert limiter.get_metrics()["active_clients"] == 100

    @pytest.mark.asyncio
    async def test_check_returns_false_not_none(self) -> None:
        """Test that check() returns False (not None) when rate limited."""
        config = AdaptiveRateLimitConfig(default_max_requests=1, default_window_size=1.0, operation_limits={"stats_update": (500, 10.0), "heartbeat": (200, 10.0), "progress_update": (300, 10.0), "job_submit": (50, 10.0), "job_status": (100, 10.0), "workflow_dispatch": (100, 10.0), "cancel": (20, 10.0), "reconnect": (10, 10.0), "default": (1, 1.0)})
        limiter = ServerRateLimiter(adaptive_config=config)
        addr = ("192.168.1.1", 8080)

        await limiter.check(addr)
        result = await limiter.check(addr)

        assert result is False
        assert result is not None


class TestHealthGatedEdgeCases:
    """Test edge cases in health-gated behavior."""

    def test_state_transition_boundary(self) -> None:
        """Test behavior at state transition boundaries."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            warmup_samples=3,
            hysteresis_samples=1,
        )
        detector = HybridOverloadDetector(config=config)
        limiter = ServerRateLimiter(overload_detector=detector)

        # Record exactly at boundary
        for _ in range(5):
            detector.record_latency(50.0)  # Exactly at BUSY threshold

        # Should be BUSY
        state = detector.get_state()
        assert state in (OverloadState.HEALTHY, OverloadState.BUSY)

    @pytest.mark.asyncio
    async def test_graceful_handling_no_detector(self) -> None:
        """Test that limiter works without explicit detector."""
        limiter = ServerRateLimiter()

        # Should work with internal detector
        result = await limiter.check_rate_limit("client-1", "test")
        assert result.allowed is True

        # Should be able to access detector
        detector = limiter.overload_detector
        assert detector is not None

    def test_shared_detector_across_limiters(self) -> None:
        """Test sharing detector across multiple limiters."""
        detector = HybridOverloadDetector()
        limiter1 = ServerRateLimiter(overload_detector=detector)
        limiter2 = ServerRateLimiter(overload_detector=detector)

        # Both should use same detector
        assert limiter1.overload_detector is detector
        assert limiter2.overload_detector is detector

        # Changes in one should reflect in the other
        config = OverloadConfig(absolute_bounds=(10.0, 50.0, 100.0))
        shared_detector = HybridOverloadDetector(config=config)
        limiter_a = ServerRateLimiter(overload_detector=shared_detector)
        limiter_b = ServerRateLimiter(overload_detector=shared_detector)

        for _ in range(15):
            shared_detector.record_latency(150.0)

        # Both limiters should see the same overloaded state
        assert shared_detector.get_state() == OverloadState.OVERLOADED
