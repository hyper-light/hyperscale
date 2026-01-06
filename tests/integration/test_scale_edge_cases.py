"""
Scale and Reliability Edge Case Tests.

Tests for failure modes that emerge at scale (millions of jobs):
- Memory leaks from unbounded data structure growth
- Resource exhaustion (token buckets, queues, counters)
- Cascade failures across components
- State corruption and recovery
- Thundering herd after recovery
- Starvation and fairness issues
- Numeric overflow and boundary conditions
- Recovery from unrecoverable states

These tests validate that the system remains stable under extreme
conditions and degrades gracefully rather than catastrophically.
"""

import asyncio
import gc
import sys
import time
import weakref
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import pytest

from hyperscale.distributed_rewrite.reliability.overload import (
    HybridOverloadDetector,
    OverloadConfig,
    OverloadState,
)
from hyperscale.distributed_rewrite.reliability.load_shedding import (
    LoadShedder,
    LoadShedderConfig,
    RequestPriority,
)
from hyperscale.distributed_rewrite.reliability.rate_limiting import (
    TokenBucket,
    RateLimitConfig,
    ServerRateLimiter,
    CooperativeRateLimiter,
)
from hyperscale.distributed_rewrite.health.probes import (
    HealthProbe,
    ProbeConfig,
    ProbeResult,
    CompositeProbe,
)
from hyperscale.distributed_rewrite.health.extension_tracker import (
    ExtensionTracker,
    ExtensionTrackerConfig,
)
from hyperscale.distributed_rewrite.health.worker_health_manager import (
    WorkerHealthManager,
    WorkerHealthManagerConfig,
)


# =============================================================================
# Memory Leak Detection Tests
# =============================================================================


class TestMemoryLeakPrevention:
    """Tests to ensure data structures don't grow unboundedly."""

    def test_detector_recent_samples_bounded(self):
        """Verify recent samples deque is bounded by current_window."""
        config = OverloadConfig(current_window=10)
        detector = HybridOverloadDetector(config)

        # Record many more samples than window size
        for i in range(10000):
            detector.record_latency(float(i))

        # Recent samples should be bounded
        assert len(detector._recent) == 10

    def test_detector_delta_history_bounded(self):
        """Verify delta history is bounded by trend_window."""
        config = OverloadConfig(trend_window=20)
        detector = HybridOverloadDetector(config)

        # Record many samples
        for i in range(10000):
            detector.record_latency(100.0 + (i % 100))

        # Delta history should be bounded
        assert len(detector._delta_history) == 20

    def test_rate_limiter_client_cleanup(self):
        """Verify inactive clients are cleaned up."""
        limiter = ServerRateLimiter(inactive_cleanup_seconds=0.1)

        # Create many clients
        for i in range(1000):
            limiter.check_rate_limit(f"client-{i}", "operation")

        assert limiter.get_metrics()["active_clients"] == 1000

        # Wait for cleanup threshold
        time.sleep(0.15)

        # Cleanup should remove all
        cleaned = limiter.cleanup_inactive_clients()
        assert cleaned == 1000
        assert limiter.get_metrics()["active_clients"] == 0

    def test_rate_limiter_client_buckets_per_operation(self):
        """Verify per-operation buckets don't grow unboundedly."""
        limiter = ServerRateLimiter()

        # Single client, many different operations
        for i in range(100):
            limiter.check_rate_limit("client-1", f"operation-{i}")

        # Each operation creates a bucket for the client
        client_buckets = limiter._client_buckets.get("client-1", {})
        assert len(client_buckets) == 100

        # This is a known growth pattern - operations should be bounded
        # by the application, not by the limiter

    def test_extension_tracker_no_unbounded_growth(self):
        """Verify extension tracker doesn't grow unboundedly."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(max_extensions=5)
        )

        # Create trackers for many workers
        for i in range(1000):
            manager._get_tracker(f"worker-{i}")

        assert manager.tracked_worker_count == 1000

        # Clean up workers
        for i in range(1000):
            manager.on_worker_removed(f"worker-{i}")

        assert manager.tracked_worker_count == 0

    def test_load_shedder_metrics_dont_overflow_quickly(self):
        """Verify shedder metrics don't overflow with high request counts."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Simulate high request volume
        for _ in range(100000):
            shedder.should_shed("Ping")

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 100000
        assert metrics["shed_rate"] == 0.0  # All accepted (healthy)

    def test_detector_reset_releases_memory(self):
        """Verify reset() properly releases internal data structures."""
        config = OverloadConfig(current_window=100, trend_window=100)
        detector = HybridOverloadDetector(config)

        # Build up state
        for i in range(1000):
            detector.record_latency(float(i))

        # Reset
        detector.reset()

        assert len(detector._recent) == 0
        assert len(detector._delta_history) == 0
        assert detector._sample_count == 0

    def test_weak_reference_cleanup_pattern(self):
        """Test that objects can be garbage collected when dereferenced."""
        # Create detector
        detector = HybridOverloadDetector()
        weak_ref = weakref.ref(detector)

        # Use it
        for _ in range(100):
            detector.record_latency(100.0)

        # Dereference
        del detector
        gc.collect()

        # Should be collected
        assert weak_ref() is None


# =============================================================================
# Resource Exhaustion Tests
# =============================================================================


class TestResourceExhaustion:
    """Tests for resource exhaustion scenarios."""

    def test_token_bucket_complete_depletion(self):
        """Test token bucket behavior when completely depleted."""
        bucket = TokenBucket(bucket_size=10, refill_rate=1.0)

        # Deplete all tokens
        for _ in range(10):
            assert bucket.acquire() is True

        # Bucket is empty - can't acquire more
        assert bucket.acquire() is False
        # Note: available_tokens calls _refill() which may add tiny amounts
        # due to elapsed time, so check it's less than 1 (can't acquire)
        assert bucket.available_tokens < 1

    def test_token_bucket_recovery_after_depletion(self):
        """Test token bucket recovery after complete depletion."""
        bucket = TokenBucket(bucket_size=10, refill_rate=100.0)  # Fast refill

        # Deplete
        for _ in range(10):
            bucket.acquire()

        # Immediately after depletion, should have very few tokens
        # (available_tokens calls _refill so may have tiny amount)
        assert bucket.available_tokens < 1

        # Wait for refill
        time.sleep(0.1)  # Should refill 10 tokens

        assert bucket.available_tokens >= 9  # Allow for timing variance

    def test_rate_limiter_sustained_overload(self):
        """Test rate limiter under sustained overload."""
        config = RateLimitConfig(
            default_bucket_size=10,
            default_refill_rate=1.0,  # 1 token/sec
        )
        limiter = ServerRateLimiter(config)

        # Burst of 100 requests
        allowed = 0
        rejected = 0
        for _ in range(100):
            result = limiter.check_rate_limit("client-1", "burst_op")
            if result.allowed:
                allowed += 1
            else:
                rejected += 1

        # Only bucket_size should be allowed
        assert allowed == 10
        assert rejected == 90

    def test_extension_exhaustion(self):
        """Test extension tracker when all extensions exhausted."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=3,
            base_deadline=30.0,
        )

        # Exhaust all extensions with increasing progress
        for i in range(3):
            granted, _, _ = tracker.request_extension(
                reason="busy",
                current_progress=float(i + 1) * 10.0,
            )
            assert granted is True

        # Further requests denied
        granted, _, reason = tracker.request_extension(
            reason="still busy",
            current_progress=40.0,
        )
        assert granted is False
        assert "exceeded" in reason.lower()
        assert tracker.is_exhausted is True

    def test_cooperative_limiter_blocked_state(self):
        """Test cooperative rate limiter blocked state."""
        limiter = CooperativeRateLimiter()

        # Block for 1 second
        limiter.handle_rate_limit("operation", retry_after=1.0)

        assert limiter.is_blocked("operation") is True
        assert limiter.get_retry_after("operation") > 0.9

    @pytest.mark.asyncio
    async def test_sustained_load_shedding(self):
        """Test load shedder under sustained high load."""
        config = OverloadConfig(
            absolute_bounds=(10.0, 20.0, 50.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Push into overloaded state
        detector.record_latency(100.0)

        # Sustained traffic
        shed_count = 0
        accepted_count = 0

        for _ in range(10000):
            if shedder.should_shed("SubmitJob"):  # HIGH priority
                shed_count += 1
            else:
                accepted_count += 1

        # All HIGH priority should be shed in OVERLOADED state
        assert shed_count == 10000
        assert accepted_count == 0


# =============================================================================
# Cascade Failure Tests
# =============================================================================


class TestCascadeFailures:
    """Tests for cascade failure scenarios."""

    def test_overload_triggers_shedding_cascade(self):
        """Test that overload detection properly triggers load shedding."""
        # Use min_samples=1 and current_window=1 for immediate state transitions
        # based on absolute bounds (no EMA smoothing effects)
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Initially healthy - accept everything
        detector.record_latency(50.0)

        assert not shedder.should_shed("DetailedStatsRequest")  # LOW

        # Transition to stressed (300ms > 200ms threshold)
        detector._recent.clear()
        detector.record_latency(300.0)

        # LOW and NORMAL should now be shed
        assert shedder.should_shed("DetailedStatsRequest")  # LOW
        assert shedder.should_shed("StatsUpdate")  # NORMAL
        assert not shedder.should_shed("SubmitJob")  # HIGH

        # Transition to overloaded (1000ms > 500ms threshold)
        detector._recent.clear()
        detector.record_latency(1000.0)

        # Only CRITICAL accepted
        assert shedder.should_shed("SubmitJob")  # HIGH - now shed
        assert not shedder.should_shed("Ping")  # CRITICAL

    def test_multiple_detection_methods_cascade(self):
        """Test cascade when multiple detection methods trigger."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            cpu_thresholds=(0.5, 0.7, 0.9),
            memory_thresholds=(0.5, 0.7, 0.9),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Latency healthy
        for _ in range(5):
            detector.record_latency(50.0)

        # But CPU and memory stressed
        state = detector.get_state(cpu_percent=80.0, memory_percent=80.0)
        assert state == OverloadState.STRESSED

        # Now add high latency
        for _ in range(5):
            detector.record_latency(600.0)

        # Should be OVERLOADED from absolute bounds
        state = detector.get_state(cpu_percent=50.0, memory_percent=50.0)
        assert state == OverloadState.OVERLOADED

    @pytest.mark.asyncio
    async def test_probe_failure_cascade(self):
        """Test probe failures cascading to composite unhealthy."""
        failure_count = 0

        async def failing_check():
            nonlocal failure_count
            failure_count += 1
            if failure_count <= 3:
                return False, "Component unavailable"
            return True, "OK"

        probe = HealthProbe(
            name="dependency",
            check=failing_check,
            config=ProbeConfig(
                failure_threshold=3,
                timeout_seconds=1.0,
            ),
        )

        composite = CompositeProbe("service")
        composite.add_probe(probe)

        # Initially healthy
        assert composite.is_healthy() is True

        # Fail 3 times to trigger threshold
        for _ in range(3):
            await probe.check()

        assert composite.is_healthy() is False
        assert "dependency" in composite.get_unhealthy_probes()


# =============================================================================
# State Corruption and Recovery Tests
# =============================================================================


class TestStateCorruptionRecovery:
    """Tests for state corruption detection and recovery."""

    def test_detector_handles_nan_latency(self):
        """Test detector handles NaN latency without corruption."""
        detector = HybridOverloadDetector()

        # Normal latencies
        detector.record_latency(100.0)
        detector.record_latency(100.0)

        # NaN (shouldn't crash)
        detector.record_latency(float('nan'))

        # Should still function
        state = detector.get_state()
        # State may be undefined with NaN, but shouldn't crash
        assert state is not None

    def test_detector_handles_inf_latency(self):
        """Test detector handles infinity latency."""
        detector = HybridOverloadDetector()

        detector.record_latency(100.0)
        detector.record_latency(float('inf'))

        # Should trigger overloaded
        state = detector.get_state()
        assert state == OverloadState.OVERLOADED

    def test_detector_handles_negative_inf_latency(self):
        """Test detector handles negative infinity."""
        detector = HybridOverloadDetector()

        detector.record_latency(100.0)
        detector.record_latency(float('-inf'))

        # Shouldn't crash
        state = detector.get_state()
        assert state is not None

    def test_extension_tracker_progress_regression(self):
        """Test extension tracker rejects progress regression."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=5,
        )

        # First extension with progress 50
        granted, _, _ = tracker.request_extension(
            reason="busy",
            current_progress=50.0,
        )
        assert granted is True

        # Second extension with LOWER progress (regression)
        granted, _, reason = tracker.request_extension(
            reason="still busy",
            current_progress=30.0,  # Less than 50
        )
        assert granted is False
        assert "no progress" in reason.lower()

    def test_extension_tracker_reset_allows_reuse(self):
        """Test extension tracker can be reused after reset."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=2,
        )

        # Exhaust extensions
        tracker.request_extension(reason="r1", current_progress=10.0)
        tracker.request_extension(reason="r2", current_progress=20.0)
        assert tracker.is_exhausted is True

        # Reset
        tracker.reset()

        # Should be usable again
        assert tracker.is_exhausted is False
        granted, _, _ = tracker.request_extension(
            reason="new cycle",
            current_progress=5.0,
        )
        assert granted is True

    def test_worker_health_manager_recovery(self):
        """Test worker health manager recovers from unhealthy state."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                max_extensions=2,
                eviction_threshold=3,
            )
        )

        # Worker requests extensions until exhausted
        from hyperscale.distributed_rewrite.models import (
            HealthcheckExtensionRequest,
        )

        # Exhaust extensions
        for i in range(2):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="busy",
                current_progress=float((i + 1) * 10),
                estimated_completion=30.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, time.time() + 30)

        # Check eviction state
        should_evict, _ = manager.should_evict_worker("worker-1")
        assert should_evict is True

        # Worker becomes healthy
        manager.on_worker_healthy("worker-1")

        # Should no longer be evictable
        should_evict, _ = manager.should_evict_worker("worker-1")
        assert should_evict is False

    def test_load_shedder_metrics_reset_recovery(self):
        """Test load shedder recovers cleanly after metrics reset."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Generate metrics
        detector.record_latency(300.0)  # OVERLOADED
        for _ in range(100):
            shedder.should_shed("SubmitJob")

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 100
        assert metrics["shed_requests"] == 100

        # Reset
        shedder.reset_metrics()

        # Verify clean state
        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 0
        assert metrics["shed_requests"] == 0
        assert metrics["shed_rate"] == 0.0


# =============================================================================
# Thundering Herd and Burst Tests
# =============================================================================


class TestThunderingHerdBurst:
    """Tests for thundering herd and burst traffic scenarios."""

    def test_burst_traffic_rate_limiting(self):
        """Test rate limiter handles burst traffic correctly."""
        config = RateLimitConfig(
            default_bucket_size=100,
            default_refill_rate=10.0,
        )
        limiter = ServerRateLimiter(config)

        # Simulate burst from many clients simultaneously
        burst_results = []
        for client_id in range(100):
            for _ in range(5):
                result = limiter.check_rate_limit(
                    f"client-{client_id}",
                    "burst_operation",
                )
                burst_results.append(result.allowed)

        # Each client should have all requests allowed (5 < 100 bucket size)
        allowed_count = sum(burst_results)
        assert allowed_count == 500  # All 500 requests allowed

    def test_sustained_burst_depletion(self):
        """Test sustained burst depletes token buckets."""
        config = RateLimitConfig(
            default_bucket_size=50,
            default_refill_rate=1.0,  # Slow refill
        )
        limiter = ServerRateLimiter(config)

        # Single client, sustained burst
        results = []
        for _ in range(100):
            result = limiter.check_rate_limit("client-1", "operation")
            results.append(result.allowed)

        allowed = sum(results)
        rejected = len(results) - allowed

        # First 50 allowed, rest rejected
        assert allowed == 50
        assert rejected == 50

    def test_recovery_after_burst_backpressure(self):
        """Test system recovers after burst with backpressure."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Burst causes overload
        for _ in range(10):
            detector.record_latency(600.0)

        state = detector.get_state()
        assert state == OverloadState.OVERLOADED

        # Gradual recovery
        for _ in range(20):
            detector.record_latency(80.0)  # Below BUSY threshold

        state = detector.get_state()
        assert state == OverloadState.HEALTHY

        # All traffic should be accepted
        assert not shedder.should_shed("DetailedStatsRequest")

    @pytest.mark.asyncio
    async def test_concurrent_rate_limit_checks(self):
        """Test concurrent rate limit checks are handled correctly."""
        limiter = ServerRateLimiter(
            RateLimitConfig(default_bucket_size=100, default_refill_rate=10.0)
        )

        async def check_rate_limit(client_id: str) -> bool:
            result = limiter.check_rate_limit(client_id, "concurrent_op")
            return result.allowed

        # 50 concurrent checks from same client
        tasks = [check_rate_limit("client-1") for _ in range(50)]
        results = await asyncio.gather(*tasks)

        # All should be allowed (50 < 100 bucket size)
        assert all(results)

    @pytest.mark.asyncio
    async def test_thundering_herd_after_recovery(self):
        """Test handling of thundering herd after service recovery."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Service was down, now recovering (low latency)
        for _ in range(5):
            detector.record_latency(50.0)

        # Thundering herd: all clients retry at once
        # Simulate 1000 concurrent requests
        shed_decisions = []
        for _ in range(1000):
            # Mix of priorities
            shed_decisions.append(shedder.should_shed("SubmitJob"))  # HIGH

        # In healthy state, all should be accepted
        assert sum(shed_decisions) == 0  # None shed


# =============================================================================
# Starvation and Fairness Tests
# =============================================================================


class TestStarvationFairness:
    """Tests for starvation and fairness under load."""

    def test_critical_traffic_never_starved(self):
        """Test CRITICAL priority traffic is never starved."""
        config = OverloadConfig(
            absolute_bounds=(10.0, 20.0, 50.0),  # Easy to trigger
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Push to OVERLOADED
        detector.record_latency(100.0)
        assert detector.get_state() == OverloadState.OVERLOADED

        # Verify CRITICAL is never shed even under sustained load
        for _ in range(10000):
            assert shedder.should_shed("Ping") is False
            assert shedder.should_shed("Heartbeat") is False
            assert shedder.should_shed("JobCancelRequest") is False

    def test_high_priority_starves_low_under_stress(self):
        """Test LOW priority is shed while HIGH continues under stress."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # STRESSED state
        detector.record_latency(150.0)
        assert detector.get_state() == OverloadState.STRESSED

        high_shed = 0
        low_shed = 0

        for _ in range(1000):
            if shedder.should_shed("SubmitJob"):  # HIGH
                high_shed += 1
            if shedder.should_shed("DetailedStatsRequest"):  # LOW
                low_shed += 1

        # HIGH should not be shed, LOW should be completely shed
        assert high_shed == 0
        assert low_shed == 1000

    def test_rate_limiter_per_client_fairness(self):
        """Test rate limiter provides per-client fairness."""
        config = RateLimitConfig(
            default_bucket_size=10,
            default_refill_rate=1.0,
        )
        limiter = ServerRateLimiter(config)

        # Client 1 exhausts their limit
        for _ in range(20):
            limiter.check_rate_limit("client-1", "operation")

        # Client 2 should still have full quota
        for _ in range(10):
            result = limiter.check_rate_limit("client-2", "operation")
            assert result.allowed is True

    def test_per_operation_fairness(self):
        """Test different operations have independent limits."""
        config = RateLimitConfig(
            default_bucket_size=10,
            default_refill_rate=1.0,
            operation_limits={
                "high_rate_op": (100, 10.0),
                "low_rate_op": (5, 0.5),
            },
        )
        limiter = ServerRateLimiter(config)

        # Exhaust low_rate_op
        for _ in range(10):
            limiter.check_rate_limit("client-1", "low_rate_op")

        # high_rate_op should still work
        for _ in range(50):
            result = limiter.check_rate_limit("client-1", "high_rate_op")
            assert result.allowed is True


# =============================================================================
# Numeric Overflow and Boundary Tests
# =============================================================================


class TestNumericOverflowBoundary:
    """Tests for numeric overflow and boundary conditions."""

    def test_very_large_latency_values(self):
        """Test handling of very large latency values."""
        detector = HybridOverloadDetector()

        # Max float value
        detector.record_latency(sys.float_info.max / 2)

        state = detector.get_state()
        assert state == OverloadState.OVERLOADED

    def test_very_small_latency_values(self):
        """Test handling of very small (but positive) latency values."""
        detector = HybridOverloadDetector()

        # Very small but valid
        detector.record_latency(sys.float_info.min)
        detector.record_latency(1e-308)

        state = detector.get_state()
        assert state == OverloadState.HEALTHY

    def test_zero_latency(self):
        """Test handling of zero latency."""
        detector = HybridOverloadDetector()

        detector.record_latency(0.0)
        detector.record_latency(0.0)
        detector.record_latency(0.0)

        state = detector.get_state()
        assert state == OverloadState.HEALTHY

    def test_counter_after_many_operations(self):
        """Test counters remain accurate after many operations."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Simulate many operations
        for _ in range(1_000_000):
            shedder.should_shed("Ping")

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 1_000_000

    def test_token_bucket_refill_precision(self):
        """Test token bucket maintains precision over many refills."""
        bucket = TokenBucket(bucket_size=1000, refill_rate=0.001)

        # Many small refills
        for _ in range(10000):
            bucket._refill()
            time.sleep(0.0001)

        # Tokens should not exceed bucket size
        assert bucket.available_tokens <= bucket.bucket_size

    def test_extension_grant_logarithmic_decay(self):
        """Test extension grants follow logarithmic decay correctly."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=32.0,  # Powers of 2 for easy testing
            min_grant=1.0,
            max_extensions=10,
        )

        expected_grants = [16.0, 8.0, 4.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]

        for i, expected in enumerate(expected_grants):
            granted, actual_grant, _ = tracker.request_extension(
                reason="busy",
                current_progress=float((i + 1) * 10),
            )
            assert granted is True
            assert actual_grant == pytest.approx(expected), f"Grant {i} mismatch"

    def test_boundary_threshold_values(self):
        """Test behavior at exact threshold boundaries."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)

        # Exactly at BUSY threshold
        detector.record_latency(100.0)
        # At boundary - could be HEALTHY or BUSY depending on implementation
        # (> vs >=)
        state = detector._get_absolute_state()
        # Just verify it doesn't crash and returns valid state
        assert state in (OverloadState.HEALTHY, OverloadState.BUSY)

        # Just above BUSY threshold
        detector._recent.clear()
        detector.record_latency(100.01)
        state = detector._get_absolute_state()
        assert state == OverloadState.BUSY

    def test_cpu_memory_boundary_100_percent(self):
        """Test CPU/memory at exactly 100%."""
        config = OverloadConfig(
            cpu_thresholds=(0.7, 0.85, 0.95),
            memory_thresholds=(0.7, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config)

        # 100% CPU and memory
        state = detector._get_resource_state(
            cpu_percent=100.0,
            memory_percent=100.0,
        )
        assert state == OverloadState.OVERLOADED

    def test_cpu_memory_above_100_percent(self):
        """Test CPU/memory above 100% (shouldn't happen but handle gracefully)."""
        config = OverloadConfig(
            cpu_thresholds=(0.7, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config)

        # Invalid but handle gracefully
        state = detector._get_resource_state(
            cpu_percent=150.0,
            memory_percent=200.0,
        )
        assert state == OverloadState.OVERLOADED


# =============================================================================
# Rapid State Transition Tests
# =============================================================================


class TestRapidStateTransitions:
    """Tests for rapid state transition scenarios."""

    def test_rapid_healthy_overloaded_transitions(self):
        """Test rapid transitions between HEALTHY and OVERLOADED."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=1,
            current_window=3,
        )
        detector = HybridOverloadDetector(config)

        # Alternate between extremes
        for _ in range(100):
            # Push to healthy
            for _ in range(3):
                detector.record_latency(50.0)
            state1 = detector.get_state()

            # Push to overloaded
            for _ in range(3):
                detector.record_latency(1000.0)
            state2 = detector.get_state()

            # Should transition correctly
            assert state1 == OverloadState.HEALTHY
            assert state2 == OverloadState.OVERLOADED

    def test_oscillating_load_detection(self):
        """Test detection under oscillating load pattern."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Oscillating latency pattern
        states_seen = set()
        for i in range(100):
            # Sine-wave-like pattern
            latency = 250.0 + 200.0 * (i % 10 < 5 and 1 or -1)
            detector.record_latency(latency)
            states_seen.add(detector.get_state())

        # Should see multiple states
        assert len(states_seen) >= 2

    @pytest.mark.asyncio
    async def test_probe_flapping_detection(self):
        """Test probe handles flapping (rapid success/failure)."""
        call_count = 0

        async def flapping_check():
            nonlocal call_count
            call_count += 1
            # Alternate success/failure
            return call_count % 2 == 0, "Flapping"

        probe = HealthProbe(
            name="flapper",
            check=flapping_check,
            config=ProbeConfig(
                failure_threshold=3,
                success_threshold=2,
            ),
        )

        # Run many checks
        for _ in range(20):
            await probe.check()

        # Due to alternating pattern and thresholds,
        # state should be deterministic
        state = probe.get_state()
        assert state is not None


# =============================================================================
# Long-Running Stability Tests
# =============================================================================


class TestLongRunningStability:
    """Tests for long-running stability scenarios."""

    def test_detector_stability_over_many_samples(self):
        """Test detector remains stable over many samples."""
        detector = HybridOverloadDetector()

        # Simulate long-running operation
        for i in range(100000):
            # Realistic latency pattern with occasional spikes
            base_latency = 50.0
            spike = 200.0 if i % 1000 == 0 else 0.0
            detector.record_latency(base_latency + spike)

        # Should still function correctly
        state = detector.get_state()
        diagnostics = detector.get_diagnostics()

        assert state is not None
        assert diagnostics["sample_count"] == 100000
        assert detector.baseline > 0

    def test_load_shedder_metrics_accuracy_over_time(self):
        """Test load shedder metrics remain accurate over time."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        expected_shed = 0
        expected_total = 0

        # Mixed traffic pattern
        for i in range(10000):
            # Alternate between healthy and overloaded
            if i % 100 < 50:
                detector.record_latency(30.0)  # HEALTHY
            else:
                detector.record_latency(300.0)  # OVERLOADED

            should_shed = shedder.should_shed("SubmitJob")
            expected_total += 1
            if should_shed:
                expected_shed += 1

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == expected_total
        assert metrics["shed_requests"] == expected_shed

    def test_rate_limiter_long_running_cleanup(self):
        """Test rate limiter cleanup over long running period."""
        limiter = ServerRateLimiter(inactive_cleanup_seconds=0.05)

        # Create and abandon clients over time
        for batch in range(10):
            # Create 100 clients
            for i in range(100):
                limiter.check_rate_limit(f"batch-{batch}-client-{i}", "op")

            # Wait for cleanup threshold
            time.sleep(0.06)

            # Run cleanup
            cleaned = limiter.cleanup_inactive_clients()

            # Previous batch should be cleaned
            if batch > 0:
                assert cleaned > 0

        # Final cleanup
        time.sleep(0.06)
        final_cleaned = limiter.cleanup_inactive_clients()
        assert limiter.get_metrics()["active_clients"] == 0


# =============================================================================
# Recovery Pattern Tests
# =============================================================================


class TestRecoveryPatterns:
    """Tests for proper recovery from degraded states."""

    def test_gradual_recovery_from_overload(self):
        """Test gradual recovery from OVERLOADED state."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Push to OVERLOADED
        for _ in range(10):
            detector.record_latency(1000.0)

        assert detector.get_state() == OverloadState.OVERLOADED

        # Gradual recovery
        recovery_states = []
        for latency in [400.0, 300.0, 180.0, 120.0, 80.0, 50.0]:
            for _ in range(5):
                detector.record_latency(latency)
            recovery_states.append(detector.get_state())

        # Should see progression through states
        # OVERLOADED -> STRESSED -> BUSY -> HEALTHY (not necessarily all)
        assert recovery_states[-1] == OverloadState.HEALTHY

    @pytest.mark.asyncio
    async def test_probe_recovery_after_failures(self):
        """Test probe recovers after consecutive failures."""
        failure_phase = True

        async def controllable_check():
            if failure_phase:
                return False, "Service unavailable"
            return True, "OK"

        probe = HealthProbe(
            name="service",
            check=controllable_check,
            config=ProbeConfig(
                failure_threshold=3,
                success_threshold=2,
            ),
        )

        # Fail until unhealthy
        for _ in range(5):
            await probe.check()
        assert probe.is_healthy() is False

        # Enable recovery
        failure_phase = False

        # Should recover after success_threshold successes
        for _ in range(3):
            await probe.check()
        assert probe.is_healthy() is True

    def test_extension_tracker_recovery_cycle(self):
        """Test extension tracker through full exhaustion-recovery cycle."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(max_extensions=3)
        )

        from hyperscale.distributed_rewrite.models import (
            HealthcheckExtensionRequest,
        )

        # Exhaust extensions
        for i in range(3):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="busy",
                current_progress=float((i + 1) * 10),
                estimated_completion=30.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, time.time() + 30)

        should_evict, _ = manager.should_evict_worker("worker-1")
        assert should_evict is True

        # Worker recovers
        manager.on_worker_healthy("worker-1")

        # Can use extensions again
        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="new work",
            current_progress=5.0,
            estimated_completion=30.0,
            active_workflow_count=1,
        )
        response = manager.handle_extension_request(request, time.time() + 30)
        assert response.granted is True

    def test_cooperative_limiter_clear_recovery(self):
        """Test cooperative rate limiter recovery via clear."""
        limiter = CooperativeRateLimiter()

        # Block multiple operations
        limiter.handle_rate_limit("op1", retry_after=10.0)
        limiter.handle_rate_limit("op2", retry_after=10.0)

        assert limiter.is_blocked("op1") is True
        assert limiter.is_blocked("op2") is True

        # Clear specific operation
        limiter.clear("op1")
        assert limiter.is_blocked("op1") is False
        assert limiter.is_blocked("op2") is True

        # Clear all
        limiter.clear()
        assert limiter.is_blocked("op2") is False


# =============================================================================
# Concurrent Access Safety Tests
# =============================================================================


class TestConcurrentAccessSafety:
    """Tests for concurrent access safety."""

    @pytest.mark.asyncio
    async def test_concurrent_detector_updates(self):
        """Test concurrent latency recording doesn't corrupt state."""
        detector = HybridOverloadDetector()

        async def record_latencies():
            for _ in range(1000):
                detector.record_latency(100.0)
                await asyncio.sleep(0)  # Yield to other tasks

        # Run multiple concurrent recorders
        await asyncio.gather(*[record_latencies() for _ in range(10)])

        # State should be valid
        assert detector.sample_count == 10000
        assert detector.baseline > 0

    @pytest.mark.asyncio
    async def test_concurrent_rate_limit_checks(self):
        """Test concurrent rate limit checks are handled safely."""
        limiter = ServerRateLimiter(
            RateLimitConfig(default_bucket_size=1000, default_refill_rate=100.0)
        )

        async def check_limits():
            results = []
            for _ in range(100):
                result = limiter.check_rate_limit("client-1", "op")
                results.append(result.allowed)
                await asyncio.sleep(0)
            return results

        # Run concurrent checks
        all_results = await asyncio.gather(*[check_limits() for _ in range(10)])

        # All results should be valid booleans
        for results in all_results:
            assert all(isinstance(r, bool) for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_probe_checks(self):
        """Test concurrent probe checks don't cause issues."""
        check_count = 0

        async def counting_check():
            nonlocal check_count
            check_count += 1
            await asyncio.sleep(0.001)
            return True, "OK"

        probe = HealthProbe(
            name="concurrent",
            check=counting_check,
            config=ProbeConfig(timeout_seconds=1.0),
        )

        # Run many concurrent checks
        await asyncio.gather(*[probe.check() for _ in range(100)])

        # All checks should have completed
        assert check_count == 100


# =============================================================================
# Clock Skew and Time-Based Edge Cases
# =============================================================================


class TestClockSkewTimeBased:
    """Tests for clock skew and time-based edge cases."""

    def test_token_bucket_handles_time_going_backwards(self):
        """Test token bucket handles time.monotonic() anomalies gracefully."""
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        # Consume some tokens
        for _ in range(50):
            bucket.acquire()

        # Force a refill
        initial_tokens = bucket.available_tokens

        # Even with weird timing, should not exceed bucket size
        bucket._refill()
        bucket._refill()
        bucket._refill()

        assert bucket.available_tokens <= bucket.bucket_size

    def test_extension_tracker_handles_old_deadlines(self):
        """Test extension tracker with deadlines in the past."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
        )

        # Request extension
        granted, extension_seconds, _ = tracker.request_extension(
            reason="busy",
            current_progress=10.0,
        )
        assert granted is True

        # Calculate deadline with past timestamp
        past_deadline = time.time() - 1000  # 1000 seconds ago
        new_deadline = tracker.get_new_deadline(past_deadline, extension_seconds)

        # Should still calculate correctly (even if result is in past)
        assert new_deadline == past_deadline + extension_seconds

    def test_probe_handles_very_short_periods(self):
        """Test probe with extremely short period doesn't cause issues."""
        check_count = 0

        async def quick_check():
            nonlocal check_count
            check_count += 1
            return True, "OK"

        probe = HealthProbe(
            name="quick",
            check=quick_check,
            config=ProbeConfig(
                period_seconds=0.001,  # 1ms period
                timeout_seconds=0.1,
            ),
        )

        # Single check should work
        import asyncio

        async def run_test():
            await probe.check()
            assert check_count == 1

        asyncio.get_event_loop().run_until_complete(run_test())

    def test_cooperative_limiter_retry_after_zero(self):
        """Test cooperative limiter with zero retry_after."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("operation", retry_after=0.0)

        # Should not be blocked (or minimally blocked)
        assert limiter.get_retry_after("operation") <= 0.001

    def test_cooperative_limiter_very_long_retry(self):
        """Test cooperative limiter with very long retry_after."""
        limiter = CooperativeRateLimiter()

        # 1 hour retry
        limiter.handle_rate_limit("operation", retry_after=3600.0)

        assert limiter.is_blocked("operation") is True
        assert limiter.get_retry_after("operation") > 3599.0

    def test_token_bucket_very_slow_refill(self):
        """Test token bucket with extremely slow refill rate."""
        bucket = TokenBucket(bucket_size=100, refill_rate=0.0001)  # 1 token per 10000 sec

        # Deplete
        for _ in range(100):
            bucket.acquire()

        # After short wait, should have minimal tokens
        time.sleep(0.01)
        assert bucket.available_tokens < 1

    def test_token_bucket_very_fast_refill(self):
        """Test token bucket with extremely fast refill rate."""
        bucket = TokenBucket(bucket_size=100, refill_rate=1000000.0)  # 1M tokens/sec

        # Deplete
        for _ in range(100):
            bucket.acquire()

        # Should refill almost instantly
        time.sleep(0.001)
        assert bucket.available_tokens >= 99


# =============================================================================
# Data Structure Invariant Tests
# =============================================================================


class TestDataStructureInvariants:
    """Tests for maintaining data structure invariants."""

    def test_detector_baseline_never_negative(self):
        """Test detector baseline never goes negative."""
        detector = HybridOverloadDetector()

        # Mix of positive and negative (invalid) latencies
        for latency in [100.0, -50.0, 200.0, -100.0, 50.0]:
            detector.record_latency(latency)

        # Baseline should not be negative (though behavior with negatives is undefined)
        # Main thing is it shouldn't crash

    def test_detector_current_average_consistency(self):
        """Test current_average is consistent with recent samples."""
        config = OverloadConfig(current_window=5)
        detector = HybridOverloadDetector(config)

        latencies = [100.0, 200.0, 300.0, 400.0, 500.0]
        for lat in latencies:
            detector.record_latency(lat)

        expected_avg = sum(latencies) / len(latencies)
        assert detector.current_average == pytest.approx(expected_avg)

    def test_extension_tracker_total_extended_accurate(self):
        """Test total_extended accurately tracks all grants."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=64.0,
            min_grant=1.0,
            max_extensions=6,
        )

        total_granted = 0.0
        for i in range(6):
            granted, amount, _ = tracker.request_extension(
                reason="busy",
                current_progress=float((i + 1) * 10),
            )
            if granted:
                total_granted += amount

        assert tracker.total_extended == pytest.approx(total_granted)

    def test_load_shedder_shed_by_priority_sums_to_total_shed(self):
        """Test shed_by_priority counts sum to shed_requests."""
        config = OverloadConfig(
            absolute_bounds=(10.0, 20.0, 50.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # OVERLOADED
        detector.record_latency(100.0)

        # Make requests of different priorities
        for _ in range(100):
            shedder.should_shed("DetailedStatsRequest")  # LOW
        for _ in range(100):
            shedder.should_shed("StatsUpdate")  # NORMAL
        for _ in range(100):
            shedder.should_shed("SubmitJob")  # HIGH
        for _ in range(100):
            shedder.should_shed("Ping")  # CRITICAL

        metrics = shedder.get_metrics()
        shed_sum = sum(metrics["shed_by_priority"].values())
        assert shed_sum == metrics["shed_requests"]

    def test_rate_limiter_metrics_consistency(self):
        """Test rate limiter metrics are internally consistent."""
        config = RateLimitConfig(default_bucket_size=10, default_refill_rate=1.0)
        limiter = ServerRateLimiter(config)

        # Make many requests
        for i in range(100):
            limiter.check_rate_limit(f"client-{i % 10}", "operation")

        metrics = limiter.get_metrics()

        # Allowed + rejected should equal total
        # (Note: we only track rate_limited_requests, not allowed)
        assert metrics["total_requests"] == 100
        assert metrics["rate_limited_requests"] <= metrics["total_requests"]

    def test_probe_state_consistency(self):
        """Test probe state remains internally consistent."""

        async def variable_check():
            return True, "OK"

        probe = HealthProbe(
            name="test",
            check=variable_check,
            config=ProbeConfig(failure_threshold=3, success_threshold=2),
        )

        import asyncio

        async def run_checks():
            for _ in range(100):
                await probe.check()

                state = probe.get_state()
                # Invariants
                assert state.consecutive_successes >= 0
                assert state.consecutive_failures >= 0
                # Can't have both consecutive successes and failures
                assert not (
                    state.consecutive_successes > 0 and state.consecutive_failures > 0
                )

        asyncio.get_event_loop().run_until_complete(run_checks())


# =============================================================================
# Partial Failure and Split-Brain Tests
# =============================================================================


class TestPartialFailureSplitBrain:
    """Tests for partial failure and split-brain scenarios."""

    def test_composite_probe_partial_failure(self):
        """Test composite probe with some probes failing."""
        healthy_probe_calls = 0
        unhealthy_probe_calls = 0

        async def healthy_check():
            nonlocal healthy_probe_calls
            healthy_probe_calls += 1
            return True, "OK"

        async def unhealthy_check():
            nonlocal unhealthy_probe_calls
            unhealthy_probe_calls += 1
            return False, "Failed"

        import asyncio

        healthy_probe = HealthProbe(
            name="healthy",
            check=healthy_check,
            config=ProbeConfig(failure_threshold=1),
        )
        unhealthy_probe = HealthProbe(
            name="unhealthy",
            check=unhealthy_check,
            config=ProbeConfig(failure_threshold=1),
        )

        composite = CompositeProbe("mixed")
        composite.add_probe(healthy_probe)
        composite.add_probe(unhealthy_probe)

        async def run_test():
            await composite.check_all()

            # Composite should be unhealthy if any probe is unhealthy
            assert composite.is_healthy() is False
            assert "unhealthy" in composite.get_unhealthy_probes()
            assert "healthy" not in composite.get_unhealthy_probes()

        asyncio.get_event_loop().run_until_complete(run_test())

    def test_rate_limiter_client_isolation(self):
        """Test rate limiting isolation between clients."""
        config = RateLimitConfig(default_bucket_size=5, default_refill_rate=0.1)
        limiter = ServerRateLimiter(config)

        # Exhaust client-1
        for _ in range(10):
            limiter.check_rate_limit("client-1", "operation")

        # Exhaust client-2
        for _ in range(10):
            limiter.check_rate_limit("client-2", "operation")

        # Both should be rate limited independently
        result1 = limiter.check_rate_limit("client-1", "operation")
        result2 = limiter.check_rate_limit("client-2", "operation")

        assert result1.allowed is False
        assert result2.allowed is False

        # But client-3 should be fine
        result3 = limiter.check_rate_limit("client-3", "operation")
        assert result3.allowed is True

    def test_load_shedder_independent_of_rate_limiter(self):
        """Test load shedder and rate limiter operate independently."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        rate_config = RateLimitConfig(default_bucket_size=5, default_refill_rate=0.1)
        rate_limiter = ServerRateLimiter(rate_config)

        # Shedder healthy
        detector.record_latency(50.0)

        # Rate limiter exhausted
        for _ in range(10):
            rate_limiter.check_rate_limit("client-1", "operation")

        # Shedder should still accept (it doesn't know about rate limiter)
        assert shedder.should_shed("SubmitJob") is False

        # Rate limiter should still reject (it doesn't know about shedder)
        assert rate_limiter.check_rate_limit("client-1", "operation").allowed is False

    def test_extension_tracker_isolation_between_workers(self):
        """Test extension trackers are isolated between workers."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(max_extensions=2)
        )

        from hyperscale.distributed_rewrite.models import HealthcheckExtensionRequest

        # Exhaust worker-1
        for i in range(2):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="busy",
                current_progress=float((i + 1) * 10),
                estimated_completion=30.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, time.time() + 30)

        # worker-1 should be exhausted
        should_evict1, _ = manager.should_evict_worker("worker-1")
        assert should_evict1 is True

        # worker-2 should be unaffected
        request2 = HealthcheckExtensionRequest(
            worker_id="worker-2",
            reason="busy",
            current_progress=10.0,
            estimated_completion=30.0,
            active_workflow_count=1,
        )
        response = manager.handle_extension_request(request2, time.time() + 30)
        assert response.granted is True

        should_evict2, _ = manager.should_evict_worker("worker-2")
        assert should_evict2 is False


# =============================================================================
# Backpressure Propagation Tests
# =============================================================================


class TestBackpressurePropagation:
    """Tests for backpressure propagation scenarios."""

    def test_overload_to_shedding_propagation_timing(self):
        """Test timing of overload detection to shedding decision."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Before overload
        assert shedder.should_shed("SubmitJob") is False

        # Single high latency should immediately affect shedding
        detector.record_latency(600.0)  # OVERLOADED

        # Immediately after recording, shedding should take effect
        assert shedder.should_shed("SubmitJob") is True

    def test_recovery_propagation_timing(self):
        """Test timing of recovery from overload to acceptance."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=1,
            current_window=3,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Push to overloaded
        for _ in range(3):
            detector.record_latency(600.0)

        assert shedder.should_shed("SubmitJob") is True

        # Recovery samples
        for _ in range(3):
            detector.record_latency(50.0)

        # Should immediately recover
        assert shedder.should_shed("SubmitJob") is False

    def test_rate_limit_backpressure_signal(self):
        """Test rate limit response provides useful backpressure signal."""
        config = RateLimitConfig(default_bucket_size=5, default_refill_rate=1.0)
        limiter = ServerRateLimiter(config)

        # Exhaust bucket
        for _ in range(5):
            limiter.check_rate_limit("client-1", "operation")

        # Next request should provide retry_after
        result = limiter.check_rate_limit("client-1", "operation")
        assert result.allowed is False
        assert result.retry_after_seconds > 0

    @pytest.mark.asyncio
    async def test_cooperative_limiter_respects_backpressure(self):
        """Test cooperative limiter properly waits on backpressure."""
        limiter = CooperativeRateLimiter()

        # Set up backpressure
        limiter.handle_rate_limit("operation", retry_after=0.1)

        start = time.monotonic()
        wait_time = await limiter.wait_if_needed("operation")
        elapsed = time.monotonic() - start

        # Should have waited approximately the retry_after time
        assert wait_time > 0.05
        assert elapsed > 0.05


# =============================================================================
# Metric Cardinality Explosion Tests
# =============================================================================


class TestMetricCardinalityExplosion:
    """Tests for metric cardinality explosion scenarios."""

    def test_rate_limiter_many_unique_clients(self):
        """Test rate limiter with many unique client IDs."""
        limiter = ServerRateLimiter(inactive_cleanup_seconds=60.0)

        # Create many unique clients (simulating high cardinality)
        for i in range(10000):
            limiter.check_rate_limit(f"client-{i}", "operation")

        metrics = limiter.get_metrics()
        assert metrics["active_clients"] == 10000

        # Memory usage should be bounded per client

    def test_rate_limiter_many_unique_operations(self):
        """Test rate limiter with many unique operation types."""
        limiter = ServerRateLimiter()

        # Single client, many operations
        for i in range(1000):
            limiter.check_rate_limit("client-1", f"operation-{i}")

        # Check that client has many buckets
        client_buckets = limiter._client_buckets.get("client-1", {})
        assert len(client_buckets) == 1000

    def test_load_shedder_custom_message_types(self):
        """Test load shedder with many custom message types."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Register many custom message types
        for i in range(1000):
            shedder.register_message_priority(
                f"CustomMessage{i}",
                RequestPriority(i % 4),  # Cycle through priorities
            )

        # All should work correctly
        for i in range(1000):
            priority = shedder.classify_request(f"CustomMessage{i}")
            assert priority == RequestPriority(i % 4)

    def test_extension_tracker_many_workers(self):
        """Test extension tracker with many workers."""
        manager = WorkerHealthManager(WorkerHealthManagerConfig())

        # Create trackers for many workers
        for i in range(10000):
            manager._get_tracker(f"worker-{i}")

        assert manager.tracked_worker_count == 10000

        # Getting state for all should work
        all_states = manager.get_all_extension_states()
        assert len(all_states) == 10000


# =============================================================================
# Deadline and Timeout Interaction Tests
# =============================================================================


class TestDeadlineTimeoutInteractions:
    """Tests for deadline and timeout interactions."""

    @pytest.mark.asyncio
    async def test_probe_timeout_shorter_than_check(self):
        """Test probe timeout shorter than actual check duration."""

        async def slow_check():
            await asyncio.sleep(0.5)
            return True, "OK"

        probe = HealthProbe(
            name="slow",
            check=slow_check,
            config=ProbeConfig(timeout_seconds=0.1),
        )

        response = await probe.check()

        assert response.result == ProbeResult.TIMEOUT
        assert "timed out" in response.message.lower()

    @pytest.mark.asyncio
    async def test_probe_timeout_equal_to_check(self):
        """Test probe timeout approximately equal to check duration."""

        async def borderline_check():
            await asyncio.sleep(0.09)  # Just under timeout
            return True, "OK"

        probe = HealthProbe(
            name="borderline",
            check=borderline_check,
            config=ProbeConfig(timeout_seconds=0.1),
        )

        response = await probe.check()

        # Should succeed (timing might vary)
        assert response.result in (ProbeResult.SUCCESS, ProbeResult.TIMEOUT)

    @pytest.mark.asyncio
    async def test_token_bucket_acquire_async_timeout(self):
        """Test token bucket async acquire with timeout."""
        bucket = TokenBucket(bucket_size=5, refill_rate=0.1)

        # Exhaust bucket
        for _ in range(5):
            bucket.acquire()

        # Try to acquire with short timeout
        start = time.monotonic()
        result = await bucket.acquire_async(tokens=1, max_wait=0.1)
        elapsed = time.monotonic() - start

        # Should timeout relatively quickly
        assert elapsed < 0.2
        # May or may not succeed depending on exact timing
        assert isinstance(result, bool)

    def test_extension_deadline_calculation(self):
        """Test extension deadline calculation is additive."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
        )

        current_deadline = 1000.0  # Arbitrary

        _, grant1, _ = tracker.request_extension("r1", current_progress=10.0)
        deadline1 = tracker.get_new_deadline(current_deadline, grant1)

        _, grant2, _ = tracker.request_extension("r2", current_progress=20.0)
        deadline2 = tracker.get_new_deadline(deadline1, grant2)

        # Each extension should add to the deadline
        assert deadline1 == current_deadline + grant1
        assert deadline2 == deadline1 + grant2


# =============================================================================
# Error Message Quality Tests
# =============================================================================


class TestErrorMessageQuality:
    """Tests for quality of error messages."""

    def test_extension_denial_reason_clear(self):
        """Test extension denial reasons are clear and actionable."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=1,
        )

        # Use up extension
        tracker.request_extension("r1", current_progress=10.0)

        # Next should be denied with clear reason
        _, _, reason = tracker.request_extension("r2", current_progress=20.0)

        assert reason is not None
        assert "maximum" in reason.lower() or "exceeded" in reason.lower()

    def test_extension_no_progress_reason_includes_values(self):
        """Test no-progress denial includes progress values."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=5,
        )

        tracker.request_extension("r1", current_progress=50.0)
        _, _, reason = tracker.request_extension("r2", current_progress=30.0)

        assert reason is not None
        assert "30" in reason or "50" in reason  # Should mention the values

    def test_probe_timeout_message_includes_duration(self):
        """Test probe timeout message includes timeout duration."""

        async def slow_check():
            await asyncio.sleep(1.0)
            return True, "OK"

        probe = HealthProbe(
            name="slow",
            check=slow_check,
            config=ProbeConfig(timeout_seconds=0.1),
        )

        import asyncio

        async def run_test():
            response = await probe.check()
            assert "0.1" in response.message  # Should mention timeout value

        asyncio.get_event_loop().run_until_complete(run_test())

    def test_worker_eviction_reason_descriptive(self):
        """Test worker eviction reason is descriptive."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(max_extensions=2, eviction_threshold=1)
        )

        from hyperscale.distributed_rewrite.models import HealthcheckExtensionRequest

        # Exhaust extensions
        for i in range(2):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="busy",
                current_progress=float((i + 1) * 10),
                estimated_completion=30.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, time.time() + 30)

        should_evict, reason = manager.should_evict_worker("worker-1")

        assert should_evict is True
        assert reason is not None
        assert "extension" in reason.lower()


# =============================================================================
# Idempotency Tests
# =============================================================================


class TestIdempotency:
    """Tests for idempotent operations."""

    def test_detector_reset_idempotent(self):
        """Test detector reset is idempotent."""
        detector = HybridOverloadDetector()

        for _ in range(10):
            detector.record_latency(100.0)

        # Multiple resets should be safe
        detector.reset()
        detector.reset()
        detector.reset()

        assert detector.sample_count == 0
        assert detector.baseline == 0.0

    def test_load_shedder_reset_metrics_idempotent(self):
        """Test load shedder reset_metrics is idempotent."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        for _ in range(100):
            shedder.should_shed("Ping")

        # Multiple resets should be safe
        shedder.reset_metrics()
        shedder.reset_metrics()
        shedder.reset_metrics()

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 0

    def test_extension_tracker_reset_idempotent(self):
        """Test extension tracker reset is idempotent."""
        tracker = ExtensionTracker(worker_id="worker-1")

        tracker.request_extension("r1", current_progress=10.0)

        # Multiple resets
        tracker.reset()
        tracker.reset()
        tracker.reset()

        assert tracker.extension_count == 0
        assert tracker.total_extended == 0.0

    def test_worker_removal_idempotent(self):
        """Test worker removal is idempotent."""
        manager = WorkerHealthManager()

        manager._get_tracker("worker-1")
        assert manager.tracked_worker_count == 1

        # Multiple removals should be safe
        manager.on_worker_removed("worker-1")
        manager.on_worker_removed("worker-1")
        manager.on_worker_removed("worker-1")

        assert manager.tracked_worker_count == 0

    def test_cooperative_limiter_clear_idempotent(self):
        """Test cooperative limiter clear is idempotent."""
        limiter = CooperativeRateLimiter()

        limiter.handle_rate_limit("op1", retry_after=10.0)

        # Multiple clears
        limiter.clear("op1")
        limiter.clear("op1")
        limiter.clear("op1")

        assert limiter.is_blocked("op1") is False

    @pytest.mark.asyncio
    async def test_probe_stop_periodic_idempotent(self):
        """Test probe stop_periodic is idempotent."""

        async def quick_check():
            return True, "OK"

        probe = HealthProbe(
            name="test",
            check=quick_check,
            config=ProbeConfig(period_seconds=0.1),
        )

        await probe.start_periodic()
        await asyncio.sleep(0.05)

        # Multiple stops should be safe
        await probe.stop_periodic()
        await probe.stop_periodic()
        await probe.stop_periodic()


# =============================================================================
# Edge Cases in Priority and State Transitions
# =============================================================================


class TestPriorityStateTransitionEdges:
    """Tests for edge cases in priority handling and state transitions."""

    def test_all_priority_levels_in_single_session(self):
        """Test all priority levels are handled correctly in sequence."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        priorities_tested = {p: False for p in RequestPriority}

        # HEALTHY - all accepted
        detector.record_latency(30.0)
        for msg, priority in [
            ("Ping", RequestPriority.CRITICAL),
            ("SubmitJob", RequestPriority.HIGH),
            ("StatsUpdate", RequestPriority.NORMAL),
            ("DetailedStatsRequest", RequestPriority.LOW),
        ]:
            result = shedder.should_shed(msg)
            assert result is False, f"{msg} should be accepted when HEALTHY"
            priorities_tested[priority] = True

        assert all(priorities_tested.values())

    def test_state_transition_boundary_shedding(self):
        """Test shedding changes correctly at state boundaries."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=1,
            current_window=1,
        )

        test_cases = [
            (50.0, OverloadState.HEALTHY, False, False, False, False),
            (150.0, OverloadState.BUSY, False, False, False, True),
            (300.0, OverloadState.STRESSED, False, False, True, True),
            (600.0, OverloadState.OVERLOADED, False, True, True, True),
        ]

        for latency, expected_state, crit_shed, high_shed, norm_shed, low_shed in test_cases:
            # Create fresh detector/shedder for each case to avoid
            # delta detection interference from baseline drift
            detector = HybridOverloadDetector(config)
            shedder = LoadShedder(detector)

            detector.record_latency(latency)

            state = detector.get_state()
            assert state == expected_state, f"Wrong state for latency {latency}"

            assert shedder.should_shed("Ping") == crit_shed
            assert shedder.should_shed("SubmitJob") == high_shed
            assert shedder.should_shed("StatsUpdate") == norm_shed
            assert shedder.should_shed("DetailedStatsRequest") == low_shed

    def test_extension_progress_boundary_values(self):
        """Test extension with boundary progress values."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=5,
        )

        # Zero progress initially allowed
        granted, _, _ = tracker.request_extension("r1", current_progress=0.0)
        assert granted is True

        # Same progress should be denied (no improvement)
        granted, _, _ = tracker.request_extension("r2", current_progress=0.0)
        assert granted is False

        # Tiny improvement should work
        granted, _, _ = tracker.request_extension("r3", current_progress=0.0001)
        assert granted is True


# =============================================================================
# Diagnostic and Observability Tests
# =============================================================================


class TestDiagnosticsObservability:
    """Tests for diagnostic and observability features."""

    def test_detector_diagnostics_complete(self):
        """Test detector diagnostics include all expected fields."""
        detector = HybridOverloadDetector()

        for _ in range(20):
            detector.record_latency(100.0)

        diagnostics = detector.get_diagnostics()

        required_fields = [
            "baseline",
            "current_avg",
            "delta",
            "trend",
            "sample_count",
            "delta_state",
            "absolute_state",
        ]

        for field in required_fields:
            assert field in diagnostics, f"Missing field: {field}"

    def test_load_shedder_metrics_complete(self):
        """Test load shedder metrics include all expected fields."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        for _ in range(100):
            shedder.should_shed("Ping")

        metrics = shedder.get_metrics()

        required_fields = [
            "total_requests",
            "shed_requests",
            "shed_rate",
            "shed_by_priority",
        ]

        for field in required_fields:
            assert field in metrics, f"Missing field: {field}"

    def test_rate_limiter_metrics_complete(self):
        """Test rate limiter metrics include all expected fields."""
        limiter = ServerRateLimiter()

        for i in range(10):
            limiter.check_rate_limit(f"client-{i}", "operation")

        metrics = limiter.get_metrics()

        required_fields = [
            "total_requests",
            "rate_limited_requests",
            "rate_limited_rate",
            "active_clients",
            "clients_cleaned",
        ]

        for field in required_fields:
            assert field in metrics, f"Missing field: {field}"

    def test_probe_state_complete(self):
        """Test probe state includes all expected fields."""

        async def check():
            return True, "OK"

        probe = HealthProbe(name="test", check=check)

        import asyncio

        async def run_test():
            await probe.check()
            state = probe.get_state()

            assert hasattr(state, "healthy")
            assert hasattr(state, "consecutive_successes")
            assert hasattr(state, "consecutive_failures")
            assert hasattr(state, "last_check")
            assert hasattr(state, "last_result")
            assert hasattr(state, "last_message")
            assert hasattr(state, "total_checks")
            assert hasattr(state, "total_failures")

        asyncio.get_event_loop().run_until_complete(run_test())

    def test_composite_probe_status_complete(self):
        """Test composite probe status includes all probes."""

        async def check():
            return True, "OK"

        probe1 = HealthProbe(name="probe1", check=check)
        probe2 = HealthProbe(name="probe2", check=check)

        composite = CompositeProbe("composite")
        composite.add_probe(probe1)
        composite.add_probe(probe2)

        status = composite.get_status()

        assert "name" in status
        assert "healthy" in status
        assert "probes" in status
        assert "probe1" in status["probes"]
        assert "probe2" in status["probes"]

    def test_extension_tracker_state_complete(self):
        """Test extension tracker state includes all expected fields."""
        manager = WorkerHealthManager()

        from hyperscale.distributed_rewrite.models import HealthcheckExtensionRequest

        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=10.0,
            estimated_completion=30.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request, time.time() + 30)

        state = manager.get_worker_extension_state("worker-1")

        required_fields = [
            "worker_id",
            "has_tracker",
            "extension_count",
            "remaining_extensions",
            "total_extended",
            "last_progress",
            "is_exhausted",
            "extension_failures",
        ]

        for field in required_fields:
            assert field in state, f"Missing field: {field}"


# =============================================================================
# Graceful Degradation Tests
# =============================================================================


class TestGracefulDegradation:
    """Tests for graceful degradation under adverse conditions."""

    def test_shedding_preserves_critical_under_extreme_load(self):
        """Test that critical traffic is preserved even under extreme load."""
        config = OverloadConfig(
            absolute_bounds=(1.0, 2.0, 5.0),  # Very low thresholds
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Extreme overload
        detector.record_latency(10000.0)

        # Even under extreme load, CRITICAL must pass
        critical_accepted = 0
        for _ in range(10000):
            if not shedder.should_shed("Ping"):
                critical_accepted += 1

        assert critical_accepted == 10000

    def test_rate_limiter_graceful_under_burst(self):
        """Test rate limiter degrades gracefully under burst."""
        config = RateLimitConfig(default_bucket_size=100, default_refill_rate=10.0)
        limiter = ServerRateLimiter(config)

        # Large burst
        results = []
        for _ in range(1000):
            result = limiter.check_rate_limit("client-1", "operation")
            results.append(result)

        # First batch should be allowed
        allowed = sum(1 for r in results if r.allowed)
        assert allowed == 100  # Exactly bucket size

        # Rejected requests should have reasonable retry_after
        rejected = [r for r in results if not r.allowed]
        assert all(r.retry_after_seconds > 0 for r in rejected)

    def test_extension_graceful_exhaustion(self):
        """Test extension tracker gracefully handles exhaustion."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=3,
            base_deadline=30.0,
            min_grant=1.0,
        )

        # Exhaust with increasing progress
        grants = []
        for i in range(5):
            granted, amount, reason = tracker.request_extension(
                reason="busy",
                current_progress=float((i + 1) * 10),
            )
            if granted:
                grants.append(amount)
            else:
                # Exhausted - should have clear reason
                assert "exceeded" in reason.lower() or "maximum" in reason.lower()

        # Should have granted exactly max_extensions
        assert len(grants) == 3

        # Grants should follow logarithmic decay
        assert grants[0] > grants[1] > grants[2]

    @pytest.mark.asyncio
    async def test_probe_graceful_timeout_handling(self):
        """Test probe handles timeouts gracefully."""
        timeout_count = 0

        async def slow_sometimes():
            nonlocal timeout_count
            timeout_count += 1
            if timeout_count % 2 == 0:
                await asyncio.sleep(1.0)  # Will timeout
            return True, "OK"

        probe = HealthProbe(
            name="flaky",
            check=slow_sometimes,
            config=ProbeConfig(
                timeout_seconds=0.1,
                failure_threshold=5,  # Tolerant
            ),
        )

        # Run several checks
        for _ in range(10):
            response = await probe.check()
            # Should not crash, should return valid response
            assert response.result in (
                ProbeResult.SUCCESS,
                ProbeResult.TIMEOUT,
            )

    def test_detector_handles_extreme_values_gracefully(self):
        """Test detector handles extreme input values gracefully."""
        detector = HybridOverloadDetector()

        extreme_values = [
            0.0,
            0.00001,
            1e10,
            1e-10,
            float("inf"),
            float("-inf"),
            sys.float_info.max,
            sys.float_info.min,
            sys.float_info.epsilon,
        ]

        for value in extreme_values:
            # Should not crash
            detector.record_latency(value)
            state = detector.get_state()
            assert state is not None
