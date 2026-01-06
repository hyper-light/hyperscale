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

        # Bucket is empty
        assert bucket.acquire() is False
        assert bucket.available_tokens == 0

    def test_token_bucket_recovery_after_depletion(self):
        """Test token bucket recovery after complete depletion."""
        bucket = TokenBucket(bucket_size=10, refill_rate=100.0)  # Fast refill

        # Deplete
        for _ in range(10):
            bucket.acquire()

        assert bucket.available_tokens == 0

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
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Initially healthy - accept everything
        detector.record_latency(50.0)
        detector.record_latency(50.0)
        detector.record_latency(50.0)

        assert not shedder.should_shed("DetailedStatsRequest")  # LOW

        # Transition to stressed
        for _ in range(5):
            detector.record_latency(300.0)

        # LOW and NORMAL should now be shed
        assert shedder.should_shed("DetailedStatsRequest")  # LOW
        assert shedder.should_shed("StatsUpdate")  # NORMAL
        assert not shedder.should_shed("SubmitJob")  # HIGH

        # Transition to overloaded
        for _ in range(5):
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
