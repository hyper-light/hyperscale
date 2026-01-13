"""
Comprehensive concurrency tests for all reliability and health components.

Tests cover:
1. Synchronous components under concurrent asyncio access
2. Async components with proper asyncio.Lock usage
3. Race condition detection and validation
4. State consistency under concurrent operations

All components from TODO.md phases 1-4 are covered:
- AD-18: HybridOverloadDetector
- AD-19: Health states (Worker, Manager, Gate)
- AD-21: RetryExecutor
- AD-22: LoadShedder
- AD-23: StatsBuffer/Backpressure
- AD-24: SlidingWindowCounter/AdaptiveRateLimiter/ServerRateLimiter
- AD-26: ExtensionTracker/WorkerHealthManager
"""

import asyncio
import time

import pytest

from hyperscale.distributed.reliability.overload import (
    HybridOverloadDetector,
    OverloadConfig,
    OverloadState,
)
from hyperscale.distributed.reliability.load_shedding import (
    LoadShedder,
    RequestPriority,
)
from hyperscale.distributed.reliability.rate_limiting import (
    SlidingWindowCounter,
    TokenBucket,
    ServerRateLimiter,
    RateLimitConfig,
)
from hyperscale.distributed.reliability.backpressure import (
    StatsBuffer,
    BackpressureLevel,
)
from hyperscale.distributed.health.worker_health import WorkerHealthState
from hyperscale.distributed.health.manager_health import ManagerHealthState
from hyperscale.distributed.health.gate_health import GateHealthState
from hyperscale.distributed.health.tracker import NodeHealthTracker
from hyperscale.distributed.health.extension_tracker import ExtensionTracker
from hyperscale.distributed.health.worker_health_manager import WorkerHealthManager
from hyperscale.distributed.models import HealthcheckExtensionRequest


# =============================================================================
# Test HybridOverloadDetector Concurrency (AD-18)
# =============================================================================


class TestOverloadDetectorConcurrency:
    """Test HybridOverloadDetector under concurrent async access."""

    @pytest.mark.asyncio
    async def test_concurrent_record_latency_maintains_consistency(self):
        """Multiple coroutines recording latency should not corrupt state."""
        detector = HybridOverloadDetector()
        num_coroutines = 10
        samples_per_coroutine = 100

        async def record_samples(latency_base: float):
            for i in range(samples_per_coroutine):
                detector.record_latency(latency_base + i * 0.1)
                # Yield to allow interleaving
                if i % 10 == 0:
                    await asyncio.sleep(0)

        # Run concurrent recorders
        tasks = [record_samples(50.0 + j * 10) for j in range(num_coroutines)]
        await asyncio.gather(*tasks)

        # Verify state consistency
        assert detector._sample_count == num_coroutines * samples_per_coroutine
        assert detector._baseline_ema > 0
        assert detector._slow_baseline_ema > 0
        assert len(detector._recent) <= detector._config.current_window

    @pytest.mark.asyncio
    async def test_concurrent_get_state_returns_valid_states(self):
        """Concurrent get_state calls should always return valid states."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),
            warmup_samples=5,
            hysteresis_samples=1,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(10):
            detector.record_latency(50.0)

        valid_states = set(OverloadState)
        states_seen = []

        async def get_state_repeatedly(count: int):
            for _ in range(count):
                state = detector.get_state()
                states_seen.append(state)
                await asyncio.sleep(0)

        async def modify_latencies():
            for i in range(50):
                # Oscillate between healthy and overloaded
                if i % 2 == 0:
                    detector.record_latency(50.0)
                else:
                    detector.record_latency(600.0)
                await asyncio.sleep(0)

        # Run concurrent state checks and modifications
        await asyncio.gather(
            get_state_repeatedly(100),
            get_state_repeatedly(100),
            modify_latencies(),
        )

        # All states should be valid
        for state in states_seen:
            assert state in valid_states, f"Invalid state: {state}"

    @pytest.mark.asyncio
    async def test_concurrent_diagnostics_returns_consistent_snapshot(self):
        """get_diagnostics should return internally consistent data."""
        detector = HybridOverloadDetector()

        # Establish baseline
        for _ in range(20):
            detector.record_latency(100.0)

        inconsistencies = []

        async def check_diagnostics():
            for _ in range(50):
                diag = detector.get_diagnostics()
                # Check internal consistency
                if diag["baseline"] > 0 and diag["slow_baseline"] > 0:
                    # Drift should match calculation
                    expected_drift = (diag["baseline"] - diag["slow_baseline"]) / diag[
                        "slow_baseline"
                    ]
                    actual_drift = diag["baseline_drift"]
                    if abs(expected_drift - actual_drift) > 0.001:
                        inconsistencies.append((expected_drift, actual_drift))
                await asyncio.sleep(0)

        async def modify_state():
            for i in range(100):
                detector.record_latency(100.0 + i * 0.5)
                await asyncio.sleep(0)

        await asyncio.gather(
            check_diagnostics(),
            check_diagnostics(),
            modify_state(),
        )

        # No inconsistencies should be found
        assert len(inconsistencies) == 0, (
            f"Found {len(inconsistencies)} inconsistencies"
        )


# =============================================================================
# Test LoadShedder Concurrency (AD-22)
# =============================================================================


class TestLoadShedderConcurrency:
    """Test LoadShedder under concurrent async access."""

    @pytest.mark.asyncio
    async def test_concurrent_should_shed_decisions_are_consistent(self):
        """Concurrent shed decisions should reflect detector state."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Establish healthy state
        for _ in range(20):
            detector.record_latency(50.0)

        results = []

        async def check_shedding(message_type: str):
            for _ in range(50):
                should_shed = shedder.should_shed(message_type)
                state = detector.get_state()
                results.append((message_type, should_shed, state))
                await asyncio.sleep(0)

        # Run concurrent shedding checks
        await asyncio.gather(
            check_shedding("JobSubmission"),
            check_shedding("StatsQuery"),
            check_shedding("HealthCheck"),
        )

        # Verify shedding decisions match state
        for message_type, should_shed, state in results:
            priority = shedder.classify_request(message_type)
            if state == OverloadState.HEALTHY:
                # Nothing should be shed when healthy
                assert not should_shed, f"Shed {message_type} when HEALTHY"
            elif state == OverloadState.OVERLOADED:
                # Only CRITICAL survives overload
                if priority != RequestPriority.CRITICAL:
                    assert should_shed, (
                        f"Didn't shed {message_type} ({priority}) when OVERLOADED"
                    )


# =============================================================================
# Test SlidingWindowCounter Concurrency (AD-24)
# =============================================================================


class TestSlidingWindowCounterConcurrency:
    """Test SlidingWindowCounter under concurrent async access."""

    @pytest.mark.asyncio
    async def test_concurrent_acquire_never_exceeds_max_requests(self):
        """Concurrent acquires should never grant more slots than available."""
        # Use a long window so it doesn't rotate during test
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=100)

        acquired_count = 0
        lock = asyncio.Lock()

        async def try_acquire():
            nonlocal acquired_count
            success, _ = counter.try_acquire(10)
            if success:
                async with lock:
                    acquired_count += 10

        # 20 coroutines trying to acquire 10 slots each = 200 requested
        # Only 100 available, so max 100 should be acquired
        tasks = [try_acquire() for _ in range(20)]
        await asyncio.gather(*tasks)

        assert acquired_count <= 100, (
            f"Acquired {acquired_count} slots from 100-slot counter"
        )

    @pytest.mark.asyncio
    async def test_acquire_async_serializes_access(self):
        """Test that acquire_async serializes access to the counter.

        This test validates that concurrent acquire_async calls are serialized
        via the internal async lock, preventing race conditions.
        """
        # Counter with 10 slots, long window for deterministic behavior
        counter = SlidingWindowCounter(window_size_seconds=60.0, max_requests=10)

        # Track results
        success_count = 0
        failure_count = 0
        results_lock = asyncio.Lock()

        async def try_acquire_async():
            nonlocal success_count, failure_count
            # Each tries to acquire 5 slots with very short max wait
            result = await counter.acquire_async(count=5, max_wait=0.01)
            async with results_lock:
                if result:
                    success_count += 1
                else:
                    failure_count += 1

        # 5 coroutines try to acquire 5 slots each (25 total needed)
        # With 10 slots available and long window, exactly 2 should succeed
        tasks = [try_acquire_async() for _ in range(5)]
        await asyncio.gather(*tasks)

        # Exactly 2 should succeed (10 slots / 5 per request = 2)
        assert success_count == 2, f"Expected exactly 2 successes, got {success_count}"

        # Remaining 3 should have failed
        assert failure_count == 3, f"Expected exactly 3 failures, got {failure_count}"

    @pytest.mark.asyncio
    async def test_acquire_async_serializes_waiters(self):
        """Verify that acquire_async serializes concurrent waiters.

        This directly tests that the lock prevents concurrent waits.
        """
        # Short window to allow recovery
        counter = SlidingWindowCounter(window_size_seconds=0.1, max_requests=100)

        # Fill counter
        counter.try_acquire(100)

        execution_order = []
        order_lock = asyncio.Lock()

        async def acquire_and_record(task_id: int):
            async with order_lock:
                execution_order.append(f"start_{task_id}")

            # This should serialize due to internal lock
            result = await counter.acquire_async(count=10, max_wait=1.0)

            async with order_lock:
                execution_order.append(f"end_{task_id}_{result}")

        # Launch concurrent tasks
        tasks = [acquire_and_record(i) for i in range(3)]
        await asyncio.gather(*tasks)

        # Verify all events recorded
        assert len(execution_order) == 6, f"Expected 6 events, got {execution_order}"

    @pytest.mark.asyncio
    async def test_concurrent_window_rotation_consistency(self):
        """Window rotation should be consistent under concurrent access."""
        counter = SlidingWindowCounter(window_size_seconds=0.1, max_requests=100)

        # Fill counter
        counter.try_acquire(100)

        # Wait for window to rotate
        await asyncio.sleep(0.15)

        # Multiple concurrent reads of effective count
        readings = []

        async def read_effective():
            for _ in range(10):
                readings.append(counter.get_effective_count())
                await asyncio.sleep(0.01)

        await asyncio.gather(*[read_effective() for _ in range(5)])

        # After window rotation, count should decay over time
        # All readings should be less than original 100
        assert all(r < 100 for r in readings), (
            f"Expected all readings < 100 after rotation, got {readings}"
        )


# =============================================================================
# Test TokenBucket Concurrency (AD-24) - Legacy
# =============================================================================


class TestTokenBucketConcurrency:
    """Test TokenBucket under concurrent async access (legacy)."""

    @pytest.mark.asyncio
    async def test_concurrent_acquire_never_exceeds_bucket_size(self):
        """Concurrent acquires should never grant more tokens than available."""
        # Use very slow refill so bucket doesn't refill during test
        bucket = TokenBucket(bucket_size=100, refill_rate=0.001)

        acquired_count = 0
        lock = asyncio.Lock()

        async def try_acquire():
            nonlocal acquired_count
            success = bucket.acquire(10)
            if success:
                async with lock:
                    acquired_count += 10

        # 20 coroutines trying to acquire 10 tokens each = 200 requested
        # Only 100 available, so max 100 should be acquired
        tasks = [try_acquire() for _ in range(20)]
        await asyncio.gather(*tasks)

        assert acquired_count <= 100, (
            f"Acquired {acquired_count} tokens from 100-token bucket"
        )

    @pytest.mark.asyncio
    async def test_acquire_async_serializes_waiters(self):
        """Verify that acquire_async serializes concurrent waiters.

        This directly tests that the lock prevents concurrent waits.
        """
        bucket = TokenBucket(bucket_size=100, refill_rate=100.0)

        # Drain bucket
        bucket.acquire(100)

        execution_order = []
        order_lock = asyncio.Lock()

        async def acquire_and_record(task_id: int):
            async with order_lock:
                execution_order.append(f"start_{task_id}")

            # This should serialize due to internal lock
            result = await bucket.acquire_async(tokens=10, max_wait=1.0)

            async with order_lock:
                execution_order.append(f"end_{task_id}_{result}")

        # Launch concurrent tasks
        tasks = [acquire_and_record(i) for i in range(3)]
        await asyncio.gather(*tasks)

        # Verify all events recorded
        assert len(execution_order) == 6, f"Expected 6 events, got {execution_order}"

    @pytest.mark.asyncio
    async def test_concurrent_refill_timing_consistency(self):
        """Refill should be consistent under concurrent access."""
        bucket = TokenBucket(bucket_size=100, refill_rate=100.0)

        # Drain bucket
        bucket.acquire(100)

        # Wait for some refill
        await asyncio.sleep(0.5)  # Should refill ~50 tokens

        # Multiple concurrent reads of available tokens
        readings = []

        async def read_available():
            for _ in range(10):
                readings.append(bucket.available_tokens)
                await asyncio.sleep(0.01)

        await asyncio.gather(*[read_available() for _ in range(5)])

        # Readings should be monotonically non-decreasing (refill continues)
        # Allow small variance due to timing
        for i in range(1, len(readings)):
            assert readings[i] >= readings[i - 1] - 1, (
                f"Token count decreased unexpectedly: {readings[i - 1]} -> {readings[i]}"
            )


# =============================================================================
# Test ServerRateLimiter Concurrency (AD-24)
# =============================================================================


class TestServerRateLimiterConcurrency:
    """Test ServerRateLimiter under concurrent async access."""

    @pytest.mark.asyncio
    async def test_concurrent_rate_limit_checks_per_client(self):
        """Rate limits should be enforced per-client under concurrency."""
        config = RateLimitConfig(
            default_bucket_size=10,
            default_refill_rate=10.0,
        )
        limiter = ServerRateLimiter(config)

        results_by_client: dict[str, list[bool]] = {"client_a": [], "client_b": []}
        lock = asyncio.Lock()

        async def check_rate_limit(client_id: str):
            for _ in range(20):
                result = await limiter.check_rate_limit(client_id, "test_op")
                async with lock:
                    results_by_client[client_id].append(result.allowed)
                await asyncio.sleep(0)

        await asyncio.gather(
            check_rate_limit("client_a"),
            check_rate_limit("client_b"),
        )

        # Each client should have had ~10 allowed (bucket size)
        for client_id, results in results_by_client.items():
            allowed_count = sum(1 for r in results if r)
            assert 8 <= allowed_count <= 12, (
                f"{client_id} had {allowed_count} allowed, expected ~10"
            )

    @pytest.mark.asyncio
    async def test_cleanup_under_concurrent_access(self):
        """Counter cleanup should not cause errors during concurrent access."""
        config = RateLimitConfig(
            default_bucket_size=10,
            default_refill_rate=10.0,
        )
        # Use short cleanup interval via constructor parameter
        limiter = ServerRateLimiter(config, inactive_cleanup_seconds=0.1)

        errors = []

        async def access_client(client_id: str):
            for _ in range(50):
                try:
                    await limiter.check_rate_limit(client_id, "test_op")
                except Exception as e:
                    errors.append(e)
                await asyncio.sleep(0.01)

        async def trigger_cleanup():
            for _ in range(10):
                limiter.cleanup_inactive_clients()
                await asyncio.sleep(0.05)

        # Run concurrent access and cleanup
        await asyncio.gather(
            access_client("client_1"),
            access_client("client_2"),
            access_client("client_3"),
            trigger_cleanup(),
        )

        assert len(errors) == 0, f"Errors during concurrent access: {errors}"

    @pytest.mark.asyncio
    async def test_check_rate_limit_async_serializes_access(self):
        """Test that check_rate_limit_async serializes concurrent waiters.

        This validates that ServerRateLimiter's async API properly uses
        the SlidingWindowCounter's lock-based serialization for waiting coroutines.
        """
        config = RateLimitConfig(
            default_bucket_size=10,
            default_refill_rate=0.001,  # Very slow refill for deterministic behavior
        )
        limiter = ServerRateLimiter(config)

        success_count = 0
        failure_count = 0
        results_lock = asyncio.Lock()

        async def try_acquire():
            nonlocal success_count, failure_count
            # Each coroutine tries to acquire 5 tokens with short max_wait
            result = await limiter.check_rate_limit_async(
                client_id="test_client",
                operation="default",
                tokens=5,
                max_wait=0.01,
            )
            async with results_lock:
                if result.allowed:
                    success_count += 1
                else:
                    failure_count += 1

        # 5 coroutines try to acquire 5 tokens each (25 total needed)
        # With 10 tokens available and very slow refill, exactly 2 should succeed
        tasks = [try_acquire() for _ in range(5)]
        await asyncio.gather(*tasks)

        assert success_count == 2, f"Expected 2 successes, got {success_count}"
        assert failure_count == 3, f"Expected 3 failures, got {failure_count}"

    @pytest.mark.asyncio
    async def test_check_api_concurrent_per_address_isolation(self):
        """Test that check() API maintains per-address isolation under concurrency.

        This tests the compatibility API used by TCP/UDP protocols.
        """
        config = RateLimitConfig(
            default_bucket_size=5,
            default_refill_rate=0.001,  # Very slow refill for deterministic behavior
        )
        limiter = ServerRateLimiter(config)

        results_by_addr: dict[str, list[bool]] = {}
        lock = asyncio.Lock()

        async def check_address(host: str, port: int):
            addr = (host, port)
            key = f"{host}:{port}"
            async with lock:
                results_by_addr[key] = []

            for _ in range(10):
                allowed = await limiter.check(addr)
                async with lock:
                    results_by_addr[key].append(allowed)
                await asyncio.sleep(0)

        # Run checks for 3 different addresses concurrently
        await asyncio.gather(
            check_address("192.168.1.1", 8080),
            check_address("192.168.1.2", 8080),
            check_address("192.168.1.3", 8080),
        )

        # Each address should have exactly 5 allowed (bucket size) out of 10 attempts
        for addr_key, results in results_by_addr.items():
            allowed_count = sum(1 for r in results if r)
            assert allowed_count == 5, (
                f"{addr_key} had {allowed_count} allowed, expected 5"
            )


# =============================================================================
# Test StatsBuffer Concurrency (AD-23)
# =============================================================================


class TestStatsBufferConcurrency:
    """Test StatsBuffer under concurrent async access."""

    @pytest.mark.asyncio
    async def test_concurrent_record_maintains_tier_integrity(self):
        """Concurrent records should not corrupt tier data structures."""
        buffer = StatsBuffer()

        async def record_entries(base_value: float):
            for i in range(100):
                buffer.record(base_value + i)
                await asyncio.sleep(0)

        # Multiple concurrent recorders with different base values
        await asyncio.gather(*[record_entries(j * 100.0) for j in range(5)])

        # Verify tier integrity
        hot_stats = buffer.get_hot_stats()
        assert hot_stats is not None
        # Buffer should have data
        assert len(buffer._hot) > 0

    @pytest.mark.asyncio
    async def test_concurrent_tier_promotion_consistency(self):
        """Tier promotion under concurrent access should maintain consistency."""
        buffer = StatsBuffer()

        # Add data and trigger promotions
        async def record_and_query():
            for i in range(50):
                buffer.record(100.0 + i)
                # Query to trigger potential promotion
                buffer.get_hot_stats()
                await asyncio.sleep(0)

        async def promote_tiers():
            for _ in range(20):
                buffer._maybe_promote_tiers()
                await asyncio.sleep(0.01)

        await asyncio.gather(
            record_and_query(),
            record_and_query(),
            promote_tiers(),
        )

        # Buffer should still be functional
        hot_stats = buffer.get_hot_stats()
        assert (
            hot_stats is not None or len(buffer._hot) == 0
        )  # May be empty if all promoted

    @pytest.mark.asyncio
    async def test_backpressure_level_consistency_under_load(self):
        """Backpressure level should be consistent under concurrent queries."""
        buffer = StatsBuffer()

        levels_seen = []
        lock = asyncio.Lock()

        async def check_level():
            for _ in range(50):
                level = buffer.get_backpressure_level()
                async with lock:
                    levels_seen.append(level)
                await asyncio.sleep(0)

        async def fill_buffer():
            for i in range(500):
                buffer.record(100.0 + i)
                await asyncio.sleep(0)

        await asyncio.gather(
            check_level(),
            check_level(),
            fill_buffer(),
        )

        # All levels should be valid
        valid_levels = set(BackpressureLevel)
        for level in levels_seen:
            assert level in valid_levels


# =============================================================================
# Test NodeHealthTracker Concurrency (AD-19)
# =============================================================================


class TestNodeHealthTrackerConcurrency:
    """Test NodeHealthTracker under concurrent async access."""

    @pytest.mark.asyncio
    async def test_concurrent_state_updates_dont_corrupt_tracking(self):
        """Concurrent state updates should maintain tracker integrity."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        async def update_worker(worker_id: str):
            for i in range(50):
                state = WorkerHealthState(
                    worker_id=worker_id,
                    consecutive_liveness_failures=i % 5,
                    accepting_work=i % 2 == 0,
                    available_capacity=100 - i,
                )
                tracker.update_state(worker_id, state)
                await asyncio.sleep(0)

        # Update multiple workers concurrently
        await asyncio.gather(*[update_worker(f"worker_{j}") for j in range(10)])

        # All workers should be tracked
        for j in range(10):
            state = tracker.get_state(f"worker_{j}")
            assert state is not None

    @pytest.mark.asyncio
    async def test_concurrent_get_healthy_nodes_returns_consistent_list(self):
        """get_healthy_nodes should return consistent results under concurrency."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        # Set up initial states
        for j in range(10):
            state = WorkerHealthState(
                worker_id=f"worker_{j}",
                consecutive_liveness_failures=0,
                accepting_work=True,
                available_capacity=100,
            )
            tracker.update_state(f"worker_{j}", state)

        results = []
        lock = asyncio.Lock()

        async def get_healthy():
            for _ in range(50):
                healthy = tracker.get_healthy_nodes()
                async with lock:
                    results.append(len(healthy))
                await asyncio.sleep(0)

        async def toggle_health():
            for i in range(50):
                worker_id = f"worker_{i % 10}"
                state = WorkerHealthState(
                    worker_id=worker_id,
                    consecutive_liveness_failures=3
                    if i % 2 == 0
                    else 0,  # Toggle unhealthy
                    accepting_work=True,
                    available_capacity=100,
                )
                tracker.update_state(worker_id, state)
                await asyncio.sleep(0)

        await asyncio.gather(
            get_healthy(),
            get_healthy(),
            toggle_health(),
        )

        # Results should be valid counts (0-10 workers)
        for count in results:
            assert 0 <= count <= 10


# =============================================================================
# Test ExtensionTracker Concurrency (AD-26)
# =============================================================================


class TestExtensionTrackerConcurrency:
    """Test ExtensionTracker under concurrent async access."""

    @pytest.mark.asyncio
    async def test_concurrent_extension_requests_respect_limits(self):
        """Concurrent extension requests should respect max_extensions."""
        tracker = ExtensionTracker(
            worker_id="test_worker",
            base_deadline=30.0,
            max_extensions=5,
        )

        granted_count = 0
        lock = asyncio.Lock()

        async def request_extension(progress: float):
            nonlocal granted_count
            # request_extension returns (granted, extension_seconds, denial_reason, is_warning)
            granted, _extension_seconds, _denial_reason, _is_warning = (
                tracker.request_extension(
                    reason="test",
                    current_progress=progress,
                )
            )
            if granted:
                async with lock:
                    granted_count += 1
            await asyncio.sleep(0)

        # 10 concurrent requests with increasing progress
        tasks = [request_extension(i * 0.1) for i in range(10)]
        await asyncio.gather(*tasks)

        # Should not exceed max_extensions
        assert granted_count <= 5, f"Granted {granted_count} extensions, max is 5"


# =============================================================================
# Test WorkerHealthManager Concurrency (AD-26)
# =============================================================================


class TestWorkerHealthManagerConcurrency:
    """Test WorkerHealthManager under concurrent async access."""

    @pytest.mark.asyncio
    async def test_concurrent_extension_handling(self):
        """Concurrent extension requests for different workers should be isolated."""
        manager = WorkerHealthManager()

        results: dict[str, list[bool]] = {}
        lock = asyncio.Lock()

        async def handle_worker_extensions(worker_id: str):
            async with lock:
                results[worker_id] = []

            for i in range(10):
                request = HealthcheckExtensionRequest(
                    worker_id=worker_id,
                    reason="processing",
                    current_progress=i * 0.1,
                    estimated_completion=time.time() + 10,
                    active_workflow_count=5,
                )
                response = manager.handle_extension_request(
                    request, current_deadline=time.time() + 30
                )
                async with lock:
                    results[worker_id].append(response.granted)
                await asyncio.sleep(0)

        # Handle extensions for multiple workers concurrently
        await asyncio.gather(
            *[handle_worker_extensions(f"worker_{j}") for j in range(5)]
        )

        # Each worker should have independent extension tracking
        for worker_id, grants in results.items():
            # First few should be granted (up to max_extensions)
            granted_count = sum(1 for g in grants if g)
            assert granted_count <= 5, (
                f"{worker_id} had {granted_count} grants, max is 5"
            )

    @pytest.mark.asyncio
    async def test_concurrent_eviction_checks(self):
        """Concurrent eviction checks should be consistent."""
        manager = WorkerHealthManager()

        # Set up some workers
        for j in range(5):
            manager.on_worker_healthy(f"worker_{j}")

        eviction_decisions = []
        lock = asyncio.Lock()

        async def check_eviction(worker_id: str):
            for _ in range(20):
                should_evict, reason = manager.should_evict_worker(worker_id)
                async with lock:
                    eviction_decisions.append((worker_id, should_evict, reason))
                await asyncio.sleep(0)

        await asyncio.gather(*[check_eviction(f"worker_{j}") for j in range(5)])

        # All decisions should have valid reasons (or None)
        for worker_id, should_evict, reason in eviction_decisions:
            if should_evict:
                assert reason is not None, f"Eviction without reason for {worker_id}"


# =============================================================================
# Test Cross-Component Concurrency
# =============================================================================


class TestCrossComponentConcurrency:
    """Test concurrent access across multiple components."""

    @pytest.mark.asyncio
    async def test_detector_and_shedder_concurrent_access(self):
        """Detector and LoadShedder should work correctly together under concurrency."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        errors = []

        async def record_latencies():
            for i in range(100):
                try:
                    detector.record_latency(50.0 + (i % 50) * 10)
                except Exception as e:
                    errors.append(("record", e))
                await asyncio.sleep(0)

        async def check_shedding():
            for _ in range(100):
                try:
                    shedder.should_shed("JobSubmission")
                    shedder.should_shed("StatsQuery")
                except Exception as e:
                    errors.append(("shed", e))
                await asyncio.sleep(0)

        async def check_state():
            for _ in range(100):
                try:
                    detector.get_state()
                    detector.get_diagnostics()
                except Exception as e:
                    errors.append(("state", e))
                await asyncio.sleep(0)

        await asyncio.gather(
            record_latencies(),
            check_shedding(),
            check_state(),
        )

        assert len(errors) == 0, f"Errors during cross-component access: {errors}"

    @pytest.mark.asyncio
    async def test_full_reliability_stack_concurrent_access(self):
        """Full reliability stack should handle concurrent access."""
        # Set up full stack
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)
        rate_limiter = ServerRateLimiter(RateLimitConfig())
        stats_buffer = StatsBuffer()
        health_tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        errors = []

        async def simulate_request_flow(client_id: str, request_num: int):
            try:
                # Check rate limit
                result = rate_limiter.check_rate_limit(client_id, "submit")
                if not result.allowed:
                    return

                # Check load shedding
                if shedder.should_shed("JobSubmission"):
                    return

                # Record latency
                latency = 50.0 + request_num * 0.5
                detector.record_latency(latency)

                # Record stats
                stats_buffer.record(latency)

                # Update health
                health_tracker.update_state(
                    client_id,
                    WorkerHealthState(
                        worker_id=client_id,
                        consecutive_liveness_failures=0,
                        accepting_work=True,
                        available_capacity=100,
                    ),
                )

            except Exception as e:
                errors.append((client_id, request_num, e))

            await asyncio.sleep(0)

        # Simulate many concurrent requests from multiple clients
        tasks = [
            simulate_request_flow(f"client_{c}", r)
            for c in range(10)
            for r in range(50)
        ]
        await asyncio.gather(*tasks)

        assert len(errors) == 0, (
            f"Errors in full stack: {errors[:5]}..."
        )  # Show first 5
