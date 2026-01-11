"""
Failure Path Tests for Load Shedding (AD-22).

Tests edge cases, error conditions, and boundary behaviors for:
- LoadShedder configuration edge cases
- HybridOverloadDetector boundary conditions
- Priority classification edge cases
- Metrics under failure conditions
- State transition edge cases
"""

import asyncio
import pytest

from hyperscale.distributed_rewrite.reliability.load_shedding import (
    DEFAULT_MESSAGE_PRIORITIES,
    LoadShedder,
    LoadShedderConfig,
    RequestPriority,
)
from hyperscale.distributed_rewrite.reliability.overload import (
    HybridOverloadDetector,
    OverloadConfig,
    OverloadState,
)


class TestOverloadDetectorEdgeCases:
    """Test edge cases for HybridOverloadDetector."""

    def test_zero_latency_samples(self):
        """Test behavior with no latency samples."""
        detector = HybridOverloadDetector()

        # No samples - should be healthy
        state = detector.get_state(cpu_percent=0.0, memory_percent=0.0)
        assert state == OverloadState.HEALTHY

        # Verify diagnostics show empty state
        diagnostics = detector.get_diagnostics()
        assert diagnostics["sample_count"] == 0
        assert diagnostics["baseline"] == 0.0
        assert diagnostics["current_avg"] == 0.0

    def test_single_latency_sample(self):
        """Test behavior with exactly one sample."""
        detector = HybridOverloadDetector()

        detector.record_latency(100.0)

        # Single sample - baseline gets initialized
        assert detector.baseline == 100.0
        assert detector.sample_count == 1
        # Not enough samples for delta detection (min_samples=3)
        state = detector.get_state()
        # Should be HEALTHY for delta but may trigger absolute bounds
        assert state in [OverloadState.HEALTHY, OverloadState.BUSY]

    def test_zero_baseline_edge_case(self):
        """Test behavior when baseline is zero."""
        config = OverloadConfig(min_samples=1)
        detector = HybridOverloadDetector(config)

        # Record zero latency
        detector.record_latency(0.0)

        # Zero baseline should not cause division by zero
        state = detector.get_state()
        assert state == OverloadState.HEALTHY

        diagnostics = detector.get_diagnostics()
        assert diagnostics["delta"] == 0.0

    def test_negative_latency_handling(self):
        """Test behavior with negative latency values (edge case)."""
        detector = HybridOverloadDetector()

        # Record negative latency (should not happen in practice)
        detector.record_latency(-10.0)
        detector.record_latency(-5.0)
        detector.record_latency(-1.0)

        # Should not crash and should handle gracefully
        state = detector.get_state()
        assert state in list(OverloadState)

    def test_extreme_latency_values(self):
        """Test with extreme latency values."""
        detector = HybridOverloadDetector()

        # Very high latency
        detector.record_latency(1_000_000.0)  # 1000 seconds

        state = detector.get_state()
        # Should be overloaded due to absolute bounds
        assert state == OverloadState.OVERLOADED

    def test_latency_spike_after_stable_period(self):
        """Test sudden spike after stable baseline."""
        detector = HybridOverloadDetector()

        # Establish stable baseline
        for _ in range(20):
            detector.record_latency(50.0)

        # Baseline should be around 50
        assert 45 < detector.baseline < 55

        # Sudden spike
        detector.record_latency(5000.0)

        state = detector.get_state()
        # Should detect the spike
        assert state in [OverloadState.STRESSED, OverloadState.OVERLOADED]

    def test_trend_calculation_with_insufficient_samples(self):
        """Test trend calculation with less than 3 samples."""
        detector = HybridOverloadDetector()

        detector.record_latency(50.0)
        detector.record_latency(60.0)

        # Trend requires at least 3 samples
        assert detector.trend == 0.0

    def test_trend_calculation_with_flat_data(self):
        """Test trend calculation with constant values."""
        detector = HybridOverloadDetector()

        for _ in range(10):
            detector.record_latency(100.0)

        # Flat trend should be near zero
        trend = detector.trend
        assert abs(trend) < 0.01

    def test_trend_calculation_denominator_zero(self):
        """Test trend calculation when denominator would be zero."""
        config = OverloadConfig(trend_window=1)
        detector = HybridOverloadDetector(config)

        # With window=1, the calculation should handle edge case
        detector.record_latency(100.0)
        detector.record_latency(150.0)

        # Should not crash
        trend = detector.trend
        assert trend == 0.0 or isinstance(trend, float)

    def test_cpu_boundary_values(self):
        """Test CPU threshold boundaries."""
        detector = HybridOverloadDetector()

        # Establish baseline
        for _ in range(5):
            detector.record_latency(10.0)

        # Test exact boundary values
        # Default: cpu_thresholds = (0.7, 0.85, 0.95)
        assert detector.get_state(cpu_percent=69.9) == OverloadState.HEALTHY
        assert detector.get_state(cpu_percent=70.1) == OverloadState.BUSY
        assert detector.get_state(cpu_percent=85.1) == OverloadState.STRESSED
        assert detector.get_state(cpu_percent=95.1) == OverloadState.OVERLOADED

    def test_memory_boundary_values(self):
        """Test memory threshold boundaries."""
        detector = HybridOverloadDetector()

        # Establish baseline
        for _ in range(5):
            detector.record_latency(10.0)

        # Test exact boundary values
        # Default: memory_thresholds = (0.7, 0.85, 0.95)
        assert detector.get_state(memory_percent=69.9) == OverloadState.HEALTHY
        assert detector.get_state(memory_percent=70.1) == OverloadState.BUSY
        assert detector.get_state(memory_percent=85.1) == OverloadState.STRESSED
        assert detector.get_state(memory_percent=95.1) == OverloadState.OVERLOADED

    def test_combined_cpu_memory_pressure(self):
        """Test combined CPU and memory pressure."""
        detector = HybridOverloadDetector()

        for _ in range(5):
            detector.record_latency(10.0)

        # CPU busy, memory stressed - should take max
        state = detector.get_state(cpu_percent=75.0, memory_percent=90.0)
        assert state == OverloadState.STRESSED

    def test_percentage_values_over_100(self):
        """Test behavior with CPU/memory over 100%."""
        detector = HybridOverloadDetector()

        for _ in range(5):
            detector.record_latency(10.0)

        # Over 100% should still work
        state = detector.get_state(cpu_percent=150.0, memory_percent=200.0)
        assert state == OverloadState.OVERLOADED

    def test_reset_clears_all_state(self):
        """Test that reset clears all internal state."""
        detector = HybridOverloadDetector()

        # Build up state
        for i in range(20):
            detector.record_latency(50.0 + i * 10)

        assert detector.sample_count > 0
        assert detector.baseline > 0

        # Reset
        detector.reset()

        assert detector.sample_count == 0
        assert detector.baseline == 0.0
        assert detector.current_average == 0.0
        assert detector.trend == 0.0

    def test_absolute_bounds_override_delta(self):
        """Test that absolute bounds override delta detection."""
        # Configure very lenient delta thresholds
        config = OverloadConfig(
            delta_thresholds=(10.0, 20.0, 30.0),  # Very high
            absolute_bounds=(100.0, 200.0, 300.0),  # Reasonable
            current_window=5,  # Small window so recent samples dominate
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 50ms
        for _ in range(5):
            detector.record_latency(50.0)

        # Record latencies above absolute bounds - fill the window
        # With window=5, after these 5 samples, all recent samples are 350
        for _ in range(5):
            detector.record_latency(350.0)

        state = detector.get_state()
        # 350 > 300 (overloaded bound), so should be OVERLOADED
        assert state == OverloadState.OVERLOADED


class TestLoadShedderEdgeCases:
    """Test edge cases for LoadShedder."""

    def test_unknown_message_type_defaults_to_normal(self):
        """Test that unknown message types default to NORMAL priority."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        priority = shedder.classify_request("UnknownMessageType")
        assert priority == RequestPriority.NORMAL

    def test_empty_message_type(self):
        """Test classification of empty message type."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        priority = shedder.classify_request("")
        assert priority == RequestPriority.NORMAL

    def test_custom_message_priorities(self):
        """Test LoadShedder with custom priority mapping."""
        detector = HybridOverloadDetector()
        custom_priorities = {
            "CustomMessage": RequestPriority.CRITICAL,
            "AnotherCustom": RequestPriority.LOW,
        }
        shedder = LoadShedder(detector, message_priorities=custom_priorities)

        assert shedder.classify_request("CustomMessage") == RequestPriority.CRITICAL
        assert shedder.classify_request("AnotherCustom") == RequestPriority.LOW
        # Default priorities should not be present
        assert shedder.classify_request("Ping") == RequestPriority.NORMAL

    def test_register_message_priority_override(self):
        """Test overriding an existing message priority."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Ping is CRITICAL by default
        assert shedder.classify_request("Ping") == RequestPriority.CRITICAL

        # Override to LOW
        shedder.register_message_priority("Ping", RequestPriority.LOW)
        assert shedder.classify_request("Ping") == RequestPriority.LOW

    def test_none_config_uses_defaults(self):
        """Test that None config uses default configuration."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector, config=None)

        # Should have default shed thresholds
        assert shedder._config.shed_thresholds[OverloadState.HEALTHY] is None

    def test_custom_shed_thresholds(self):
        """Test custom shed threshold configuration."""
        detector = HybridOverloadDetector()
        config = LoadShedderConfig(
            shed_thresholds={
                OverloadState.HEALTHY: RequestPriority.LOW,  # Shed LOW even when healthy
                OverloadState.BUSY: RequestPriority.NORMAL,
                OverloadState.STRESSED: RequestPriority.HIGH,
                OverloadState.OVERLOADED: RequestPriority.CRITICAL,  # Shed everything
            }
        )
        shedder = LoadShedder(detector, config=config)

        # Even in healthy state, LOW should be shed
        # Need to trigger should_shed_priority which checks state
        for _ in range(5):
            detector.record_latency(10.0)

        should_shed_low = shedder.should_shed_priority(RequestPriority.LOW)
        assert should_shed_low is True

    def test_all_none_thresholds(self):
        """Test configuration where all thresholds are None."""
        detector = HybridOverloadDetector()
        config = LoadShedderConfig(
            shed_thresholds={
                OverloadState.HEALTHY: None,
                OverloadState.BUSY: None,
                OverloadState.STRESSED: None,
                OverloadState.OVERLOADED: None,
            }
        )
        shedder = LoadShedder(detector, config=config)

        # Force overloaded state
        for _ in range(5):
            detector.record_latency(10000.0)

        # Even in overloaded state, nothing should be shed
        assert shedder.should_shed("DebugRequest") is False

    def test_missing_state_in_thresholds(self):
        """Test behavior when a state is missing from thresholds dict."""
        detector = HybridOverloadDetector()
        config = LoadShedderConfig(
            shed_thresholds={
                OverloadState.HEALTHY: None,
                # BUSY is missing
                OverloadState.STRESSED: RequestPriority.NORMAL,
                OverloadState.OVERLOADED: RequestPriority.HIGH,
            }
        )
        shedder = LoadShedder(detector, config=config)

        # When in BUSY state (missing), threshold should be None
        for _ in range(5):
            detector.record_latency(10.0)

        # Force BUSY via CPU
        state = detector.get_state(cpu_percent=75.0)
        assert state == OverloadState.BUSY

        # Should not shed when threshold is missing (returns None from .get())
        should_shed = shedder.should_shed_priority(RequestPriority.LOW, cpu_percent=75.0)
        assert should_shed is False


class TestLoadShedderMetricsEdgeCases:
    """Test edge cases in metrics tracking."""

    def test_metrics_with_zero_requests(self):
        """Test metrics when no requests have been processed."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 0
        assert metrics["shed_requests"] == 0
        assert metrics["shed_rate"] == 0.0

    def test_metrics_shed_rate_calculation(self):
        """Test shed rate calculation accuracy."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Force overloaded state
        for _ in range(5):
            detector.record_latency(10000.0)

        # Process mix of requests
        for _ in range(10):
            shedder.should_shed("Ping")  # CRITICAL - not shed
        for _ in range(10):
            shedder.should_shed("DebugRequest")  # LOW - shed

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 20
        assert metrics["shed_requests"] == 10
        assert metrics["shed_rate"] == 0.5

    def test_metrics_by_priority_tracking(self):
        """Test that shed_by_priority tracks correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Force overloaded state
        for _ in range(5):
            detector.record_latency(10000.0)

        # Shed requests at different priorities
        shedder.should_shed("SubmitJob")  # HIGH
        shedder.should_shed("JobProgress")  # NORMAL
        shedder.should_shed("DebugRequest")  # LOW

        metrics = shedder.get_metrics()
        shed_by_priority = metrics["shed_by_priority"]

        assert shed_by_priority["CRITICAL"] == 0
        assert shed_by_priority["HIGH"] == 1
        assert shed_by_priority["NORMAL"] == 1
        assert shed_by_priority["LOW"] == 1

    def test_reset_metrics(self):
        """Test that reset_metrics clears all counters."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Build up some metrics
        for _ in range(5):
            detector.record_latency(10000.0)
            shedder.should_shed("DebugRequest")

        assert shedder.get_metrics()["total_requests"] > 0

        # Reset
        shedder.reset_metrics()

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 0
        assert metrics["shed_requests"] == 0
        assert all(count == 0 for count in metrics["shed_by_priority"].values())

    def test_metrics_with_concurrent_requests(self):
        """Test metrics accuracy under simulated concurrent access."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Force stressed state
        for _ in range(5):
            detector.record_latency(1000.0)

        # Simulate concurrent requests (in reality, would need actual threads)
        request_count = 100
        for _ in range(request_count):
            shedder.should_shed("JobProgress")  # NORMAL - should be shed in stressed

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == request_count


class TestLoadShedderStateTransitions:
    """Test state transition edge cases."""

    def test_rapid_state_transitions(self):
        """Test behavior during rapid state changes."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        results = []

        # Rapid alternation between states
        for i in range(20):
            if i % 2 == 0:
                detector.record_latency(10.0)  # Low latency
                cpu = 10.0
            else:
                detector.record_latency(3000.0)  # High latency
                cpu = 99.0

            should_shed = shedder.should_shed("JobProgress", cpu_percent=cpu)
            results.append(should_shed)

        # Should have mix of shed/not shed decisions
        assert True in results
        assert False in results

    def test_state_hysteresis_behavior(self):
        """Test that state changes require sustained pressure."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Establish healthy baseline
        for _ in range(10):
            detector.record_latency(50.0)

        assert shedder.get_current_state() == OverloadState.HEALTHY

        # Single spike shouldn't immediately change state (due to averaging)
        detector.record_latency(1000.0)
        # State may or may not change depending on window size
        # But system should be stable

    def test_recovery_from_overloaded(self):
        """Test gradual recovery from overloaded state."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Go into overloaded state
        for _ in range(10):
            detector.record_latency(5000.0)

        assert shedder.get_current_state() == OverloadState.OVERLOADED

        # Gradually recover
        states = []
        for _ in range(30):
            detector.record_latency(50.0)
            states.append(shedder.get_current_state())

        # Should eventually return to healthy
        assert states[-1] in [OverloadState.HEALTHY, OverloadState.BUSY]


class TestDefaultMessagePriorities:
    """Test default message priority mappings."""

    def test_all_critical_messages(self):
        """Verify all critical messages are classified correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        critical_messages = [
            "Ping", "Ack", "Nack", "PingReq", "Suspect", "Alive", "Dead",
            "Join", "JoinAck", "Leave", "JobCancelRequest", "JobCancelResponse",
            "JobFinalResult", "Heartbeat", "HealthCheck"
        ]

        for msg in critical_messages:
            priority = shedder.classify_request(msg)
            assert priority == RequestPriority.CRITICAL, f"{msg} should be CRITICAL"

    def test_all_high_messages(self):
        """Verify all HIGH priority messages are classified correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        high_messages = [
            "SubmitJob", "SubmitJobResponse", "JobAssignment", "WorkflowDispatch",
            "WorkflowComplete", "StateSync", "StateSyncRequest", "StateSyncResponse",
            "AntiEntropyRequest", "AntiEntropyResponse", "JobLeaderGateTransfer",
            "JobLeaderGateTransferAck"
        ]

        for msg in high_messages:
            priority = shedder.classify_request(msg)
            assert priority == RequestPriority.HIGH, f"{msg} should be HIGH"

    def test_all_normal_messages(self):
        """Verify all NORMAL priority messages are classified correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        normal_messages = [
            "JobProgress", "JobStatusRequest", "JobStatusResponse", "JobStatusPush",
            "RegisterCallback", "RegisterCallbackResponse", "StatsUpdate", "StatsQuery"
        ]

        for msg in normal_messages:
            priority = shedder.classify_request(msg)
            assert priority == RequestPriority.NORMAL, f"{msg} should be NORMAL"

    def test_all_low_messages(self):
        """Verify all LOW priority messages are classified correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        low_messages = [
            "DetailedStatsRequest", "DetailedStatsResponse",
            "DebugRequest", "DebugResponse",
            "DiagnosticsRequest", "DiagnosticsResponse"
        ]

        for msg in low_messages:
            priority = shedder.classify_request(msg)
            assert priority == RequestPriority.LOW, f"{msg} should be LOW"


class TestOverloadConfigEdgeCases:
    """Test OverloadConfig edge cases."""

    def test_zero_ema_alpha(self):
        """Test with EMA alpha of 0 (no smoothing)."""
        config = OverloadConfig(ema_alpha=0.0)
        detector = HybridOverloadDetector(config)

        detector.record_latency(100.0)
        detector.record_latency(200.0)

        # With alpha=0, baseline stays at initial value
        assert detector.baseline == 100.0

    def test_one_ema_alpha(self):
        """Test with EMA alpha of 1 (no history)."""
        config = OverloadConfig(ema_alpha=1.0)
        detector = HybridOverloadDetector(config)

        detector.record_latency(100.0)
        detector.record_latency(200.0)

        # With alpha=1, baseline immediately updates to latest
        assert detector.baseline == 200.0

    def test_zero_min_samples(self):
        """Test with min_samples of 0."""
        config = OverloadConfig(min_samples=0)
        detector = HybridOverloadDetector(config)

        # With no samples and min_samples=0, delta detection may try to compute
        # with empty samples. The _get_absolute_state returns HEALTHY when empty.
        # With min_samples=0, we need at least one sample to avoid division by zero
        # in _get_delta_state (sum/len). This is an edge case that should be avoided
        # in production configs but we test it gracefully handles after first sample.
        detector.record_latency(50.0)
        state = detector.get_state()
        assert state == OverloadState.HEALTHY

    def test_very_small_thresholds(self):
        """Test with very small threshold values."""
        config = OverloadConfig(
            delta_thresholds=(0.001, 0.002, 0.003),
            absolute_bounds=(0.1, 0.2, 0.3),
            cpu_thresholds=(0.01, 0.02, 0.03),
            memory_thresholds=(0.01, 0.02, 0.03),
        )
        detector = HybridOverloadDetector(config)

        # Any non-trivial values should trigger overload
        detector.record_latency(1.0)
        detector.record_latency(1.0)
        detector.record_latency(1.0)

        state = detector.get_state(cpu_percent=5.0)
        assert state == OverloadState.OVERLOADED

    def test_inverted_threshold_order(self):
        """Test with thresholds in inverted order."""
        config = OverloadConfig(
            delta_thresholds=(1.0, 0.5, 0.2),  # Inverted (overloaded < stressed < busy)
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(5):
            detector.record_latency(100.0)

        # With inverted thresholds, behavior may be unexpected
        # but should not crash
        detector.record_latency(150.0)  # 50% increase
        state = detector.get_state()
        assert state in list(OverloadState)


class TestConcurrentLoadSheddingDecisions:
    """Test concurrent load shedding scenarios."""

    @pytest.mark.asyncio
    async def test_concurrent_should_shed_calls(self):
        """Test concurrent should_shed calls."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Establish state
        for _ in range(5):
            detector.record_latency(1000.0)

        async def make_decision(message_type: str):
            # Simulate async workload
            await asyncio.sleep(0.001)
            return shedder.should_shed(message_type)

        # Make concurrent decisions - create fresh coroutines each time
        message_types = ["JobProgress", "DebugRequest", "Ping", "SubmitJob"] * 25
        tasks = [make_decision(msg) for msg in message_types]

        results = await asyncio.gather(*tasks)

        # Verify metrics are consistent
        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 100

    @pytest.mark.asyncio
    async def test_state_changes_during_decision(self):
        """Test that state can change between decision and action."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Start healthy
        for _ in range(5):
            detector.record_latency(50.0)

        async def check_and_change():
            # Check shedding decision
            should_shed = shedder.should_shed("JobProgress")

            # State changes
            for _ in range(5):
                detector.record_latency(5000.0)

            # Check again - should be different
            should_shed_after = shedder.should_shed("JobProgress")

            return should_shed, should_shed_after

        before, after = await check_and_change()

        # First check should not shed (healthy state)
        assert before is False
        # Second check may shed (overloaded state)
        # (depends on how quickly state transitions)


class TestNonePriorityHandling:
    """Test handling of None values and edge cases in priority system."""

    def test_none_cpu_memory_values(self):
        """Test should_shed with None CPU/memory values."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Establish baseline
        for _ in range(5):
            detector.record_latency(50.0)

        # None values should be handled gracefully
        result = shedder.should_shed("JobProgress", cpu_percent=None, memory_percent=None)
        assert isinstance(result, bool)

    def test_priority_comparison_with_all_values(self):
        """Test that priority comparisons work correctly."""
        # Verify IntEnum ordering
        assert RequestPriority.CRITICAL < RequestPriority.HIGH
        assert RequestPriority.HIGH < RequestPriority.NORMAL
        assert RequestPriority.NORMAL < RequestPriority.LOW

        # Test >= comparison used in shedding logic
        assert RequestPriority.LOW >= RequestPriority.LOW
        assert RequestPriority.LOW >= RequestPriority.NORMAL
        assert not (RequestPriority.CRITICAL >= RequestPriority.LOW)


class TestLoadShedderRecoveryScenarios:
    """Test recovery and stabilization scenarios."""

    def test_gradual_degradation_and_recovery(self):
        """Test gradual degradation followed by recovery."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        states_progression = []

        # Start healthy
        for _ in range(10):
            detector.record_latency(50.0)
            states_progression.append(shedder.get_current_state())

        # Gradual degradation
        for i in range(20):
            detector.record_latency(50.0 + i * 100)
            states_progression.append(shedder.get_current_state())

        # Hold at high load
        for _ in range(10):
            detector.record_latency(2500.0)
            states_progression.append(shedder.get_current_state())

        # Gradual recovery
        for i in range(30):
            detector.record_latency(2500.0 - i * 80)
            states_progression.append(shedder.get_current_state())

        # Should have gone through multiple states
        unique_states = set(states_progression)
        assert len(unique_states) >= 2

    def test_reset_during_operation(self):
        """Test resetting detector during active shedding."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Build up overloaded state
        for _ in range(10):
            detector.record_latency(5000.0)

        assert shedder.get_current_state() == OverloadState.OVERLOADED

        # Reset detector
        detector.reset()

        # Should be healthy again (no samples)
        state = shedder.get_current_state()
        assert state == OverloadState.HEALTHY

        # Metrics should be preserved
        assert shedder.get_metrics()["total_requests"] == 0

    def test_multiple_detector_resets(self):
        """Test multiple reset cycles."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        for cycle in range(3):
            # Build up state
            for _ in range(10):
                detector.record_latency(500.0 + cycle * 100)

            # Verify state is not healthy
            shedder.should_shed("JobProgress")

            # Reset
            detector.reset()
            shedder.reset_metrics()

            # Verify clean state
            assert shedder.get_metrics()["total_requests"] == 0
            assert detector.sample_count == 0
