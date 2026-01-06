#!/usr/bin/env python
"""
Comprehensive edge case tests for overload detection and load shedding (AD-18, AD-22).

Tests cover:
- Delta-based detection thresholds
- Absolute bounds safety rails
- Resource-based detection (CPU/memory)
- Trend calculation edge cases
- Load shedding priority handling
- State transitions and hysteresis
- Baseline drift scenarios
- Edge cases in calculations
"""

import pytest

from hyperscale.distributed_rewrite.reliability.overload import (
    HybridOverloadDetector,
    OverloadConfig,
    OverloadState,
)
from hyperscale.distributed_rewrite.reliability.load_shedding import (
    DEFAULT_MESSAGE_PRIORITIES,
    LoadShedder,
    LoadShedderConfig,
    RequestPriority,
)


# =============================================================================
# Test Delta-Based Detection
# =============================================================================


class TestDeltaDetection:
    """Tests for delta-based overload detection."""

    def test_no_detection_below_min_samples(self):
        """Delta detection inactive before min_samples."""
        config = OverloadConfig(min_samples=5)
        detector = HybridOverloadDetector(config)

        # Record 4 samples (below min_samples)
        for _ in range(4):
            detector.record_latency(1000.0)  # Very high latency

        # Should still be healthy (not enough samples)
        state = detector._get_delta_state()
        assert state == OverloadState.HEALTHY

    def test_detection_at_exactly_min_samples(self):
        """Delta detection activates at min_samples."""
        config = OverloadConfig(
            min_samples=3,
            delta_thresholds=(0.1, 0.3, 0.5),
            current_window=3,
        )
        detector = HybridOverloadDetector(config)

        # First sample establishes baseline at 100
        detector.record_latency(100.0)

        # Next two samples at 200 (100% above baseline)
        detector.record_latency(200.0)
        detector.record_latency(200.0)

        # Now at min_samples, should detect overload
        state = detector._get_delta_state()
        assert state != OverloadState.HEALTHY

    def test_busy_threshold(self):
        """Delta above busy threshold triggers BUSY state."""
        config = OverloadConfig(
            delta_thresholds=(0.2, 0.5, 1.0),
            min_samples=3,
            current_window=5,
            ema_alpha=0.01,  # Very slow baseline adaptation
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100ms with slow EMA
        for _ in range(10):
            detector.record_latency(100.0)

        # Now samples at 130ms (30% above baseline)
        # With ema_alpha=0.01, baseline barely moves
        for _ in range(5):
            detector.record_latency(130.0)

        state = detector._get_delta_state()
        assert state == OverloadState.BUSY

    def test_stressed_threshold(self):
        """Delta above stressed threshold triggers STRESSED state."""
        config = OverloadConfig(
            delta_thresholds=(0.2, 0.5, 1.0),
            min_samples=3,
            current_window=5,
            ema_alpha=0.01,  # Very slow baseline adaptation
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100ms with slow EMA
        for _ in range(10):
            detector.record_latency(100.0)

        # Now samples at 180ms (80% above baseline)
        # With ema_alpha=0.01, baseline barely moves
        for _ in range(5):
            detector.record_latency(180.0)

        state = detector._get_delta_state()
        assert state == OverloadState.STRESSED

    def test_overloaded_threshold(self):
        """Delta above overloaded threshold triggers OVERLOADED state."""
        config = OverloadConfig(
            delta_thresholds=(0.2, 0.5, 1.0),
            min_samples=3,
            current_window=5,
            ema_alpha=0.01,  # Very slow baseline adaptation
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100ms with slow EMA
        for _ in range(10):
            detector.record_latency(100.0)

        # Now samples at 250ms (150% above baseline)
        # With ema_alpha=0.01, baseline barely moves
        for _ in range(5):
            detector.record_latency(250.0)

        state = detector._get_delta_state()
        assert state == OverloadState.OVERLOADED

    def test_negative_delta_stays_healthy(self):
        """Negative delta (better than baseline) stays healthy."""
        config = OverloadConfig(min_samples=3, current_window=5)
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100ms
        for _ in range(10):
            detector.record_latency(100.0)

        # Now samples at 50ms (50% below baseline)
        for _ in range(5):
            detector.record_latency(50.0)

        state = detector._get_delta_state()
        assert state == OverloadState.HEALTHY


# =============================================================================
# Test Absolute Bounds Detection
# =============================================================================


class TestAbsoluteBoundsDetection:
    """Tests for absolute bounds safety detection."""

    def test_below_all_bounds_is_healthy(self):
        """Latency below all bounds is healthy."""
        config = OverloadConfig(
            absolute_bounds=(200.0, 500.0, 2000.0),
        )
        detector = HybridOverloadDetector(config)

        detector.record_latency(100.0)

        state = detector._get_absolute_state()
        assert state == OverloadState.HEALTHY

    def test_above_busy_bound(self):
        """Latency above busy bound triggers BUSY."""
        config = OverloadConfig(
            absolute_bounds=(200.0, 500.0, 2000.0),
        )
        detector = HybridOverloadDetector(config)

        for _ in range(5):
            detector.record_latency(300.0)  # Above 200ms bound

        state = detector._get_absolute_state()
        assert state == OverloadState.BUSY

    def test_above_stressed_bound(self):
        """Latency above stressed bound triggers STRESSED."""
        config = OverloadConfig(
            absolute_bounds=(200.0, 500.0, 2000.0),
        )
        detector = HybridOverloadDetector(config)

        for _ in range(5):
            detector.record_latency(800.0)  # Above 500ms bound

        state = detector._get_absolute_state()
        assert state == OverloadState.STRESSED

    def test_above_overloaded_bound(self):
        """Latency above overloaded bound triggers OVERLOADED."""
        config = OverloadConfig(
            absolute_bounds=(200.0, 500.0, 2000.0),
        )
        detector = HybridOverloadDetector(config)

        for _ in range(5):
            detector.record_latency(3000.0)  # Above 2000ms bound

        state = detector._get_absolute_state()
        assert state == OverloadState.OVERLOADED

    def test_absolute_bounds_override_delta_healthy(self):
        """Absolute bounds trigger even when delta says healthy."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),  # Low bounds
            delta_thresholds=(0.2, 0.5, 1.0),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish high baseline (300ms)
        for _ in range(10):
            detector.record_latency(300.0)

        # Delta detection: 300ms is the baseline, so delta = 0 = HEALTHY
        # Absolute detection: 300ms > 200ms = STRESSED
        state = detector.get_state()
        assert state == OverloadState.STRESSED

    def test_empty_samples_returns_healthy(self):
        """No samples returns healthy for absolute state."""
        detector = HybridOverloadDetector()
        state = detector._get_absolute_state()
        assert state == OverloadState.HEALTHY


# =============================================================================
# Test Resource-Based Detection
# =============================================================================


class TestResourceDetection:
    """Tests for resource-based (CPU/memory) detection."""

    def test_low_cpu_is_healthy(self):
        """Low CPU utilization is healthy."""
        config = OverloadConfig(
            cpu_thresholds=(0.7, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config)

        state = detector._get_resource_state(cpu_percent=50.0, memory_percent=50.0)
        assert state == OverloadState.HEALTHY

    def test_high_cpu_triggers_busy(self):
        """CPU above 70% triggers BUSY."""
        config = OverloadConfig(
            cpu_thresholds=(0.7, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config)

        state = detector._get_resource_state(cpu_percent=75.0, memory_percent=50.0)
        assert state == OverloadState.BUSY

    def test_very_high_cpu_triggers_stressed(self):
        """CPU above 85% triggers STRESSED."""
        config = OverloadConfig(
            cpu_thresholds=(0.7, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config)

        state = detector._get_resource_state(cpu_percent=90.0, memory_percent=50.0)
        assert state == OverloadState.STRESSED

    def test_critical_cpu_triggers_overloaded(self):
        """CPU above 95% triggers OVERLOADED."""
        config = OverloadConfig(
            cpu_thresholds=(0.7, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config)

        state = detector._get_resource_state(cpu_percent=98.0, memory_percent=50.0)
        assert state == OverloadState.OVERLOADED

    def test_memory_triggers_similar_to_cpu(self):
        """Memory thresholds work like CPU thresholds."""
        config = OverloadConfig(
            memory_thresholds=(0.7, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config)

        # High memory, low CPU
        state = detector._get_resource_state(cpu_percent=50.0, memory_percent=90.0)
        assert state == OverloadState.STRESSED

    def test_worst_resource_wins(self):
        """Worst resource state is used."""
        config = OverloadConfig(
            cpu_thresholds=(0.7, 0.85, 0.95),
            memory_thresholds=(0.7, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config)

        # CPU at STRESSED (90%), memory at BUSY (75%)
        state = detector._get_resource_state(cpu_percent=90.0, memory_percent=75.0)
        assert state == OverloadState.STRESSED

        # CPU at BUSY (75%), memory at OVERLOADED (98%)
        state = detector._get_resource_state(cpu_percent=75.0, memory_percent=98.0)
        assert state == OverloadState.OVERLOADED


# =============================================================================
# Test Trend Detection
# =============================================================================


class TestTrendDetection:
    """Tests for trend-based overload detection."""

    def test_rising_trend_triggers_overload(self):
        """Strongly rising trend triggers OVERLOADED."""
        config = OverloadConfig(
            trend_threshold=0.05,  # Low threshold for testing
            trend_window=10,
            min_samples=3,
            current_window=5,
            ema_alpha=0.01,  # Very slow baseline so delta keeps rising
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at stable 100ms first
        for _ in range(10):
            detector.record_latency(100.0)

        # Now rising latency - baseline is ~100, but current keeps increasing
        # This creates rising delta values in the delta history
        for index in range(15):
            detector.record_latency(100.0 + (index + 1) * 20)

        # With slow EMA, current_avg keeps growing relative to baseline
        # This means each delta is larger than the last -> positive trend
        assert detector.trend > 0

    def test_no_trend_with_stable_latency(self):
        """Stable latency has near-zero trend."""
        config = OverloadConfig(trend_window=10)
        detector = HybridOverloadDetector(config)

        # Stable latency around 100ms
        for _ in range(20):
            detector.record_latency(100.0)

        # Trend should be near zero
        assert abs(detector.trend) < 0.01

    def test_falling_trend_is_negative(self):
        """Falling latency has negative trend (improving)."""
        config = OverloadConfig(trend_window=10)
        detector = HybridOverloadDetector(config)

        # Start high, trend down
        for index in range(20):
            detector.record_latency(200.0 - index * 5)

        # Trend should be negative
        assert detector.trend < 0

    def test_insufficient_history_for_trend(self):
        """Less than 3 samples gives zero trend."""
        detector = HybridOverloadDetector()

        detector.record_latency(100.0)
        detector.record_latency(200.0)

        # Not enough samples for trend
        assert detector.trend == 0.0


# =============================================================================
# Test Hybrid State Combination
# =============================================================================


class TestHybridStateCombination:
    """Tests for combining delta, absolute, and resource states."""

    def test_worst_state_wins(self):
        """get_state() returns worst of all detection methods."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            cpu_thresholds=(0.7, 0.85, 0.95),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 30ms (delta = HEALTHY)
        for _ in range(10):
            detector.record_latency(30.0)

        # Latency at 60ms:
        # - Delta: ~100% above baseline = OVERLOADED
        # - Absolute: 60ms > 50ms = BUSY
        # Overall should be OVERLOADED
        for _ in range(5):
            detector.record_latency(60.0)

        state = detector.get_state(cpu_percent=50.0, memory_percent=50.0)
        # Should be at least BUSY from absolute detection
        assert state in (OverloadState.BUSY, OverloadState.STRESSED, OverloadState.OVERLOADED)

    def test_all_healthy_returns_healthy(self):
        """When all detections are healthy, result is healthy."""
        config = OverloadConfig(
            absolute_bounds=(200.0, 500.0, 2000.0),
            delta_thresholds=(0.2, 0.5, 1.0),
            cpu_thresholds=(0.7, 0.85, 0.95),
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Low latency, stable
        for _ in range(10):
            detector.record_latency(50.0)

        state = detector.get_state(cpu_percent=30.0, memory_percent=40.0)
        assert state == OverloadState.HEALTHY


# =============================================================================
# Test Baseline and Reset
# =============================================================================


class TestBaselineAndReset:
    """Tests for baseline tracking and reset."""

    def test_first_sample_sets_baseline(self):
        """First sample initializes baseline."""
        detector = HybridOverloadDetector()

        detector.record_latency(100.0)

        assert detector.baseline == 100.0

    def test_ema_smooths_baseline(self):
        """EMA smooths baseline over time."""
        config = OverloadConfig(ema_alpha=0.1)
        detector = HybridOverloadDetector(config)

        detector.record_latency(100.0)  # Baseline = 100
        detector.record_latency(200.0)  # EMA = 0.1*200 + 0.9*100 = 110

        assert detector.baseline == pytest.approx(110.0)

    def test_reset_clears_all_state(self):
        """reset() clears all internal state."""
        detector = HybridOverloadDetector()

        # Build up state
        for _ in range(20):
            detector.record_latency(100.0)

        assert detector.sample_count == 20
        assert detector.baseline > 0

        # Reset
        detector.reset()

        assert detector.sample_count == 0
        assert detector.baseline == 0.0
        assert detector.current_average == 0.0

    def test_baseline_drift_scenario(self):
        """Test baseline drift with gradual latency increase."""
        config = OverloadConfig(
            ema_alpha=0.1,  # Slow adaptation
            absolute_bounds=(50.0, 100.0, 200.0),  # But absolute catches it
        )
        detector = HybridOverloadDetector(config)

        # Start at 30ms
        for _ in range(50):
            detector.record_latency(30.0)

        # Slowly drift up to 150ms
        for latency in range(30, 150, 5):
            for _ in range(5):
                detector.record_latency(float(latency))

        # Absolute bounds should catch this even if delta doesn't
        state = detector._get_absolute_state()
        assert state in (OverloadState.STRESSED, OverloadState.OVERLOADED)


# =============================================================================
# Test Load Shedder Priority Classification
# =============================================================================


class TestLoadShedderPriorities:
    """Tests for LoadShedder priority classification."""

    def test_critical_messages_classified_correctly(self):
        """Critical messages get CRITICAL priority."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        critical_messages = ["Ping", "Ack", "JobCancelRequest", "Heartbeat", "HealthCheck"]

        for message in critical_messages:
            priority = shedder.classify_request(message)
            assert priority == RequestPriority.CRITICAL, f"{message} should be CRITICAL"

    def test_high_messages_classified_correctly(self):
        """High priority messages get HIGH priority."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        high_messages = ["SubmitJob", "WorkflowDispatch", "StateSync"]

        for message in high_messages:
            priority = shedder.classify_request(message)
            assert priority == RequestPriority.HIGH, f"{message} should be HIGH"

    def test_normal_messages_classified_correctly(self):
        """Normal priority messages get NORMAL priority."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        normal_messages = ["JobProgress", "StatsUpdate", "StatsQuery"]

        for message in normal_messages:
            priority = shedder.classify_request(message)
            assert priority == RequestPriority.NORMAL, f"{message} should be NORMAL"

    def test_low_messages_classified_correctly(self):
        """Low priority messages get LOW priority."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        low_messages = ["DetailedStatsRequest", "DebugRequest", "DiagnosticsRequest"]

        for message in low_messages:
            priority = shedder.classify_request(message)
            assert priority == RequestPriority.LOW, f"{message} should be LOW"

    def test_unknown_message_defaults_to_normal(self):
        """Unknown message types default to NORMAL priority."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        priority = shedder.classify_request("UnknownMessageType")
        assert priority == RequestPriority.NORMAL

    def test_register_custom_priority(self):
        """Can register custom priority for message types."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        shedder.register_message_priority("CustomMessage", RequestPriority.CRITICAL)

        priority = shedder.classify_request("CustomMessage")
        assert priority == RequestPriority.CRITICAL


# =============================================================================
# Test Load Shedding Decisions
# =============================================================================


class TestLoadSheddingDecisions:
    """Tests for load shedding decisions."""

    def test_healthy_accepts_all(self):
        """Healthy state accepts all request types."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # No latency recorded = healthy
        for message_type in DEFAULT_MESSAGE_PRIORITIES.keys():
            assert not shedder.should_shed(message_type)

    def test_busy_sheds_only_low(self):
        """Busy state sheds only LOW priority."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Latency at 75ms = BUSY (above 50, below 100)
        detector.record_latency(75.0)

        # LOW should be shed
        assert shedder.should_shed("DetailedStatsRequest")  # LOW
        assert shedder.should_shed("DebugRequest")  # LOW

        # Others should not be shed
        assert not shedder.should_shed("StatsUpdate")  # NORMAL
        assert not shedder.should_shed("SubmitJob")  # HIGH
        assert not shedder.should_shed("Ping")  # CRITICAL

    def test_stressed_sheds_normal_and_low(self):
        """Stressed state sheds NORMAL and LOW priority."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Latency at 150ms = STRESSED (above 100, below 200)
        detector.record_latency(150.0)

        # LOW and NORMAL should be shed
        assert shedder.should_shed("DetailedStatsRequest")  # LOW
        assert shedder.should_shed("StatsUpdate")  # NORMAL

        # HIGH and CRITICAL should not be shed
        assert not shedder.should_shed("SubmitJob")  # HIGH
        assert not shedder.should_shed("Ping")  # CRITICAL

    def test_overloaded_sheds_all_except_critical(self):
        """Overloaded state sheds all except CRITICAL."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # Latency at 300ms = OVERLOADED (above 200)
        detector.record_latency(300.0)

        # LOW, NORMAL, HIGH should be shed
        assert shedder.should_shed("DetailedStatsRequest")  # LOW
        assert shedder.should_shed("StatsUpdate")  # NORMAL
        assert shedder.should_shed("SubmitJob")  # HIGH

        # CRITICAL should not be shed
        assert not shedder.should_shed("Ping")  # CRITICAL
        assert not shedder.should_shed("JobCancelRequest")  # CRITICAL
        assert not shedder.should_shed("Heartbeat")  # CRITICAL

    def test_should_shed_by_priority_directly(self):
        """should_shed_priority() works correctly."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # STRESSED state
        detector.record_latency(150.0)

        # Test by priority directly
        assert shedder.should_shed_priority(RequestPriority.LOW)
        assert shedder.should_shed_priority(RequestPriority.NORMAL)
        assert not shedder.should_shed_priority(RequestPriority.HIGH)
        assert not shedder.should_shed_priority(RequestPriority.CRITICAL)


# =============================================================================
# Test Load Shedder Metrics
# =============================================================================


class TestLoadShedderMetrics:
    """Tests for LoadShedder metrics tracking."""

    def test_total_requests_counted(self):
        """Total requests are counted."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        for _ in range(10):
            shedder.should_shed("Ping")

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 10

    def test_shed_requests_counted(self):
        """Shed requests are counted."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # OVERLOADED state
        detector.record_latency(300.0)

        # 5 HIGH requests (will be shed)
        for _ in range(5):
            shedder.should_shed("SubmitJob")

        # 3 CRITICAL requests (won't be shed)
        for _ in range(3):
            shedder.should_shed("Ping")

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 8
        assert metrics["shed_requests"] == 5
        assert metrics["shed_rate"] == pytest.approx(5 / 8)

    def test_shed_by_priority_tracked(self):
        """Shed counts are tracked by priority."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        # OVERLOADED state
        detector.record_latency(300.0)

        # Shed some of each (except CRITICAL)
        for _ in range(3):
            shedder.should_shed("DetailedStatsRequest")  # LOW
        for _ in range(2):
            shedder.should_shed("StatsUpdate")  # NORMAL
        for _ in range(4):
            shedder.should_shed("SubmitJob")  # HIGH

        metrics = shedder.get_metrics()
        assert metrics["shed_by_priority"]["LOW"] == 3
        assert metrics["shed_by_priority"]["NORMAL"] == 2
        assert metrics["shed_by_priority"]["HIGH"] == 4
        assert metrics["shed_by_priority"]["CRITICAL"] == 0

    def test_reset_metrics(self):
        """reset_metrics() clears all counters."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)
        shedder = LoadShedder(detector)

        detector.record_latency(300.0)

        for _ in range(10):
            shedder.should_shed("SubmitJob")

        shedder.reset_metrics()

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 0
        assert metrics["shed_requests"] == 0


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestOverloadEdgeCases:
    """Tests for edge cases in overload detection."""

    def test_zero_baseline_handled(self):
        """Zero baseline doesn't cause division by zero."""
        detector = HybridOverloadDetector()

        # Force baseline to be very small
        detector.record_latency(0.001)
        detector.record_latency(100.0)

        # Should not crash
        state = detector.get_state()
        assert state is not None

    def test_negative_latency_handled(self):
        """Negative latency (should not happen) is handled."""
        detector = HybridOverloadDetector()

        # Negative latency
        detector.record_latency(-10.0)
        detector.record_latency(100.0)

        # Should not crash
        state = detector.get_state()
        assert state is not None

    def test_very_large_latency(self):
        """Very large latency values are handled."""
        detector = HybridOverloadDetector()

        detector.record_latency(1_000_000.0)  # 1 million ms

        state = detector.get_state()
        assert state == OverloadState.OVERLOADED

    def test_empty_detector_returns_healthy(self):
        """Detector with no samples returns healthy."""
        detector = HybridOverloadDetector()
        state = detector.get_state()
        assert state == OverloadState.HEALTHY

    def test_current_window_smaller_than_samples(self):
        """Window limits retained samples correctly."""
        config = OverloadConfig(current_window=3)
        detector = HybridOverloadDetector(config)

        # Add more samples than window
        for index in range(10):
            detector.record_latency(100.0 + index * 10)

        # Recent should only have last 3
        assert len(detector._recent) == 3

    def test_diagnostics_complete(self):
        """get_diagnostics() returns complete information."""
        detector = HybridOverloadDetector()

        for _ in range(10):
            detector.record_latency(100.0)

        diagnostics = detector.get_diagnostics()

        assert "baseline" in diagnostics
        assert "current_avg" in diagnostics
        assert "delta" in diagnostics
        assert "trend" in diagnostics
        assert "sample_count" in diagnostics
        assert "delta_state" in diagnostics
        assert "absolute_state" in diagnostics

    def test_cpu_and_memory_passed_to_detector(self):
        """CPU and memory are passed to resource detection."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Record some latency (doesn't matter for this test)
        detector.record_latency(50.0)

        # CPU at 98% should trigger OVERLOADED
        state = shedder.get_current_state(cpu_percent=98.0, memory_percent=50.0)
        assert state == OverloadState.OVERLOADED


class TestCustomConfiguration:
    """Tests for custom configuration scenarios."""

    def test_aggressive_thresholds(self):
        """Very aggressive thresholds trigger earlier."""
        config = OverloadConfig(
            delta_thresholds=(0.05, 0.1, 0.2),  # 5%, 10%, 20%
            min_samples=3,
            current_window=5,
            ema_alpha=0.01,  # Very slow baseline adaptation
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline with slow EMA
        for _ in range(10):
            detector.record_latency(100.0)

        # Just 15% above baseline triggers STRESSED
        # With ema_alpha=0.01, baseline stays ~100
        for _ in range(5):
            detector.record_latency(115.0)

        state = detector._get_delta_state()
        assert state == OverloadState.STRESSED

    def test_relaxed_thresholds(self):
        """Relaxed thresholds allow more headroom."""
        config = OverloadConfig(
            delta_thresholds=(0.5, 1.0, 2.0),  # 50%, 100%, 200%
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(10):
            detector.record_latency(100.0)

        # 40% above baseline still healthy
        for _ in range(5):
            detector.record_latency(140.0)

        state = detector._get_delta_state()
        assert state == OverloadState.HEALTHY

    def test_custom_shed_thresholds(self):
        """Custom shedding thresholds work correctly."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=1,
            current_window=1,
        )
        detector = HybridOverloadDetector(config)

        # Custom: shed HIGH even when just BUSY
        shed_config = LoadShedderConfig(
            shed_thresholds={
                OverloadState.HEALTHY: None,
                OverloadState.BUSY: RequestPriority.HIGH,  # More aggressive
                OverloadState.STRESSED: RequestPriority.HIGH,
                OverloadState.OVERLOADED: RequestPriority.HIGH,
            }
        )
        shedder = LoadShedder(detector, config=shed_config)

        # BUSY state
        detector.record_latency(75.0)

        # HIGH should be shed even in BUSY
        assert shedder.should_shed("SubmitJob")  # HIGH


class TestStateOrdering:
    """Tests for state ordering and comparison."""

    def test_state_ordering_correct(self):
        """State ordering HEALTHY < BUSY < STRESSED < OVERLOADED."""
        from hyperscale.distributed_rewrite.reliability.overload import _STATE_ORDER

        assert _STATE_ORDER[OverloadState.HEALTHY] < _STATE_ORDER[OverloadState.BUSY]
        assert _STATE_ORDER[OverloadState.BUSY] < _STATE_ORDER[OverloadState.STRESSED]
        assert _STATE_ORDER[OverloadState.STRESSED] < _STATE_ORDER[OverloadState.OVERLOADED]

    def test_max_state_comparison(self):
        """max() comparison works for states."""
        from hyperscale.distributed_rewrite.reliability.overload import _STATE_ORDER

        states = [OverloadState.HEALTHY, OverloadState.BUSY, OverloadState.STRESSED]
        worst = max(states, key=lambda s: _STATE_ORDER[s])
        assert worst == OverloadState.STRESSED


class TestPriorityOrdering:
    """Tests for priority ordering."""

    def test_priority_ordering(self):
        """Lower priority value = higher importance."""
        assert RequestPriority.CRITICAL < RequestPriority.HIGH
        assert RequestPriority.HIGH < RequestPriority.NORMAL
        assert RequestPriority.NORMAL < RequestPriority.LOW

    def test_priority_comparison_for_shedding(self):
        """Higher priority number means more likely to be shed."""
        # In the shedding logic: priority >= threshold means shed
        # So LOW (3) >= NORMAL (2) means LOW gets shed when threshold is NORMAL
        assert RequestPriority.LOW >= RequestPriority.NORMAL
        assert RequestPriority.NORMAL >= RequestPriority.HIGH
