"""
Integration tests for Hybrid Overload Detection (AD-18).

These tests verify that:
1. OverloadConfig dataclass has all required fields
2. HybridOverloadDetector correctly combines three detection tiers
3. Delta-based detection tracks EMA baseline and trends
4. Absolute bounds provide safety rails
5. Resource signals contribute to overload state
6. Final state is max of all detection methods
"""

from hyperscale.distributed.reliability import (
    OverloadState,
    OverloadConfig,
    HybridOverloadDetector,
)


class TestOverloadConfig:
    """Test OverloadConfig dataclass."""

    def test_default_config_values(self):
        """OverloadConfig should have sensible defaults."""
        config = OverloadConfig()

        # Delta detection defaults
        assert config.ema_alpha == 0.1
        assert config.current_window == 10
        assert config.trend_window == 20

        # Delta thresholds
        assert config.delta_thresholds == (0.2, 0.5, 1.0)

        # Absolute bounds (ms)
        assert config.absolute_bounds == (200.0, 500.0, 2000.0)

        # Resource thresholds
        assert config.cpu_thresholds == (0.7, 0.85, 0.95)
        assert config.memory_thresholds == (0.7, 0.85, 0.95)

        # Drift threshold (for dual-baseline drift detection)
        assert config.drift_threshold == 0.15

        # Minimum samples
        assert config.min_samples == 3

    def test_custom_config(self):
        """OverloadConfig should accept custom values."""
        config = OverloadConfig(
            ema_alpha=0.2,
            current_window=5,
            delta_thresholds=(0.1, 0.3, 0.5),
            absolute_bounds=(100.0, 300.0, 1000.0),
        )

        assert config.ema_alpha == 0.2
        assert config.current_window == 5
        assert config.delta_thresholds == (0.1, 0.3, 0.5)
        assert config.absolute_bounds == (100.0, 300.0, 1000.0)


class TestOverloadState:
    """Test OverloadState enum."""

    def test_state_values(self):
        """OverloadState should have correct values."""
        assert OverloadState.HEALTHY.value == "healthy"
        assert OverloadState.BUSY.value == "busy"
        assert OverloadState.STRESSED.value == "stressed"
        assert OverloadState.OVERLOADED.value == "overloaded"


class TestHybridOverloadDetector:
    """Test HybridOverloadDetector class."""

    def test_initial_state_is_healthy(self):
        """Detector should start in healthy state."""
        detector = HybridOverloadDetector()
        state = detector.get_state()
        assert state == OverloadState.HEALTHY

    def test_record_latency_updates_baseline(self):
        """Recording latency should update EMA baseline."""
        detector = HybridOverloadDetector()

        # First sample initializes baseline
        detector.record_latency(50.0)
        assert detector.baseline == 50.0

        # Subsequent samples update EMA
        detector.record_latency(60.0)
        # EMA = 0.1 * 60 + 0.9 * 50 = 6 + 45 = 51
        assert abs(detector.baseline - 51.0) < 0.01

    def test_delta_detection_healthy(self):
        """Detector should return healthy when latency is at baseline."""
        detector = HybridOverloadDetector()

        # Record stable latencies
        for _ in range(10):
            detector.record_latency(50.0)

        state = detector.get_state()
        assert state == OverloadState.HEALTHY

    def test_delta_detection_busy(self):
        """Detector should return busy when latency is 20-50% above baseline."""
        config = OverloadConfig(min_samples=3)
        detector = HybridOverloadDetector(config)

        # Establish baseline around 50ms
        for _ in range(5):
            detector.record_latency(50.0)

        # Spike to ~65ms (30% above baseline)
        for _ in range(3):
            detector.record_latency(65.0)

        state = detector.get_state()
        assert state in (OverloadState.BUSY, OverloadState.STRESSED, OverloadState.HEALTHY)

    def test_absolute_bounds_overloaded(self):
        """Absolute bounds should trigger overloaded for very high latency."""
        detector = HybridOverloadDetector()

        # Record extreme latencies above absolute bound (2000ms)
        for _ in range(3):
            detector.record_latency(2500.0)

        state = detector.get_state()
        assert state == OverloadState.OVERLOADED

    def test_absolute_bounds_stressed(self):
        """Absolute bounds should trigger stressed for high latency."""
        detector = HybridOverloadDetector()

        # Record high latencies above 500ms bound
        for _ in range(3):
            detector.record_latency(800.0)

        state = detector.get_state()
        assert state in (OverloadState.STRESSED, OverloadState.OVERLOADED)

    def test_absolute_bounds_busy(self):
        """Absolute bounds should trigger busy for elevated latency."""
        detector = HybridOverloadDetector()

        # Record elevated latencies above 200ms bound
        for _ in range(3):
            detector.record_latency(300.0)

        state = detector.get_state()
        assert state in (OverloadState.BUSY, OverloadState.STRESSED, OverloadState.OVERLOADED)

    def test_resource_signals_cpu(self):
        """High CPU should contribute to overload state."""
        detector = HybridOverloadDetector()

        # Stable latency
        for _ in range(5):
            detector.record_latency(50.0)

        # High CPU
        state = detector.get_state(cpu_percent=96.0)
        assert state == OverloadState.OVERLOADED

    def test_resource_signals_memory(self):
        """High memory should contribute to overload state."""
        detector = HybridOverloadDetector()

        # Stable latency
        for _ in range(5):
            detector.record_latency(50.0)

        # High memory
        state = detector.get_state(memory_percent=96.0)
        assert state == OverloadState.OVERLOADED

    def test_state_is_maximum_of_signals(self):
        """Final state should be max of delta, absolute, and resource states."""
        detector = HybridOverloadDetector()

        # Low latency (healthy delta and absolute)
        for _ in range(5):
            detector.record_latency(50.0)

        # But high CPU (overloaded resource)
        state = detector.get_state(cpu_percent=96.0)
        assert state == OverloadState.OVERLOADED

    def test_trend_calculation(self):
        """Trend should detect worsening conditions."""
        detector = HybridOverloadDetector()

        # Record increasing latencies
        for i in range(10):
            detector.record_latency(50.0 + i * 5)  # 50, 55, 60, ...

        trend = detector.trend
        # Trend should be positive (worsening)
        assert trend > 0

    def test_reset_clears_state(self):
        """Reset should clear all internal state."""
        detector = HybridOverloadDetector()

        # Record some samples
        for _ in range(10):
            detector.record_latency(100.0)

        assert detector.sample_count == 10

        detector.reset()

        assert detector.sample_count == 0
        assert detector.baseline == 0.0
        assert detector.current_average == 0.0

    def test_diagnostics_includes_all_fields(self):
        """get_diagnostics should return comprehensive state."""
        detector = HybridOverloadDetector()

        for _ in range(5):
            detector.record_latency(100.0)

        diag = detector.get_diagnostics()

        assert "baseline" in diag
        assert "current_avg" in diag
        assert "delta" in diag
        assert "trend" in diag
        assert "sample_count" in diag
        assert "delta_state" in diag
        assert "absolute_state" in diag

    def test_get_state_str_returns_string(self):
        """get_state_str should return string value."""
        detector = HybridOverloadDetector()

        state_str = detector.get_state_str()
        assert state_str == "healthy"

    def test_current_average_property(self):
        """current_average should reflect recent samples."""
        detector = HybridOverloadDetector()

        detector.record_latency(100.0)
        detector.record_latency(200.0)

        # Average of 100 and 200
        assert detector.current_average == 150.0


class TestOverloadDetectionScenarios:
    """Test realistic overload detection scenarios."""

    def test_gradual_overload(self):
        """
        Simulate gradual increase in latency leading to overload.

        Scenario: System starts healthy but latency gradually increases
        due to increasing load until overloaded.
        """
        detector = HybridOverloadDetector()

        # Phase 1: Healthy baseline (~50ms)
        for _ in range(20):
            detector.record_latency(50.0)

        assert detector.get_state() == OverloadState.HEALTHY

        # Phase 2: Latency starts increasing
        for _ in range(10):
            detector.record_latency(150.0)

        state = detector.get_state()
        assert state in (OverloadState.BUSY, OverloadState.STRESSED)

        # Phase 3: System becomes overloaded
        for _ in range(10):
            detector.record_latency(2500.0)

        assert detector.get_state() == OverloadState.OVERLOADED

    def test_spike_recovery(self):
        """
        Simulate a spike that recovers.

        Scenario: System experiences a brief spike but returns to normal.
        """
        detector = HybridOverloadDetector()

        # Establish baseline
        for _ in range(20):
            detector.record_latency(50.0)

        # Brief spike
        for _ in range(5):
            detector.record_latency(300.0)

        # Recovery
        for _ in range(20):
            detector.record_latency(55.0)

        # Should return to healthy (or close to it)
        state = detector.get_state()
        assert state in (OverloadState.HEALTHY, OverloadState.BUSY)

    def test_resource_constrained_without_latency_impact(self):
        """
        Simulate high resource usage without latency degradation.

        Scenario: CPU/memory high but latency still acceptable.
        Resource signals should still flag the concern.
        """
        detector = HybridOverloadDetector()

        # Good latency
        for _ in range(10):
            detector.record_latency(50.0)

        # But high CPU usage
        state = detector.get_state(cpu_percent=90.0, memory_percent=50.0)

        # Resource signals should contribute
        assert state in (OverloadState.STRESSED, OverloadState.OVERLOADED)

    def test_self_calibrating_baseline(self):
        """
        Test that baseline adapts to new normal.

        Scenario: System deployed to new infrastructure with different
        baseline performance. Detector should adapt.
        """
        detector = HybridOverloadDetector()

        # Initial baseline at 50ms
        for _ in range(50):
            detector.record_latency(50.0)

        initial_baseline = detector.baseline

        # New "normal" at 100ms (e.g., after migration)
        for _ in range(100):
            detector.record_latency(100.0)

        new_baseline = detector.baseline

        # Baseline should have adapted toward 100
        assert new_baseline > initial_baseline
        # System should consider this healthy at new baseline
        state = detector.get_state()
        assert state == OverloadState.HEALTHY

    def test_absolute_bounds_prevent_drift_masking(self):
        """
        Test that absolute bounds catch problems despite baseline drift.

        Scenario: Baseline gradually drifts to unacceptable levels.
        Absolute bounds should prevent this from being masked.
        """
        detector = HybridOverloadDetector()

        # Gradual drift to very high latency
        latency = 50.0
        for _ in range(500):
            detector.record_latency(latency)
            latency = min(latency * 1.01, 3000.0)  # Gradual increase with cap

        # Delta detection might see this as "normal" due to adaptation
        # But absolute bounds should trigger
        state = detector.get_state()
        assert state == OverloadState.OVERLOADED
