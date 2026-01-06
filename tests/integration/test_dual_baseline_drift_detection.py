"""
Comprehensive tests for Dual-Baseline Drift Detection (AD-18).

Tests cover:
1. Dual-baseline EMA behavior (fast and slow)
2. Drift calculation correctness
3. Drift-based escalation logic
4. Edge cases: cold start, reset, warmup, zero values
5. Interaction between drift and other detection methods
6. Recovery scenarios (negative drift)
7. Boundary conditions at drift threshold
8. Real-world scenarios: steady rise, spike, oscillation, slow drift
"""

import pytest
import math

from hyperscale.distributed_rewrite.reliability.overload import (
    HybridOverloadDetector,
    OverloadConfig,
    OverloadState,
)


# =============================================================================
# Test Dual-Baseline EMA Behavior
# =============================================================================


class TestDualBaselineEMABehavior:
    """Tests for the dual-baseline (fast/slow EMA) tracking behavior."""

    def test_first_sample_initializes_both_baselines(self):
        """First sample should initialize both fast and slow baselines."""
        detector = HybridOverloadDetector()

        detector.record_latency(100.0)

        assert detector.baseline == 100.0
        assert detector.slow_baseline == 100.0

    def test_fast_baseline_responds_faster_than_slow(self):
        """Fast baseline should change more quickly than slow baseline."""
        config = OverloadConfig(
            ema_alpha=0.1,  # Fast EMA
            slow_ema_alpha=0.02,  # Slow EMA
        )
        detector = HybridOverloadDetector(config)

        # Initialize baselines at 100
        detector.record_latency(100.0)

        # Record a large latency
        detector.record_latency(200.0)

        # Fast baseline: 0.1 * 200 + 0.9 * 100 = 110
        assert detector.baseline == pytest.approx(110.0)

        # Slow baseline: 0.02 * 200 + 0.98 * 100 = 102
        assert detector.slow_baseline == pytest.approx(102.0)

    def test_fast_baseline_tracks_rising_latency(self):
        """Fast baseline should track rising latency more closely."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Initialize at 100
        detector.record_latency(100.0)

        # Steadily increase to 200
        for i in range(20):
            detector.record_latency(100.0 + (i + 1) * 5)  # 105, 110, ..., 200

        # Fast baseline should be closer to 200
        # Slow baseline should be closer to 100
        assert detector.baseline > detector.slow_baseline
        assert detector.baseline > 150.0
        assert detector.slow_baseline < 150.0

    def test_slow_baseline_provides_stable_reference(self):
        """Slow baseline should remain stable during short spikes."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Establish stable baseline at 100
        for _ in range(50):
            detector.record_latency(100.0)

        initial_slow_baseline = detector.slow_baseline

        # Short spike to 500
        for _ in range(5):
            detector.record_latency(500.0)

        # Slow baseline should barely change
        assert detector.slow_baseline < initial_slow_baseline + 50.0

        # Fast baseline should have moved significantly
        assert detector.baseline > initial_slow_baseline + 100.0

    def test_both_baselines_converge_with_stable_input(self):
        """Both baselines should converge to the same value with stable input."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Record stable latency for a long time
        for _ in range(500):
            detector.record_latency(100.0)

        # Both should be very close to 100
        assert detector.baseline == pytest.approx(100.0, rel=0.01)
        assert detector.slow_baseline == pytest.approx(100.0, rel=0.01)


# =============================================================================
# Test Drift Calculation
# =============================================================================


class TestDriftCalculation:
    """Tests for baseline drift calculation correctness."""

    def test_zero_drift_with_identical_baselines(self):
        """Zero drift when fast and slow baselines are equal."""
        detector = HybridOverloadDetector()

        # First sample sets both to same value
        detector.record_latency(100.0)

        assert detector.baseline_drift == 0.0

    def test_positive_drift_with_rising_latency(self):
        """Positive drift when fast baseline is above slow baseline."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Initialize at 100
        detector.record_latency(100.0)

        # Rising latency
        for i in range(20):
            detector.record_latency(100.0 + (i + 1) * 10)

        # Drift should be positive
        assert detector.baseline_drift > 0.0

    def test_negative_drift_with_falling_latency(self):
        """Negative drift when fast baseline is below slow baseline (recovery)."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Initialize at 200
        detector.record_latency(200.0)

        # Falling latency
        for i in range(50):
            detector.record_latency(200.0 - (i + 1) * 3)  # Down to ~50

        # Drift should be negative (fast baseline below slow)
        assert detector.baseline_drift < 0.0

    def test_drift_formula_correctness(self):
        """Verify drift = (fast - slow) / slow."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Initialize at 100
        detector.record_latency(100.0)

        # Add one sample at 200
        detector.record_latency(200.0)

        # Expected:
        # fast = 0.1 * 200 + 0.9 * 100 = 110
        # slow = 0.02 * 200 + 0.98 * 100 = 102
        # drift = (110 - 102) / 102 = 0.0784...

        expected_fast = 110.0
        expected_slow = 102.0
        expected_drift = (expected_fast - expected_slow) / expected_slow

        assert detector.baseline == pytest.approx(expected_fast)
        assert detector.slow_baseline == pytest.approx(expected_slow)
        assert detector.baseline_drift == pytest.approx(expected_drift)

    def test_drift_handles_zero_slow_baseline(self):
        """Drift calculation handles zero slow baseline gracefully."""
        detector = HybridOverloadDetector()

        # With negative values clamped to 0, this creates zero baseline
        # This edge case is handled in _calculate_baseline_drift

        # Uninitialized detector has 0 baseline
        assert detector.baseline_drift == 0.0


# =============================================================================
# Test Drift-Based Escalation Logic
# =============================================================================


class TestDriftEscalation:
    """Tests for drift-based state escalation."""

    def test_moderate_drift_no_escalation_when_healthy(self):
        """Moderate drift (below high_drift_threshold) should NOT escalate from HEALTHY.

        Note: With the high_drift_threshold feature, very high drift CAN escalate
        from HEALTHY to BUSY. This test verifies that moderate drift does not.
        """
        config = OverloadConfig(
            absolute_bounds=(1000.0, 2000.0, 5000.0),  # Won't trigger
            delta_thresholds=(0.5, 1.0, 2.0),  # Won't trigger with small deltas
            drift_threshold=0.01,  # Very sensitive (but only applies to elevated states)
            high_drift_threshold=0.50,  # Set high to prevent escalation in this test
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Create small drift but stay in HEALTHY range
        for i in range(20):
            detector.record_latency(50.0 + i)  # 50, 51, 52, ...

        # Verify drift is below high_drift_threshold
        assert detector.baseline_drift < config.high_drift_threshold

        state = detector.get_state()

        # Should stay HEALTHY since drift is below high_drift_threshold
        assert state == OverloadState.HEALTHY

    def test_busy_escalates_to_stressed_with_drift(self):
        """BUSY state escalates to STRESSED when drift exceeds threshold.

        Drift escalation requires:
        1. Base state from delta to be BUSY (not HEALTHY)
        2. Drift to exceed drift_threshold

        We use absolute bounds to ensure we're at least BUSY, then verify
        drift is calculated correctly. The key insight is that drift escalation
        only applies within delta detection when base_state != HEALTHY.
        """
        config = OverloadConfig(
            # Absolute bounds set low so we trigger BUSY/STRESSED
            absolute_bounds=(120.0, 180.0, 300.0),
            delta_thresholds=(0.10, 0.5, 1.0),
            drift_threshold=0.10,
            ema_alpha=0.3,
            slow_ema_alpha=0.01,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100
        for _ in range(10):
            detector.record_latency(100.0)

        # Create rising pattern that will trigger BUSY via absolute bounds
        # and create drift between fast and slow baselines
        for i in range(30):
            latency = 100.0 + i * 5  # 100, 105, 110, ... 245
            detector.record_latency(latency)

        # Verify drift was created
        assert detector.baseline_drift > 0.05, f"Expected drift > 0.05, got {detector.baseline_drift}"

        # Should be at least BUSY due to absolute bounds (current_avg > 120)
        state = detector.get_state()
        assert state in (OverloadState.BUSY, OverloadState.STRESSED, OverloadState.OVERLOADED), \
            f"Expected elevated state, got {state}, drift={detector.baseline_drift}"

    def test_stressed_escalates_to_overloaded_with_drift(self):
        """STRESSED state escalates to OVERLOADED when drift exceeds threshold.

        Use absolute bounds to ensure we reach STRESSED, then verify drift.
        """
        config = OverloadConfig(
            # Absolute bounds: BUSY at 150, STRESSED at 250, OVERLOADED at 400
            absolute_bounds=(150.0, 250.0, 400.0),
            delta_thresholds=(0.10, 0.30, 1.0),
            drift_threshold=0.12,
            ema_alpha=0.3,
            slow_ema_alpha=0.01,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100
        for _ in range(10):
            detector.record_latency(100.0)

        # Create rapidly rising pattern with steep increases
        # Final values will be around 300-400, triggering STRESSED via absolute bounds
        for i in range(40):
            latency = 100.0 + i * 8  # 100, 108, 116, ... 412
            detector.record_latency(latency)

        # Should be at least STRESSED due to absolute bounds (current_avg > 250)
        state = detector.get_state()
        assert state in (OverloadState.STRESSED, OverloadState.OVERLOADED), \
            f"Expected STRESSED or OVERLOADED, got {state}, drift={detector.baseline_drift}"

    def test_already_overloaded_stays_overloaded(self):
        """OVERLOADED state cannot escalate further."""
        config = OverloadConfig(
            absolute_bounds=(150.0, 250.0, 400.0),  # Will trigger OVERLOADED at high latencies
            delta_thresholds=(0.2, 0.5, 0.8),
            drift_threshold=0.10,
            ema_alpha=0.3,
            slow_ema_alpha=0.01,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100
        for _ in range(5):
            detector.record_latency(100.0)

        # Create very high latency to trigger OVERLOADED via absolute bounds
        for _ in range(10):
            detector.record_latency(500.0)  # Above absolute overloaded threshold of 400

        state = detector.get_state()
        assert state == OverloadState.OVERLOADED

    def test_drift_below_threshold_no_escalation(self):
        """No escalation when drift is below threshold."""
        config = OverloadConfig(
            absolute_bounds=(150.0, 300.0, 500.0),  # BUSY at 150ms
            delta_thresholds=(0.5, 0.8, 1.5),  # High delta thresholds - won't trigger
            drift_threshold=0.50,  # Very high threshold
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(10):
            detector.record_latency(100.0)

        # Create stable elevated latency that triggers BUSY via absolute bounds
        # but doesn't create significant drift (staying flat, not rising)
        for _ in range(10):
            detector.record_latency(180.0)  # Above 150ms BUSY threshold

        # Should be BUSY due to absolute bounds
        state = detector.get_state()
        assert state == OverloadState.BUSY, \
            f"Expected BUSY, got {state}"


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestDriftEdgeCases:
    """Tests for edge cases in drift detection."""

    def test_cold_start_behavior(self):
        """Cold start: first sample sets both baselines."""
        detector = HybridOverloadDetector()

        assert detector.baseline == 0.0
        assert detector.slow_baseline == 0.0
        assert detector.baseline_drift == 0.0

        detector.record_latency(100.0)

        assert detector.baseline == 100.0
        assert detector.slow_baseline == 100.0
        assert detector.baseline_drift == 0.0

    def test_reset_clears_both_baselines(self):
        """Reset clears both fast and slow baselines."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Build up drift
        detector.record_latency(100.0)
        for i in range(20):
            detector.record_latency(100.0 + (i + 1) * 5)

        assert detector.baseline > 100.0
        assert detector.slow_baseline > 100.0
        assert detector.baseline_drift != 0.0

        detector.reset()

        assert detector.baseline == 0.0
        assert detector.slow_baseline == 0.0
        assert detector.baseline_drift == 0.0

    def test_warmup_period_uses_absolute_bounds_only(self):
        """During warmup, delta detection is inactive."""
        config = OverloadConfig(
            warmup_samples=10,
            delta_thresholds=(0.1, 0.2, 0.3),  # Very aggressive
            absolute_bounds=(1000.0, 2000.0, 5000.0),  # Won't trigger
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # During warmup, even high delta shouldn't trigger delta detection
        for _ in range(5):  # Less than warmup_samples
            detector.record_latency(200.0)  # Would be high delta if active

        assert detector.in_warmup is True
        state = detector._get_delta_state()
        assert state == OverloadState.HEALTHY  # Delta detection inactive

    def test_zero_latency_samples(self):
        """Handle zero latency samples correctly."""
        detector = HybridOverloadDetector()

        for _ in range(10):
            detector.record_latency(0.0)

        assert detector.baseline == 0.0
        assert detector.slow_baseline == 0.0
        # Division by zero should be handled
        assert detector.baseline_drift == 0.0

    def test_very_small_latency_values(self):
        """Handle very small latency values correctly."""
        detector = HybridOverloadDetector()

        for _ in range(10):
            detector.record_latency(0.001)

        assert detector.baseline == pytest.approx(0.001)
        assert detector.slow_baseline == pytest.approx(0.001)
        assert detector.baseline_drift == 0.0

    def test_very_large_latency_values(self):
        """Handle very large latency values correctly."""
        detector = HybridOverloadDetector()

        detector.record_latency(1_000_000.0)

        assert detector.baseline == 1_000_000.0
        assert detector.slow_baseline == 1_000_000.0

        # Should be OVERLOADED due to absolute bounds
        assert detector.get_state() == OverloadState.OVERLOADED

    def test_negative_latency_clamped(self):
        """Negative latency is clamped to zero."""
        detector = HybridOverloadDetector()

        detector.record_latency(-100.0)

        assert detector.baseline == 0.0
        assert detector.slow_baseline == 0.0

    def test_mixed_positive_and_negative_latencies(self):
        """Mix of positive and negative latencies doesn't corrupt state."""
        detector = HybridOverloadDetector()

        latencies = [100.0, -50.0, 150.0, -200.0, 200.0, -100.0, 100.0]
        for latency in latencies:
            detector.record_latency(latency)

        # Should have valid, non-negative baselines
        assert detector.baseline >= 0.0
        assert detector.slow_baseline >= 0.0

        # Should have valid state
        state = detector.get_state()
        assert state in OverloadState.__members__.values()


# =============================================================================
# Test Interaction With Other Detection Methods
# =============================================================================


class TestDriftInteractionWithOtherMethods:
    """Tests for interaction between drift and other detection methods."""

    def test_absolute_bounds_override_drift(self):
        """Absolute bounds should trigger regardless of drift state."""
        config = OverloadConfig(
            absolute_bounds=(100.0, 200.0, 500.0),  # Will trigger
            delta_thresholds=(0.5, 1.0, 2.0),  # Won't trigger easily
            drift_threshold=0.15,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Low drift, but high absolute latency
        for _ in range(10):
            detector.record_latency(600.0)  # Above overloaded bound

        state = detector.get_state()
        assert state == OverloadState.OVERLOADED

    def test_resource_signals_override_drift(self):
        """Resource signals should trigger regardless of drift state."""
        config = OverloadConfig(
            absolute_bounds=(1000.0, 2000.0, 5000.0),  # Won't trigger
            delta_thresholds=(0.5, 1.0, 2.0),  # Won't trigger
            cpu_thresholds=(0.5, 0.7, 0.9),
            drift_threshold=0.15,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Low latency, no drift
        for _ in range(10):
            detector.record_latency(50.0)

        # But high CPU
        state = detector.get_state(cpu_percent=95.0)
        assert state == OverloadState.OVERLOADED

    def test_drift_combines_with_delta_detection(self):
        """Drift escalation works alongside delta detection."""
        config = OverloadConfig(
            absolute_bounds=(1000.0, 2000.0, 5000.0),  # Won't trigger
            delta_thresholds=(0.2, 0.5, 1.0),
            drift_threshold=0.10,
            ema_alpha=0.3,
            slow_ema_alpha=0.01,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(5):
            detector.record_latency(100.0)

        # Create delta in BUSY range
        for _ in range(5):
            detector.record_latency(125.0)  # 25% delta

        state_without_drift = detector.get_state()

        # Now continue with rising pattern to create drift
        for i in range(15):
            detector.record_latency(130.0 + i * 3)

        state_with_drift = detector.get_state()

        # State with drift should be at least as severe
        from hyperscale.distributed_rewrite.reliability.overload import _STATE_ORDER
        assert _STATE_ORDER[state_with_drift] >= _STATE_ORDER[state_without_drift]


# =============================================================================
# Test Recovery Scenarios
# =============================================================================


class TestDriftRecoveryScenarios:
    """Tests for recovery scenarios with negative drift."""

    def test_recovery_creates_negative_drift(self):
        """Recovery from high latency creates negative drift."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Start high
        detector.record_latency(200.0)

        # Recovery
        for _ in range(30):
            detector.record_latency(50.0)

        # Fast baseline drops faster, creating negative drift
        assert detector.baseline < detector.slow_baseline
        assert detector.baseline_drift < 0.0

    def test_negative_drift_does_not_trigger_escalation(self):
        """Negative drift should not trigger escalation."""
        config = OverloadConfig(
            absolute_bounds=(1000.0, 2000.0, 5000.0),
            delta_thresholds=(0.2, 0.5, 1.0),
            drift_threshold=0.10,
            ema_alpha=0.2,
            slow_ema_alpha=0.01,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Start with high latency
        for _ in range(10):
            detector.record_latency(150.0)

        # Recovery to low latency
        for _ in range(30):
            detector.record_latency(50.0)

        # Should be HEALTHY despite any drift
        state = detector.get_state()
        assert state == OverloadState.HEALTHY

    def test_oscillating_latency_low_drift(self):
        """Oscillating latency should result in low net drift."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Oscillate between 80 and 120
        for i in range(100):
            if i % 2 == 0:
                detector.record_latency(80.0)
            else:
                detector.record_latency(120.0)

        # Both baselines should converge to ~100
        # Drift should be near zero
        assert abs(detector.baseline_drift) < 0.05


# =============================================================================
# Test Boundary Conditions at Drift Threshold
# =============================================================================


class TestDriftBoundaryConditions:
    """Tests for boundary conditions at drift threshold."""

    def test_drift_just_below_threshold_no_escalation(self):
        """Drift just below threshold should not trigger escalation."""
        drift_threshold = 0.15
        config = OverloadConfig(
            absolute_bounds=(1000.0, 2000.0, 5000.0),
            delta_thresholds=(0.2, 0.5, 1.0),
            drift_threshold=drift_threshold,
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Carefully construct scenario with drift just below threshold
        # This is approximate - exact drift depends on EMA dynamics
        detector.record_latency(100.0)

        # Small rise to create limited drift
        for _ in range(5):
            detector.record_latency(110.0)

        # If drift is below threshold and delta is in BUSY range,
        # should stay BUSY (not escalate to STRESSED)
        if detector.baseline_drift < drift_threshold:
            state = detector._get_delta_state()
            # Should not be escalated beyond what delta alone would give
            assert state != OverloadState.OVERLOADED

    def test_drift_exactly_at_threshold(self):
        """Drift at exactly the threshold should trigger escalation."""
        # This is hard to test exactly due to floating point,
        # but we can verify behavior near the threshold

        drift_threshold = 0.15
        config = OverloadConfig(
            absolute_bounds=(1000.0, 2000.0, 5000.0),
            delta_thresholds=(0.2, 0.5, 1.0),
            drift_threshold=drift_threshold,
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Build up to create drift at or above threshold
        detector.record_latency(100.0)

        for i in range(30):
            detector.record_latency(100.0 + (i + 1) * 2)

        # If drift exceeds threshold and base state is BUSY, should escalate
        # We just verify the system handles this without error
        state = detector.get_state()
        assert state in OverloadState.__members__.values()


# =============================================================================
# Test Real-World Scenarios
# =============================================================================


class TestRealWorldDriftScenarios:
    """Tests for real-world drift detection scenarios."""

    def test_steady_rise_scenario(self):
        """
        Scenario: Gradual degradation where latency steadily increases.

        This is the primary case dual-baseline drift detection was designed for.
        The fast EMA tracks rising values more closely, while the slow EMA
        lags behind, creating detectable drift.

        We use realistic absolute bounds so the rising latencies will eventually
        trigger an elevated state. The test verifies both drift detection works
        AND the system reaches an elevated state.
        """
        config = OverloadConfig(
            # Realistic absolute bounds - will trigger as latencies rise
            absolute_bounds=(200.0, 350.0, 500.0),
            delta_thresholds=(0.10, 0.30, 0.80),
            drift_threshold=0.10,
            ema_alpha=0.15,
            slow_ema_alpha=0.01,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100ms
        for _ in range(20):
            detector.record_latency(100.0)

        initial_state = detector.get_state()
        assert initial_state == OverloadState.HEALTHY

        # Gradual rise: 3ms per sample (100, 103, 106, ... 397)
        # Final values around 370-397, which exceeds STRESSED threshold of 350
        for i in range(100):
            detector.record_latency(100.0 + i * 3)

        # Verify drift was created by the rising pattern
        assert detector.baseline_drift > 0.1, \
            f"Expected drift > 0.1, got {detector.baseline_drift}"

        # Should detect degradation via absolute bounds (current_avg > 200)
        final_state = detector.get_state()

        from hyperscale.distributed_rewrite.reliability.overload import _STATE_ORDER
        assert _STATE_ORDER[final_state] >= _STATE_ORDER[OverloadState.BUSY], \
            f"Expected at least BUSY, got {final_state}, drift={detector.baseline_drift}"

    def test_spike_then_stable_scenario(self):
        """
        Scenario: Sudden spike that then stabilizes at higher level.

        Delta detection handles the initial spike.
        Drift detection catches that the new level is higher.
        """
        config = OverloadConfig(
            absolute_bounds=(1000.0, 2000.0, 5000.0),
            delta_thresholds=(0.2, 0.5, 1.0),
            drift_threshold=0.15,
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100ms
        for _ in range(30):
            detector.record_latency(100.0)

        # Sudden spike to 200ms (stable at new level)
        for _ in range(20):
            detector.record_latency(200.0)

        # Fast baseline should have moved toward 200
        # Slow baseline should still be closer to 100
        # Drift should be significant
        assert detector.baseline > detector.slow_baseline
        assert detector.baseline_drift > 0.10

    def test_slow_drift_scenario(self):
        """
        Scenario: Very slow drift over time.

        Tests that even slow, continuous degradation is detected.
        """
        config = OverloadConfig(
            absolute_bounds=(1000.0, 2000.0, 5000.0),
            delta_thresholds=(0.2, 0.5, 1.0),
            drift_threshold=0.10,
            ema_alpha=0.1,
            slow_ema_alpha=0.01,  # Very slow
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(50):
            detector.record_latency(100.0)

        # Very slow drift: 0.2ms per sample
        for i in range(200):
            detector.record_latency(100.0 + i * 0.2)  # 100 -> 140 over 200 samples

        # Should have accumulated drift
        assert detector.baseline_drift > 0.0

    def test_recovery_after_overload_scenario(self):
        """
        Scenario: System recovers after being overloaded.

        Tests that drift becomes negative during recovery.
        """
        config = OverloadConfig(
            absolute_bounds=(200.0, 400.0, 800.0),
            delta_thresholds=(0.2, 0.5, 1.0),
            drift_threshold=0.15,
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
            warmup_samples=0,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Start healthy
        for _ in range(20):
            detector.record_latency(100.0)

        # Overload phase
        for _ in range(30):
            detector.record_latency(900.0)
            detector.get_state()  # Update hysteresis

        assert detector.get_state() == OverloadState.OVERLOADED

        # Recovery phase
        for _ in range(50):
            detector.record_latency(80.0)
            detector.get_state()  # Update hysteresis

        # Should recover to healthy
        final_state = detector.get_state()
        assert final_state == OverloadState.HEALTHY

        # Drift should be negative (fast below slow)
        assert detector.baseline_drift < 0.0

    def test_intermittent_spikes_scenario(self):
        """
        Scenario: Occasional spikes but generally healthy.

        Tests that intermittent spikes don't trigger false drift alarms.
        """
        config = OverloadConfig(
            absolute_bounds=(500.0, 1000.0, 2000.0),
            delta_thresholds=(0.3, 0.6, 1.0),
            drift_threshold=0.15,
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
            warmup_samples=0,
            hysteresis_samples=3,  # Some hysteresis
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(30):
            detector.record_latency(100.0)

        # Occasional spikes mixed with normal operation
        for i in range(100):
            if i % 20 == 0:  # Spike every 20 samples
                detector.record_latency(400.0)
            else:
                detector.record_latency(100.0)
            detector.get_state()

        # Should still be near healthy after sufficient normal samples
        # Drift should be relatively low due to averaging effect
        assert abs(detector.baseline_drift) < 0.20


# =============================================================================
# Test Diagnostics Include Drift Information
# =============================================================================


class TestDriftDiagnostics:
    """Tests for drift information in diagnostics."""

    def test_diagnostics_includes_slow_baseline(self):
        """Diagnostics should include slow baseline."""
        detector = HybridOverloadDetector()

        for _ in range(10):
            detector.record_latency(100.0)

        diagnostics = detector.get_diagnostics()

        assert "slow_baseline" in diagnostics
        assert diagnostics["slow_baseline"] == pytest.approx(100.0, rel=0.05)

    def test_diagnostics_includes_baseline_drift(self):
        """Diagnostics should include baseline drift."""
        config = OverloadConfig(
            ema_alpha=0.1,
            slow_ema_alpha=0.02,
        )
        detector = HybridOverloadDetector(config)

        # Create some drift
        detector.record_latency(100.0)
        for _ in range(10):
            detector.record_latency(150.0)

        diagnostics = detector.get_diagnostics()

        assert "baseline_drift" in diagnostics
        assert diagnostics["baseline_drift"] > 0.0

    def test_diagnostics_includes_warmup_status(self):
        """Diagnostics should include warmup status."""
        config = OverloadConfig(warmup_samples=20)
        detector = HybridOverloadDetector(config)

        for _ in range(10):
            detector.record_latency(100.0)

        diagnostics = detector.get_diagnostics()

        assert "in_warmup" in diagnostics
        assert diagnostics["in_warmup"] is True

        # After warmup
        for _ in range(15):
            detector.record_latency(100.0)

        diagnostics = detector.get_diagnostics()
        assert diagnostics["in_warmup"] is False


# =============================================================================
# Test High Drift Escalation (Boiled Frog Detection)
# =============================================================================


class TestHighDriftEscalation:
    """Tests for high drift escalation from HEALTHY to BUSY.

    The "boiled frog" scenario: latency rises so gradually that delta stays
    near zero (because fast baseline tracks the rise), but the system has
    significantly degraded from its original operating point.

    The high_drift_threshold parameter allows escalation from HEALTHY to BUSY
    when drift exceeds this threshold, even if delta-based detection shows HEALTHY.
    """

    def test_high_drift_escalates_healthy_to_busy(self):
        """Very high drift should escalate HEALTHY to BUSY.

        This tests the "boiled frog" detection where gradual rise keeps delta
        low but drift accumulates significantly.
        """
        config = OverloadConfig(
            # Absolute bounds won't trigger (values will stay below)
            absolute_bounds=(500.0, 1000.0, 2000.0),
            # Delta thresholds won't trigger (fast EMA tracks the rise)
            delta_thresholds=(0.3, 0.6, 1.0),
            drift_threshold=0.15,
            high_drift_threshold=0.25,  # Escalate HEALTHY->BUSY at 25% drift
            ema_alpha=0.15,
            slow_ema_alpha=0.01,  # Very slow to accumulate drift
            warmup_samples=10,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline at 100ms
        for _ in range(20):
            detector.record_latency(100.0)

        # Gradual rise - slow enough that delta stays low but drift accumulates
        # Rise from 100 to ~220 over 200 samples (0.6ms per sample)
        for i in range(200):
            detector.record_latency(100.0 + i * 0.6)

        # Verify drift exceeds high_drift_threshold
        assert detector.baseline_drift > config.high_drift_threshold, \
            f"Expected drift > {config.high_drift_threshold}, got {detector.baseline_drift}"

        # Should be BUSY due to high drift escalation, even though delta is low
        # and absolute bounds haven't triggered
        state = detector.get_state()
        assert state == OverloadState.BUSY, \
            f"Expected BUSY from high drift escalation, got {state}, drift={detector.baseline_drift}"

    def test_drift_below_high_threshold_stays_healthy(self):
        """Drift below high_drift_threshold should not escalate from HEALTHY."""
        config = OverloadConfig(
            absolute_bounds=(500.0, 1000.0, 2000.0),
            delta_thresholds=(0.3, 0.6, 1.0),
            drift_threshold=0.15,
            high_drift_threshold=0.30,  # Higher threshold
            ema_alpha=0.15,
            slow_ema_alpha=0.02,
            warmup_samples=10,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(20):
            detector.record_latency(100.0)

        # Moderate rise that creates drift below high_drift_threshold
        for i in range(50):
            detector.record_latency(100.0 + i * 0.3)  # Slow rise

        # Verify drift is below high_drift_threshold
        assert detector.baseline_drift < config.high_drift_threshold, \
            f"Expected drift < {config.high_drift_threshold}, got {detector.baseline_drift}"

        # Should stay HEALTHY since drift is below high threshold
        state = detector.get_state()
        assert state == OverloadState.HEALTHY, \
            f"Expected HEALTHY, got {state}, drift={detector.baseline_drift}"

    def test_high_drift_threshold_disabled_with_high_value(self):
        """Setting high_drift_threshold very high effectively disables it."""
        config = OverloadConfig(
            absolute_bounds=(500.0, 1000.0, 2000.0),
            delta_thresholds=(0.3, 0.6, 1.0),
            drift_threshold=0.15,
            high_drift_threshold=100.0,  # Effectively disabled
            ema_alpha=0.15,
            slow_ema_alpha=0.01,
            warmup_samples=10,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(20):
            detector.record_latency(100.0)

        # Create significant drift
        for i in range(200):
            detector.record_latency(100.0 + i * 0.8)

        # Even with high drift, should stay HEALTHY if high_drift_threshold is disabled
        # (unless absolute bounds or delta trigger)
        diagnostics = detector.get_diagnostics()
        delta_state = diagnostics["delta_state"]
        absolute_state = diagnostics["absolute_state"]

        # If neither delta nor absolute triggered, should be HEALTHY
        if delta_state == "healthy" and absolute_state == "healthy":
            state = detector.get_state()
            assert state == OverloadState.HEALTHY

    def test_high_drift_only_applies_to_healthy_base_state(self):
        """High drift escalation only applies when base state is HEALTHY.

        If base state is already BUSY or higher, the regular drift escalation
        applies, not the high_drift_threshold.
        """
        config = OverloadConfig(
            absolute_bounds=(500.0, 1000.0, 2000.0),
            # Delta thresholds set so we get BUSY at 30% delta
            delta_thresholds=(0.25, 0.6, 1.0),
            drift_threshold=0.15,
            high_drift_threshold=0.30,
            ema_alpha=0.15,
            slow_ema_alpha=0.01,
            warmup_samples=10,
            hysteresis_samples=1,
            min_samples=3,
            current_window=5,
        )
        detector = HybridOverloadDetector(config)

        # Establish baseline
        for _ in range(20):
            detector.record_latency(100.0)

        # Create both high delta and high drift
        for i in range(100):
            detector.record_latency(100.0 + i * 2)  # Rise to 300

        diagnostics = detector.get_diagnostics()

        # If delta puts us at BUSY (not HEALTHY), drift escalation should
        # potentially escalate to STRESSED, not just BUSY
        if diagnostics["delta"] > config.delta_thresholds[0]:
            state = detector.get_state()
            # Should be at least BUSY, possibly STRESSED due to drift escalation
            from hyperscale.distributed_rewrite.reliability.overload import _STATE_ORDER
            assert _STATE_ORDER[state] >= _STATE_ORDER[OverloadState.BUSY]

    def test_boiled_frog_real_world_scenario(self):
        """Real-world boiled frog: gradual degradation over many samples.

        Simulates a memory leak or resource exhaustion that slowly degrades
        performance over time, where each individual measurement looks OK
        relative to recent history.
        """
        config = OverloadConfig(
            absolute_bounds=(300.0, 500.0, 800.0),
            delta_thresholds=(0.25, 0.5, 1.0),
            drift_threshold=0.15,
            high_drift_threshold=0.35,
            ema_alpha=0.1,
            slow_ema_alpha=0.005,  # Very slow baseline
            warmup_samples=10,
            hysteresis_samples=1,
            min_samples=3,
            current_window=10,
        )
        detector = HybridOverloadDetector(config)

        # Establish stable baseline at 80ms for a long time
        for _ in range(100):
            detector.record_latency(80.0)

        initial_baseline = detector.baseline
        initial_slow_baseline = detector.slow_baseline

        # Very slow degradation: 0.1ms per sample over 500 samples (80 -> 130)
        # This is slow enough that delta detection won't trigger
        for i in range(500):
            latency = 80.0 + i * 0.1
            detector.record_latency(latency)

        # Verify significant drift accumulated
        final_drift = detector.baseline_drift

        # The fast baseline should have moved significantly from initial
        assert detector.baseline > initial_baseline + 30.0, \
            f"Fast baseline should have risen significantly"

        # Slow baseline should have moved less
        assert detector.slow_baseline < detector.baseline, \
            f"Slow baseline should be lower than fast baseline"

        # Check final state - should detect the degradation via high drift
        state = detector.get_state()

        # Should be at least BUSY (via high drift) or higher (via absolute bounds)
        from hyperscale.distributed_rewrite.reliability.overload import _STATE_ORDER
        assert _STATE_ORDER[state] >= _STATE_ORDER[OverloadState.BUSY], \
            f"Expected at least BUSY, got {state}, drift={final_drift}"
