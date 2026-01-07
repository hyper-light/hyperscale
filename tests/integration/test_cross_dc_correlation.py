"""
Integration tests for CrossDCCorrelationDetector (Phase 7).

Tests cross-DC correlation detection for eviction decisions to prevent
cascade evictions when multiple datacenters fail simultaneously.

Test Categories:
1. Basic functionality - recording failures and recoveries
2. Correlation detection - threshold-based severity classification
3. Backoff behavior - correlation backoff timing
4. Edge cases - boundary conditions and error handling
5. Statistics and monitoring - stats tracking
6. Concurrent failures - simultaneous failure scenarios
"""

import time
import pytest

from hyperscale.distributed_rewrite.datacenters import (
    CrossDCCorrelationDetector,
    CrossDCCorrelationConfig,
    CorrelationDecision,
    CorrelationSeverity,
    DCFailureRecord,
)


# ============================================================================
# Test Configuration
# ============================================================================


class TestCrossDCCorrelationConfig:
    """Tests for CrossDCCorrelationConfig defaults and customization."""

    def test_default_config_values(self):
        """Test default configuration values are sensible."""
        config = CrossDCCorrelationConfig()

        assert config.correlation_window_seconds == 30.0
        assert config.low_threshold == 2
        assert config.medium_threshold == 3
        assert config.high_threshold_fraction == 0.5
        assert config.correlation_backoff_seconds == 60.0
        assert config.max_failures_per_dc == 100

    def test_custom_config_values(self):
        """Test custom configuration is applied."""
        config = CrossDCCorrelationConfig(
            correlation_window_seconds=60.0,
            low_threshold=3,
            medium_threshold=5,
            high_threshold_fraction=0.7,
            correlation_backoff_seconds=120.0,
            max_failures_per_dc=50,
        )

        assert config.correlation_window_seconds == 60.0
        assert config.low_threshold == 3
        assert config.medium_threshold == 5
        assert config.high_threshold_fraction == 0.7
        assert config.correlation_backoff_seconds == 120.0
        assert config.max_failures_per_dc == 50


# ============================================================================
# Basic Functionality Tests
# ============================================================================


class TestBasicFunctionality:
    """Tests for basic recording and tracking functionality."""

    def test_add_datacenter(self):
        """Test adding datacenters for tracking."""
        detector = CrossDCCorrelationDetector()

        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")
        detector.add_datacenter("dc-central")

        stats = detector.get_stats()
        assert stats["known_datacenters"] == 3

    def test_remove_datacenter(self):
        """Test removing datacenters from tracking."""
        detector = CrossDCCorrelationDetector()

        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")
        detector.remove_datacenter("dc-west")

        stats = detector.get_stats()
        assert stats["known_datacenters"] == 1

    def test_record_failure(self):
        """Test recording a datacenter failure."""
        detector = CrossDCCorrelationDetector()

        detector.record_failure("dc-west", "unhealthy", manager_count_affected=3)

        stats = detector.get_stats()
        assert stats["total_failures_recorded"] == 1
        assert stats["datacenters_with_failures"] == 1
        assert "dc-west" in stats["recent_failing_dcs"]

    def test_record_failure_auto_adds_datacenter(self):
        """Test that recording a failure auto-adds the datacenter."""
        detector = CrossDCCorrelationDetector()

        # Don't explicitly add the DC
        detector.record_failure("dc-unknown", "timeout")

        stats = detector.get_stats()
        assert stats["known_datacenters"] == 1
        assert "dc-unknown" in stats["recent_failing_dcs"]

    def test_record_recovery_clears_failures(self):
        """Test that recording recovery clears failure history."""
        detector = CrossDCCorrelationDetector()

        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-west", "timeout")
        assert detector.get_recent_failure_count("dc-west") == 2

        detector.record_recovery("dc-west")
        assert detector.get_recent_failure_count("dc-west") == 0

    def test_multiple_failures_same_dc(self):
        """Test recording multiple failures for the same DC."""
        detector = CrossDCCorrelationDetector()

        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-west", "timeout")
        detector.record_failure("dc-west", "unreachable")

        stats = detector.get_stats()
        assert stats["total_failures_recorded"] == 3
        assert detector.get_recent_failure_count("dc-west") == 3


# ============================================================================
# Correlation Detection Tests
# ============================================================================


class TestCorrelationDetection:
    """Tests for correlation detection logic."""

    def test_no_correlation_single_dc_failure(self):
        """Test no correlation detected for single DC failure."""
        detector = CrossDCCorrelationDetector()
        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")
        detector.add_datacenter("dc-central")

        detector.record_failure("dc-west", "unhealthy")

        decision = detector.check_correlation("dc-west")
        assert decision.severity == CorrelationSeverity.NONE
        assert not decision.should_delay_eviction

    def test_low_correlation_two_dc_failures(self):
        """Test LOW correlation when 2 DCs fail within window."""
        config = CrossDCCorrelationConfig(
            low_threshold=2,
            medium_threshold=3,
        )
        detector = CrossDCCorrelationDetector(config=config)
        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")
        detector.add_datacenter("dc-central")
        detector.add_datacenter("dc-north")

        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")

        decision = detector.check_correlation("dc-west")
        assert decision.severity == CorrelationSeverity.LOW
        assert not decision.should_delay_eviction  # LOW doesn't delay
        assert len(decision.affected_datacenters) == 2

    def test_medium_correlation_three_dc_failures(self):
        """Test MEDIUM correlation when 3 DCs fail within window."""
        config = CrossDCCorrelationConfig(
            low_threshold=2,
            medium_threshold=3,
            high_threshold_fraction=0.8,  # Set high so we don't trigger HIGH
        )
        detector = CrossDCCorrelationDetector(config=config)
        for dc in ["dc-west", "dc-east", "dc-central", "dc-north", "dc-south"]:
            detector.add_datacenter(dc)

        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")
        detector.record_failure("dc-central", "unhealthy")

        decision = detector.check_correlation("dc-west")
        assert decision.severity == CorrelationSeverity.MEDIUM
        assert decision.should_delay_eviction
        assert len(decision.affected_datacenters) == 3

    def test_high_correlation_majority_dc_failures(self):
        """Test HIGH correlation when majority of DCs fail."""
        config = CrossDCCorrelationConfig(
            high_threshold_fraction=0.5,  # 50% threshold
            high_count_threshold=3,  # Need at least 3 for HIGH
        )
        detector = CrossDCCorrelationDetector(config=config)
        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")
        detector.add_datacenter("dc-central")
        detector.add_datacenter("dc-north")

        # 3 out of 4 = 75% >= 50% AND 3 >= high_count_threshold=3 → HIGH
        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")
        detector.record_failure("dc-central", "unhealthy")

        decision = detector.check_correlation("dc-west")
        assert decision.severity == CorrelationSeverity.HIGH
        assert decision.should_delay_eviction
        assert len(decision.affected_datacenters) == 3

    def test_correlation_decision_should_delay_eviction(self):
        """Test should_delay_eviction property for different severities."""
        # NONE - don't delay
        decision_none = CorrelationDecision(
            severity=CorrelationSeverity.NONE,
            reason="test",
        )
        assert not decision_none.should_delay_eviction

        # LOW - don't delay
        decision_low = CorrelationDecision(
            severity=CorrelationSeverity.LOW,
            reason="test",
        )
        assert not decision_low.should_delay_eviction

        # MEDIUM - delay
        decision_medium = CorrelationDecision(
            severity=CorrelationSeverity.MEDIUM,
            reason="test",
        )
        assert decision_medium.should_delay_eviction

        # HIGH - delay
        decision_high = CorrelationDecision(
            severity=CorrelationSeverity.HIGH,
            reason="test",
        )
        assert decision_high.should_delay_eviction


# ============================================================================
# Correlation Window Tests
# ============================================================================


class TestCorrelationWindow:
    """Tests for time-window based correlation detection."""

    def test_failures_within_window_correlated(self):
        """Test failures within window are correlated."""
        config = CrossDCCorrelationConfig(
            correlation_window_seconds=10.0,
            low_threshold=2,
        )
        detector = CrossDCCorrelationDetector(config=config)
        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")

        # Both failures within window
        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")

        decision = detector.check_correlation("dc-west")
        assert decision.severity != CorrelationSeverity.NONE
        assert len(decision.affected_datacenters) == 2

    def test_cleanup_old_records(self):
        """Test that old records are cleaned up."""
        config = CrossDCCorrelationConfig(
            correlation_window_seconds=0.1,  # Very short window for testing
        )
        detector = CrossDCCorrelationDetector(config=config)

        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")

        # Wait for window to expire
        time.sleep(0.15)

        removed = detector.cleanup_old_records()
        assert removed == 2

        stats = detector.get_stats()
        assert stats["recent_failing_count"] == 0

    def test_max_failures_per_dc_enforced(self):
        """Test that max failures per DC is enforced."""
        config = CrossDCCorrelationConfig(
            max_failures_per_dc=3,
        )
        detector = CrossDCCorrelationDetector(config=config)

        # Record more than max
        for i in range(5):
            detector.record_failure("dc-west", f"failure-{i}")

        # Should only keep the last 3
        assert detector.get_recent_failure_count("dc-west") == 3


# ============================================================================
# Backoff Behavior Tests
# ============================================================================


class TestBackoffBehavior:
    """Tests for correlation backoff timing."""

    def test_backoff_after_correlation_detected(self):
        """Test that backoff is triggered after correlation detected."""
        config = CrossDCCorrelationConfig(
            correlation_backoff_seconds=0.2,  # Short for testing
            medium_threshold=2,
        )
        detector = CrossDCCorrelationDetector(config=config)
        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")

        # Trigger correlation
        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")
        decision1 = detector.check_correlation("dc-west")
        assert decision1.severity == CorrelationSeverity.MEDIUM

        # Recovery
        detector.record_recovery("dc-west")
        detector.record_recovery("dc-east")

        # New failure should still be in backoff
        detector.record_failure("dc-west", "unhealthy")
        decision2 = detector.check_correlation("dc-west")
        assert decision2.should_delay_eviction
        assert "backoff" in decision2.reason.lower()

    def test_backoff_expires(self):
        """Test that backoff expires after configured duration."""
        config = CrossDCCorrelationConfig(
            correlation_backoff_seconds=0.1,  # Very short for testing
            medium_threshold=2,
        )
        detector = CrossDCCorrelationDetector(config=config)
        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")

        # Trigger correlation
        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")
        detector.check_correlation("dc-west")  # This sets backoff time

        # Recovery and wait for backoff
        detector.record_recovery("dc-west")
        detector.record_recovery("dc-east")
        time.sleep(0.15)

        # New single failure should NOT be in backoff
        detector.record_failure("dc-west", "unhealthy")
        decision = detector.check_correlation("dc-west")
        assert decision.severity == CorrelationSeverity.NONE
        assert "backoff" not in decision.reason.lower()


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_check_correlation_unknown_dc(self):
        """Test checking correlation for unknown datacenter."""
        detector = CrossDCCorrelationDetector()

        # DC not added, no failures
        decision = detector.check_correlation("dc-unknown")
        assert decision.severity == CorrelationSeverity.NONE
        assert not decision.should_delay_eviction

    def test_empty_detector(self):
        """Test operations on empty detector."""
        detector = CrossDCCorrelationDetector()

        # All operations should work on empty detector
        detector.cleanup_old_records()
        detector.clear_all()
        decision = detector.check_correlation("any-dc")

        assert decision.severity == CorrelationSeverity.NONE
        stats = detector.get_stats()
        assert stats["known_datacenters"] == 0

    def test_zero_known_datacenters(self):
        """Test correlation check with no known datacenters."""
        config = CrossDCCorrelationConfig(
            high_threshold_fraction=0.5,
            high_count_threshold=2,  # Lower threshold for testing with few DCs
        )
        detector = CrossDCCorrelationDetector(config=config)

        # Record failure without adding DC first
        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")

        # Should handle division by known_dc_count gracefully
        decision = detector.check_correlation("dc-west")
        # With 2 known DCs (auto-added), 2 failing = 100% >= 50% AND 2 >= high_count_threshold=2
        assert decision.severity == CorrelationSeverity.HIGH

    def test_clear_all_resets_state(self):
        """Test that clear_all resets all state."""
        detector = CrossDCCorrelationDetector()
        detector.add_datacenter("dc-west")
        detector.record_failure("dc-west", "unhealthy")

        detector.clear_all()

        stats = detector.get_stats()
        assert stats["datacenters_with_failures"] == 0
        assert stats["total_failures_recorded"] == 1  # Total count not reset
        assert not stats["in_backoff"]

    def test_different_failure_types(self):
        """Test recording different failure types."""
        detector = CrossDCCorrelationDetector()

        detector.record_failure("dc-west", "unhealthy", manager_count_affected=3)
        detector.record_failure("dc-east", "timeout", manager_count_affected=1)
        detector.record_failure("dc-central", "unreachable", manager_count_affected=5)

        stats = detector.get_stats()
        assert stats["total_failures_recorded"] == 3


# ============================================================================
# Statistics and Monitoring Tests
# ============================================================================


class TestStatisticsAndMonitoring:
    """Tests for statistics tracking and monitoring."""

    def test_stats_tracking_complete(self):
        """Test that stats track all relevant information."""
        config = CrossDCCorrelationConfig(
            correlation_window_seconds=30.0,
            low_threshold=2,
            medium_threshold=3,
        )
        detector = CrossDCCorrelationDetector(config=config)

        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")
        detector.record_failure("dc-west", "unhealthy")

        stats = detector.get_stats()

        # Verify all expected fields
        assert "known_datacenters" in stats
        assert "datacenters_with_failures" in stats
        assert "recent_failing_count" in stats
        assert "recent_failing_dcs" in stats
        assert "total_failures_recorded" in stats
        assert "correlation_events_detected" in stats
        assert "in_backoff" in stats
        assert "config" in stats

        # Verify config is included
        assert stats["config"]["correlation_window_seconds"] == 30.0
        assert stats["config"]["low_threshold"] == 2

    def test_correlation_events_counter(self):
        """Test that correlation events are counted."""
        config = CrossDCCorrelationConfig(
            medium_threshold=2,
        )
        detector = CrossDCCorrelationDetector(config=config)
        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")

        # Trigger correlation
        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")
        detector.check_correlation("dc-west")

        stats = detector.get_stats()
        assert stats["correlation_events_detected"] == 1

    def test_in_backoff_tracking(self):
        """Test that backoff state is tracked in stats."""
        config = CrossDCCorrelationConfig(
            correlation_backoff_seconds=1.0,
            medium_threshold=2,
        )
        detector = CrossDCCorrelationDetector(config=config)
        detector.add_datacenter("dc-west")
        detector.add_datacenter("dc-east")

        # Initially not in backoff
        stats1 = detector.get_stats()
        assert not stats1["in_backoff"]

        # Trigger correlation to enter backoff
        detector.record_failure("dc-west", "unhealthy")
        detector.record_failure("dc-east", "unhealthy")
        detector.check_correlation("dc-west")

        stats2 = detector.get_stats()
        assert stats2["in_backoff"]


# ============================================================================
# Concurrent Failure Scenarios
# ============================================================================


class TestConcurrentFailureScenarios:
    """Tests for realistic concurrent failure scenarios."""

    def test_network_partition_simulation(self):
        """Test simulating a network partition affecting multiple DCs."""
        config = CrossDCCorrelationConfig(
            high_threshold_fraction=0.5,
            high_count_threshold=3,  # Need 3 for HIGH
        )
        detector = CrossDCCorrelationDetector(config=config)

        # 4 datacenters
        for dc in ["dc-west", "dc-east", "dc-central", "dc-north"]:
            detector.add_datacenter(dc)

        # Network partition causes 3 DCs to fail almost simultaneously
        detector.record_failure("dc-west", "unreachable", manager_count_affected=3)
        detector.record_failure("dc-east", "unreachable", manager_count_affected=2)
        detector.record_failure("dc-central", "unreachable", manager_count_affected=4)

        # Check any of the failing DCs
        decision = detector.check_correlation("dc-west")

        # Should detect HIGH correlation (75% of DCs failing AND count >= 3)
        assert decision.severity == CorrelationSeverity.HIGH
        assert decision.should_delay_eviction
        assert "network" in decision.recommendation.lower()

    def test_genuine_dc_failure_no_correlation(self):
        """Test that genuine single DC failure is not flagged as correlated."""
        detector = CrossDCCorrelationDetector()

        for dc in ["dc-west", "dc-east", "dc-central", "dc-north"]:
            detector.add_datacenter(dc)

        # Only one DC fails (genuine failure)
        detector.record_failure("dc-west", "unhealthy", manager_count_affected=3)

        decision = detector.check_correlation("dc-west")

        # Should NOT detect correlation
        assert decision.severity == CorrelationSeverity.NONE
        assert not decision.should_delay_eviction
        assert "safe to proceed" in decision.recommendation.lower()

    def test_rolling_update_scenario(self):
        """Test rolling update where DCs go down sequentially (not correlated)."""
        config = CrossDCCorrelationConfig(
            correlation_window_seconds=0.2,  # Short window
            low_threshold=2,
        )
        detector = CrossDCCorrelationDetector(config=config)

        for dc in ["dc-west", "dc-east", "dc-central"]:
            detector.add_datacenter(dc)

        # DC1 fails and recovers
        detector.record_failure("dc-west", "unhealthy")
        decision1 = detector.check_correlation("dc-west")
        assert decision1.severity == CorrelationSeverity.NONE

        # Wait for window to expire
        time.sleep(0.25)

        detector.record_recovery("dc-west")

        # DC2 fails (outside correlation window)
        detector.record_failure("dc-east", "unhealthy")
        decision2 = detector.check_correlation("dc-east")

        # Should NOT be correlated (failures in different windows)
        assert decision2.severity == CorrelationSeverity.NONE

    def test_cascading_failure_detection(self):
        """Test detecting cascading failures across DCs."""
        config = CrossDCCorrelationConfig(
            correlation_window_seconds=30.0,
            low_threshold=2,
            medium_threshold=3,
        )
        detector = CrossDCCorrelationDetector(config=config)

        for dc in ["dc-primary", "dc-secondary", "dc-tertiary", "dc-backup"]:
            detector.add_datacenter(dc)

        # Primary fails
        detector.record_failure("dc-primary", "unhealthy")
        decision1 = detector.check_correlation("dc-primary")
        assert decision1.severity == CorrelationSeverity.NONE

        # Secondary fails (triggers LOW)
        detector.record_failure("dc-secondary", "degraded")
        decision2 = detector.check_correlation("dc-secondary")
        assert decision2.severity == CorrelationSeverity.LOW

        # Tertiary fails (triggers MEDIUM)
        detector.record_failure("dc-tertiary", "timeout")
        decision3 = detector.check_correlation("dc-tertiary")
        assert decision3.severity == CorrelationSeverity.MEDIUM
        assert decision3.should_delay_eviction

    def test_partial_recovery_scenario(self):
        """Test behavior when some DCs recover but others remain failed."""
        config = CrossDCCorrelationConfig(
            medium_threshold=3,
        )
        detector = CrossDCCorrelationDetector(config=config)

        for dc in ["dc-a", "dc-b", "dc-c", "dc-d"]:
            detector.add_datacenter(dc)

        # Three DCs fail
        detector.record_failure("dc-a", "unhealthy")
        detector.record_failure("dc-b", "unhealthy")
        detector.record_failure("dc-c", "unhealthy")

        decision1 = detector.check_correlation("dc-a")
        assert decision1.severity == CorrelationSeverity.MEDIUM

        # One DC recovers
        detector.record_recovery("dc-a")

        # Check remaining failures
        decision2 = detector.check_correlation("dc-b")
        # Still 2 failing DCs = LOW (not MEDIUM anymore)
        assert decision2.severity == CorrelationSeverity.LOW


# ============================================================================
# DCFailureRecord Tests
# ============================================================================


class TestDCFailureRecord:
    """Tests for DCFailureRecord dataclass."""

    def test_failure_record_creation(self):
        """Test creating a failure record."""
        record = DCFailureRecord(
            datacenter_id="dc-west",
            timestamp=time.monotonic(),
            failure_type="unhealthy",
            manager_count_affected=5,
        )

        assert record.datacenter_id == "dc-west"
        assert record.failure_type == "unhealthy"
        assert record.manager_count_affected == 5

    def test_failure_record_defaults(self):
        """Test failure record default values."""
        record = DCFailureRecord(
            datacenter_id="dc-east",
            timestamp=1000.0,
            failure_type="timeout",
        )

        assert record.manager_count_affected == 0


# ============================================================================
# Negative Path Tests
# ============================================================================


class TestNegativePaths:
    """Tests for negative paths and failure handling."""

    def test_remove_nonexistent_datacenter(self):
        """Test removing a datacenter that doesn't exist."""
        detector = CrossDCCorrelationDetector()

        # Should not raise
        detector.remove_datacenter("nonexistent")

        stats = detector.get_stats()
        assert stats["known_datacenters"] == 0

    def test_record_recovery_nonexistent_dc(self):
        """Test recording recovery for DC with no failures."""
        detector = CrossDCCorrelationDetector()

        # Should not raise
        detector.record_recovery("nonexistent")

    def test_get_recent_failure_count_unknown_dc(self):
        """Test getting failure count for unknown DC."""
        detector = CrossDCCorrelationDetector()

        count = detector.get_recent_failure_count("unknown")
        assert count == 0

    def test_correlation_with_single_known_dc(self):
        """Test correlation detection with only one known DC."""
        detector = CrossDCCorrelationDetector()

        detector.add_datacenter("dc-only")
        detector.record_failure("dc-only", "unhealthy")

        # With only 1 known DC, can't have multi-DC correlation
        decision = detector.check_correlation("dc-only")
        assert decision.severity == CorrelationSeverity.NONE
