#!/usr/bin/env python
"""
Comprehensive edge case tests for healthcheck extensions (AD-26).

Tests cover:
- Extension tracking logarithmic decay
- Progress requirement enforcement
- Maximum extension limits
- Worker eviction thresholds
- Concurrent extension requests
- State reset behavior
- Edge cases in deadline calculations
- Worker lifecycle interactions
"""

import time

from hyperscale.distributed.health.extension_tracker import (
    ExtensionTracker,
    ExtensionTrackerConfig,
)
from hyperscale.distributed.health.worker_health_manager import (
    WorkerHealthManager,
    WorkerHealthManagerConfig,
)
from hyperscale.distributed.models import (
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
)


# =============================================================================
# Test Logarithmic Decay
# =============================================================================


class TestLogarithmicDecay:
    """Tests for extension grant logarithmic decay."""

    def test_first_extension_is_half_base(self):
        """First extension grants base_deadline / 2."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        granted, extension_seconds, denial_reason, _ = tracker.request_extension(
            reason="long workflow",
            current_progress=1.0,
        )

        assert granted
        assert extension_seconds == 15.0  # 30 / 2
        assert denial_reason is None

    def test_second_extension_is_quarter_base(self):
        """Second extension grants base_deadline / 4."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        # First extension
        tracker.request_extension(reason="first", current_progress=1.0)

        # Second extension
        granted, extension_seconds, _, _ = tracker.request_extension(
            reason="second",
            current_progress=2.0,  # Must show progress
        )

        assert granted
        assert extension_seconds == 7.5  # 30 / 4

    def test_full_decay_sequence(self):
        """Test complete decay sequence until min_grant."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=32.0,  # Powers of 2 for clean math
            min_grant=1.0,
            max_extensions=10,
        )

        expected_grants = [
            16.0,  # 32 / 2^1
            8.0,  # 32 / 2^2
            4.0,  # 32 / 2^3
            2.0,  # 32 / 2^4
            1.0,  # 32 / 2^5 = 1.0 (at min_grant)
            1.0,  # Would be 0.5, but min_grant is 1.0
        ]

        for index, expected in enumerate(expected_grants):
            granted, extension_seconds, _, _ = tracker.request_extension(
                reason=f"extension {index + 1}",
                current_progress=float(index + 1),
            )
            assert granted, f"Extension {index + 1} should be granted"
            assert extension_seconds == expected, f"Extension {index + 1}: expected {expected}, got {extension_seconds}"

    def test_min_grant_floor(self):
        """Extensions never go below min_grant."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=4.0,
            min_grant=2.0,  # Higher min_grant
            max_extensions=10,
        )

        # First: 4/2 = 2.0
        _, grant_1, _, _ = tracker.request_extension(reason="1", current_progress=1.0)
        assert grant_1 == 2.0

        # Second: 4/4 = 1.0, but min_grant is 2.0
        _, grant_2, _, _ = tracker.request_extension(reason="2", current_progress=2.0)
        assert grant_2 == 2.0  # Floored to min_grant

        # Third: 4/8 = 0.5, but min_grant is 2.0
        _, grant_3, _, _ = tracker.request_extension(reason="3", current_progress=3.0)
        assert grant_3 == 2.0  # Floored to min_grant

    def test_very_small_base_deadline(self):
        """Very small base_deadline immediately hits min_grant."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=0.5,
            min_grant=1.0,
            max_extensions=5,
        )

        # 0.5 / 2 = 0.25, but min_grant is 1.0
        granted, extension_seconds, _, _ = tracker.request_extension(
            reason="small deadline",
            current_progress=1.0,
        )

        assert granted
        assert extension_seconds == 1.0  # min_grant

    def test_large_base_deadline(self):
        """Large base_deadline decays correctly."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=3600.0,  # 1 hour
            min_grant=60.0,  # 1 minute minimum
            max_extensions=10,
        )

        expected = 1800.0  # 3600 / 2
        granted, extension_seconds, _, _ = tracker.request_extension(
            reason="very long workflow",
            current_progress=1.0,
        )

        assert granted
        assert extension_seconds == expected


# =============================================================================
# Test Progress Requirements
# =============================================================================


class TestProgressRequirements:
    """Tests for progress requirement enforcement."""

    def test_first_extension_no_progress_required(self):
        """First extension doesn't require prior progress."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        # First extension with progress=0 should work
        granted, _, _, _ = tracker.request_extension(
            reason="starting work",
            current_progress=0.0,
        )

        assert granted

    def test_second_extension_requires_progress(self):
        """Second extension requires progress since first."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        # First extension
        tracker.request_extension(reason="first", current_progress=5.0)

        # Second extension with same progress - should be denied
        granted, extension_seconds, denial_reason, _ = tracker.request_extension(
            reason="second",
            current_progress=5.0,  # No progress
        )

        assert not granted
        assert extension_seconds == 0.0
        assert "No progress" in denial_reason

    def test_progress_must_strictly_increase(self):
        """Progress must strictly increase (not equal)."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        tracker.request_extension(reason="first", current_progress=10.0)

        # Equal progress - denied
        granted, _, denial_reason, _ = tracker.request_extension(
            reason="no change",
            current_progress=10.0,
        )
        assert not granted
        assert "No progress" in denial_reason

    def test_regression_in_progress_denied(self):
        """Decreased progress is denied."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        tracker.request_extension(reason="first", current_progress=10.0)

        # Decreased progress - denied
        granted, _, denial_reason, _ = tracker.request_extension(
            reason="went backwards",
            current_progress=5.0,  # Less than 10.0
        )

        assert not granted
        assert "No progress" in denial_reason
        assert "current=5.0" in denial_reason
        assert "last=10.0" in denial_reason

    def test_tiny_progress_increment_accepted(self):
        """Even tiny progress increments are accepted."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        tracker.request_extension(reason="first", current_progress=100.0)

        # Tiny increment
        granted, _, _, _ = tracker.request_extension(
            reason="tiny progress",
            current_progress=100.0001,
        )

        assert granted

    def test_negative_progress_first_extension(self):
        """Negative progress values work for first extension."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        granted, _, _, _ = tracker.request_extension(
            reason="negative start",
            current_progress=-100.0,
        )

        assert granted

    def test_negative_to_less_negative_is_progress(self):
        """Progress from -100 to -50 is forward progress."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        tracker.request_extension(reason="first", current_progress=-100.0)

        # -50 > -100, so this is progress
        granted, _, _, _ = tracker.request_extension(
            reason="less negative",
            current_progress=-50.0,
        )

        assert granted


# =============================================================================
# Test Maximum Extension Limits
# =============================================================================


class TestMaximumExtensionLimits:
    """Tests for maximum extension limits."""

    def test_max_extensions_enforced(self):
        """Cannot exceed max_extensions count."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=3,
        )

        # Use all 3 extensions
        for index in range(3):
            granted, _, _, _ = tracker.request_extension(
                reason=f"extension {index + 1}",
                current_progress=float(index + 1),
            )
            assert granted, f"Extension {index + 1} should be granted"

        # 4th request should be denied
        granted, extension_seconds, denial_reason, _ = tracker.request_extension(
            reason="one too many",
            current_progress=4.0,
        )

        assert not granted
        assert extension_seconds == 0.0
        assert "Maximum extensions (3) exceeded" in denial_reason

    def test_max_extensions_zero(self):
        """max_extensions=0 means no extensions allowed."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=0,
        )

        granted, extension_seconds, denial_reason, _ = tracker.request_extension(
            reason="please extend",
            current_progress=1.0,
        )

        assert not granted
        assert extension_seconds == 0.0
        assert "Maximum extensions (0) exceeded" in denial_reason

    def test_max_extensions_one(self):
        """max_extensions=1 allows exactly one extension."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=1,
        )

        # First extension works
        granted, _, _, _ = tracker.request_extension(
            reason="only chance",
            current_progress=1.0,
        )
        assert granted

        # Second is denied
        granted, _, denial_reason, _ = tracker.request_extension(
            reason="no more",
            current_progress=2.0,
        )
        assert not granted
        assert "Maximum extensions (1) exceeded" in denial_reason

    def test_is_exhausted_property(self):
        """is_exhausted property tracks extension exhaustion."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=2,
        )

        assert not tracker.is_exhausted

        tracker.request_extension(reason="1", current_progress=1.0)
        assert not tracker.is_exhausted

        tracker.request_extension(reason="2", current_progress=2.0)
        assert tracker.is_exhausted

    def test_get_remaining_extensions(self):
        """get_remaining_extensions() tracks count correctly."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=3,
        )

        assert tracker.get_remaining_extensions() == 3

        tracker.request_extension(reason="1", current_progress=1.0)
        assert tracker.get_remaining_extensions() == 2

        tracker.request_extension(reason="2", current_progress=2.0)
        assert tracker.get_remaining_extensions() == 1

        tracker.request_extension(reason="3", current_progress=3.0)
        assert tracker.get_remaining_extensions() == 0

        # After exhaustion, stays at 0
        tracker.request_extension(reason="4", current_progress=4.0)  # Will be denied
        assert tracker.get_remaining_extensions() == 0


# =============================================================================
# Test State Reset
# =============================================================================


class TestStateReset:
    """Tests for reset behavior."""

    def test_reset_clears_extension_count(self):
        """reset() clears extension count."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=3,
        )

        # Use some extensions
        tracker.request_extension(reason="1", current_progress=1.0)
        tracker.request_extension(reason="2", current_progress=2.0)
        assert tracker.extension_count == 2

        # Reset
        tracker.reset()

        assert tracker.extension_count == 0
        assert tracker.get_remaining_extensions() == 3

    def test_reset_clears_progress_tracking(self):
        """reset() clears last_progress."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        tracker.request_extension(reason="1", current_progress=100.0)
        assert tracker.last_progress == 100.0

        tracker.reset()

        assert tracker.last_progress == 0.0

    def test_reset_allows_new_extension_cycle(self):
        """After reset(), new extensions are granted fresh."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=2,
        )

        # Exhaust extensions
        tracker.request_extension(reason="1", current_progress=1.0)
        tracker.request_extension(reason="2", current_progress=2.0)
        assert tracker.is_exhausted

        # Reset
        tracker.reset()

        # New extension should work with full grant
        granted, extension_seconds, _, _ = tracker.request_extension(
            reason="after reset",
            current_progress=1.0,
        )

        assert granted
        assert extension_seconds == 15.0  # First extension = base / 2
        assert not tracker.is_exhausted

    def test_reset_clears_total_extended(self):
        """reset() clears total_extended accumulator."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        tracker.request_extension(reason="1", current_progress=1.0)
        tracker.request_extension(reason="2", current_progress=2.0)
        assert tracker.total_extended > 0

        tracker.reset()

        assert tracker.total_extended == 0.0


# =============================================================================
# Test Deadline Calculations
# =============================================================================


class TestDeadlineCalculations:
    """Tests for deadline calculation edge cases."""

    def test_get_new_deadline_simple(self):
        """get_new_deadline() adds grant to current deadline."""
        tracker = ExtensionTracker(worker_id="worker-1")

        current_deadline = 1000.0
        grant = 15.0

        new_deadline = tracker.get_new_deadline(current_deadline, grant)
        assert new_deadline == 1015.0

    def test_get_new_deadline_with_real_timestamps(self):
        """get_new_deadline() works with real timestamps."""
        tracker = ExtensionTracker(worker_id="worker-1")

        current_deadline = time.time() + 30.0  # 30 seconds from now
        grant = 15.0

        new_deadline = tracker.get_new_deadline(current_deadline, grant)
        assert new_deadline == current_deadline + grant

    def test_total_extended_accumulates(self):
        """total_extended tracks sum of all grants."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=32.0,
            min_grant=1.0,
            max_extensions=5,
        )

        # Grant sequence: 16 + 8 + 4 + 2 + 1 = 31
        expected_total = 0.0

        for index in range(5):
            granted, extension_seconds, _, _ = tracker.request_extension(
                reason=f"{index + 1}",
                current_progress=float(index + 1),
            )
            assert granted
            expected_total += extension_seconds
            assert tracker.total_extended == expected_total


# =============================================================================
# Test Worker Health Manager
# =============================================================================


class TestWorkerHealthManager:
    """Tests for WorkerHealthManager edge cases."""

    def test_handle_extension_request_success(self):
        """Manager grants valid extension requests."""
        manager = WorkerHealthManager()

        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="long workflow",
            current_progress=1.0,
            estimated_completion=10.0,
            active_workflow_count=5,
        )

        response = manager.handle_extension_request(request, current_deadline=1000.0)

        assert response.granted
        assert response.extension_seconds > 0
        assert response.new_deadline > 1000.0
        assert response.denial_reason is None

    def test_handle_extension_request_no_progress(self):
        """Manager denies extension without progress."""
        manager = WorkerHealthManager()

        # First request succeeds
        first_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="first",
            current_progress=10.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(first_request, current_deadline=1000.0)

        # Second request without progress fails
        second_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="second",
            current_progress=10.0,  # Same progress
            estimated_completion=3.0,
            active_workflow_count=1,
        )
        response = manager.handle_extension_request(second_request, current_deadline=1015.0)

        assert not response.granted
        assert response.extension_seconds == 0.0
        assert response.new_deadline == 1015.0  # Unchanged
        assert "No progress" in response.denial_reason

    def test_on_worker_healthy_resets_tracker(self):
        """on_worker_healthy() resets the worker's tracker."""
        manager = WorkerHealthManager()

        # Use some extensions
        for index in range(3):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason=f"extension {index + 1}",
                current_progress=float(index + 1),
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, current_deadline=1000.0)

        state_before = manager.get_worker_extension_state("worker-1")
        assert state_before["extension_count"] == 3

        # Worker becomes healthy
        manager.on_worker_healthy("worker-1")

        state_after = manager.get_worker_extension_state("worker-1")
        assert state_after["extension_count"] == 0

    def test_on_worker_removed_cleans_up(self):
        """on_worker_removed() cleans up all tracking state."""
        manager = WorkerHealthManager()

        # Create tracking state
        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="tracking",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request, current_deadline=1000.0)

        assert manager.tracked_worker_count == 1

        # Remove worker
        manager.on_worker_removed("worker-1")

        assert manager.tracked_worker_count == 0
        state = manager.get_worker_extension_state("worker-1")
        assert not state["has_tracker"]


class TestEvictionThresholds:
    """Tests for worker eviction decisions."""

    def test_should_evict_after_max_extensions(self):
        """Worker should be evicted after exhausting extensions and grace period."""
        # Set grace_period=0 so eviction happens immediately after exhaustion
        config = WorkerHealthManagerConfig(max_extensions=2, grace_period=0.0)
        manager = WorkerHealthManager(config)

        # Exhaust all extensions
        for index in range(2):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason=f"extension {index + 1}",
                current_progress=float(index + 1),
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, current_deadline=1000.0)

        # Make one more request to trigger exhaustion_time to be set
        final_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="exhausted",
            current_progress=3.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(final_request, current_deadline=1000.0)

        should_evict, reason = manager.should_evict_worker("worker-1")

        assert should_evict
        assert "exhausted all 2 extensions" in reason

    def test_should_evict_after_extension_failures(self):
        """Worker should be evicted after consecutive extension failures."""
        config = WorkerHealthManagerConfig(eviction_threshold=2)
        manager = WorkerHealthManager(config)

        # First extension succeeds
        first_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="first",
            current_progress=10.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(first_request, current_deadline=1000.0)

        # Next 2 fail (no progress)
        for index in range(2):
            bad_request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason=f"stuck {index + 1}",
                current_progress=10.0,  # No progress
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(bad_request, current_deadline=1000.0)

        should_evict, reason = manager.should_evict_worker("worker-1")

        assert should_evict
        assert "exhausted 2 extension requests without progress" in reason

    def test_no_eviction_for_healthy_worker(self):
        """Healthy worker should not be evicted."""
        manager = WorkerHealthManager()

        should_evict, reason = manager.should_evict_worker("unknown-worker")

        assert not should_evict
        assert reason is None

    def test_success_clears_failure_count(self):
        """Successful extension clears failure count."""
        config = WorkerHealthManagerConfig(eviction_threshold=3)
        manager = WorkerHealthManager(config)

        # First extension
        first_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="first",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(first_request, current_deadline=1000.0)

        # One failure (no progress)
        bad_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="stuck",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(bad_request, current_deadline=1000.0)

        # Successful extension (with progress)
        good_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="progress",
            current_progress=2.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(good_request, current_deadline=1000.0)

        state = manager.get_worker_extension_state("worker-1")
        assert state["extension_failures"] == 0


# =============================================================================
# Test Multiple Workers
# =============================================================================


class TestMultipleWorkers:
    """Tests for managing multiple workers."""

    def test_independent_worker_tracking(self):
        """Each worker has independent extension tracking."""
        manager = WorkerHealthManager()

        # Worker 1 uses extensions
        for index in range(3):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason=f"w1-{index + 1}",
                current_progress=float(index + 1),
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, current_deadline=1000.0)

        # Worker 2 starts fresh
        request_w2 = HealthcheckExtensionRequest(
            worker_id="worker-2",
            reason="w2-first",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response_w2 = manager.handle_extension_request(request_w2, current_deadline=1000.0)

        # Worker 2 should get full first extension
        assert response_w2.granted
        assert response_w2.extension_seconds == 15.0  # First extension

        # Worker 1 state unchanged
        state_w1 = manager.get_worker_extension_state("worker-1")
        assert state_w1["extension_count"] == 3

    def test_get_all_extension_states(self):
        """get_all_extension_states() returns all tracked workers."""
        manager = WorkerHealthManager()

        worker_ids = ["worker-1", "worker-2", "worker-3"]

        for worker_id in worker_ids:
            request = HealthcheckExtensionRequest(
                worker_id=worker_id,
                reason="tracking",
                current_progress=1.0,
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, current_deadline=1000.0)

        all_states = manager.get_all_extension_states()

        assert len(all_states) == 3
        assert set(all_states.keys()) == set(worker_ids)

    def test_removing_one_worker_preserves_others(self):
        """Removing one worker doesn't affect others."""
        manager = WorkerHealthManager()

        for worker_id in ["worker-1", "worker-2"]:
            request = HealthcheckExtensionRequest(
                worker_id=worker_id,
                reason="tracking",
                current_progress=1.0,
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, current_deadline=1000.0)

        manager.on_worker_removed("worker-1")

        assert manager.tracked_worker_count == 1
        state_w2 = manager.get_worker_extension_state("worker-2")
        assert state_w2["has_tracker"]


# =============================================================================
# Test Configuration
# =============================================================================


class TestExtensionTrackerConfig:
    """Tests for ExtensionTrackerConfig."""

    def test_create_tracker_with_config(self):
        """Config creates trackers with correct settings."""
        config = ExtensionTrackerConfig(
            base_deadline=60.0,
            min_grant=5.0,
            max_extensions=10,
        )

        tracker = config.create_tracker("worker-1")

        assert tracker.worker_id == "worker-1"
        assert tracker.base_deadline == 60.0
        assert tracker.min_grant == 5.0
        assert tracker.max_extensions == 10

    def test_manager_uses_config(self):
        """Manager uses provided config for extension tracking."""
        config = WorkerHealthManagerConfig(
            base_deadline=120.0,
            min_grant=10.0,
            max_extensions=3,
        )
        manager = WorkerHealthManager(config)

        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="test",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response = manager.handle_extension_request(request, current_deadline=1000.0)

        # First extension = base / 2 = 120 / 2 = 60
        assert response.extension_seconds == 60.0
        assert response.remaining_extensions == 2  # Started with 3, used 1


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for additional edge cases."""

    def test_extension_request_on_unknown_worker_creates_tracker(self):
        """First request for unknown worker creates tracker."""
        manager = WorkerHealthManager()

        assert manager.tracked_worker_count == 0

        request = HealthcheckExtensionRequest(
            worker_id="new-worker",
            reason="first contact",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response = manager.handle_extension_request(request, current_deadline=1000.0)

        assert response.granted
        assert manager.tracked_worker_count == 1

    def test_on_worker_healthy_for_unknown_worker_is_safe(self):
        """on_worker_healthy() on unknown worker is a no-op."""
        manager = WorkerHealthManager()

        # Should not raise
        manager.on_worker_healthy("unknown-worker")

        assert manager.tracked_worker_count == 0

    def test_on_worker_removed_for_unknown_worker_is_safe(self):
        """on_worker_removed() on unknown worker is a no-op."""
        manager = WorkerHealthManager()

        # Should not raise
        manager.on_worker_removed("unknown-worker")

    def test_zero_progress_workflow(self):
        """Worker with zero progress can still get first extension."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        granted, _, _, _ = tracker.request_extension(
            reason="initializing",
            current_progress=0.0,
        )

        assert granted

    def test_response_contains_remaining_extensions(self):
        """Response always contains remaining extension count."""
        manager = WorkerHealthManager()

        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="test",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response = manager.handle_extension_request(request, current_deadline=1000.0)

        assert response.remaining_extensions == 4  # Default is 5, used 1

    def test_denied_response_shows_remaining_extensions(self):
        """Denied responses also show remaining extensions."""
        config = WorkerHealthManagerConfig(max_extensions=1)
        manager = WorkerHealthManager(config)

        # Use the one extension
        first_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="only one",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(first_request, current_deadline=1000.0)

        # Second request denied
        second_request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="denied",
            current_progress=2.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response = manager.handle_extension_request(second_request, current_deadline=1000.0)

        assert not response.granted
        assert response.remaining_extensions == 0


# =============================================================================
# Test Timing Behavior
# =============================================================================


class TestTimingBehavior:
    """Tests for timing-related behavior."""

    def test_last_extension_time_updated(self):
        """last_extension_time is updated on each extension."""
        tracker = ExtensionTracker(worker_id="worker-1")

        time_before = tracker.last_extension_time

        # Small delay to ensure time difference
        time.sleep(0.01)

        tracker.request_extension(reason="test", current_progress=1.0)

        assert tracker.last_extension_time > time_before

    def test_reset_updates_last_extension_time(self):
        """reset() updates last_extension_time."""
        tracker = ExtensionTracker(worker_id="worker-1")

        tracker.request_extension(reason="test", current_progress=1.0)
        time_after_extension = tracker.last_extension_time

        time.sleep(0.01)

        tracker.reset()

        assert tracker.last_extension_time > time_after_extension
