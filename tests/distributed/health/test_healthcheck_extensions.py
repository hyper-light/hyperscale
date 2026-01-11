"""
Integration tests for Adaptive Healthcheck Extensions (AD-26).

These tests verify that:
1. ExtensionTracker correctly implements logarithmic decay
2. Progress requirement prevents stuck workers from getting extensions
3. HealthcheckExtensionRequest/Response message serialization works
4. WorkerHealthManager properly handles extension requests
5. Extension failures lead to eviction recommendations

The Adaptive Healthcheck Extension pattern ensures:
- Workers can request deadline extensions when busy with legitimate work
- Extensions use logarithmic decay to prevent indefinite extension
- Progress must be demonstrated for extensions to be granted
- Stuck workers are eventually evicted
"""

import time
import pytest

from hyperscale.distributed.health import (
    ExtensionTracker,
    ExtensionTrackerConfig,
    WorkerHealthManager,
    WorkerHealthManagerConfig,
)
from hyperscale.distributed.models import (
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
)


class TestExtensionTracker:
    """Test ExtensionTracker logarithmic decay and progress requirements."""

    def test_tracker_initialization(self):
        """ExtensionTracker should initialize with correct defaults."""
        tracker = ExtensionTracker(worker_id="worker-1")
        assert tracker.worker_id == "worker-1"
        assert tracker.base_deadline == 30.0
        assert tracker.min_grant == 1.0
        assert tracker.max_extensions == 5
        assert tracker.extension_count == 0
        assert tracker.total_extended == 0.0
        assert not tracker.is_exhausted

    def test_first_extension_grants_half_base(self):
        """First extension should grant base/2 seconds."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
        )

        granted, seconds, reason, _ = tracker.request_extension(
            reason="busy with workflow",
            current_progress=1.0,
        )

        assert granted is True
        assert seconds == 15.0  # 30 / 2^1 = 15
        assert reason is None
        assert tracker.extension_count == 1

    def test_logarithmic_decay(self):
        """Extensions should follow logarithmic decay: base / 2^n."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=32.0,  # Powers of 2 for easy math
            min_grant=1.0,
        )

        # First extension: 32/2 = 16
        granted, seconds, _, _ = tracker.request_extension("busy", 1.0)
        assert granted is True
        assert seconds == 16.0

        # Second extension: 32/4 = 8
        granted, seconds, _, _ = tracker.request_extension("busy", 2.0)
        assert granted is True
        assert seconds == 8.0

        # Third extension: 32/8 = 4
        granted, seconds, _, _ = tracker.request_extension("busy", 3.0)
        assert granted is True
        assert seconds == 4.0

        # Fourth extension: 32/16 = 2
        granted, seconds, _, _ = tracker.request_extension("busy", 4.0)
        assert granted is True
        assert seconds == 2.0

        # Fifth extension: 32/32 = 1 (min_grant)
        granted, seconds, _, _ = tracker.request_extension("busy", 5.0)
        assert granted is True
        assert seconds == 1.0

    def test_min_grant_floor(self):
        """Extensions should never go below min_grant."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=4.0,
            min_grant=2.0,
            max_extensions=5,
        )

        # Request multiple extensions
        for i in range(5):
            granted, seconds, _, _ = tracker.request_extension(
                reason="busy",
                current_progress=float(i + 1),
            )
            assert granted is True
            assert seconds >= 2.0  # Never below min_grant

    def test_progress_required_for_subsequent_extensions(self):
        """Subsequent extensions require progress since last extension."""
        tracker = ExtensionTracker(worker_id="worker-1")

        # First extension succeeds (no prior progress to compare)
        granted, _, _, _ = tracker.request_extension("busy", 1.0)
        assert granted is True

        # Same progress - should be denied
        granted, _, reason, _ = tracker.request_extension("busy", 1.0)
        assert granted is False
        assert "No progress" in reason

        # Lower progress - should be denied
        granted, _, reason, _ = tracker.request_extension("busy", 0.5)
        assert granted is False
        assert "No progress" in reason

        # Higher progress - should be granted
        granted, _, _, _ = tracker.request_extension("busy", 2.0)
        assert granted is True

    def test_max_extensions_enforced(self):
        """Extensions should be denied after max_extensions reached."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=3,
        )

        # Use up all extensions
        for i in range(3):
            granted, _, _, _ = tracker.request_extension("busy", float(i + 1))
            assert granted is True

        assert tracker.is_exhausted is True

        # Next request should be denied
        granted, _, reason, _ = tracker.request_extension("busy", 4.0)
        assert granted is False
        assert "exceeded" in reason.lower()

    def test_reset_clears_state(self):
        """Reset should clear all extension tracking state."""
        tracker = ExtensionTracker(worker_id="worker-1")

        # Use some extensions
        tracker.request_extension("busy", 1.0)
        tracker.request_extension("busy", 2.0)

        assert tracker.extension_count == 2
        assert tracker.total_extended > 0

        # Reset
        tracker.reset()

        assert tracker.extension_count == 0
        assert tracker.total_extended == 0.0
        assert tracker.last_progress == 0.0
        assert tracker.get_remaining_extensions() == 5

    def test_total_extended_tracking(self):
        """total_extended should accumulate all granted extensions."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=16.0,
        )

        # First: 8s, Second: 4s, Third: 2s = 14s total
        tracker.request_extension("busy", 1.0)
        tracker.request_extension("busy", 2.0)
        tracker.request_extension("busy", 3.0)

        assert tracker.total_extended == 14.0  # 8 + 4 + 2


class TestExtensionTrackerConfig:
    """Test ExtensionTrackerConfig factory."""

    def test_config_creates_tracker(self):
        """Config should create tracker with correct settings."""
        config = ExtensionTrackerConfig(
            base_deadline=60.0,
            min_grant=2.0,
            max_extensions=10,
        )

        tracker = config.create_tracker("worker-test")

        assert tracker.worker_id == "worker-test"
        assert tracker.base_deadline == 60.0
        assert tracker.min_grant == 2.0
        assert tracker.max_extensions == 10


class TestHealthcheckExtensionMessages:
    """Test message serialization for extension protocol."""

    def test_request_serialization(self):
        """HealthcheckExtensionRequest should serialize correctly."""
        original = HealthcheckExtensionRequest(
            worker_id="worker-abc",
            reason="executing long workflow",
            current_progress=42.5,
            estimated_completion=10.0,
            active_workflow_count=3,
        )

        serialized = original.dump()
        restored = HealthcheckExtensionRequest.load(serialized)

        assert restored.worker_id == "worker-abc"
        assert restored.reason == "executing long workflow"
        assert restored.current_progress == 42.5
        assert restored.estimated_completion == 10.0
        assert restored.active_workflow_count == 3

    def test_response_granted_serialization(self):
        """HealthcheckExtensionResponse (granted) should serialize correctly."""
        original = HealthcheckExtensionResponse(
            granted=True,
            extension_seconds=15.0,
            new_deadline=time.monotonic() + 15.0,
            remaining_extensions=4,
            denial_reason=None,
        )

        serialized = original.dump()
        restored = HealthcheckExtensionResponse.load(serialized)

        assert restored.granted is True
        assert restored.extension_seconds == 15.0
        assert restored.remaining_extensions == 4
        assert restored.denial_reason is None

    def test_response_denied_serialization(self):
        """HealthcheckExtensionResponse (denied) should serialize correctly."""
        original = HealthcheckExtensionResponse(
            granted=False,
            extension_seconds=0.0,
            new_deadline=0.0,
            remaining_extensions=0,
            denial_reason="Maximum extensions exceeded",
        )

        serialized = original.dump()
        restored = HealthcheckExtensionResponse.load(serialized)

        assert restored.granted is False
        assert restored.extension_seconds == 0.0
        assert restored.denial_reason == "Maximum extensions exceeded"


class TestWorkerHealthManager:
    """Test WorkerHealthManager extension handling."""

    def test_manager_handles_extension_request(self):
        """Manager should properly handle extension requests."""
        manager = WorkerHealthManager()

        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy with workflow",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=2,
        )

        current_deadline = time.monotonic() + 10.0
        response = manager.handle_extension_request(request, current_deadline)

        assert response.granted is True
        assert response.extension_seconds > 0
        assert response.new_deadline > current_deadline
        assert response.remaining_extensions >= 0

    def test_manager_tracks_per_worker(self):
        """Manager should maintain separate trackers per worker."""
        manager = WorkerHealthManager()

        # Worker 1 requests
        request1 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )

        # Worker 2 requests
        request2 = HealthcheckExtensionRequest(
            worker_id="worker-2",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )

        deadline = time.monotonic() + 30.0

        # Both should get full first extension (15s with default base=30)
        response1 = manager.handle_extension_request(request1, deadline)
        response2 = manager.handle_extension_request(request2, deadline)

        assert response1.granted is True
        assert response2.granted is True
        assert response1.extension_seconds == 15.0
        assert response2.extension_seconds == 15.0

    def test_manager_resets_on_healthy(self):
        """Manager should reset tracker when worker becomes healthy."""
        manager = WorkerHealthManager()

        # Use up extensions
        for i in range(3):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="busy",
                current_progress=float(i + 1),
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, time.monotonic() + 30.0)

        state_before = manager.get_worker_extension_state("worker-1")
        assert state_before["extension_count"] == 3

        # Worker becomes healthy
        manager.on_worker_healthy("worker-1")

        state_after = manager.get_worker_extension_state("worker-1")
        assert state_after["extension_count"] == 0

    def test_manager_cleanup_on_remove(self):
        """Manager should clean up state when worker is removed."""
        manager = WorkerHealthManager()

        # Create some state
        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request, time.monotonic() + 30.0)

        assert manager.tracked_worker_count == 1

        # Remove worker
        manager.on_worker_removed("worker-1")

        assert manager.tracked_worker_count == 0

    def test_manager_eviction_recommendation(self):
        """Manager should recommend eviction after threshold failures."""
        config = WorkerHealthManagerConfig(
            max_extensions=2,
            eviction_threshold=2,
        )
        manager = WorkerHealthManager(config)

        # Exhaust extensions (2 max)
        for i in range(2):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="busy",
                current_progress=float(i + 1),
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, time.monotonic() + 30.0)

        # Next requests will fail (no progress, or max exceeded)
        # These failures should accumulate
        for _ in range(2):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="busy",
                current_progress=2.0,  # Same progress - will fail
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, time.monotonic() + 30.0)

        # Should recommend eviction
        should_evict, reason = manager.should_evict_worker("worker-1")
        assert should_evict is True
        assert reason is not None


class TestExtensionScenarios:
    """Test realistic extension scenarios."""

    def test_long_running_workflow_scenario(self):
        """
        Scenario: Worker executing a long-running workflow.

        1. Worker starts workflow, gets 5 extensions as it progresses
        2. Each extension is smaller than the previous
        3. Worker eventually completes or exhausts extensions
        """
        tracker = ExtensionTracker(
            worker_id="worker-1",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        # Simulate 5 extension requests with increasing progress
        extensions_granted = []
        for i in range(5):
            granted, seconds, _, _ = tracker.request_extension(
                reason=f"step {i + 1} of 5",
                current_progress=float(i + 1) * 20,  # 20, 40, 60, 80, 100
            )
            assert granted is True
            extensions_granted.append(seconds)

        # Verify logarithmic decay
        for i in range(1, len(extensions_granted)):
            assert extensions_granted[i] <= extensions_granted[i - 1]

        # Total extended time
        total = sum(extensions_granted)
        assert total == tracker.total_extended

    def test_stuck_worker_scenario(self):
        """
        Scenario: Worker is stuck and not making progress.

        1. Worker gets first extension
        2. Subsequent requests fail due to no progress
        3. Eventually manager recommends eviction
        """
        config = WorkerHealthManagerConfig(
            max_extensions=5,
            eviction_threshold=3,
        )
        manager = WorkerHealthManager(config)

        deadline = time.monotonic() + 30.0

        # First request succeeds
        request = HealthcheckExtensionRequest(
            worker_id="stuck-worker",
            reason="processing",
            current_progress=10.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response = manager.handle_extension_request(request, deadline)
        assert response.granted is True

        # Subsequent requests fail (same progress)
        for _ in range(3):
            request = HealthcheckExtensionRequest(
                worker_id="stuck-worker",
                reason="still processing",
                current_progress=10.0,  # No progress!
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            response = manager.handle_extension_request(request, deadline)
            assert response.granted is False

        # Should recommend eviction
        should_evict, _ = manager.should_evict_worker("stuck-worker")
        assert should_evict is True

    def test_recovery_after_healthy(self):
        """
        Scenario: Worker becomes healthy, then needs extensions again.

        1. Worker uses 3 extensions
        2. Worker becomes healthy (reset)
        3. Worker can get 5 more extensions
        """
        manager = WorkerHealthManager()
        deadline = time.monotonic() + 30.0

        # Use 3 extensions
        for i in range(3):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="busy",
                current_progress=float(i + 1),
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            manager.handle_extension_request(request, deadline)

        state = manager.get_worker_extension_state("worker-1")
        assert state["extension_count"] == 3
        assert state["remaining_extensions"] == 2

        # Worker becomes healthy
        manager.on_worker_healthy("worker-1")

        # Worker can get 5 more extensions
        for i in range(5):
            request = HealthcheckExtensionRequest(
                worker_id="worker-1",
                reason="new workflow",
                current_progress=float(i + 1),
                estimated_completion=5.0,
                active_workflow_count=1,
            )
            response = manager.handle_extension_request(request, deadline)
            assert response.granted is True

        state = manager.get_worker_extension_state("worker-1")
        assert state["extension_count"] == 5


class TestGracefulExhaustion:
    """Test the graceful exhaustion feature for deadline extensions.

    The graceful exhaustion feature ensures workers have time to checkpoint
    and save state before being forcefully evicted. Key behaviors:

    1. Warning threshold: When remaining extensions hit warning_threshold,
       is_warning=True is returned so the worker can prepare for exhaustion.

    2. Grace period: After exhaustion, the worker has grace_period seconds
       to complete any final operations before being marked for eviction.

    3. Eviction: Only after both exhaustion AND grace_period expiry does
       should_evict return True.
    """

    def test_is_warning_triggers_at_warning_threshold(self):
        """is_warning should be True when remaining extensions hit warning_threshold."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=3,
            warning_threshold=1,  # Warn when 1 extension remains
        )

        # First extension: 2 remaining - no warning
        granted, _, _, is_warning = tracker.request_extension("busy", 1.0)
        assert granted is True
        assert is_warning is False
        assert tracker.get_remaining_extensions() == 2

        # Second extension: 1 remaining - WARNING
        granted, _, _, is_warning = tracker.request_extension("busy", 2.0)
        assert granted is True
        assert is_warning is True
        assert tracker.get_remaining_extensions() == 1

        # Third extension: 0 remaining - no warning (already sent)
        granted, _, _, is_warning = tracker.request_extension("busy", 3.0)
        assert granted is True
        assert is_warning is False  # Warning already sent
        assert tracker.get_remaining_extensions() == 0

    def test_is_warning_only_sent_once(self):
        """is_warning should only be True once per cycle."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=5,
            warning_threshold=2,  # Warn when 2 extensions remain
        )

        warnings_received = []
        for i in range(5):
            granted, _, _, is_warning = tracker.request_extension("busy", float(i + 1))
            assert granted is True
            warnings_received.append(is_warning)

        # Only one warning should have been sent
        assert warnings_received.count(True) == 1
        # Warning should be at the 3rd request (when remaining == 2)
        assert warnings_received[2] is True

    def test_warning_sent_flag_reset_on_reset(self):
        """warning_sent should be cleared when tracker is reset."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=2,
            warning_threshold=1,
        )

        # First extension triggers warning (remaining=1 after grant, hits threshold)
        # Warning triggers when remaining <= warning_threshold
        _, _, _, is_warning = tracker.request_extension("busy", 1.0)
        assert is_warning is True
        assert tracker.warning_sent is True

        # Second extension - warning already sent
        _, _, _, is_warning = tracker.request_extension("busy", 2.0)
        assert is_warning is False

        # Reset tracker
        tracker.reset()
        assert tracker.warning_sent is False

        # New cycle - warning should be sent again at threshold
        _, _, _, is_warning = tracker.request_extension("busy", 1.0)
        assert is_warning is True

    def test_exhaustion_time_set_on_first_denial_after_max(self):
        """exhaustion_time should be set when first request is denied after max."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=2,
            grace_period=10.0,
        )

        # Use up all extensions
        tracker.request_extension("busy", 1.0)
        tracker.request_extension("busy", 2.0)
        assert tracker.is_exhausted is True
        assert tracker.exhaustion_time is None  # Not set yet

        # First denial sets exhaustion_time
        granted, _, _, _ = tracker.request_extension("busy", 3.0)
        assert granted is False
        assert tracker.exhaustion_time is not None

        # Remember the exhaustion time
        exhaustion_time = tracker.exhaustion_time

        # Subsequent denials don't change exhaustion_time
        tracker.request_extension("busy", 4.0)
        assert tracker.exhaustion_time == exhaustion_time

    def test_is_in_grace_period_after_exhaustion(self):
        """is_in_grace_period should be True after exhaustion until grace_period expires."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=1,
            grace_period=1.0,  # 1 second grace period for fast test
        )

        # Use up extension
        tracker.request_extension("busy", 1.0)
        assert tracker.is_exhausted is True
        assert tracker.is_in_grace_period is False  # Not yet

        # Trigger exhaustion_time by requesting when exhausted
        tracker.request_extension("busy", 2.0)
        assert tracker.is_in_grace_period is True
        assert tracker.grace_period_remaining > 0

    def test_grace_period_remaining_decreases(self):
        """grace_period_remaining should decrease over time."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=1,
            grace_period=5.0,
        )

        # Exhaust and trigger grace period
        tracker.request_extension("busy", 1.0)
        tracker.request_extension("busy", 2.0)

        initial_remaining = tracker.grace_period_remaining
        assert initial_remaining > 0
        assert initial_remaining <= 5.0

        # Sleep briefly and check remaining decreases
        time.sleep(0.1)
        later_remaining = tracker.grace_period_remaining
        assert later_remaining < initial_remaining

    def test_should_evict_false_during_grace_period(self):
        """should_evict should be False while in grace period."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=1,
            grace_period=5.0,  # Long grace period
        )

        # Exhaust and trigger grace period
        tracker.request_extension("busy", 1.0)
        tracker.request_extension("busy", 2.0)

        assert tracker.is_exhausted is True
        assert tracker.is_in_grace_period is True
        assert tracker.should_evict is False

    def test_should_evict_true_after_grace_period_expires(self):
        """should_evict should be True after grace period expires."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=1,
            grace_period=0.0,  # Immediate expiry
        )

        # Exhaust and trigger grace period
        tracker.request_extension("busy", 1.0)
        tracker.request_extension("busy", 2.0)

        assert tracker.is_exhausted is True
        assert tracker.should_evict is True  # Grace period already expired

    def test_exhaustion_time_reset_clears(self):
        """reset should clear exhaustion_time and grace period state."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=1,
            grace_period=5.0,
        )

        # Exhaust and trigger grace period
        tracker.request_extension("busy", 1.0)
        tracker.request_extension("busy", 2.0)

        assert tracker.exhaustion_time is not None
        assert tracker.is_in_grace_period is True

        # Reset
        tracker.reset()

        assert tracker.exhaustion_time is None
        assert tracker.is_in_grace_period is False
        assert tracker.grace_period_remaining == 0.0
        assert tracker.should_evict is False


class TestGracefulExhaustionWithManager:
    """Test graceful exhaustion through the WorkerHealthManager interface."""

    def test_manager_response_includes_warning_flag(self):
        """handle_extension_request response should include is_exhaustion_warning."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                max_extensions=2,
                warning_threshold=1,
            )
        )
        deadline = time.monotonic() + 30.0

        # First request - WARNING (remaining=1 after grant, hits threshold=1)
        # Warning triggers when remaining <= warning_threshold
        request1 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response1 = manager.handle_extension_request(request1, deadline)
        assert response1.granted is True
        assert response1.is_exhaustion_warning is True

        # Second request - no warning (already sent)
        request2 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=2.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response2 = manager.handle_extension_request(request2, deadline)
        assert response2.granted is True
        assert response2.is_exhaustion_warning is False

    def test_manager_response_includes_grace_period_info(self):
        """handle_extension_request denial should include grace period info."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                max_extensions=1,
                grace_period=10.0,
            )
        )
        deadline = time.monotonic() + 30.0

        # Use up extensions
        request1 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request1, deadline)

        # Denied request - triggers grace period
        request2 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="still busy",
            current_progress=2.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        response2 = manager.handle_extension_request(request2, deadline)

        assert response2.granted is False
        assert response2.in_grace_period is True
        assert response2.grace_period_remaining > 0

    def test_manager_should_evict_respects_grace_period(self):
        """should_evict_worker should respect grace period."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                max_extensions=1,
                grace_period=5.0,  # Long grace period
            )
        )
        deadline = time.monotonic() + 30.0

        # Use up extensions
        request1 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request1, deadline)

        # Trigger exhaustion
        request2 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="still busy",
            current_progress=2.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request2, deadline)

        # Should NOT evict during grace period
        should_evict, reason = manager.should_evict_worker("worker-1")
        assert should_evict is False
        assert reason is None

    def test_manager_should_evict_after_grace_period_expires(self):
        """should_evict_worker should return True after grace period expires."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                max_extensions=1,
                grace_period=0.0,  # Immediate expiry
            )
        )
        deadline = time.monotonic() + 30.0

        # Use up extensions
        request1 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request1, deadline)

        # Trigger exhaustion
        request2 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="still busy",
            current_progress=2.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request2, deadline)

        # Should evict - grace period already expired
        should_evict, reason = manager.should_evict_worker("worker-1")
        assert should_evict is True
        assert "exhausted all 1 extensions" in reason
        assert "0.0s grace period" in reason

    def test_manager_state_includes_grace_period_info(self):
        """get_worker_extension_state should include grace period info."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                max_extensions=1,
                grace_period=10.0,
            )
        )
        deadline = time.monotonic() + 30.0

        # Use up extensions
        request1 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request1, deadline)

        # Trigger exhaustion
        request2 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="still busy",
            current_progress=2.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request2, deadline)

        state = manager.get_worker_extension_state("worker-1")

        assert state["is_exhausted"] is True
        assert state["in_grace_period"] is True
        assert state["grace_period_remaining"] > 0
        assert state["should_evict"] is False
        assert state["warning_sent"] is True

    def test_manager_healthy_resets_grace_period(self):
        """on_worker_healthy should reset grace period state."""
        manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                max_extensions=1,
                grace_period=10.0,
            )
        )
        deadline = time.monotonic() + 30.0

        # Use up extensions and trigger exhaustion
        request1 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="busy",
            current_progress=1.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request1, deadline)

        request2 = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="still busy",
            current_progress=2.0,
            estimated_completion=5.0,
            active_workflow_count=1,
        )
        manager.handle_extension_request(request2, deadline)

        state_before = manager.get_worker_extension_state("worker-1")
        assert state_before["is_exhausted"] is True
        assert state_before["in_grace_period"] is True

        # Worker becomes healthy
        manager.on_worker_healthy("worker-1")

        state_after = manager.get_worker_extension_state("worker-1")
        assert state_after["is_exhausted"] is False
        assert state_after["in_grace_period"] is False
        assert state_after["grace_period_remaining"] == 0.0
        assert state_after["warning_sent"] is False


class TestWarningThresholdConfigurations:
    """Test different warning_threshold configurations."""

    def test_warning_threshold_zero_warns_on_last(self):
        """warning_threshold=0 should warn only on the last extension (when remaining=0)."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=5,
            warning_threshold=0,
        )

        warnings = []
        for i in range(5):
            granted, _, _, is_warning = tracker.request_extension("busy", float(i + 1))
            assert granted is True
            warnings.append(is_warning)

        # Only the last extension should trigger warning (remaining=0 <= threshold=0)
        assert warnings == [False, False, False, False, True]

    def test_warning_threshold_equals_max_extensions(self):
        """warning_threshold=max_extensions should warn on first request."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=3,
            warning_threshold=3,  # Warn immediately
        )

        # First request should trigger warning (3 remaining == 3 threshold)
        granted, _, _, is_warning = tracker.request_extension("busy", 1.0)
        assert granted is True
        assert is_warning is True

    def test_warning_threshold_larger_than_max_warns_all(self):
        """warning_threshold > max_extensions should warn on first request only."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=3,
            warning_threshold=10,  # Much larger than max
        )

        warnings = []
        for i in range(3):
            granted, _, _, is_warning = tracker.request_extension("busy", float(i + 1))
            assert granted is True
            warnings.append(is_warning)

        # Only first should warn (warning_sent prevents subsequent warnings)
        assert warnings[0] is True
        assert warnings[1] is False
        assert warnings[2] is False
