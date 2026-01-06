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

from hyperscale.distributed_rewrite.health import (
    ExtensionTracker,
    ExtensionTrackerConfig,
    WorkerHealthManager,
    WorkerHealthManagerConfig,
)
from hyperscale.distributed_rewrite.models import (
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

        granted, seconds, reason = tracker.request_extension(
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
        granted, seconds, _ = tracker.request_extension("busy", 1.0)
        assert granted is True
        assert seconds == 16.0

        # Second extension: 32/4 = 8
        granted, seconds, _ = tracker.request_extension("busy", 2.0)
        assert granted is True
        assert seconds == 8.0

        # Third extension: 32/8 = 4
        granted, seconds, _ = tracker.request_extension("busy", 3.0)
        assert granted is True
        assert seconds == 4.0

        # Fourth extension: 32/16 = 2
        granted, seconds, _ = tracker.request_extension("busy", 4.0)
        assert granted is True
        assert seconds == 2.0

        # Fifth extension: 32/32 = 1 (min_grant)
        granted, seconds, _ = tracker.request_extension("busy", 5.0)
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
            granted, seconds, _ = tracker.request_extension(
                reason="busy",
                current_progress=float(i + 1),
            )
            assert granted is True
            assert seconds >= 2.0  # Never below min_grant

    def test_progress_required_for_subsequent_extensions(self):
        """Subsequent extensions require progress since last extension."""
        tracker = ExtensionTracker(worker_id="worker-1")

        # First extension succeeds (no prior progress to compare)
        granted, _, _ = tracker.request_extension("busy", 1.0)
        assert granted is True

        # Same progress - should be denied
        granted, _, reason = tracker.request_extension("busy", 1.0)
        assert granted is False
        assert "No progress" in reason

        # Lower progress - should be denied
        granted, _, reason = tracker.request_extension("busy", 0.5)
        assert granted is False
        assert "No progress" in reason

        # Higher progress - should be granted
        granted, _, _ = tracker.request_extension("busy", 2.0)
        assert granted is True

    def test_max_extensions_enforced(self):
        """Extensions should be denied after max_extensions reached."""
        tracker = ExtensionTracker(
            worker_id="worker-1",
            max_extensions=3,
        )

        # Use up all extensions
        for i in range(3):
            granted, _, _ = tracker.request_extension("busy", float(i + 1))
            assert granted is True

        assert tracker.is_exhausted is True

        # Next request should be denied
        granted, _, reason = tracker.request_extension("busy", 4.0)
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
            granted, seconds, _ = tracker.request_extension(
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
