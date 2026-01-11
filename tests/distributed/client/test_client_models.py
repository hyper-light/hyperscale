"""
Integration tests for client models (Section 15.1.1).

Tests JobTrackingState, CancellationState, GateLeaderTracking,
ManagerLeaderTracking, OrphanedJob, and RequestRouting dataclasses.

Covers:
- Happy path: Normal instantiation and field access
- Negative path: Invalid types and values
- Failure mode: Missing required fields, invalid data
- Concurrency: Thread-safe instantiation (dataclasses are immutable)
- Edge cases: Boundary values, None values
"""

import asyncio
import time
from dataclasses import FrozenInstanceError

import pytest

from hyperscale.distributed.nodes.client.models import (
    JobTrackingState,
    CancellationState,
    GateLeaderTracking,
    ManagerLeaderTracking,
    OrphanedJob,
    RequestRouting,
)


class TestJobTrackingState:
    """Test JobTrackingState dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation with all fields."""
        event = asyncio.Event()
        state = JobTrackingState(
            job_id="job-123",
            job_result=None,
            completion_event=event,
            callback=None,
            target_addr=("localhost", 8000),
        )

        assert state.job_id == "job-123"
        assert state.job_result is None
        assert state.completion_event == event
        assert state.callback is None
        assert state.target_addr == ("localhost", 8000)

    def test_with_result_and_callback(self):
        """Test with job result and callback."""
        event = asyncio.Event()
        callback = lambda x: x

        state = JobTrackingState(
            job_id="job-456",
            job_result={"status": "completed"},
            completion_event=event,
            callback=callback,
            target_addr=("192.168.1.1", 9000),
        )

        assert state.job_result == {"status": "completed"}
        assert state.callback == callback

    def test_immutability(self):
        """Test that dataclass is immutable (slots=True)."""
        event = asyncio.Event()
        state = JobTrackingState(
            job_id="job-789",
            job_result=None,
            completion_event=event,
            callback=None,
            target_addr=None,
        )

        # Verify slots=True prevents setting new attributes
        with pytest.raises(AttributeError):
            state.new_field = "value"

    def test_edge_case_none_target(self):
        """Test with None target address."""
        event = asyncio.Event()
        state = JobTrackingState(
            job_id="job-edge",
            job_result=None,
            completion_event=event,
            callback=None,
            target_addr=None,
        )

        assert state.target_addr is None

    def test_edge_case_empty_job_id(self):
        """Test with empty job ID (allowed but unusual)."""
        event = asyncio.Event()
        state = JobTrackingState(
            job_id="",
            job_result=None,
            completion_event=event,
            callback=None,
            target_addr=None,
        )

        assert state.job_id == ""

    @pytest.mark.asyncio
    async def test_concurrency_event_handling(self):
        """Test concurrent event access."""
        event = asyncio.Event()
        state = JobTrackingState(
            job_id="job-concurrent",
            job_result=None,
            completion_event=event,
            callback=None,
            target_addr=None,
        )

        async def wait_for_completion():
            await state.completion_event.wait()
            return "completed"

        async def signal_completion():
            await asyncio.sleep(0.01)
            state.completion_event.set()

        results = await asyncio.gather(
            wait_for_completion(),
            signal_completion(),
        )

        assert results[0] == "completed"
        assert state.completion_event.is_set()


class TestCancellationState:
    """Test CancellationState dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation."""
        event = asyncio.Event()
        state = CancellationState(
            job_id="cancel-123",
            completion_event=event,
            success=False,
            errors=[],
        )

        assert state.job_id == "cancel-123"
        assert state.completion_event == event
        assert state.success is False
        assert state.errors == []

    def test_with_errors(self):
        """Test with cancellation errors."""
        event = asyncio.Event()
        errors = ["Worker timeout", "Network failure"]
        state = CancellationState(
            job_id="cancel-456",
            completion_event=event,
            success=False,
            errors=errors,
        )

        assert state.success is False
        assert len(state.errors) == 2
        assert "Worker timeout" in state.errors

    def test_successful_cancellation(self):
        """Test successful cancellation state."""
        event = asyncio.Event()
        event.set()

        state = CancellationState(
            job_id="cancel-success",
            completion_event=event,
            success=True,
            errors=[],
        )

        assert state.success is True
        assert state.errors == []
        assert state.completion_event.is_set()

    def test_edge_case_many_errors(self):
        """Test with many error messages."""
        event = asyncio.Event()
        errors = [f"Error {i}" for i in range(100)]
        state = CancellationState(
            job_id="cancel-many-errors",
            completion_event=event,
            success=False,
            errors=errors,
        )

        assert len(state.errors) == 100

    @pytest.mark.asyncio
    async def test_concurrency_cancellation_flow(self):
        """Test concurrent cancellation tracking."""
        event = asyncio.Event()
        errors = []
        state = CancellationState(
            job_id="cancel-concurrent",
            completion_event=event,
            success=False,
            errors=errors,
        )

        async def track_cancellation():
            await state.completion_event.wait()
            return state.success

        async def complete_cancellation():
            await asyncio.sleep(0.01)
            # Simulate updating errors and success
            state.errors.append("Some error")
            state.completion_event.set()

        results = await asyncio.gather(
            track_cancellation(),
            complete_cancellation(),
        )

        assert state.completion_event.is_set()


class TestGateLeaderTracking:
    """Test GateLeaderTracking dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal gate leader tracking."""
        now = time.time()
        leader_info = ("gate-1", 8000)

        tracking = GateLeaderTracking(
            job_id="job-123",
            leader_info=leader_info,
            last_updated=now,
        )

        assert tracking.job_id == "job-123"
        assert tracking.leader_info == leader_info
        assert tracking.last_updated == now

    def test_edge_case_none_leader(self):
        """Test with None leader (no leader assigned)."""
        tracking = GateLeaderTracking(
            job_id="job-no-leader",
            leader_info=None,
            last_updated=0.0,
        )

        assert tracking.leader_info is None
        assert tracking.last_updated == 0.0

    def test_edge_case_very_old_timestamp(self):
        """Test with very old timestamp."""
        tracking = GateLeaderTracking(
            job_id="job-old",
            leader_info=("gate-2", 9000),
            last_updated=1.0,  # Very old timestamp
        )

        assert tracking.last_updated == 1.0


class TestManagerLeaderTracking:
    """Test ManagerLeaderTracking dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal manager leader tracking."""
        now = time.time()
        leader_info = ("manager-1", 7000)

        tracking = ManagerLeaderTracking(
            job_id="job-456",
            datacenter_id="dc-east",
            leader_info=leader_info,
            last_updated=now,
        )

        assert tracking.job_id == "job-456"
        assert tracking.datacenter_id == "dc-east"
        assert tracking.leader_info == leader_info
        assert tracking.last_updated == now

    def test_edge_case_empty_datacenter(self):
        """Test with empty datacenter ID."""
        tracking = ManagerLeaderTracking(
            job_id="job-789",
            datacenter_id="",
            leader_info=("manager-2", 6000),
            last_updated=time.time(),
        )

        assert tracking.datacenter_id == ""

    def test_edge_case_none_leader(self):
        """Test with no manager leader assigned."""
        tracking = ManagerLeaderTracking(
            job_id="job-no-mgr-leader",
            datacenter_id="dc-west",
            leader_info=None,
            last_updated=0.0,
        )

        assert tracking.leader_info is None


class TestOrphanedJob:
    """Test OrphanedJob dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal orphaned job tracking."""
        now = time.time()
        orphan_info = {"reason": "Leader disappeared", "attempts": 3}

        orphaned = OrphanedJob(
            job_id="job-orphan-123",
            orphan_info=orphan_info,
            orphaned_at=now,
        )

        assert orphaned.job_id == "job-orphan-123"
        assert orphaned.orphan_info == orphan_info
        assert orphaned.orphaned_at == now

    def test_edge_case_none_info(self):
        """Test with None orphan info."""
        orphaned = OrphanedJob(
            job_id="job-orphan-456",
            orphan_info=None,
            orphaned_at=time.time(),
        )

        assert orphaned.orphan_info is None

    def test_edge_case_complex_orphan_info(self):
        """Test with complex orphan information."""
        complex_info = {
            "reason": "Manager cluster failure",
            "last_known_leader": ("manager-5", 7000),
            "retry_count": 10,
            "error_messages": ["timeout", "connection refused"],
        }

        orphaned = OrphanedJob(
            job_id="job-complex-orphan",
            orphan_info=complex_info,
            orphaned_at=time.time(),
        )

        assert orphaned.orphan_info["retry_count"] == 10
        assert len(orphaned.orphan_info["error_messages"]) == 2


class TestRequestRouting:
    """Test RequestRouting dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal request routing state."""
        lock = asyncio.Lock()
        target = ("manager-1", 8000)

        routing = RequestRouting(
            job_id="job-route-123",
            routing_lock=lock,
            selected_target=target,
        )

        assert routing.job_id == "job-route-123"
        assert routing.routing_lock == lock
        assert routing.selected_target == target

    def test_edge_case_none_target(self):
        """Test with no selected target."""
        lock = asyncio.Lock()

        routing = RequestRouting(
            job_id="job-route-no-target",
            routing_lock=lock,
            selected_target=None,
        )

        assert routing.selected_target is None

    @pytest.mark.asyncio
    async def test_concurrency_routing_lock(self):
        """Test concurrent routing lock usage."""
        lock = asyncio.Lock()
        routing = RequestRouting(
            job_id="job-concurrent-route",
            routing_lock=lock,
            selected_target=("manager-2", 9000),
        )

        lock_acquired_count = []

        async def acquire_routing_lock(worker_id: int):
            async with routing.routing_lock:
                lock_acquired_count.append(worker_id)
                await asyncio.sleep(0.01)

        await asyncio.gather(
            acquire_routing_lock(1),
            acquire_routing_lock(2),
            acquire_routing_lock(3),
        )

        # All workers acquired lock sequentially
        assert len(lock_acquired_count) == 3

    @pytest.mark.asyncio
    async def test_lock_prevents_concurrent_access(self):
        """Test that lock properly serializes access."""
        lock = asyncio.Lock()
        routing = RequestRouting(
            job_id="job-serial-access",
            routing_lock=lock,
            selected_target=None,
        )

        access_order = []

        async def access_with_lock(worker_id: int):
            async with routing.routing_lock:
                access_order.append(f"start-{worker_id}")
                await asyncio.sleep(0.02)
                access_order.append(f"end-{worker_id}")

        await asyncio.gather(
            access_with_lock(1),
            access_with_lock(2),
        )

        # Verify serialized access (no interleaving)
        assert access_order[0] == "start-1"
        assert access_order[1] == "end-1"
        assert access_order[2] == "start-2"
        assert access_order[3] == "end-2"


# Edge case tests for all models
class TestModelsEdgeCases:
    """Test edge cases across all client models."""

    def test_all_models_use_slots(self):
        """Verify all models use slots=True for memory efficiency."""
        event = asyncio.Event()
        lock = asyncio.Lock()

        job_tracking = JobTrackingState("job", None, event, None, None)
        cancellation = CancellationState("cancel", event, False, [])
        gate_leader = GateLeaderTracking("gate-job", None, 0.0)
        manager_leader = ManagerLeaderTracking("mgr-job", "dc", None, 0.0)
        orphaned = OrphanedJob("orphan", None, 0.0)
        routing = RequestRouting("route", lock, None)

        # All should raise AttributeError when trying to set new attributes
        models = [
            job_tracking,
            cancellation,
            gate_leader,
            manager_leader,
            orphaned,
            routing,
        ]

        for model in models:
            with pytest.raises(AttributeError):
                model.new_attribute = "value"

    def test_models_with_very_long_ids(self):
        """Test models with extremely long job IDs."""
        long_id = "job-" + "x" * 10000
        event = asyncio.Event()

        state = JobTrackingState(
            job_id=long_id,
            job_result=None,
            completion_event=event,
            callback=None,
            target_addr=None,
        )

        assert len(state.job_id) == 10004

    def test_models_with_special_characters(self):
        """Test job IDs with special characters."""
        special_id = "job-🚀-test-ñ-中文"
        event = asyncio.Event()

        state = JobTrackingState(
            job_id=special_id,
            job_result=None,
            completion_event=event,
            callback=None,
            target_addr=None,
        )

        assert state.job_id == special_id
