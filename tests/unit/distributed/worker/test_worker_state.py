"""
Integration tests for WorkerState (Section 15.2.4).

Tests WorkerState mutable runtime state container.

Covers:
- Happy path: Normal state operations
- Negative path: Invalid state transitions
- Failure mode: Missing keys, invalid operations
- Concurrency: Thread-safe state updates, lock management
- Edge cases: Empty state, boundary values
"""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from hyperscale.distributed.nodes.worker.state import WorkerState
from hyperscale.distributed.models import (
    ManagerInfo,
    WorkflowProgress,
    PendingTransfer,
)
from hyperscale.distributed.reliability import BackpressureLevel


class MockCoreAllocator:
    """Mock CoreAllocator for testing."""

    def __init__(self, total_cores: int = 8):
        self.total_cores = total_cores
        self.available_cores = total_cores


class TestWorkerStateInitialization:
    """Test WorkerState initialization."""

    def test_happy_path_instantiation(self):
        """Test normal state initialization."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert state._core_allocator == allocator
        assert isinstance(state._known_managers, dict)
        assert isinstance(state._healthy_manager_ids, set)
        assert state._primary_manager_id is None

    def test_empty_collections_on_init(self):
        """Test that all collections are empty on initialization."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert len(state._known_managers) == 0
        assert len(state._healthy_manager_ids) == 0
        assert len(state._active_workflows) == 0
        assert len(state._workflow_tokens) == 0
        assert len(state._orphaned_workflows) == 0
        assert len(state._pending_transfers) == 0

    def test_initial_counters(self):
        """Test initial counter values."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert state._state_version == 0
        assert state._transfer_metrics_received == 0
        assert state._transfer_metrics_accepted == 0
        assert state._backpressure_delay_ms == 0
        assert state._throughput_completions == 0


class TestWorkerStateVersionManagement:
    """Test state version management."""

    @pytest.mark.asyncio
    async def test_increment_version(self):
        """Test version increment."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert state.state_version == 0

        new_version = await state.increment_version()
        assert new_version == 1
        assert state.state_version == 1

    @pytest.mark.asyncio
    async def test_multiple_version_increments(self):
        """Test multiple version increments."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        for i in range(10):
            version = await state.increment_version()
            assert version == i + 1


class TestWorkerStateManagerTracking:
    """Test manager tracking methods."""

    def test_add_manager(self):
        """Test adding a manager."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        manager_info = MagicMock(spec=ManagerInfo)
        manager_info.tcp_host = "192.168.1.1"
        manager_info.tcp_port = 8000

        state.add_manager("mgr-1", manager_info)

        assert "mgr-1" in state._known_managers
        assert state._known_managers["mgr-1"] == manager_info

    def test_get_manager(self):
        """Test getting a manager by ID."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        manager_info = MagicMock(spec=ManagerInfo)
        state.add_manager("mgr-1", manager_info)

        result = state.get_manager("mgr-1")
        assert result == manager_info

    def test_get_manager_not_found(self):
        """Test getting a non-existent manager."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        result = state.get_manager("non-existent")
        assert result is None

    def test_mark_manager_healthy(self):
        """Test marking a manager as healthy."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.mark_manager_healthy("mgr-1")

        assert "mgr-1" in state._healthy_manager_ids
        assert state.is_manager_healthy("mgr-1") is True

    @pytest.mark.asyncio
    async def test_mark_manager_unhealthy(self):
        """Test marking a manager as unhealthy."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.mark_manager_healthy("mgr-1")
        await state.mark_manager_unhealthy("mgr-1")

        assert "mgr-1" not in state._healthy_manager_ids
        assert state.is_manager_healthy("mgr-1") is False
        assert "mgr-1" in state._manager_unhealthy_since

    @pytest.mark.asyncio
    async def test_mark_manager_unhealthy_records_time(self):
        """Test that marking unhealthy records timestamp."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        before = time.monotonic()
        await state.mark_manager_unhealthy("mgr-1")
        after = time.monotonic()

        assert "mgr-1" in state._manager_unhealthy_since
        assert before <= state._manager_unhealthy_since["mgr-1"] <= after

    @pytest.mark.asyncio
    async def test_mark_manager_healthy_clears_unhealthy_since(self):
        """Test that marking healthy clears unhealthy timestamp."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        await state.mark_manager_unhealthy("mgr-1")
        assert "mgr-1" in state._manager_unhealthy_since

        state.mark_manager_healthy("mgr-1")
        assert "mgr-1" not in state._manager_unhealthy_since

    def test_get_healthy_manager_tcp_addrs(self):
        """Test getting healthy manager TCP addresses."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        mgr1 = MagicMock(spec=ManagerInfo)
        mgr1.tcp_host = "192.168.1.1"
        mgr1.tcp_port = 8000

        mgr2 = MagicMock(spec=ManagerInfo)
        mgr2.tcp_host = "192.168.1.2"
        mgr2.tcp_port = 8001

        state.add_manager("mgr-1", mgr1)
        state.add_manager("mgr-2", mgr2)
        state.mark_manager_healthy("mgr-1")
        state.mark_manager_healthy("mgr-2")

        addrs = state.get_healthy_manager_tcp_addrs()

        assert len(addrs) == 2
        assert ("192.168.1.1", 8000) in addrs
        assert ("192.168.1.2", 8001) in addrs

    def test_get_or_create_manager_lock(self):
        """Test getting or creating a manager lock."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        lock1 = state.get_or_create_manager_lock("mgr-1")
        lock2 = state.get_or_create_manager_lock("mgr-1")

        assert lock1 is lock2
        assert isinstance(lock1, asyncio.Lock)

    def test_increment_manager_epoch(self):
        """Test incrementing manager epoch."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert state.get_manager_epoch("mgr-1") == 0

        epoch1 = state.increment_manager_epoch("mgr-1")
        assert epoch1 == 1

        epoch2 = state.increment_manager_epoch("mgr-1")
        assert epoch2 == 2


class TestWorkerStateWorkflowTracking:
    """Test workflow tracking methods."""

    def test_add_active_workflow(self):
        """Test adding an active workflow."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        progress = MagicMock(spec=WorkflowProgress)
        leader_addr = ("192.168.1.1", 8000)

        state.add_active_workflow("wf-1", progress, leader_addr)

        assert "wf-1" in state._active_workflows
        assert state._active_workflows["wf-1"] == progress
        assert state._workflow_job_leader["wf-1"] == leader_addr
        assert "wf-1" in state._workflow_cores_completed

    def test_get_active_workflow(self):
        """Test getting an active workflow."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        progress = MagicMock(spec=WorkflowProgress)
        state.add_active_workflow("wf-1", progress, ("h", 1))

        result = state.get_active_workflow("wf-1")
        assert result == progress

    def test_get_active_workflow_not_found(self):
        """Test getting a non-existent workflow."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        result = state.get_active_workflow("non-existent")
        assert result is None

    def test_remove_active_workflow(self):
        """Test removing an active workflow."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        progress = MagicMock(spec=WorkflowProgress)
        state.add_active_workflow("wf-1", progress, ("h", 1))
        state._workflow_tokens["wf-1"] = "token"
        state._workflow_id_to_name["wf-1"] = "my-workflow"
        state._workflow_cancel_events["wf-1"] = asyncio.Event()
        state._orphaned_workflows["wf-1"] = time.monotonic()

        removed = state.remove_active_workflow("wf-1")

        assert removed == progress
        assert "wf-1" not in state._active_workflows
        assert "wf-1" not in state._workflow_job_leader
        assert "wf-1" not in state._workflow_cores_completed
        assert "wf-1" not in state._workflow_tokens
        assert "wf-1" not in state._workflow_id_to_name
        assert "wf-1" not in state._workflow_cancel_events
        assert "wf-1" not in state._orphaned_workflows

    def test_remove_active_workflow_not_found(self):
        """Test removing a non-existent workflow."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        removed = state.remove_active_workflow("non-existent")
        assert removed is None

    def test_get_workflow_job_leader(self):
        """Test getting workflow job leader."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        progress = MagicMock(spec=WorkflowProgress)
        leader_addr = ("192.168.1.1", 8000)
        state.add_active_workflow("wf-1", progress, leader_addr)

        result = state.get_workflow_job_leader("wf-1")
        assert result == leader_addr

    def test_set_workflow_job_leader(self):
        """Test setting workflow job leader."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        progress = MagicMock(spec=WorkflowProgress)
        state.add_active_workflow("wf-1", progress, ("old", 1))

        state.set_workflow_job_leader("wf-1", ("new", 2))

        assert state._workflow_job_leader["wf-1"] == ("new", 2)

    def test_update_workflow_fence_token_success(self):
        """Test updating fence token with newer value."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        result = state.update_workflow_fence_token("wf-1", 5)
        assert result is True
        assert state._workflow_fence_tokens["wf-1"] == 5

    def test_update_workflow_fence_token_stale(self):
        """Test rejecting stale fence token."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.update_workflow_fence_token("wf-1", 10)
        result = state.update_workflow_fence_token("wf-1", 5)

        assert result is False
        assert state._workflow_fence_tokens["wf-1"] == 10

    def test_get_workflow_fence_token(self):
        """Test getting workflow fence token."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert state.get_workflow_fence_token("wf-1") == -1

        state.update_workflow_fence_token("wf-1", 42)
        assert state.get_workflow_fence_token("wf-1") == 42


class TestWorkerStateOrphanTracking:
    """Test orphan tracking methods (Section 2.7)."""

    def test_mark_workflow_orphaned(self):
        """Test marking a workflow as orphaned."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        before = time.monotonic()
        state.mark_workflow_orphaned("wf-1")
        after = time.monotonic()

        assert "wf-1" in state._orphaned_workflows
        assert before <= state._orphaned_workflows["wf-1"] <= after

    def test_mark_workflow_orphaned_idempotent(self):
        """Test that marking orphaned is idempotent."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.mark_workflow_orphaned("wf-1")
        first_time = state._orphaned_workflows["wf-1"]

        state.mark_workflow_orphaned("wf-1")
        second_time = state._orphaned_workflows["wf-1"]

        assert first_time == second_time

    def test_clear_workflow_orphaned(self):
        """Test clearing orphan status."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.mark_workflow_orphaned("wf-1")
        state.clear_workflow_orphaned("wf-1")

        assert "wf-1" not in state._orphaned_workflows

    def test_clear_workflow_orphaned_not_found(self):
        """Test clearing orphan status for non-orphaned workflow."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        # Should not raise
        state.clear_workflow_orphaned("non-existent")

    def test_is_workflow_orphaned(self):
        """Test checking orphan status."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert state.is_workflow_orphaned("wf-1") is False

        state.mark_workflow_orphaned("wf-1")
        assert state.is_workflow_orphaned("wf-1") is True

    def test_get_orphaned_workflows_expired(self):
        """Test getting expired orphaned workflows."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        # Add orphaned workflows with different times
        state._orphaned_workflows["wf-old"] = time.monotonic() - 200
        state._orphaned_workflows["wf-new"] = time.monotonic()

        expired = state.get_orphaned_workflows_expired(grace_period_seconds=100)

        assert "wf-old" in expired
        assert "wf-new" not in expired


class TestWorkerStateJobLeadershipTransfer:
    """Test job leadership transfer methods (Section 8)."""

    def test_get_or_create_job_transfer_lock(self):
        """Test getting or creating a job transfer lock."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        lock1 = state.get_or_create_job_transfer_lock("job-1")
        lock2 = state.get_or_create_job_transfer_lock("job-1")

        assert lock1 is lock2
        assert isinstance(lock1, asyncio.Lock)

    def test_update_job_fence_token_success(self):
        """Test updating job fence token with newer value."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        result = state.update_job_fence_token("job-1", 10)
        assert result is True
        assert state._job_fence_tokens["job-1"] == 10

    def test_update_job_fence_token_stale(self):
        """Test rejecting stale job fence token."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.update_job_fence_token("job-1", 10)
        result = state.update_job_fence_token("job-1", 5)

        assert result is False
        assert state._job_fence_tokens["job-1"] == 10

    def test_get_job_fence_token(self):
        """Test getting job fence token."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert state.get_job_fence_token("job-1") == -1

        state.update_job_fence_token("job-1", 42)
        assert state.get_job_fence_token("job-1") == 42

    def test_add_pending_transfer(self):
        """Test adding a pending transfer."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        transfer = MagicMock(spec=PendingTransfer)
        state.add_pending_transfer("job-1", transfer)

        assert state._pending_transfers["job-1"] == transfer

    def test_get_pending_transfer(self):
        """Test getting a pending transfer."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        transfer = MagicMock(spec=PendingTransfer)
        state.add_pending_transfer("job-1", transfer)

        result = state.get_pending_transfer("job-1")
        assert result == transfer

    def test_get_pending_transfer_not_found(self):
        """Test getting a non-existent pending transfer."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        result = state.get_pending_transfer("non-existent")
        assert result is None

    def test_remove_pending_transfer(self):
        """Test removing a pending transfer."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        transfer = MagicMock(spec=PendingTransfer)
        state.add_pending_transfer("job-1", transfer)

        removed = state.remove_pending_transfer("job-1")

        assert removed == transfer
        assert "job-1" not in state._pending_transfers


class TestWorkerStateTransferMetrics:
    """Test transfer metrics methods (Section 8.6)."""

    def test_increment_transfer_received(self):
        """Test incrementing transfer received counter."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert state._transfer_metrics_received == 0

        state.increment_transfer_received()
        assert state._transfer_metrics_received == 1

    def test_increment_transfer_accepted(self):
        """Test incrementing transfer accepted counter."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.increment_transfer_accepted()
        assert state._transfer_metrics_accepted == 1

    def test_increment_transfer_rejected_stale_token(self):
        """Test incrementing stale token rejection counter."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.increment_transfer_rejected_stale_token()
        assert state._transfer_metrics_rejected_stale_token == 1

    def test_increment_transfer_rejected_unknown_manager(self):
        """Test incrementing unknown manager rejection counter."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.increment_transfer_rejected_unknown_manager()
        assert state._transfer_metrics_rejected_unknown_manager == 1

    def test_increment_transfer_rejected_other(self):
        """Test incrementing other rejection counter."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.increment_transfer_rejected_other()
        assert state._transfer_metrics_rejected_other == 1

    def test_get_transfer_metrics(self):
        """Test getting transfer metrics summary."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.increment_transfer_received()
        state.increment_transfer_received()
        state.increment_transfer_accepted()
        state.increment_transfer_rejected_stale_token()

        metrics = state.get_transfer_metrics()

        assert metrics["received"] == 2
        assert metrics["accepted"] == 1
        assert metrics["rejected_stale_token"] == 1
        assert metrics["rejected_unknown_manager"] == 0
        assert metrics["rejected_other"] == 0


class TestWorkerStateBackpressure:
    """Test backpressure tracking methods (AD-23)."""

    def test_set_manager_backpressure(self):
        """Test setting manager backpressure level."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.set_manager_backpressure("mgr-1", BackpressureLevel.THROTTLE)

        assert state._manager_backpressure["mgr-1"] == BackpressureLevel.THROTTLE

    def test_get_max_backpressure_level_none(self):
        """Test max backpressure with no managers."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        level = state.get_max_backpressure_level()
        assert level == BackpressureLevel.NONE

    def test_get_max_backpressure_level(self):
        """Test max backpressure level across managers."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.set_manager_backpressure("mgr-1", BackpressureLevel.NONE)
        state.set_manager_backpressure("mgr-2", BackpressureLevel.BATCH)
        state.set_manager_backpressure("mgr-3", BackpressureLevel.THROTTLE)

        level = state.get_max_backpressure_level()
        assert level == BackpressureLevel.BATCH

    def test_set_backpressure_delay_ms(self):
        """Test setting backpressure delay."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.set_backpressure_delay_ms(500)
        assert state.get_backpressure_delay_ms() == 500


class TestWorkerStateThroughputTracking:
    """Test throughput tracking methods (AD-19)."""

    def test_record_completion(self):
        """Test recording a workflow completion."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.record_completion(1.5)

        assert state._throughput_completions == 1
        assert len(state._completion_times) == 1
        assert state._completion_times[0] == 1.5

    def test_record_completion_max_samples(self):
        """Test completion times max samples limit."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        for i in range(60):
            state.record_completion(float(i))

        assert len(state._completion_times) == 50
        assert state._completion_times[0] == 10.0  # First 10 removed

    def test_get_throughput_initial(self):
        """Test initial throughput."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        throughput = state.get_throughput()
        assert throughput == 0.0

    def test_get_expected_throughput_empty(self):
        """Test expected throughput with no samples."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        expected = state.get_expected_throughput()
        assert expected == 0.0

    def test_get_expected_throughput_with_samples(self):
        """Test expected throughput calculation."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        # Record 10 completions, each taking 2 seconds
        for _ in range(10):
            state.record_completion(2.0)

        expected = state.get_expected_throughput()
        assert expected == 0.5  # 1 / 2.0 = 0.5 per second

    def test_get_expected_throughput_zero_duration(self):
        """Test expected throughput with zero duration."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        state.record_completion(0.0)

        expected = state.get_expected_throughput()
        assert expected == 0.0


class TestWorkerStateConcurrency:
    """Test concurrency aspects of WorkerState."""

    @pytest.mark.asyncio
    async def test_concurrent_manager_lock_access(self):
        """Test concurrent access to manager locks."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        access_order = []

        async def access_with_lock(manager_id: str, worker_id: int):
            lock = state.get_or_create_manager_lock(manager_id)
            async with lock:
                access_order.append(f"start-{worker_id}")
                await asyncio.sleep(0.01)
                access_order.append(f"end-{worker_id}")

        await asyncio.gather(
            access_with_lock("mgr-1", 1),
            access_with_lock("mgr-1", 2),
        )

        # Verify serialized access
        assert access_order[0] == "start-1"
        assert access_order[1] == "end-1"
        assert access_order[2] == "start-2"
        assert access_order[3] == "end-2"

    @pytest.mark.asyncio
    async def test_concurrent_job_transfer_lock_access(self):
        """Test concurrent access to job transfer locks."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        access_order = []

        async def access_with_lock(job_id: str, worker_id: int):
            lock = state.get_or_create_job_transfer_lock(job_id)
            async with lock:
                access_order.append(f"start-{worker_id}")
                await asyncio.sleep(0.01)
                access_order.append(f"end-{worker_id}")

        await asyncio.gather(
            access_with_lock("job-1", 1),
            access_with_lock("job-1", 2),
        )

        # Verify serialized access
        assert access_order[0] == "start-1"
        assert access_order[1] == "end-1"

    @pytest.mark.asyncio
    async def test_concurrent_workflow_updates(self):
        """Test concurrent workflow state updates."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        async def add_workflow(workflow_id: str):
            progress = MagicMock(spec=WorkflowProgress)
            state.add_active_workflow(workflow_id, progress, ("h", 1))
            await asyncio.sleep(0.001)

        await asyncio.gather(*[add_workflow(f"wf-{i}") for i in range(10)])

        assert len(state._active_workflows) == 10

    @pytest.mark.asyncio
    async def test_progress_buffer_lock(self):
        """Test progress buffer lock exists and is asyncio.Lock."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        assert isinstance(state._progress_buffer_lock, asyncio.Lock)


class TestWorkerStateEdgeCases:
    """Test edge cases for WorkerState."""

    def test_many_managers(self):
        """Test with many managers."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        for i in range(100):
            mgr = MagicMock(spec=ManagerInfo)
            mgr.tcp_host = f"192.168.1.{i}"
            mgr.tcp_port = 8000 + i
            state.add_manager(f"mgr-{i}", mgr)
            state.mark_manager_healthy(f"mgr-{i}")

        assert len(state._known_managers) == 100
        assert len(state._healthy_manager_ids) == 100

    def test_many_active_workflows(self):
        """Test with many active workflows."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        for i in range(1000):
            progress = MagicMock(spec=WorkflowProgress)
            state.add_active_workflow(f"wf-{i}", progress, ("h", 1))

        assert len(state._active_workflows) == 1000

    def test_special_characters_in_ids(self):
        """Test IDs with special characters."""
        allocator = MockCoreAllocator()
        state = WorkerState(allocator)

        special_id = "wf-🚀-test-ñ-中文"
        progress = MagicMock(spec=WorkflowProgress)
        state.add_active_workflow(special_id, progress, ("h", 1))

        assert special_id in state._active_workflows
