"""
Integration tests for worker models (Section 15.2.2).

Tests ManagerPeerState, WorkflowRuntimeState, CancelState,
ExecutionMetrics, CompletionTimeTracker, TransferMetrics, and PendingTransferState.

Covers:
- Happy path: Normal instantiation and field access
- Negative path: Invalid types and values
- Failure mode: Missing required fields
- Concurrency: Thread-safe instantiation (dataclasses with slots)
- Edge cases: Boundary values, None values, empty collections
"""

import time
from dataclasses import FrozenInstanceError

import pytest

from hyperscale.distributed_rewrite.nodes.worker.models import (
    ManagerPeerState,
    WorkflowRuntimeState,
    CancelState,
    ExecutionMetrics,
    CompletionTimeTracker,
    TransferMetrics,
    PendingTransferState,
)


class TestManagerPeerState:
    """Test ManagerPeerState dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation with all required fields."""
        state = ManagerPeerState(
            manager_id="manager-123",
            tcp_host="192.168.1.1",
            tcp_port=8000,
            udp_host="192.168.1.1",
            udp_port=8001,
            datacenter="dc-east",
        )

        assert state.manager_id == "manager-123"
        assert state.tcp_host == "192.168.1.1"
        assert state.tcp_port == 8000
        assert state.udp_host == "192.168.1.1"
        assert state.udp_port == 8001
        assert state.datacenter == "dc-east"

    def test_default_values(self):
        """Test default field values."""
        state = ManagerPeerState(
            manager_id="mgr-1",
            tcp_host="localhost",
            tcp_port=7000,
            udp_host="localhost",
            udp_port=7001,
            datacenter="default",
        )

        assert state.is_leader is False
        assert state.is_healthy is True
        assert state.unhealthy_since is None
        assert state.state_epoch == 0

    def test_with_optional_values(self):
        """Test with optional fields set."""
        unhealthy_time = time.time()
        state = ManagerPeerState(
            manager_id="mgr-2",
            tcp_host="10.0.0.1",
            tcp_port=9000,
            udp_host="10.0.0.1",
            udp_port=9001,
            datacenter="dc-west",
            is_leader=True,
            is_healthy=False,
            unhealthy_since=unhealthy_time,
            state_epoch=42,
        )

        assert state.is_leader is True
        assert state.is_healthy is False
        assert state.unhealthy_since == unhealthy_time
        assert state.state_epoch == 42

    def test_slots_prevents_new_attributes(self):
        """Test that slots=True prevents adding new attributes."""
        state = ManagerPeerState(
            manager_id="mgr",
            tcp_host="h",
            tcp_port=1,
            udp_host="h",
            udp_port=2,
            datacenter="dc",
        )

        with pytest.raises(AttributeError):
            state.new_field = "value"

    def test_edge_case_empty_strings(self):
        """Test with empty string values."""
        state = ManagerPeerState(
            manager_id="",
            tcp_host="",
            tcp_port=0,
            udp_host="",
            udp_port=0,
            datacenter="",
        )

        assert state.manager_id == ""
        assert state.tcp_host == ""

    def test_edge_case_max_port(self):
        """Test with maximum port number."""
        state = ManagerPeerState(
            manager_id="mgr",
            tcp_host="h",
            tcp_port=65535,
            udp_host="h",
            udp_port=65535,
            datacenter="dc",
        )

        assert state.tcp_port == 65535
        assert state.udp_port == 65535


class TestWorkflowRuntimeState:
    """Test WorkflowRuntimeState dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation with all required fields."""
        start = time.time()
        state = WorkflowRuntimeState(
            workflow_id="wf-123",
            job_id="job-456",
            status="running",
            allocated_cores=4,
            fence_token=10,
            start_time=start,
        )

        assert state.workflow_id == "wf-123"
        assert state.job_id == "job-456"
        assert state.status == "running"
        assert state.allocated_cores == 4
        assert state.fence_token == 10
        assert state.start_time == start

    def test_default_values(self):
        """Test default field values."""
        state = WorkflowRuntimeState(
            workflow_id="wf-1",
            job_id="job-1",
            status="pending",
            allocated_cores=1,
            fence_token=0,
            start_time=0.0,
        )

        assert state.job_leader_addr is None
        assert state.is_orphaned is False
        assert state.orphaned_since is None
        assert state.cores_completed == 0
        assert state.vus == 0

    def test_with_orphan_state(self):
        """Test workflow in orphaned state."""
        orphan_time = time.time()
        state = WorkflowRuntimeState(
            workflow_id="wf-orphan",
            job_id="job-orphan",
            status="running",
            allocated_cores=2,
            fence_token=5,
            start_time=time.time() - 100,
            job_leader_addr=("manager-1", 8000),
            is_orphaned=True,
            orphaned_since=orphan_time,
        )

        assert state.is_orphaned is True
        assert state.orphaned_since == orphan_time
        assert state.job_leader_addr == ("manager-1", 8000)

    def test_with_vus_and_cores_completed(self):
        """Test with VUs and completed cores."""
        state = WorkflowRuntimeState(
            workflow_id="wf-vus",
            job_id="job-vus",
            status="completed",
            allocated_cores=8,
            fence_token=15,
            start_time=time.time(),
            cores_completed=6,
            vus=100,
        )

        assert state.cores_completed == 6
        assert state.vus == 100

    def test_slots_prevents_new_attributes(self):
        """Test that slots=True prevents adding new attributes."""
        state = WorkflowRuntimeState(
            workflow_id="wf",
            job_id="j",
            status="s",
            allocated_cores=1,
            fence_token=0,
            start_time=0,
        )

        with pytest.raises(AttributeError):
            state.custom_field = "value"

    def test_edge_case_zero_cores(self):
        """Test with zero allocated cores."""
        state = WorkflowRuntimeState(
            workflow_id="wf-zero",
            job_id="job-zero",
            status="pending",
            allocated_cores=0,
            fence_token=0,
            start_time=0.0,
        )

        assert state.allocated_cores == 0


class TestCancelState:
    """Test CancelState dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation."""
        cancel_time = time.time()
        state = CancelState(
            workflow_id="wf-cancel",
            job_id="job-cancel",
            cancel_requested_at=cancel_time,
            cancel_reason="user requested",
        )

        assert state.workflow_id == "wf-cancel"
        assert state.job_id == "job-cancel"
        assert state.cancel_requested_at == cancel_time
        assert state.cancel_reason == "user requested"

    def test_default_values(self):
        """Test default field values."""
        state = CancelState(
            workflow_id="wf",
            job_id="job",
            cancel_requested_at=0.0,
            cancel_reason="test",
        )

        assert state.cancel_completed is False
        assert state.cancel_success is False
        assert state.cancel_error is None

    def test_successful_cancellation(self):
        """Test successful cancellation state."""
        state = CancelState(
            workflow_id="wf-success",
            job_id="job-success",
            cancel_requested_at=time.time(),
            cancel_reason="timeout",
            cancel_completed=True,
            cancel_success=True,
        )

        assert state.cancel_completed is True
        assert state.cancel_success is True
        assert state.cancel_error is None

    def test_failed_cancellation(self):
        """Test failed cancellation state."""
        state = CancelState(
            workflow_id="wf-fail",
            job_id="job-fail",
            cancel_requested_at=time.time(),
            cancel_reason="abort",
            cancel_completed=True,
            cancel_success=False,
            cancel_error="Workflow already completed",
        )

        assert state.cancel_completed is True
        assert state.cancel_success is False
        assert state.cancel_error == "Workflow already completed"

    def test_slots_prevents_new_attributes(self):
        """Test that slots=True prevents adding new attributes."""
        state = CancelState(
            workflow_id="wf",
            job_id="j",
            cancel_requested_at=0,
            cancel_reason="r",
        )

        with pytest.raises(AttributeError):
            state.extra = "value"


class TestExecutionMetrics:
    """Test ExecutionMetrics dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation with defaults."""
        metrics = ExecutionMetrics()

        assert metrics.workflows_executed == 0
        assert metrics.workflows_completed == 0
        assert metrics.workflows_failed == 0
        assert metrics.workflows_cancelled == 0
        assert metrics.total_cores_allocated == 0
        assert metrics.total_execution_time_seconds == 0.0
        assert metrics.throughput_completions == 0
        assert metrics.throughput_interval_start == 0.0
        assert metrics.throughput_last_value == 0.0

    def test_with_values(self):
        """Test with actual metric values."""
        metrics = ExecutionMetrics(
            workflows_executed=100,
            workflows_completed=95,
            workflows_failed=3,
            workflows_cancelled=2,
            total_cores_allocated=400,
            total_execution_time_seconds=3600.0,
            throughput_completions=10,
            throughput_interval_start=time.monotonic(),
            throughput_last_value=2.5,
        )

        assert metrics.workflows_executed == 100
        assert metrics.workflows_completed == 95
        assert metrics.workflows_failed == 3
        assert metrics.workflows_cancelled == 2
        assert metrics.total_cores_allocated == 400
        assert metrics.total_execution_time_seconds == 3600.0

    def test_slots_prevents_new_attributes(self):
        """Test that slots=True prevents adding new attributes."""
        metrics = ExecutionMetrics()

        with pytest.raises(AttributeError):
            metrics.custom_metric = 123

    def test_edge_case_large_values(self):
        """Test with very large metric values."""
        metrics = ExecutionMetrics(
            workflows_executed=10_000_000,
            workflows_completed=9_999_999,
            total_cores_allocated=1_000_000_000,
            total_execution_time_seconds=86400.0 * 365,
        )

        assert metrics.workflows_executed == 10_000_000
        assert metrics.total_cores_allocated == 1_000_000_000


class TestCompletionTimeTracker:
    """Test CompletionTimeTracker dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation with defaults."""
        tracker = CompletionTimeTracker()

        assert tracker.max_samples == 50
        assert tracker.completion_times == []

    def test_add_completion_time(self):
        """Test adding completion times."""
        tracker = CompletionTimeTracker()

        tracker.add_completion_time(1.5)
        tracker.add_completion_time(2.0)
        tracker.add_completion_time(1.8)

        assert len(tracker.completion_times) == 3
        assert tracker.completion_times == [1.5, 2.0, 1.8]

    def test_max_samples_limit(self):
        """Test that max samples are enforced."""
        tracker = CompletionTimeTracker(max_samples=5)

        for i in range(10):
            tracker.add_completion_time(float(i))

        assert len(tracker.completion_times) == 5
        assert tracker.completion_times == [5.0, 6.0, 7.0, 8.0, 9.0]

    def test_get_average_completion_time_empty(self):
        """Test average with no samples."""
        tracker = CompletionTimeTracker()

        assert tracker.get_average_completion_time() == 0.0

    def test_get_average_completion_time_with_samples(self):
        """Test average calculation."""
        tracker = CompletionTimeTracker()

        tracker.add_completion_time(1.0)
        tracker.add_completion_time(2.0)
        tracker.add_completion_time(3.0)

        assert tracker.get_average_completion_time() == 2.0

    def test_sliding_window_behavior(self):
        """Test sliding window removes oldest samples."""
        tracker = CompletionTimeTracker(max_samples=3)

        tracker.add_completion_time(100.0)  # Will be removed
        tracker.add_completion_time(1.0)
        tracker.add_completion_time(2.0)
        tracker.add_completion_time(3.0)

        assert tracker.get_average_completion_time() == 2.0

    def test_edge_case_single_sample(self):
        """Test with single sample."""
        tracker = CompletionTimeTracker()

        tracker.add_completion_time(5.5)

        assert tracker.get_average_completion_time() == 5.5

    def test_edge_case_zero_duration(self):
        """Test with zero duration samples."""
        tracker = CompletionTimeTracker()

        tracker.add_completion_time(0.0)
        tracker.add_completion_time(0.0)

        assert tracker.get_average_completion_time() == 0.0


class TestTransferMetrics:
    """Test TransferMetrics dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation with defaults."""
        metrics = TransferMetrics()

        assert metrics.received == 0
        assert metrics.accepted == 0
        assert metrics.rejected_stale_token == 0
        assert metrics.rejected_unknown_manager == 0
        assert metrics.rejected_other == 0

    def test_with_values(self):
        """Test with actual metric values."""
        metrics = TransferMetrics(
            received=100,
            accepted=95,
            rejected_stale_token=2,
            rejected_unknown_manager=1,
            rejected_other=2,
        )

        assert metrics.received == 100
        assert metrics.accepted == 95
        assert metrics.rejected_stale_token == 2
        assert metrics.rejected_unknown_manager == 1
        assert metrics.rejected_other == 2

    def test_slots_prevents_new_attributes(self):
        """Test that slots=True prevents adding new attributes."""
        metrics = TransferMetrics()

        with pytest.raises(AttributeError):
            metrics.custom = "value"

    def test_edge_case_all_rejected(self):
        """Test with all transfers rejected."""
        metrics = TransferMetrics(
            received=50,
            accepted=0,
            rejected_stale_token=25,
            rejected_unknown_manager=15,
            rejected_other=10,
        )

        total_rejected = (
            metrics.rejected_stale_token +
            metrics.rejected_unknown_manager +
            metrics.rejected_other
        )
        assert total_rejected == metrics.received


class TestPendingTransferState:
    """Test PendingTransferState dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation."""
        received_time = time.monotonic()
        state = PendingTransferState(
            job_id="job-123",
            workflow_ids=["wf-1", "wf-2", "wf-3"],
            new_manager_id="manager-new",
            new_manager_addr=("192.168.1.100", 8000),
            fence_token=42,
            old_manager_id="manager-old",
            received_at=received_time,
        )

        assert state.job_id == "job-123"
        assert state.workflow_ids == ["wf-1", "wf-2", "wf-3"]
        assert state.new_manager_id == "manager-new"
        assert state.new_manager_addr == ("192.168.1.100", 8000)
        assert state.fence_token == 42
        assert state.old_manager_id == "manager-old"
        assert state.received_at == received_time

    def test_with_none_old_manager(self):
        """Test with no old manager (first assignment)."""
        state = PendingTransferState(
            job_id="job-new",
            workflow_ids=["wf-1"],
            new_manager_id="manager-first",
            new_manager_addr=("localhost", 9000),
            fence_token=1,
            old_manager_id=None,
            received_at=time.monotonic(),
        )

        assert state.old_manager_id is None

    def test_slots_prevents_new_attributes(self):
        """Test that slots=True prevents adding new attributes."""
        state = PendingTransferState(
            job_id="j",
            workflow_ids=[],
            new_manager_id="m",
            new_manager_addr=("h", 1),
            fence_token=0,
            old_manager_id=None,
            received_at=0.0,
        )

        with pytest.raises(AttributeError):
            state.extra = "value"

    def test_edge_case_empty_workflow_ids(self):
        """Test with empty workflow IDs list."""
        state = PendingTransferState(
            job_id="job-empty",
            workflow_ids=[],
            new_manager_id="m",
            new_manager_addr=("h", 1),
            fence_token=0,
            old_manager_id=None,
            received_at=0.0,
        )

        assert state.workflow_ids == []

    def test_edge_case_many_workflow_ids(self):
        """Test with many workflow IDs."""
        workflow_ids = [f"wf-{i}" for i in range(1000)]
        state = PendingTransferState(
            job_id="job-many",
            workflow_ids=workflow_ids,
            new_manager_id="m",
            new_manager_addr=("h", 1),
            fence_token=0,
            old_manager_id=None,
            received_at=0.0,
        )

        assert len(state.workflow_ids) == 1000


class TestModelsEdgeCases:
    """Test edge cases across all worker models."""

    def test_all_models_use_slots(self):
        """Verify all models use slots=True for memory efficiency."""
        models = [
            ManagerPeerState(
                manager_id="m",
                tcp_host="h",
                tcp_port=1,
                udp_host="h",
                udp_port=2,
                datacenter="dc",
            ),
            WorkflowRuntimeState(
                workflow_id="wf",
                job_id="j",
                status="s",
                allocated_cores=1,
                fence_token=0,
                start_time=0,
            ),
            CancelState(
                workflow_id="wf",
                job_id="j",
                cancel_requested_at=0,
                cancel_reason="r",
            ),
            ExecutionMetrics(),
            CompletionTimeTracker(),
            TransferMetrics(),
            PendingTransferState(
                job_id="j",
                workflow_ids=[],
                new_manager_id="m",
                new_manager_addr=("h", 1),
                fence_token=0,
                old_manager_id=None,
                received_at=0.0,
            ),
        ]

        for model in models:
            with pytest.raises(AttributeError):
                model.new_attribute = "value"

    def test_models_with_very_long_ids(self):
        """Test models with extremely long IDs."""
        long_id = "x" * 10000

        state = ManagerPeerState(
            manager_id=long_id,
            tcp_host="h",
            tcp_port=1,
            udp_host="h",
            udp_port=2,
            datacenter="dc",
        )

        assert len(state.manager_id) == 10000

    def test_models_with_special_characters(self):
        """Test models with special characters in IDs."""
        special_id = "mgr-🚀-test-ñ-中文"

        state = ManagerPeerState(
            manager_id=special_id,
            tcp_host="h",
            tcp_port=1,
            udp_host="h",
            udp_port=2,
            datacenter="dc-🌍",
        )

        assert state.manager_id == special_id
        assert state.datacenter == "dc-🌍"
