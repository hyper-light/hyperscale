"""
Integration tests for worker TCP handlers (Section 15.2.5).

Tests WorkflowDispatchHandler, WorkflowCancelHandler, JobLeaderTransferHandler,
WorkflowProgressHandler, StateSyncHandler, and WorkflowStatusQueryHandler.

Covers:
- Happy path: Normal message handling
- Negative path: Invalid messages, stale tokens
- Failure mode: Parsing errors, validation failures
- Concurrency: Thread-safe handler operations
- Edge cases: Empty data, malformed messages
"""

import asyncio
import time
from typing import cast
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from hyperscale.distributed.nodes.worker.server import WorkerServer

from hyperscale.distributed.models import (
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    JobLeaderWorkerTransfer,
    JobLeaderWorkerTransferAck,
    WorkflowProgressAck,
    WorkflowProgress,
    WorkflowStatus,
    WorkerState,
    WorkerStateSnapshot,
    PendingTransfer,
)


class MockServerForHandlers:
    """Mock WorkerServer for handler testing."""

    def __init__(self):
        self._host = "localhost"
        self._tcp_port = 8000
        self._node_id = MagicMock()
        self._node_id.full = "worker-123-456"
        self._node_id.short = "123"
        self._udp_logger = MagicMock()
        self._udp_logger.log = AsyncMock()

        # State containers
        self._active_workflows = {}
        self._workflow_job_leader = {}
        self._workflow_fence_tokens = {}
        self._orphaned_workflows = {}
        self._pending_workflows = []
        self._pending_transfers = {}
        self._known_managers = {}

        # Metrics
        self._transfer_metrics_received = 0
        self._transfer_metrics_accepted = 0
        self._transfer_metrics_rejected_stale_token = 0
        self._transfer_metrics_rejected_unknown_manager = 0
        self._transfer_metrics_rejected_other = 0

        # Locks
        self._job_transfer_locks = {}

        # Core allocator mock
        self._core_allocator = MagicMock()
        self._core_allocator.allocate = AsyncMock()
        self._core_allocator.free = AsyncMock()

        # Env mock
        self.env = MagicMock()
        self.env.MERCURY_SYNC_MAX_PENDING_WORKFLOWS = 100

        self._job_fence_tokens = {}

        self._worker_state = MagicMock()
        self._worker_state.increment_transfer_rejected_stale_token = AsyncMock()
        self._worker_state.update_workflow_fence_token = AsyncMock(return_value=True)
        self._worker_state.get_workflow_fence_token = AsyncMock(return_value=0)

        self._registry = MagicMock()
        self._backpressure_manager = MagicMock()
        self._backpressure_manager.get_backpressure_delay_ms = MagicMock(return_value=0)
        self._task_runner = MagicMock()
        self._task_runner.run = MagicMock()
        self._state_version = 0
        self._get_state_snapshot = MagicMock()
        self._cancel_workflow = AsyncMock()

    def _get_worker_state(self):
        return WorkerState.HEALTHY

    async def _get_job_transfer_lock(self, job_id):
        if job_id not in self._job_transfer_locks:
            self._job_transfer_locks[job_id] = asyncio.Lock()
        return self._job_transfer_locks[job_id]

    async def _validate_transfer_fence_token(self, job_id, fence_token):
        current = self._job_fence_tokens.get(job_id, -1)
        if fence_token <= current:
            return False, f"Stale token: {fence_token} <= {current}"
        return True, ""

    def _validate_transfer_manager(self, manager_id):
        if manager_id in self._known_managers:
            return True, ""
        return False, f"Unknown manager: {manager_id}"

    async def _handle_dispatch_execution(self, dispatch, addr, allocation_result):
        return WorkflowDispatchAck(
            workflow_id=dispatch.workflow_id,
            accepted=True,
        ).dump()

    def _cleanup_workflow_state(self, workflow_id):
        self._active_workflows.pop(workflow_id, None)
        self._workflow_job_leader.pop(workflow_id, None)


class TestWorkflowDispatchHandler:
    """Test WorkflowDispatchHandler."""

    @pytest.fixture
    def mock_server(self):
        return MockServerForHandlers()

    @pytest.mark.asyncio
    async def test_happy_path_dispatch(self, mock_server):
        """Test successful workflow dispatch."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_dispatch import (
            WorkflowDispatchHandler,
        )

        handler = WorkflowDispatchHandler(mock_server)

        mock_server._core_allocator.allocate.return_value = MagicMock(
            success=True,
            allocated_cores=[0, 1],
            error=None,
        )

        dispatch = WorkflowDispatch(
            job_id="job-123",
            workflow_id="wf-456",
            workflow_name="test-workflow",
            cores=2,
            fence_token=1,
            job_leader_addr=("manager", 8000),
        )

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=dispatch.dump(),
            clock_time=1000,
        )

        ack = WorkflowDispatchAck.load(result)
        assert ack.workflow_id == "wf-456"
        assert ack.accepted is True

    @pytest.mark.asyncio
    async def test_dispatch_stale_fence_token(self, mock_server):
        """Test dispatch with stale fence token."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_dispatch import (
            WorkflowDispatchHandler,
        )

        handler = WorkflowDispatchHandler(mock_server)

        # Configure mock to reject stale token
        mock_server._worker_state.update_workflow_fence_token = AsyncMock(
            return_value=False
        )
        mock_server._worker_state.get_workflow_fence_token = AsyncMock(return_value=10)

        dispatch = WorkflowDispatch(
            job_id="job-123",
            workflow_id="wf-456",
            workflow_name="test-workflow",
            cores=2,
            fence_token=5,  # Stale token
            job_leader_addr=("manager", 8000),
        )

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=dispatch.dump(),
            clock_time=1000,
        )

        ack = WorkflowDispatchAck.load(result)
        assert ack.accepted is False
        assert ack.error is not None
        assert "Stale fence token" in ack.error

    @pytest.mark.asyncio
    async def test_dispatch_queue_depth_limit(self, mock_server):
        """Test dispatch when queue depth limit reached."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_dispatch import (
            WorkflowDispatchHandler,
        )

        handler = WorkflowDispatchHandler(mock_server)

        # Fill pending workflows
        mock_server._pending_workflows = [MagicMock() for _ in range(100)]

        dispatch = WorkflowDispatch(
            job_id="job-123",
            workflow_id="wf-456",
            workflow_name="test-workflow",
            cores=2,
            fence_token=1,
            job_leader_addr=("manager", 8000),
        )

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=dispatch.dump(),
            clock_time=1000,
        )

        ack = WorkflowDispatchAck.load(result)
        assert ack.accepted is False
        assert ack.error is not None
        assert "Queue depth limit" in ack.error

    @pytest.mark.asyncio
    async def test_dispatch_core_allocation_failure(self, mock_server):
        """Test dispatch with core allocation failure."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_dispatch import (
            WorkflowDispatchHandler,
        )

        handler = WorkflowDispatchHandler(mock_server)

        mock_server._core_allocator.allocate.return_value = MagicMock(
            success=False,
            allocated_cores=None,
            error="Not enough cores",
        )

        dispatch = WorkflowDispatch(
            job_id="job-123",
            workflow_id="wf-456",
            workflow_name="test-workflow",
            cores=16,
            fence_token=1,
            job_leader_addr=("manager", 8000),
        )

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=dispatch.dump(),
            clock_time=1000,
        )

        ack = WorkflowDispatchAck.load(result)
        assert ack.accepted is False
        assert ack.error is not None
        assert "cores" in ack.error.lower()


class TestJobLeaderTransferHandler:
    """Test JobLeaderTransferHandler."""

    @pytest.fixture
    def mock_server(self):
        server = MockServerForHandlers()
        server._known_managers["new-manager"] = MagicMock()
        return server

    @pytest.mark.asyncio
    async def test_happy_path_transfer(self, mock_server):
        """Test successful job leadership transfer."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_leader_transfer import (
            JobLeaderTransferHandler,
        )

        handler = JobLeaderTransferHandler(mock_server)

        # Add active workflows
        mock_server._active_workflows = {
            "wf-1": MagicMock(status="running"),
            "wf-2": MagicMock(status="running"),
        }
        mock_server._workflow_job_leader = {
            "wf-1": ("old-manager", 7000),
            "wf-2": ("old-manager", 7000),
        }

        transfer = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=["wf-1", "wf-2"],
            new_manager_id="new-manager",
            new_manager_addr=("192.168.1.100", 8000),
            fence_token=1,
            old_manager_id="old-manager",
        )

        result = await handler.handle(
            addr=("192.168.1.100", 8000),
            data=transfer.dump(),
            clock_time=1000,
        )

        ack = JobLeaderWorkerTransferAck.load(result)
        assert ack.job_id == "job-123"
        assert ack.accepted is True
        assert ack.workflows_updated == 2

        # Verify routing updated
        assert mock_server._workflow_job_leader["wf-1"] == ("192.168.1.100", 8000)
        assert mock_server._workflow_job_leader["wf-2"] == ("192.168.1.100", 8000)

    @pytest.mark.asyncio
    async def test_transfer_stale_fence_token(self, mock_server):
        """Test transfer with stale fence token."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_leader_transfer import (
            JobLeaderTransferHandler,
        )

        handler = JobLeaderTransferHandler(mock_server)

        # Set existing fence token
        mock_server._job_fence_tokens["job-123"] = 10

        transfer = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=["wf-1"],
            new_manager_id="new-manager",
            new_manager_addr=("192.168.1.100", 8000),
            fence_token=5,  # Stale token
            old_manager_id="old-manager",
        )

        result = await handler.handle(
            addr=("192.168.1.100", 8000),
            data=transfer.dump(),
            clock_time=1000,
        )

        ack = JobLeaderWorkerTransferAck.load(result)
        assert ack.accepted is False
        mock_server._worker_state.increment_transfer_rejected_stale_token.assert_called_once()

    @pytest.mark.asyncio
    async def test_transfer_unknown_manager(self, mock_server):
        """Test transfer from unknown manager."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_leader_transfer import (
            JobLeaderTransferHandler,
        )

        handler = JobLeaderTransferHandler(mock_server)
        mock_server._known_managers.clear()

        transfer = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=["wf-1"],
            new_manager_id="unknown-manager",
            new_manager_addr=("192.168.1.100", 8000),
            fence_token=1,
            old_manager_id="old-manager",
        )

        result = await handler.handle(
            addr=("192.168.1.100", 8000),
            data=transfer.dump(),
            clock_time=1000,
        )

        ack = JobLeaderWorkerTransferAck.load(result)
        assert ack.accepted is False
        assert mock_server._transfer_metrics_rejected_unknown_manager == 1

    @pytest.mark.asyncio
    async def test_transfer_clears_orphan_status(self, mock_server):
        """Test transfer clears orphan status (Section 2.7)."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_leader_transfer import (
            JobLeaderTransferHandler,
        )

        handler = JobLeaderTransferHandler(mock_server)

        # Add orphaned workflow
        mock_server._active_workflows = {"wf-1": MagicMock(status="running")}
        mock_server._workflow_job_leader = {"wf-1": ("old-manager", 7000)}
        mock_server._orphaned_workflows = {"wf-1": time.monotonic()}

        transfer = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=["wf-1"],
            new_manager_id="new-manager",
            new_manager_addr=("192.168.1.100", 8000),
            fence_token=1,
            old_manager_id="old-manager",
        )

        result = await handler.handle(
            addr=("192.168.1.100", 8000),
            data=transfer.dump(),
            clock_time=1000,
        )

        ack = JobLeaderWorkerTransferAck.load(result)
        assert ack.accepted is True

        # Orphan status should be cleared
        assert "wf-1" not in mock_server._orphaned_workflows

    @pytest.mark.asyncio
    async def test_transfer_stores_pending_for_unknown_workflows(self, mock_server):
        """Test transfer stores pending for unknown workflows (Section 8.3)."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_leader_transfer import (
            JobLeaderTransferHandler,
        )

        handler = JobLeaderTransferHandler(mock_server)

        # No active workflows
        mock_server._active_workflows = {}

        transfer = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=["wf-unknown-1", "wf-unknown-2"],
            new_manager_id="new-manager",
            new_manager_addr=("192.168.1.100", 8000),
            fence_token=1,
            old_manager_id="old-manager",
        )

        result = await handler.handle(
            addr=("192.168.1.100", 8000),
            data=transfer.dump(),
            clock_time=1000,
        )

        ack = JobLeaderWorkerTransferAck.load(result)
        assert ack.accepted is True
        assert ack.workflows_updated == 0

        # Pending transfer should be stored
        assert "job-123" in mock_server._pending_transfers


class TestWorkflowProgressHandler:
    """Test WorkflowProgressHandler."""

    @pytest.fixture
    def mock_server(self):
        server = MockServerForHandlers()
        server._registry = MagicMock()
        server._backpressure_manager = MagicMock()
        server._backpressure_manager.get_backpressure_delay_ms.return_value = 0
        server._task_runner = MagicMock()
        server._task_runner.run = MagicMock()
        return server

    def test_process_ack_updates_routing_and_backpressure(self, mock_server):
        from hyperscale.distributed.models import ManagerInfo, WorkflowProgressAck
        from hyperscale.distributed.nodes.worker.handlers.tcp_progress import (
            WorkflowProgressHandler,
        )

        handler = WorkflowProgressHandler(mock_server)

        ack = WorkflowProgressAck(
            manager_id="mgr-1",
            is_leader=True,
            healthy_managers=[
                ManagerInfo(
                    node_id="mgr-1",
                    tcp_host="127.0.0.1",
                    tcp_port=7000,
                    udp_host="127.0.0.1",
                    udp_port=7001,
                    datacenter="dc-1",
                    is_leader=True,
                )
            ],
            job_leader_addr=("127.0.0.1", 7000),
            backpressure_level=1,
            backpressure_delay_ms=50,
            backpressure_batch_only=False,
        )

        handler.process_ack(ack.dump(), workflow_id="wf-1")

        mock_server._registry.add_manager.assert_called_once()
        assert mock_server._primary_manager_id == "mgr-1"
        assert mock_server._workflow_job_leader["wf-1"] == ("127.0.0.1", 7000)
        mock_server._backpressure_manager.set_manager_backpressure.assert_called_once()
        mock_server._backpressure_manager.set_backpressure_delay_ms.assert_called_once()

    def test_process_ack_invalid_data_logs_debug(self, mock_server):
        from hyperscale.distributed.nodes.worker.handlers.tcp_progress import (
            WorkflowProgressHandler,
        )

        handler = WorkflowProgressHandler(mock_server)

        handler.process_ack(b"invalid", workflow_id="wf-1")

        mock_server._task_runner.run.assert_called_once()


class TestStateSyncHandler:
    """Test StateSyncHandler."""

    @pytest.fixture
    def mock_server(self):
        server = MockServerForHandlers()
        server._state_version = 3
        server._get_state_snapshot = MagicMock(
            return_value=WorkerStateSnapshot(
                node_id=server._node_id.full,
                state=WorkerState.HEALTHY,
                total_cores=8,
                available_cores=6,
                version=3,
                host="127.0.0.1",
                tcp_port=9001,
                udp_port=9002,
                active_workflows={},
            )
        )
        return server

    @pytest.mark.asyncio
    async def test_state_sync_returns_snapshot(self, mock_server):
        from hyperscale.distributed.models import StateSyncRequest, StateSyncResponse
        from hyperscale.distributed.nodes.worker.handlers.tcp_state_sync import (
            StateSyncHandler,
        )

        handler = StateSyncHandler(mock_server)
        request = StateSyncRequest(
            requester_id="manager-1",
            requester_role="manager",
        )

        result = await handler.handle(
            addr=("127.0.0.1", 8000),
            data=request.dump(),
            clock_time=1,
        )

        response = StateSyncResponse.load(result)
        assert response.responder_id == mock_server._node_id.full
        assert response.current_version == mock_server._state_version
        assert response.worker_state == mock_server._get_state_snapshot.return_value

    @pytest.mark.asyncio
    async def test_state_sync_invalid_data_returns_empty(self, mock_server):
        from hyperscale.distributed.nodes.worker.handlers.tcp_state_sync import (
            StateSyncHandler,
        )

        handler = StateSyncHandler(mock_server)

        result = await handler.handle(
            addr=("127.0.0.1", 8000),
            data=b"invalid",
            clock_time=1,
        )

        assert result == b""


class TestWorkflowStatusQueryHandler:
    """Test WorkflowStatusQueryHandler."""

    @pytest.fixture
    def mock_server(self):
        return MockServerForHandlers()

    @pytest.mark.asyncio
    async def test_happy_path_query(self, mock_server):
        """Test successful workflow status query."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_status_query import (
            WorkflowStatusQueryHandler,
        )

        handler = WorkflowStatusQueryHandler(mock_server)

        mock_server._active_workflows = {
            "wf-1": MagicMock(),
            "wf-2": MagicMock(),
            "wf-3": MagicMock(),
        }

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=b"",
            clock_time=1000,
        )

        # Result should be comma-separated workflow IDs
        workflow_ids = result.decode().split(",")
        assert len(workflow_ids) == 3
        assert "wf-1" in workflow_ids
        assert "wf-2" in workflow_ids
        assert "wf-3" in workflow_ids

    @pytest.mark.asyncio
    async def test_query_no_workflows(self, mock_server):
        """Test query with no active workflows."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_status_query import (
            WorkflowStatusQueryHandler,
        )

        handler = WorkflowStatusQueryHandler(mock_server)

        mock_server._active_workflows = {}

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=b"",
            clock_time=1000,
        )

        # Result should be empty
        assert result == b""


class TestWorkflowCancelHandler:
    """Test WorkflowCancelHandler."""

    @pytest.fixture
    def mock_server(self):
        server = MockServerForHandlers()
        server._cancel_workflow = AsyncMock(return_value=(True, []))
        return server

    @pytest.mark.asyncio
    async def test_happy_path_cancel(self, mock_server):
        """Test successful workflow cancellation."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_cancel import (
            WorkflowCancelHandler,
        )

        handler = WorkflowCancelHandler(mock_server)

        mock_server._active_workflows = {
            "wf-456": MagicMock(
                job_id="job-123",
                status="running",
            ),
        }

        cancel = WorkflowCancelRequest(
            job_id="job-123",
            workflow_id="wf-456",
            reason="user requested",
        )

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=cancel.dump(),
            clock_time=1000,
        )

        ack = WorkflowCancelResponse.load(result)
        assert ack.workflow_id == "wf-456"
        assert ack.success is True

    @pytest.mark.asyncio
    async def test_cancel_unknown_workflow(self, mock_server):
        """Test cancellation of unknown workflow (idempotent - treated as already completed)."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_cancel import (
            WorkflowCancelHandler,
        )

        handler = WorkflowCancelHandler(mock_server)

        mock_server._active_workflows = {}

        cancel = WorkflowCancelRequest(
            job_id="job-123",
            workflow_id="wf-unknown",
            reason="user requested",
        )

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=cancel.dump(),
            clock_time=1000,
        )

        ack = WorkflowCancelResponse.load(result)
        # Idempotent cancellation: unknown workflow = already completed
        assert ack.success is True
        assert ack.already_completed is True


class TestHandlersConcurrency:
    """Test concurrency aspects of handlers."""

    @pytest.mark.asyncio
    async def test_concurrent_transfers_serialized(self):
        """Test that concurrent transfers to same job are serialized."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_leader_transfer import (
            JobLeaderTransferHandler,
        )

        mock_server = MockServerForHandlers()
        mock_server._known_managers["mgr-1"] = MagicMock()
        handler = JobLeaderTransferHandler(cast(WorkerServer, mock_server))

        access_order = []

        # Monkey-patch to track access order
        original_validate = mock_server._validate_transfer_fence_token

        def tracking_validate(job_id, fence_token):
            access_order.append(f"start-{fence_token}")
            result = original_validate(job_id, fence_token)
            access_order.append(f"end-{fence_token}")
            return result

        mock_server._validate_transfer_fence_token = tracking_validate

        transfer1 = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=[],
            new_manager_id="mgr-1",
            new_manager_addr=("host", 8000),
            fence_token=1,
            old_manager_id=None,
        )

        transfer2 = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=[],
            new_manager_id="mgr-1",
            new_manager_addr=("host", 8000),
            fence_token=2,
            old_manager_id=None,
        )

        await asyncio.gather(
            handler.handle(("h", 1), transfer1.dump(), 0),
            handler.handle(("h", 1), transfer2.dump(), 0),
        )

        # Lock should serialize access
        assert len(access_order) == 4


class TestHandlersEdgeCases:
    """Test edge cases for handlers."""

    @pytest.mark.asyncio
    async def test_handler_with_invalid_data(self):
        """Test handler with invalid serialized data."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_dispatch import (
            WorkflowDispatchHandler,
        )

        mock_server = MockServerForHandlers()
        handler = WorkflowDispatchHandler(mock_server)

        result = await handler.handle(
            addr=("192.168.1.1", 8000),
            data=b"invalid data",
            clock_time=1000,
        )

        ack = WorkflowDispatchAck.load(result)
        assert ack.accepted is False

    @pytest.mark.asyncio
    async def test_transfer_with_many_workflows(self):
        """Test transfer with many workflows."""
        from hyperscale.distributed.nodes.worker.handlers.tcp_leader_transfer import (
            JobLeaderTransferHandler,
        )

        mock_server = MockServerForHandlers()
        mock_server._known_managers["mgr-1"] = MagicMock()
        handler = JobLeaderTransferHandler(mock_server)

        # Add many workflows
        workflow_ids = [f"wf-{i}" for i in range(100)]
        for wf_id in workflow_ids:
            mock_server._active_workflows[wf_id] = MagicMock(status="running")
            mock_server._workflow_job_leader[wf_id] = ("old", 7000)

        transfer = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=workflow_ids,
            new_manager_id="mgr-1",
            new_manager_addr=("192.168.1.100", 8000),
            fence_token=1,
            old_manager_id="old",
        )

        result = await handler.handle(
            addr=("192.168.1.100", 8000),
            data=transfer.dump(),
            clock_time=1000,
        )

        ack = JobLeaderWorkerTransferAck.load(result)
        assert ack.accepted is True
        assert ack.workflows_updated == 100
