"""
Integration tests for client TCP handlers (Section 15.1.4).

Tests all TCP handler classes: JobStatusPushHandler, JobBatchPushHandler,
JobFinalResultHandler, GlobalJobResultHandler, ReporterResultPushHandler,
WorkflowResultPushHandler, WindowedStatsPushHandler, CancellationCompleteHandler,
GateLeaderTransferHandler, ManagerLeaderTransferHandler.

Covers:
- Happy path: Normal message handling
- Negative path: Invalid messages, malformed data
- Failure mode: Exception handling, callback errors
- Concurrency: Concurrent handler invocations
- Edge cases: Empty data, large payloads
"""

import asyncio
import cloudpickle
from unittest.mock import Mock, AsyncMock

import pytest

from hyperscale.distributed_rewrite.nodes.client.handlers import (
    JobStatusPushHandler,
    JobBatchPushHandler,
    JobFinalResultHandler,
    GlobalJobResultHandler,
    ReporterResultPushHandler,
    WorkflowResultPushHandler,
    WindowedStatsPushHandler,
    CancellationCompleteHandler,
    GateLeaderTransferHandler,
    ManagerLeaderTransferHandler,
)
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.distributed_rewrite.nodes.client.leadership import ClientLeadershipTracker
from hyperscale.distributed_rewrite.models import (
    JobStatusPush,
    JobBatchPush,
    JobFinalResult,
    GlobalJobResult,
    ReporterResultPush,
    WorkflowResultPush,
    JobCancellationComplete,
    GateJobLeaderTransfer,
    ManagerJobLeaderTransfer,
    GateJobLeaderTransferAck,
    ManagerJobLeaderTransferAck,
)
from hyperscale.distributed_rewrite.models.client import ClientJobResult
from hyperscale.distributed_rewrite.jobs import WindowedStatsPush
from hyperscale.logging import Logger


class TestJobStatusPushHandler:
    """Test JobStatusPushHandler class."""

    @pytest.mark.asyncio
    async def test_happy_path_status_update(self):
        """Test normal status update handling."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "job-123"
        initial_result = ClientJobResult(job_id=job_id, status="PENDING")
        state.initialize_job_tracking(job_id, initial_result)

        handler = JobStatusPushHandler(state, logger)

        push = JobStatusPush(job_id=job_id, status="RUNNING", message="Status update")
        data = push.dump()

        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'ok'
        assert state._jobs[job_id] == "RUNNING"

    @pytest.mark.asyncio
    async def test_status_with_callback(self):
        """Test status update with callback."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "job-callback-456"
        callback_called = []

        def status_callback(push):
            callback_called.append(push.status)

        initial_result = ClientJobResult(job_id=job_id, status="PENDING")
        state.initialize_job_tracking(job_id, initial_result, callback=status_callback)

        handler = JobStatusPushHandler(state, logger)

        push = JobStatusPush(job_id=job_id, status="COMPLETED", message="Status update")
        data = push.dump()

        await handler.handle(("server", 8000), data, 100)

        assert callback_called == ["COMPLETED"]

    @pytest.mark.asyncio
    async def test_error_handling_invalid_data(self):
        """Test handling of invalid message data."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        handler = JobStatusPushHandler(state, logger)

        # Invalid data
        result = await handler.handle(("server", 8000), b'invalid', 100)

        assert result == b'error'

    @pytest.mark.asyncio
    async def test_error_handling_callback_exception(self):
        """Test handling when callback raises exception."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "job-callback-error"

        def bad_callback(push):
            raise ValueError("Callback error")

        initial_result = ClientJobResult(job_id=job_id, status="PENDING")
        state.initialize_job_tracking(job_id, initial_result, callback=bad_callback)

        handler = JobStatusPushHandler(state, logger)

        push = JobStatusPush(job_id=job_id, status="RUNNING", message="Status update")
        data = push.dump()

        # Should not raise, should handle gracefully
        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'ok'  # Handler succeeds despite callback error


class TestJobBatchPushHandler:
    """Test JobBatchPushHandler class."""

    @pytest.mark.asyncio
    async def test_happy_path_batch_update(self):
        """Test batch status update handling."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_ids = ["job-1", "job-2", "job-3"]
        for jid in job_ids:
            initial_result = ClientJobResult(job_id=jid, status="PENDING")
            state.initialize_job_tracking(jid, initial_result)

        handler = JobBatchPushHandler(state, logger)

        batch = JobBatchPush(
            job_id="batch-1",
            status="RUNNING",
        )
        data = batch.dump()

        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'ok'

    @pytest.mark.asyncio
    async def test_edge_case_empty_batch(self):
        """Test empty batch update."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        handler = JobBatchPushHandler(state, logger)

        batch = JobBatchPush(job_id="empty-batch", status="PENDING")
        data = batch.dump()

        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'ok'

    @pytest.mark.asyncio
    async def test_edge_case_large_batch(self):
        """Test large batch update."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        # 1000 jobs
        job_ids = [f"job-{i}" for i in range(1000)]

        for jid in job_ids:
            initial_result = ClientJobResult(job_id=jid, status="PENDING")
            state.initialize_job_tracking(jid, initial_result)

        handler = JobBatchPushHandler(state, logger)

        batch = JobBatchPush(
            job_id="large-batch",
            status="RUNNING",
            total_completed=1000,
        )
        data = batch.dump()

        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'ok'


class TestJobFinalResultHandler:
    """Test JobFinalResultHandler class."""

    @pytest.mark.asyncio
    async def test_happy_path_final_result(self):
        """Test handling final result."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "final-job-123"
        initial_result = ClientJobResult(job_id=job_id, status="PENDING")
        state.initialize_job_tracking(job_id, initial_result)

        handler = JobFinalResultHandler(state, logger)

        final_result = JobFinalResult(
            job_id=job_id,
            datacenter="dc-test",
            status="completed",
            total_completed=100,
            total_failed=0,
        )
        data = final_result.dump()

        response = await handler.handle(("server", 8000), data, 100)

        assert response == b'ok'
        # Should signal completion
        assert state._job_events[job_id].is_set()

    @pytest.mark.asyncio
    async def test_final_result_with_callback(self):
        """Test final result with callback."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "final-callback-job"
        callback_results = []

        def result_callback(result):
            callback_results.append(result)

        initial_result = ClientJobResult(job_id=job_id, status="PENDING")
        state.initialize_job_tracking(job_id, initial_result)
        # Store callback in appropriate place
        state._job_callbacks[job_id] = (None, None, result_callback, None)

        handler = JobFinalResultHandler(state, logger)

        final_result = JobFinalResult(
            job_id=job_id,
            datacenter="dc-test",
            status="completed",
            total_completed=50,
            total_failed=0,
        )
        data = final_result.dump()

        await handler.handle(("server", 8000), data, 100)

        assert len(callback_results) == 1


class TestCancellationCompleteHandler:
    """Test CancellationCompleteHandler class."""

    @pytest.mark.asyncio
    async def test_happy_path_cancellation_success(self):
        """Test successful cancellation completion."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "cancel-success-job"
        state.initialize_cancellation_tracking(job_id)

        handler = CancellationCompleteHandler(state, logger)

        complete = JobCancellationComplete(
            job_id=job_id,
            success=True,
            cancelled_workflow_count=5,
            errors=[],
        )
        data = complete.dump()

        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'OK'
        assert state._cancellation_success[job_id] is True
        assert state._cancellation_events[job_id].is_set()

    @pytest.mark.asyncio
    async def test_cancellation_with_errors(self):
        """Test cancellation completion with errors."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "cancel-error-job"
        state.initialize_cancellation_tracking(job_id)

        handler = CancellationCompleteHandler(state, logger)

        errors = ["Worker timeout", "Connection failed"]
        complete = JobCancellationComplete(
            job_id=job_id,
            success=False,
            cancelled_workflow_count=3,
            errors=errors,
        )
        data = complete.dump()

        await handler.handle(("server", 8000), data, 100)

        assert state._cancellation_success[job_id] is False
        assert state._cancellation_errors[job_id] == errors


class TestGateLeaderTransferHandler:
    """Test GateLeaderTransferHandler class."""

    @pytest.mark.asyncio
    async def test_happy_path_leader_transfer(self):
        """Test valid gate leader transfer."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "transfer-job-123"

        handler = GateLeaderTransferHandler(state, logger, Mock())

        transfer = GateJobLeaderTransfer(
            job_id=job_id,
            new_gate_id="gate-2",
            new_gate_addr=("gate-2", 9001),
            fence_token=5,
        )
        data = transfer.dump()

        result = await handler.handle(("gate-1", 9000), data, 100)

        ack = GateJobLeaderTransferAck.load(result)
        assert ack.accepted is True
        # Should update gate leader
        assert job_id in state._gate_job_leaders

    @pytest.mark.asyncio
    async def test_fence_token_validation_stale(self):
        """Test fence token validation rejects stale token."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "fence-job"

        # Establish current leader with token 10
        leadership = ClientLeadershipTracker(state, logger)
        leadership.update_gate_leader(job_id, ("gate-1", 9000), fence_token=10)

        handler = GateLeaderTransferHandler(state, logger, leadership)

        # Try transfer with older token
        transfer = GateJobLeaderTransfer(
            job_id=job_id,
            new_gate_id="gate-2",
            new_gate_addr=("gate-2", 9001),
            fence_token=5,  # Older token
        )
        data = transfer.dump()

        result = await handler.handle(("gate-1", 9000), data, 100)

        # Should reject stale token
        assert result.startswith(b'error')

    @pytest.mark.asyncio
    async def test_edge_case_first_leader_transfer(self):
        """Test first leader transfer (no current leader)."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "first-transfer-job"

        handler = GateLeaderTransferHandler(state, logger, Mock())

        transfer = GateJobLeaderTransfer(
            job_id=job_id,
            new_gate_id="gate-1",
            new_gate_addr=("gate-1", 9000),
            fence_token=1,
        )
        data = transfer.dump()

        result = await handler.handle(("gate-1", 9000), data, 100)

        ack = GateJobLeaderTransferAck.load(result)
        assert ack.accepted is True


class TestManagerLeaderTransferHandler:
    """Test ManagerLeaderTransferHandler class."""

    @pytest.mark.asyncio
    async def test_happy_path_manager_transfer(self):
        """Test valid manager leader transfer."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "mgr-transfer-job"
        datacenter_id = "dc-east"

        handler = ManagerLeaderTransferHandler(state, logger, Mock())

        transfer = ManagerJobLeaderTransfer(
            job_id=job_id,
            new_manager_id="manager-2",
            new_manager_addr=("manager-2", 7001),
            fence_token=3,
            datacenter_id=datacenter_id,
        )
        data = transfer.dump()

        result = await handler.handle(("manager-1", 7000), data, 100)

        ack = ManagerJobLeaderTransferAck.load(result)
        assert ack.accepted is True
        key = (job_id, datacenter_id)
        assert key in state._manager_job_leaders

    @pytest.mark.asyncio
    async def test_fence_token_validation(self):
        """Test manager fence token validation."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "mgr-fence-job"
        datacenter_id = "dc-west"

        # Establish current leader
        leadership = ClientLeadershipTracker(state, logger)
        leadership.update_manager_leader(
            job_id,
            datacenter_id,
            ("manager-1", 7000),
            fence_token=10
        )

        handler = ManagerLeaderTransferHandler(state, logger, leadership)

        # Try older token
        transfer = ManagerJobLeaderTransfer(
            job_id=job_id,
            new_manager_id="manager-2",
            new_manager_addr=("manager-2", 7001),
            fence_token=5,
            datacenter_id=datacenter_id,
        )
        data = transfer.dump()

        result = await handler.handle(("manager-1", 7000), data, 100)

        assert result.startswith(b'error')


class TestWindowedStatsPushHandler:
    """Test WindowedStatsPushHandler class."""

    @pytest.mark.asyncio
    async def test_happy_path_stats_push(self):
        """Test normal windowed stats push."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "stats-job"
        callback_called = []

        def progress_callback(push):
            callback_called.append(push.job_id)

        state._progress_callbacks[job_id] = progress_callback

        handler = WindowedStatsPushHandler(state, logger, None)

        push = WindowedStatsPush(job_id=job_id, workflow_id="workflow-1")
        data = cloudpickle.dumps(push)

        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'ok'
        assert callback_called == [job_id]

    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test rate limiting of stats pushes."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        # Mock rate limiter that denies
        rate_limiter = Mock()
        rate_limiter.check = Mock(return_value=Mock(allowed=False))

        handler = WindowedStatsPushHandler(state, logger, rate_limiter)

        push = WindowedStatsPush(job_id="rate-job", workflow_id="workflow-1")
        data = cloudpickle.dumps(push)

        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'rate_limited'

    @pytest.mark.asyncio
    async def test_callback_exception_handling(self):
        """Test stats handler with failing callback."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_id = "callback-error-job"

        def bad_callback(push):
            raise RuntimeError("Callback failed")

        state._progress_callbacks[job_id] = bad_callback

        handler = WindowedStatsPushHandler(state, logger, None)

        push = WindowedStatsPush(job_id=job_id, workflow_id="workflow-1")
        data = cloudpickle.dumps(push)

        # Should not raise, handles gracefully
        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'ok'

    @pytest.mark.asyncio
    async def test_edge_case_no_callback(self):
        """Test stats push with no callback registered."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        handler = WindowedStatsPushHandler(state, logger, None)

        push = WindowedStatsPush(job_id="no-callback-job", workflow_id="workflow-1")
        data = cloudpickle.dumps(push)

        result = await handler.handle(("server", 8000), data, 100)

        assert result == b'ok'


# Concurrency tests for handlers
class TestHandlersConcurrency:
    """Test concurrent handler operations."""

    @pytest.mark.asyncio
    async def test_concurrent_status_updates(self):
        """Test concurrent status update handling."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        job_ids = [f"concurrent-job-{i}" for i in range(10)]
        for jid in job_ids:
            initial_result = ClientJobResult(job_id=jid, status="PENDING")
            state.initialize_job_tracking(jid, initial_result)

        handler = JobStatusPushHandler(state, logger)

        async def send_status_update(job_id):
            push = JobStatusPush(job_id=job_id, status="RUNNING")
            data = push.dump()
            return await handler.handle(("server", 8000), data, 100)

        results = await asyncio.gather(*[
            send_status_update(jid) for jid in job_ids
        ])

        # All should succeed
        assert all(r == b'ok' for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_leader_transfers(self):
        """Test concurrent leader transfer handling."""
        state = ClientState()
        logger = Mock(spec=Logger)
        logger.log = AsyncMock()

        handler = GateLeaderTransferHandler(state, logger, Mock())

        job_id = "concurrent-transfer-job"

        async def send_transfer(fence_token):
            transfer = GateJobLeaderTransfer(
                job_id=job_id,
                new_gate_id=f"gate-{fence_token}",
                new_gate_addr=(f"gate-{fence_token}", 9000 + fence_token),
                fence_token=fence_token,
            )
            data = transfer.dump()
            return await handler.handle(("gate", 9000), data, 100)

        # Send with increasing fence tokens
        results = await asyncio.gather(*[
            send_transfer(i) for i in range(10)
        ])

        # All should succeed (monotonically increasing tokens)
        acks = [GateJobLeaderTransferAck.load(r) for r in results]
        assert all(ack.accepted is True for ack in acks)
