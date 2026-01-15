"""
Unit tests for Manager TCP Handlers from Section 15.4.5 of REFACTOR.md.

Tests cover:
- CancelJobHandler
- JobCancelRequestHandler
- WorkflowCancellationCompleteHandler

Each test class validates:
- Happy path (normal operations)
- Negative path (invalid inputs, error conditions)
- Failure modes (exception handling)
- Concurrency and race conditions
- Edge cases (boundary conditions, special values)
"""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock

from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.nodes.manager.config import ManagerConfig
from hyperscale.distributed.nodes.manager.handlers.tcp_cancellation import (
    CancelJobHandler,
    JobCancelRequestHandler,
    WorkflowCancellationCompleteHandler,
)
from hyperscale.distributed.models import (
    CancelJob,
    JobCancelRequest,
    JobCancelResponse,
    WorkflowCancellationComplete,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def manager_state():
    """Create a fresh ManagerState for testing."""
    state = ManagerState()
    state.initialize_locks()
    return state


@pytest.fixture
def manager_config():
    """Create a ManagerConfig for testing."""
    return ManagerConfig(
        host="127.0.0.1",
        tcp_port=8000,
        udp_port=8001,
        datacenter_id="dc-test",
    )


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


@pytest.fixture
def mock_task_runner():
    """Create a mock task runner."""
    runner = MagicMock()
    runner.run = MagicMock()
    return runner


# =============================================================================
# CancelJobHandler Tests
# =============================================================================


class TestCancelJobHandlerHappyPath:
    """Happy path tests for CancelJobHandler."""

    @pytest.mark.asyncio
    async def test_handle_legacy_cancel_request(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Can handle legacy CancelJob request."""
        cancel_impl = AsyncMock(return_value=b'{"job_id": "job-123", "accepted": true}')

        handler = CancelJobHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=cancel_impl,
        )

        # Create legacy cancel request
        request = CancelJob(job_id="job-123")
        data = request.dump()

        result = await handler.handle(
            addr=("10.0.0.1", 9000),
            data=data,
            clock_time=1,
        )

        # Should have called the implementation
        cancel_impl.assert_called_once()
        # The call should have been with a JobCancelRequest
        call_args = cancel_impl.call_args
        assert call_args[0][0].job_id == "job-123"

    @pytest.mark.asyncio
    async def test_handle_normalizes_to_ad20_format(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Legacy format is normalized to AD-20 JobCancelRequest."""
        captured_request = None

        async def capture_request(request, addr):
            nonlocal captured_request
            captured_request = request
            return b'{"job_id": "job-123", "accepted": true}'

        handler = CancelJobHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=capture_request,
        )

        request = CancelJob(job_id="job-456")
        await handler.handle(("10.0.0.1", 9000), request.dump(), 1)

        assert captured_request is not None
        assert captured_request.job_id == "job-456"
        assert captured_request.requester_id == "manager-1"


class TestCancelJobHandlerNegativePath:
    """Negative path tests for CancelJobHandler."""

    @pytest.mark.asyncio
    async def test_handle_invalid_data(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Invalid data returns error response."""
        cancel_impl = AsyncMock()

        handler = CancelJobHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=cancel_impl,
        )

        result = await handler.handle(
            addr=("10.0.0.1", 9000),
            data=b"invalid data",
            clock_time=1,
        )

        # Should return error response
        response = JobCancelResponse.load(result)
        assert response.success is False
        assert response.error is not None


class TestCancelJobHandlerEdgeCases:
    """Edge case tests for CancelJobHandler."""

    @pytest.mark.asyncio
    async def test_handle_empty_job_id(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Empty job_id is passed through."""
        captured_request = None

        async def capture_request(request, addr):
            nonlocal captured_request
            captured_request = request
            return b'{"job_id": "", "accepted": true}'

        handler = CancelJobHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=capture_request,
        )

        request = CancelJob(job_id="")
        await handler.handle(("10.0.0.1", 9000), request.dump(), 1)

        assert captured_request.job_id == ""

    @pytest.mark.asyncio
    async def test_implementation_exception_handled(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Exception in implementation returns error."""
        async def failing_impl(request, addr):
            raise RuntimeError("Implementation failed")

        handler = CancelJobHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=failing_impl,
        )

        request = CancelJob(job_id="job-123")
        result = await handler.handle(("10.0.0.1", 9000), request.dump(), 1)

        response = JobCancelResponse.load(result)
        assert response.success is False


# =============================================================================
# JobCancelRequestHandler Tests
# =============================================================================


class TestJobCancelRequestHandlerHappyPath:
    """Happy path tests for JobCancelRequestHandler."""

    @pytest.mark.asyncio
    async def test_handle_ad20_cancel_request(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Can handle AD-20 JobCancelRequest."""
        cancel_impl = AsyncMock(return_value=b'{"job_id": "job-123", "accepted": true}')

        handler = JobCancelRequestHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=cancel_impl,
        )

        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-456",
            timestamp=0.0,
            reason="User requested cancellation",
        )

        result = await handler.handle(
            addr=("10.0.0.1", 9000),
            data=request.dump(),
            clock_time=1,
        )

        cancel_impl.assert_called_once()
        call_args = cancel_impl.call_args
        assert call_args[0][0].job_id == "job-123"
        assert call_args[0][0].requester_id == "client-456"
        assert call_args[0][0].reason == "User requested cancellation"

    @pytest.mark.asyncio
    async def test_handle_preserves_request_fields(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """All request fields are preserved."""
        captured_request = None

        async def capture_request(request, addr):
            nonlocal captured_request
            captured_request = request
            return b'{"job_id": "job-123", "accepted": true}'

        handler = JobCancelRequestHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=capture_request,
        )

        request = JobCancelRequest(
            job_id="job-789",
            requester_id="gate-abc",
            timestamp=0.0,
            reason="Timeout exceeded",
        )
        await handler.handle(("10.0.0.1", 9000), request.dump(), 1)

        assert captured_request.job_id == "job-789"
        assert captured_request.requester_id == "gate-abc"
        assert captured_request.reason == "Timeout exceeded"


class TestJobCancelRequestHandlerNegativePath:
    """Negative path tests for JobCancelRequestHandler."""

    @pytest.mark.asyncio
    async def test_handle_invalid_data(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Invalid data returns error response."""
        cancel_impl = AsyncMock()

        handler = JobCancelRequestHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=cancel_impl,
        )

        result = await handler.handle(
            addr=("10.0.0.1", 9000),
            data=b"not valid msgpack",
            clock_time=1,
        )

        response = JobCancelResponse.load(result)
        assert response.success is False
        assert response.job_id == "unknown"

    @pytest.mark.asyncio
    async def test_handle_implementation_error(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Implementation error returns error response."""
        async def failing_impl(request, addr):
            raise ValueError("Bad request")

        handler = JobCancelRequestHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=failing_impl,
        )

        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-456",
            timestamp=0.0,
            reason="Test",
        )

        result = await handler.handle(("10.0.0.1", 9000), request.dump(), 1)

        response = JobCancelResponse.load(result)
        assert response.success is False
        assert "Bad request" in response.error


# =============================================================================
# WorkflowCancellationCompleteHandler Tests
# =============================================================================


class TestWorkflowCancellationCompleteHandlerHappyPath:
    """Happy path tests for WorkflowCancellationCompleteHandler."""

    @pytest.mark.asyncio
    async def test_handle_completion_notification(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Can handle workflow cancellation completion."""
        handle_impl = AsyncMock()

        handler = WorkflowCancellationCompleteHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            handle_workflow_cancelled=handle_impl,
        )

        notification = WorkflowCancellationComplete(
            job_id="job-456",
            workflow_id="wf-123",
            success=True,
        )

        result = await handler.handle(
            addr=("10.0.0.50", 6000),
            data=notification.dump(),
            clock_time=1,
        )

        assert result == b'ok'
        handle_impl.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_passes_notification_to_impl(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Notification is passed to implementation."""
        captured_notification = None

        async def capture_notification(notification):
            nonlocal captured_notification
            captured_notification = notification

        handler = WorkflowCancellationCompleteHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            handle_workflow_cancelled=capture_notification,
        )

        notification = WorkflowCancellationComplete(
            job_id="job-abc",
            workflow_id="wf-789",
            success=False,
            errors=["Worker timeout"],
        )

        await handler.handle(("10.0.0.50", 6000), notification.dump(), 1)

        assert captured_notification.workflow_id == "wf-789"
        assert captured_notification.job_id == "job-abc"
        assert captured_notification.success is False
        assert captured_notification.errors[0] == "Worker timeout"


class TestWorkflowCancellationCompleteHandlerNegativePath:
    """Negative path tests for WorkflowCancellationCompleteHandler."""

    @pytest.mark.asyncio
    async def test_handle_invalid_data(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Invalid data returns error."""
        handle_impl = AsyncMock()

        handler = WorkflowCancellationCompleteHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            handle_workflow_cancelled=handle_impl,
        )

        result = await handler.handle(
            addr=("10.0.0.50", 6000),
            data=b"invalid data",
            clock_time=1,
        )

        assert result == b'error'
        handle_impl.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_implementation_error(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Implementation error returns error."""
        async def failing_impl(notification):
            raise RuntimeError("Processing failed")

        handler = WorkflowCancellationCompleteHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            handle_workflow_cancelled=failing_impl,
        )

        notification = WorkflowCancellationComplete(
            job_id="job-456",
            workflow_id="wf-123",
            success=True,
        )

        result = await handler.handle(("10.0.0.50", 6000), notification.dump(), 1)

        assert result == b'error'


class TestWorkflowCancellationCompleteHandlerEdgeCases:
    """Edge case tests for WorkflowCancellationCompleteHandler."""

    @pytest.mark.asyncio
    async def test_handle_with_long_error_message(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Long error messages are handled."""
        captured_notification = None

        async def capture_notification(notification):
            nonlocal captured_notification
            captured_notification = notification

        handler = WorkflowCancellationCompleteHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            handle_workflow_cancelled=capture_notification,
        )

        long_error = "Error: " + "x" * 10000

        notification = WorkflowCancellationComplete(
            job_id="job-456",
            workflow_id="wf-123",
            success=False,
            errors=[long_error],
        )

        result = await handler.handle(("10.0.0.50", 6000), notification.dump(), 1)

        assert result == b'ok'
        assert captured_notification.errors[0] == long_error


# =============================================================================
# Handler Concurrency Tests
# =============================================================================


class TestHandlersConcurrency:
    """Concurrency tests for handlers."""

    @pytest.mark.asyncio
    async def test_concurrent_cancel_requests(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Multiple concurrent cancel requests are handled."""
        call_count = 0
        call_lock = asyncio.Lock()

        async def counting_impl(request, addr):
            nonlocal call_count
            async with call_lock:
                call_count += 1
            await asyncio.sleep(0.01)  # Simulate processing
            return JobCancelResponse(
                job_id=request.job_id,
                success=True,
            ).dump()

        handler = JobCancelRequestHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=counting_impl,
        )

        # Create multiple concurrent requests
        requests = [
            JobCancelRequest(
                job_id=f"job-{i}",
                requester_id=f"client-{i}",
                timestamp=0.0,
                reason="Concurrent test",
            )
            for i in range(10)
        ]

        tasks = [
            handler.handle(("10.0.0.1", 9000), req.dump(), i)
            for i, req in enumerate(requests)
        ]

        results = await asyncio.gather(*tasks)

        assert call_count == 10
        assert all(r is not None for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_completion_notifications(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Multiple concurrent completion notifications are handled."""
        handled_ids = []
        handle_lock = asyncio.Lock()

        async def tracking_impl(notification):
            async with handle_lock:
                handled_ids.append(notification.workflow_id)
            await asyncio.sleep(0.01)

        handler = WorkflowCancellationCompleteHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            handle_workflow_cancelled=tracking_impl,
        )

        notifications = [
            WorkflowCancellationComplete(
                job_id="job-concurrent",
                workflow_id=f"wf-{i}",
                success=True,
            )
            for i in range(20)
        ]

        tasks = [
            handler.handle(("10.0.0.50", 6000), notif.dump(), i)
            for i, notif in enumerate(notifications)
        ]

        results = await asyncio.gather(*tasks)

        assert len(handled_ids) == 20
        assert all(r == b'ok' for r in results)


# =============================================================================
# Handler Integration Tests
# =============================================================================


class TestHandlerIntegration:
    """Integration tests for handlers working together."""

    @pytest.mark.asyncio
    async def test_cancel_and_completion_flow(self, manager_state, manager_config, mock_logger, mock_task_runner):
        """Cancel request followed by completion notifications."""
        completion_event = asyncio.Event()
        pending_workflows = {"wf-1", "wf-2", "wf-3"}

        async def cancel_impl(request, addr):
            # Simulate initiating cancellation
            return JobCancelResponse(
                job_id=request.job_id,
                success=True,
                cancelled_workflow_count=len(pending_workflows),
            ).dump()

        async def completion_impl(notification):
            pending_workflows.discard(notification.workflow_id)
            if not pending_workflows:
                completion_event.set()

        cancel_handler = JobCancelRequestHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            cancel_job_impl=cancel_impl,
        )

        completion_handler = WorkflowCancellationCompleteHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            handle_workflow_cancelled=completion_impl,
        )

        # Send cancel request
        cancel_request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-1",
            timestamp=0.0,
            reason="Test flow",
        )
        cancel_result = await cancel_handler.handle(
            ("10.0.0.1", 9000),
            cancel_request.dump(),
            1,
        )

        response = JobCancelResponse.load(cancel_result)
        assert response.success is True

        # Send completion notifications
        for wf_id in ["wf-1", "wf-2", "wf-3"]:
            notification = WorkflowCancellationComplete(
                job_id="job-123",
                workflow_id=wf_id,
                success=True,
            )
            await completion_handler.handle(
                ("10.0.0.50", 6000),
                notification.dump(),
                1,
            )

        # All workflows should be complete
        assert completion_event.is_set()
        assert len(pending_workflows) == 0
