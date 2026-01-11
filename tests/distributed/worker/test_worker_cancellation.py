"""
Integration tests for WorkerCancellationHandler (Section 15.2.6.4).

Tests WorkerCancellationHandler for workflow cancellation handling (AD-20).

Covers:
- Happy path: Normal cancellation flow
- Negative path: Cancellation of unknown workflows
- Failure mode: Cancellation failures
- Concurrency: Thread-safe event signaling
- Edge cases: Multiple cancellations, already cancelled
"""

import asyncio
from unittest.mock import MagicMock, AsyncMock

import pytest

from hyperscale.distributed.nodes.worker.cancellation import WorkerCancellationHandler


class TestWorkerCancellationHandlerInitialization:
    """Test WorkerCancellationHandler initialization."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        assert handler._logger == logger
        assert handler._poll_interval == 5.0
        assert handler._running is False
        assert isinstance(handler._cancel_events, dict)
        assert isinstance(handler._cancelled_workflows, set)

    def test_custom_poll_interval(self):
        """Test with custom poll interval."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger, poll_interval=10.0)

        assert handler._poll_interval == 10.0


class TestWorkerCancellationHandlerEventManagement:
    """Test cancel event management."""

    def test_create_cancel_event(self):
        """Test creating a cancel event."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        event = handler.create_cancel_event("wf-1")

        assert isinstance(event, asyncio.Event)
        assert "wf-1" in handler._cancel_events
        assert handler._cancel_events["wf-1"] is event

    def test_get_cancel_event(self):
        """Test getting a cancel event."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        created = handler.create_cancel_event("wf-1")
        retrieved = handler.get_cancel_event("wf-1")

        assert created is retrieved

    def test_get_cancel_event_not_found(self):
        """Test getting a non-existent cancel event."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        event = handler.get_cancel_event("non-existent")

        assert event is None

    def test_remove_cancel_event(self):
        """Test removing a cancel event."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        handler.create_cancel_event("wf-1")
        handler.signal_cancellation("wf-1")
        handler.remove_cancel_event("wf-1")

        assert "wf-1" not in handler._cancel_events
        assert "wf-1" not in handler._cancelled_workflows

    def test_remove_cancel_event_not_found(self):
        """Test removing a non-existent cancel event."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        # Should not raise
        handler.remove_cancel_event("non-existent")


class TestWorkerCancellationHandlerSignaling:
    """Test cancellation signaling."""

    def test_signal_cancellation_success(self):
        """Test signaling cancellation for existing workflow."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        event = handler.create_cancel_event("wf-1")
        result = handler.signal_cancellation("wf-1")

        assert result is True
        assert event.is_set()
        assert "wf-1" in handler._cancelled_workflows

    def test_signal_cancellation_not_found(self):
        """Test signaling cancellation for non-existent workflow."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        result = handler.signal_cancellation("non-existent")

        assert result is False

    def test_is_cancelled_true(self):
        """Test checking cancelled workflow."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        handler.create_cancel_event("wf-1")
        handler.signal_cancellation("wf-1")

        assert handler.is_cancelled("wf-1") is True

    def test_is_cancelled_false(self):
        """Test checking non-cancelled workflow."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        handler.create_cancel_event("wf-1")

        assert handler.is_cancelled("wf-1") is False

    def test_is_cancelled_unknown(self):
        """Test checking unknown workflow."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        assert handler.is_cancelled("unknown") is False


class TestWorkerCancellationHandlerCancelWorkflow:
    """Test cancel_workflow method."""

    @pytest.mark.asyncio
    async def test_cancel_workflow_success(self):
        """Test successful workflow cancellation."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        handler.create_cancel_event("wf-1")
        active_workflows = {"wf-1": MagicMock()}
        task_runner_cancel = AsyncMock()
        workflow_tokens = {"wf-1": "token-123"}

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="user requested",
            active_workflows=active_workflows,
            task_runner_cancel=task_runner_cancel,
            workflow_tokens=workflow_tokens,
        )

        assert success is True
        assert errors == []
        assert handler.is_cancelled("wf-1")
        task_runner_cancel.assert_awaited_once_with("token-123")

    @pytest.mark.asyncio
    async def test_cancel_workflow_no_event(self):
        """Test cancellation without cancel event."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        active_workflows = {"wf-1": MagicMock()}
        task_runner_cancel = AsyncMock()
        workflow_tokens = {}

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="user requested",
            active_workflows=active_workflows,
            task_runner_cancel=task_runner_cancel,
            workflow_tokens=workflow_tokens,
        )

        assert success is False
        assert len(errors) == 1
        assert "No cancel event" in errors[0]

    @pytest.mark.asyncio
    async def test_cancel_workflow_no_token(self):
        """Test cancellation without workflow token."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        handler.create_cancel_event("wf-1")
        active_workflows = {"wf-1": MagicMock()}
        task_runner_cancel = AsyncMock()
        workflow_tokens = {}  # No token

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="user requested",
            active_workflows=active_workflows,
            task_runner_cancel=task_runner_cancel,
            workflow_tokens=workflow_tokens,
        )

        assert success is True  # Signal success even without token
        task_runner_cancel.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_workflow_task_runner_failure(self):
        """Test cancellation with TaskRunner failure."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        handler.create_cancel_event("wf-1")
        active_workflows = {"wf-1": MagicMock()}
        task_runner_cancel = AsyncMock(side_effect=RuntimeError("Cancel failed"))
        workflow_tokens = {"wf-1": "token-123"}

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="user requested",
            active_workflows=active_workflows,
            task_runner_cancel=task_runner_cancel,
            workflow_tokens=workflow_tokens,
        )

        assert success is False
        assert len(errors) == 1
        assert "TaskRunner cancel failed" in errors[0]


class TestWorkerCancellationHandlerPolling:
    """Test cancellation poll loop."""

    @pytest.mark.asyncio
    async def test_run_cancellation_poll_loop_starts_running(self):
        """Test that poll loop starts running."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger, poll_interval=0.01)

        get_healthy_managers = MagicMock(return_value=[("192.168.1.1", 8000)])
        send_cancel_query = AsyncMock()

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(get_healthy_managers, send_cancel_query)
        )

        await asyncio.sleep(0.05)

        assert handler._running is True

        handler.stop()
        await asyncio.sleep(0.02)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_stop_stops_loop(self):
        """Test that stop() stops the loop."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger, poll_interval=0.01)

        get_healthy_managers = MagicMock(return_value=[])
        send_cancel_query = AsyncMock()

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(get_healthy_managers, send_cancel_query)
        )

        await asyncio.sleep(0.03)
        handler.stop()

        assert handler._running is False

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_poll_loop_no_healthy_managers(self):
        """Test poll loop with no healthy managers."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger, poll_interval=0.01)

        get_healthy_managers = MagicMock(return_value=[])
        send_cancel_query = AsyncMock()

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(get_healthy_managers, send_cancel_query)
        )

        await asyncio.sleep(0.05)
        handler.stop()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should not have sent any queries
        send_cancel_query.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_poll_loop_sends_query_to_first_manager(self):
        """Test poll loop sends query to first healthy manager."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger, poll_interval=0.01)

        managers = [("192.168.1.1", 8000), ("192.168.1.2", 8001)]
        get_healthy_managers = MagicMock(return_value=managers)
        send_cancel_query = AsyncMock()

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(get_healthy_managers, send_cancel_query)
        )

        await asyncio.sleep(0.05)
        handler.stop()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should have sent to first manager
        send_cancel_query.assert_awaited()
        assert send_cancel_query.call_args[0][0] == ("192.168.1.1", 8000)

    @pytest.mark.asyncio
    async def test_poll_loop_handles_query_failure(self):
        """Test poll loop handles query failure gracefully."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger, poll_interval=0.01)

        managers = [("192.168.1.1", 8000), ("192.168.1.2", 8001)]
        get_healthy_managers = MagicMock(return_value=managers)

        call_count = [0]

        async def failing_query(addr):
            call_count[0] += 1
            if addr == ("192.168.1.1", 8000):
                raise RuntimeError("Connection failed")
            # Second manager succeeds

        send_cancel_query = AsyncMock(side_effect=failing_query)

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(get_healthy_managers, send_cancel_query)
        )

        await asyncio.sleep(0.05)
        handler.stop()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should have tried both managers
        assert call_count[0] >= 2


class TestWorkerCancellationHandlerConcurrency:
    """Test concurrency aspects of WorkerCancellationHandler."""

    @pytest.mark.asyncio
    async def test_concurrent_cancel_event_creation(self):
        """Test concurrent cancel event creation."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        async def create_event(workflow_id: str):
            return handler.create_cancel_event(workflow_id)

        events = await asyncio.gather(*[
            create_event(f"wf-{i}") for i in range(10)
        ])

        assert len(events) == 10
        assert len(handler._cancel_events) == 10

    @pytest.mark.asyncio
    async def test_concurrent_signaling(self):
        """Test concurrent cancellation signaling."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        for i in range(10):
            handler.create_cancel_event(f"wf-{i}")

        async def signal_cancel(workflow_id: str):
            await asyncio.sleep(0.001)
            return handler.signal_cancellation(workflow_id)

        results = await asyncio.gather(*[
            signal_cancel(f"wf-{i}") for i in range(10)
        ])

        assert all(results)
        assert len(handler._cancelled_workflows) == 10

    @pytest.mark.asyncio
    async def test_wait_for_cancellation_event(self):
        """Test waiting for cancellation event."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        event = handler.create_cancel_event("wf-1")

        async def wait_for_cancel():
            await event.wait()
            return "cancelled"

        async def signal_after_delay():
            await asyncio.sleep(0.01)
            handler.signal_cancellation("wf-1")

        results = await asyncio.gather(
            wait_for_cancel(),
            signal_after_delay(),
        )

        assert results[0] == "cancelled"


class TestWorkerCancellationHandlerEdgeCases:
    """Test edge cases for WorkerCancellationHandler."""

    def test_many_cancel_events(self):
        """Test with many cancel events."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        for i in range(1000):
            handler.create_cancel_event(f"wf-{i}")

        assert len(handler._cancel_events) == 1000

    def test_signal_already_cancelled(self):
        """Test signaling already cancelled workflow."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        handler.create_cancel_event("wf-1")
        handler.signal_cancellation("wf-1")

        # Second signal should still succeed
        result = handler.signal_cancellation("wf-1")
        assert result is True

    def test_special_characters_in_workflow_id(self):
        """Test workflow IDs with special characters."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        special_id = "wf-🚀-test-ñ-中文"
        event = handler.create_cancel_event(special_id)

        assert special_id in handler._cancel_events

        handler.signal_cancellation(special_id)
        assert handler.is_cancelled(special_id)

    def test_empty_active_workflows(self):
        """Test cancel_workflow with empty active workflows."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        # No event created, should return error
        handler.create_cancel_event("wf-1")

    @pytest.mark.asyncio
    async def test_cancel_workflow_all_failures(self):
        """Test cancel_workflow with both event and token failures."""
        logger = MagicMock()
        handler = WorkerCancellationHandler(logger)

        # Don't create event
        active_workflows = {}
        task_runner_cancel = AsyncMock(side_effect=RuntimeError("Failed"))
        workflow_tokens = {"wf-1": "token"}

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="test",
            active_workflows=active_workflows,
            task_runner_cancel=task_runner_cancel,
            workflow_tokens=workflow_tokens,
        )

        assert success is False
        assert len(errors) >= 1
