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
from hyperscale.distributed.models import WorkflowStatus


class MockWorkerState:
    """Mock WorkerState for cancellation handler testing."""

    def __init__(self):
        self._workflow_cancel_events: dict[str, asyncio.Event] = {}
        self._workflow_tokens: dict[str, str] = {}
        self._active_workflows: dict[str, MagicMock] = {}
        self._workflow_id_to_name: dict[str, str] = {}

    def add_workflow(
        self,
        workflow_id: str,
        job_id: str = "job-123",
        status: str = "running",
        token: str | None = None,
        name: str = "test-workflow",
    ) -> None:
        """Helper to add a workflow for testing."""
        progress = MagicMock()
        progress.job_id = job_id
        progress.status = status
        self._active_workflows[workflow_id] = progress
        self._workflow_id_to_name[workflow_id] = name
        if token:
            self._workflow_tokens[workflow_id] = token


class TestWorkerCancellationHandlerInitialization:
    """Test WorkerCancellationHandler initialization."""

    def test_happy_path_instantiation(self) -> None:
        """Test normal instantiation with required state argument."""
        state = MockWorkerState()
        logger = MagicMock()
        handler = WorkerCancellationHandler(state, logger=logger)

        assert handler._state == state
        assert handler._logger == logger
        assert handler._poll_interval == 5.0
        assert handler._running is False

    def test_custom_poll_interval(self) -> None:
        """Test with custom poll interval."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state, poll_interval=10.0)

        assert handler._poll_interval == 10.0

    def test_no_logger(self) -> None:
        """Test instantiation without logger."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        assert handler._logger is None


class TestWorkerCancellationHandlerEventManagement:
    """Test cancel event management."""

    def test_create_cancel_event(self) -> None:
        """Test creating a cancel event."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        event = handler.create_cancel_event("wf-1")

        assert isinstance(event, asyncio.Event)
        assert "wf-1" in state._workflow_cancel_events
        assert state._workflow_cancel_events["wf-1"] is event

    def test_get_cancel_event(self) -> None:
        """Test getting a cancel event."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        created = handler.create_cancel_event("wf-1")
        retrieved = handler.get_cancel_event("wf-1")

        assert created is retrieved

    def test_get_cancel_event_not_found(self) -> None:
        """Test getting a non-existent cancel event."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        event = handler.get_cancel_event("non-existent")

        assert event is None

    def test_remove_cancel_event(self) -> None:
        """Test removing a cancel event."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        handler.create_cancel_event("wf-1")
        handler.remove_cancel_event("wf-1")

        assert "wf-1" not in state._workflow_cancel_events

    def test_remove_cancel_event_not_found(self) -> None:
        """Test removing a non-existent cancel event (should not raise)."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        # Should not raise
        handler.remove_cancel_event("non-existent")


class TestWorkerCancellationHandlerSignaling:
    """Test cancellation signaling."""

    def test_signal_cancellation_success(self) -> None:
        """Test signaling cancellation for existing workflow."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        event = handler.create_cancel_event("wf-1")
        result = handler.signal_cancellation("wf-1")

        assert result is True
        assert event.is_set()

    def test_signal_cancellation_not_found(self) -> None:
        """Test signaling cancellation for non-existent workflow."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        result = handler.signal_cancellation("non-existent")

        assert result is False


class TestWorkerCancellationHandlerCancelWorkflow:
    """Test cancel_workflow method."""

    @pytest.mark.asyncio
    async def test_cancel_workflow_success(self) -> None:
        """Test successful workflow cancellation."""
        state = MockWorkerState()
        state.add_workflow("wf-1", token="token-123", name="test-workflow")
        handler = WorkerCancellationHandler(state)

        # Create cancel event
        handler.create_cancel_event("wf-1")

        # Mock task runner cancel
        task_runner_cancel = AsyncMock()
        increment_version = MagicMock()

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="user requested",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        assert success is True
        assert errors == []
        task_runner_cancel.assert_awaited_once_with("token-123")
        increment_version.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancel_workflow_no_token(self) -> None:
        """Test cancellation without workflow token."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        # No token set
        task_runner_cancel = AsyncMock()
        increment_version = MagicMock()

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-unknown",
            reason="user requested",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        assert success is False
        assert len(errors) == 1
        assert "not found" in errors[0]
        task_runner_cancel.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_workflow_task_runner_failure(self) -> None:
        """Test cancellation with TaskRunner failure."""
        state = MockWorkerState()
        state.add_workflow("wf-1", token="token-123")
        handler = WorkerCancellationHandler(state)
        handler.create_cancel_event("wf-1")

        task_runner_cancel = AsyncMock(side_effect=RuntimeError("Cancel failed"))
        increment_version = AsyncMock()

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="user requested",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        assert success is True
        assert len(errors) == 1
        assert "TaskRunner cancel failed" in errors[0]

    @pytest.mark.asyncio
    async def test_cancel_workflow_updates_status(self) -> None:
        state = MockWorkerState()
        state.add_workflow("wf-1", token="token-123")
        handler = WorkerCancellationHandler(state)
        handler.create_cancel_event("wf-1")

        task_runner_cancel = AsyncMock()
        increment_version = AsyncMock()

        await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="test",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        assert state._active_workflows["wf-1"].status == WorkflowStatus.CANCELLED.value

    @pytest.mark.asyncio
    async def test_cancel_workflow_signals_event(self) -> None:
        state = MockWorkerState()
        state.add_workflow("wf-1", token="token-123")
        handler = WorkerCancellationHandler(state)
        event = handler.create_cancel_event("wf-1")

        task_runner_cancel = AsyncMock()
        increment_version = AsyncMock()

        await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="test",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        assert event.is_set()


class TestWorkerCancellationHandlerWithRemoteManager:
    """Test cancellation with RemoteGraphManager integration."""

    @pytest.mark.asyncio
    async def test_cancel_with_remote_manager_success(self) -> None:
        """Test cancellation with RemoteGraphManager."""
        state = MockWorkerState()
        state.add_workflow("wf-1", token="token-123", name="test-workflow")
        handler = WorkerCancellationHandler(state)
        handler.create_cancel_event("wf-1")

        # Set up mock remote manager
        remote_manager = MagicMock()
        remote_manager.await_workflow_cancellation = AsyncMock(return_value=(True, []))
        handler.set_remote_manager(remote_manager)

        task_runner_cancel = AsyncMock()
        increment_version = MagicMock()

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="test",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        assert success is True
        assert errors == []
        remote_manager.await_workflow_cancellation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cancel_with_remote_manager_timeout(self) -> None:
        """Test cancellation when RemoteGraphManager times out."""
        state = MockWorkerState()
        state.add_workflow("wf-1", token="token-123", name="test-workflow")
        handler = WorkerCancellationHandler(state)
        handler.create_cancel_event("wf-1")

        # Set up mock remote manager that times out
        remote_manager = MagicMock()
        remote_manager.await_workflow_cancellation = AsyncMock(
            return_value=(False, ["timeout"])
        )
        handler.set_remote_manager(remote_manager)

        task_runner_cancel = AsyncMock()
        increment_version = MagicMock()

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="test",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        assert success is True  # Overall success despite remote timeout
        assert any("timed out" in e.lower() or "timeout" in e.lower() for e in errors)

    @pytest.mark.asyncio
    async def test_cancel_with_remote_manager_exception(self) -> None:
        """Test cancellation when RemoteGraphManager raises exception."""
        state = MockWorkerState()
        state.add_workflow("wf-1", token="token-123", name="test-workflow")
        handler = WorkerCancellationHandler(state)
        handler.create_cancel_event("wf-1")

        # Set up mock remote manager that raises
        remote_manager = MagicMock()
        remote_manager.await_workflow_cancellation = AsyncMock(
            side_effect=RuntimeError("Remote error")
        )
        handler.set_remote_manager(remote_manager)

        task_runner_cancel = AsyncMock()
        increment_version = MagicMock()

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="test",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        assert success is True
        assert any("RemoteGraphManager" in e for e in errors)


class TestWorkerCancellationHandlerPolling:
    """Test cancellation poll loop."""

    @pytest.mark.asyncio
    async def test_run_cancellation_poll_loop_starts_running(self) -> None:
        """Test that poll loop starts running."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state, poll_interval=0.01)

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(
                get_manager_addr=MagicMock(return_value=None),
                is_circuit_open=MagicMock(return_value=False),
                send_tcp=AsyncMock(),
                node_host="localhost",
                node_port=8000,
                node_id_short="abc",
                task_runner_run=MagicMock(),
                is_running=MagicMock(return_value=True),
            )
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
    async def test_stop_stops_loop(self) -> None:
        """Test that stop() stops the loop."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state, poll_interval=0.01)

        running_flag = [True]

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(
                get_manager_addr=MagicMock(return_value=None),
                is_circuit_open=MagicMock(return_value=False),
                send_tcp=AsyncMock(),
                node_host="localhost",
                node_port=8000,
                node_id_short="abc",
                task_runner_run=MagicMock(),
                is_running=lambda: running_flag[0],
            )
        )

        await asyncio.sleep(0.03)
        handler.stop()
        running_flag[0] = False

        assert handler._running is False

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_poll_loop_no_manager_addr(self) -> None:
        """Test poll loop with no manager address."""
        state = MockWorkerState()
        state.add_workflow("wf-1")
        handler = WorkerCancellationHandler(state, poll_interval=0.01)

        send_tcp = AsyncMock()

        running_count = [0]

        def is_running():
            running_count[0] += 1
            return running_count[0] < 5

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(
                get_manager_addr=MagicMock(return_value=None),  # No manager
                is_circuit_open=MagicMock(return_value=False),
                send_tcp=send_tcp,
                node_host="localhost",
                node_port=8000,
                node_id_short="abc",
                task_runner_run=MagicMock(),
                is_running=is_running,
            )
        )

        await asyncio.sleep(0.1)
        handler.stop()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should not have sent any queries (no manager)
        send_tcp.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_poll_loop_circuit_open(self) -> None:
        """Test poll loop skips when circuit is open."""
        state = MockWorkerState()
        state.add_workflow("wf-1")
        handler = WorkerCancellationHandler(state, poll_interval=0.01)

        send_tcp = AsyncMock()

        running_count = [0]

        def is_running():
            running_count[0] += 1
            return running_count[0] < 5

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(
                get_manager_addr=MagicMock(return_value=("localhost", 8000)),
                is_circuit_open=MagicMock(return_value=True),  # Circuit open
                send_tcp=send_tcp,
                node_host="localhost",
                node_port=8000,
                node_id_short="abc",
                task_runner_run=MagicMock(),
                is_running=is_running,
            )
        )

        await asyncio.sleep(0.1)
        handler.stop()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should not have sent any queries (circuit open)
        send_tcp.assert_not_awaited()


class TestWorkerCancellationHandlerConcurrency:
    """Test concurrency aspects of WorkerCancellationHandler."""

    @pytest.mark.asyncio
    async def test_concurrent_cancel_event_creation(self) -> None:
        """Test concurrent cancel event creation."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        async def create_event(workflow_id: str):
            return handler.create_cancel_event(workflow_id)

        events = await asyncio.gather(*[create_event(f"wf-{i}") for i in range(10)])

        assert len(events) == 10
        assert len(state._workflow_cancel_events) == 10

    @pytest.mark.asyncio
    async def test_concurrent_signaling(self) -> None:
        """Test concurrent cancellation signaling."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        for i in range(10):
            handler.create_cancel_event(f"wf-{i}")

        async def signal_cancel(workflow_id: str):
            await asyncio.sleep(0.001)
            return handler.signal_cancellation(workflow_id)

        results = await asyncio.gather(*[signal_cancel(f"wf-{i}") for i in range(10)])

        assert all(results)
        # All events should be set
        for i in range(10):
            assert state._workflow_cancel_events[f"wf-{i}"].is_set()

    @pytest.mark.asyncio
    async def test_wait_for_cancellation_event(self) -> None:
        """Test waiting for cancellation event."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

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

    @pytest.mark.asyncio
    async def test_concurrent_cancel_workflow_calls(self) -> None:
        """Test concurrent cancel_workflow calls for different workflows."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        for i in range(5):
            state.add_workflow(f"wf-{i}", token=f"token-{i}")
            handler.create_cancel_event(f"wf-{i}")

        task_runner_cancel = AsyncMock()
        increment_version = MagicMock()

        async def cancel_one(workflow_id: str):
            return await handler.cancel_workflow(
                workflow_id=workflow_id,
                reason="concurrent test",
                task_runner_cancel=task_runner_cancel,
                increment_version=increment_version,
            )

        results = await asyncio.gather(*[cancel_one(f"wf-{i}") for i in range(5)])

        assert all(success for success, _ in results)
        assert task_runner_cancel.await_count == 5


class TestWorkerCancellationHandlerEdgeCases:
    """Test edge cases for WorkerCancellationHandler."""

    def test_many_cancel_events(self) -> None:
        """Test with many cancel events."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        for i in range(1000):
            handler.create_cancel_event(f"wf-{i}")

        assert len(state._workflow_cancel_events) == 1000

    def test_signal_already_signaled(self) -> None:
        """Test signaling already signaled workflow."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        handler.create_cancel_event("wf-1")
        handler.signal_cancellation("wf-1")

        # Second signal should still succeed
        result = handler.signal_cancellation("wf-1")
        assert result is True

    def test_special_characters_in_workflow_id(self) -> None:
        """Test workflow IDs with special characters."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        special_id = "wf-🚀-test-ñ-中文"
        event = handler.create_cancel_event(special_id)

        assert special_id in state._workflow_cancel_events

        handler.signal_cancellation(special_id)
        assert event.is_set()

    @pytest.mark.asyncio
    async def test_cancel_workflow_no_active_workflow(self) -> None:
        """Test cancel_workflow when workflow not in active_workflows but has token."""
        state = MockWorkerState()
        state._workflow_tokens["wf-1"] = "token-123"
        handler = WorkerCancellationHandler(state)
        handler.create_cancel_event("wf-1")

        task_runner_cancel = AsyncMock()
        increment_version = MagicMock()

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="test",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        # Should succeed because token exists
        assert success is True
        task_runner_cancel.assert_awaited_once()

    def test_set_remote_manager(self) -> None:
        """Test setting remote manager."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        remote_manager = MagicMock()
        handler.set_remote_manager(remote_manager)

        assert handler._remote_manager is remote_manager

    def test_stop_when_not_running(self) -> None:
        """Test stop() when handler is not running."""
        state = MockWorkerState()
        handler = WorkerCancellationHandler(state)

        # Should not raise
        handler.stop()
        assert handler._running is False


class TestWorkerCancellationHandlerFailureModes:
    """Test failure modes for WorkerCancellationHandler."""

    @pytest.mark.asyncio
    async def test_cancel_workflow_all_failures(self) -> None:
        """Test cancel_workflow with all possible failures."""
        state = MockWorkerState()
        state.add_workflow("wf-1", token="token-123", name="test-workflow")
        handler = WorkerCancellationHandler(state)
        handler.create_cancel_event("wf-1")

        # Remote manager that fails
        remote_manager = MagicMock()
        remote_manager.await_workflow_cancellation = AsyncMock(
            side_effect=RuntimeError("Remote failed")
        )
        handler.set_remote_manager(remote_manager)

        # Task runner that fails
        task_runner_cancel = AsyncMock(side_effect=RuntimeError("Task failed"))
        increment_version = MagicMock()

        success, errors = await handler.cancel_workflow(
            workflow_id="wf-1",
            reason="test",
            task_runner_cancel=task_runner_cancel,
            increment_version=increment_version,
        )

        # Should still complete (overall success) but with errors
        assert success is True
        assert len(errors) >= 2  # Both failures recorded

    @pytest.mark.asyncio
    async def test_poll_loop_handles_exception_gracefully(self) -> None:
        """Test poll loop handles exceptions gracefully."""
        state = MockWorkerState()
        state.add_workflow("wf-1")
        handler = WorkerCancellationHandler(state, poll_interval=0.01)

        exception_count = [0]

        async def failing_send(*args, **kwargs):
            exception_count[0] += 1
            raise RuntimeError("Send failed")

        running_count = [0]

        def is_running():
            running_count[0] += 1
            return running_count[0] < 10

        task = asyncio.create_task(
            handler.run_cancellation_poll_loop(
                get_manager_addr=MagicMock(return_value=("localhost", 8000)),
                is_circuit_open=MagicMock(return_value=False),
                send_tcp=failing_send,
                node_host="localhost",
                node_port=8000,
                node_id_short="abc",
                task_runner_run=MagicMock(),
                is_running=is_running,
            )
        )

        await asyncio.sleep(0.2)
        handler.stop()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Loop should have continued despite exceptions
        assert exception_count[0] >= 1
