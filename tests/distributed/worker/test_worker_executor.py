"""
Integration tests for WorkerExecutor (Section 15.2.6.1).

Tests WorkerExecutor for workflow execution, progress reporting,
and throughput tracking (AD-19, AD-33, AD-37).

Covers:
- Happy path: Normal execution, progress buffering, throughput tracking
- Negative path: Core allocation failures
- Failure mode: Progress flush failures
- Concurrency: Thread-safe progress buffering
- Edge cases: Zero cores, empty buffer, backpressure levels
"""

import asyncio
from unittest.mock import MagicMock, AsyncMock

import pytest

from hyperscale.distributed_rewrite.nodes.worker.execution import WorkerExecutor
from hyperscale.distributed_rewrite.models import WorkflowProgress, WorkflowStatus
from hyperscale.distributed_rewrite.reliability import BackpressureLevel


class MockCoreAllocator:
    """Mock CoreAllocator for testing."""

    def __init__(self, total_cores: int = 8):
        self._total_cores = total_cores
        self._available_cores = total_cores
        self._allocations: dict[str, list[int]] = {}

    @property
    def total_cores(self) -> int:
        return self._total_cores

    @property
    def available_cores(self) -> int:
        return self._available_cores

    async def allocate(self, workflow_id: str, cores: int):
        result = MagicMock()
        if cores <= self._available_cores:
            allocated = list(range(cores))
            self._allocations[workflow_id] = allocated
            self._available_cores -= cores
            result.success = True
            result.allocated_cores = allocated
            result.error = None
        else:
            result.success = False
            result.allocated_cores = None
            result.error = f"Not enough cores: requested {cores}, available {self._available_cores}"
        return result

    async def free(self, workflow_id: str):
        if workflow_id in self._allocations:
            freed = len(self._allocations.pop(workflow_id))
            self._available_cores += freed


class MockWorkerState:
    """Mock WorkerState for testing."""

    def __init__(self):
        self._throughput_completions: int = 0
        self._completion_times: list[float] = []
        self._progress_buffer: dict[str, WorkflowProgress] = {}
        self._progress_buffer_lock = asyncio.Lock()
        self._throughput_last_value: float = 0.0

    def record_completion(self, duration_seconds: float) -> None:
        """Record a workflow completion for throughput tracking."""
        self._throughput_completions += 1
        self._completion_times.append(duration_seconds)
        if len(self._completion_times) > 50:
            self._completion_times.pop(0)

    def get_throughput(self) -> float:
        """Get current throughput (completions per second)."""
        return self._throughput_last_value

    def get_expected_throughput(self) -> float:
        """Get expected throughput based on average completion time."""
        if not self._completion_times:
            return 0.0
        avg_completion_time = sum(self._completion_times) / len(self._completion_times)
        if avg_completion_time <= 0:
            return 0.0
        return 1.0 / avg_completion_time

    async def buffer_progress_update(
        self,
        workflow_id: str,
        progress: WorkflowProgress,
    ) -> None:
        """Buffer a progress update for later flush."""
        async with self._progress_buffer_lock:
            self._progress_buffer[workflow_id] = progress

    async def flush_progress_buffer(self) -> dict[str, WorkflowProgress]:
        """Flush and return all buffered progress updates."""
        async with self._progress_buffer_lock:
            updates = dict(self._progress_buffer)
            self._progress_buffer.clear()
        return updates

    async def clear_progress_buffer(self) -> None:
        """Clear all buffered progress updates without returning them."""
        async with self._progress_buffer_lock:
            self._progress_buffer.clear()

    def get_completion_sample_count(self) -> int:
        """Get count of completion time samples."""
        return len(self._completion_times)

    def get_buffered_update_count(self) -> int:
        """Get count of buffered progress updates."""
        return len(self._progress_buffer)


class MockBackpressureManager:
    """Mock backpressure manager for testing."""

    def __init__(self, level: BackpressureLevel = BackpressureLevel.NONE):
        self._level = level
        self._delay_seconds = 0.0

    def should_throttle(self) -> bool:
        return self._level.value >= BackpressureLevel.THROTTLE.value

    def should_batch_only(self) -> bool:
        return self._level.value >= BackpressureLevel.BATCH.value

    def should_reject_updates(self) -> bool:
        return self._level.value >= BackpressureLevel.REJECT.value

    def get_throttle_delay_seconds(self) -> float:
        return self._delay_seconds


class TestWorkerExecutorInitialization:
    """Test WorkerExecutor initialization."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        assert executor._core_allocator == allocator
        assert executor._logger == logger
        assert executor._state == state
        assert executor._progress_update_interval == 1.0
        assert executor._progress_flush_interval == 0.5

    def test_custom_intervals(self):
        """Test with custom intervals."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(
            allocator,
            logger,
            state,
            progress_update_interval=2.0,
            progress_flush_interval=1.0,
        )

        assert executor._progress_update_interval == 2.0
        assert executor._progress_flush_interval == 1.0

    def test_with_backpressure_manager(self):
        """Test with backpressure manager."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        bp_manager = MockBackpressureManager()
        executor = WorkerExecutor(
            allocator,
            logger,
            state,
            backpressure_manager=bp_manager,
        )

        assert executor._backpressure_manager == bp_manager


class TestWorkerExecutorCoreAllocation:
    """Test core allocation methods."""

    def test_available_cores(self):
        """Test available cores property."""
        allocator = MockCoreAllocator(total_cores=16)
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        assert executor.available_cores == 16

    def test_total_cores(self):
        """Test total cores property."""
        allocator = MockCoreAllocator(total_cores=16)
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        assert executor.total_cores == 16

    @pytest.mark.asyncio
    async def test_allocate_cores_success(self):
        """Test successful core allocation."""
        allocator = MockCoreAllocator(total_cores=8)
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        success, cores, error = await executor.allocate_cores("wf-1", 4)

        assert success is True
        assert cores == [0, 1, 2, 3]
        assert error is None
        assert executor.available_cores == 4

    @pytest.mark.asyncio
    async def test_allocate_cores_failure(self):
        """Test core allocation failure."""
        allocator = MockCoreAllocator(total_cores=4)
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        success, cores, error = await executor.allocate_cores("wf-1", 8)

        assert success is False
        assert cores is None
        assert "Not enough cores" in error

    @pytest.mark.asyncio
    async def test_free_cores(self):
        """Test freeing cores."""
        allocator = MockCoreAllocator(total_cores=8)
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        await executor.allocate_cores("wf-1", 4)
        assert executor.available_cores == 4

        await executor.free_cores("wf-1")
        assert executor.available_cores == 8


class TestWorkerExecutorThroughput:
    """Test throughput tracking (AD-19)."""

    def test_record_throughput_event(self):
        """Test recording throughput event."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        executor.record_throughput_event(1.5)

        assert state._throughput_completions == 1
        assert len(state._completion_times) == 1
        assert state._completion_times[0] == 1.5

    def test_record_throughput_max_samples(self):
        """Test throughput max samples limit."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        for i in range(60):
            executor.record_throughput_event(float(i))

        assert len(state._completion_times) == 50

    def test_get_throughput_initial(self):
        """Test initial throughput."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        throughput = executor.get_throughput()
        assert throughput == 0.0

    def test_get_expected_throughput_empty(self):
        """Test expected throughput with no samples."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        expected = executor.get_expected_throughput()
        assert expected == 0.0

    def test_get_expected_throughput_with_samples(self):
        """Test expected throughput calculation."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        for _ in range(10):
            executor.record_throughput_event(2.0)

        expected = executor.get_expected_throughput()
        assert expected == 0.5  # 1 / 2.0

    def test_get_expected_throughput_zero_time(self):
        """Test expected throughput with zero completion time."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        executor.record_throughput_event(0.0)

        expected = executor.get_expected_throughput()
        assert expected == 0.0


class TestWorkerExecutorProgressBuffering:
    """Test progress buffering methods."""

    @pytest.mark.asyncio
    async def test_buffer_progress_update(self):
        """Test buffering a progress update."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        progress = MagicMock(spec=WorkflowProgress)
        await executor.buffer_progress_update("wf-1", progress)

        assert "wf-1" in state._progress_buffer
        assert state._progress_buffer["wf-1"] == progress

    @pytest.mark.asyncio
    async def test_buffer_progress_update_replaces(self):
        """Test buffering replaces previous update."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        progress1 = MagicMock(spec=WorkflowProgress)
        progress2 = MagicMock(spec=WorkflowProgress)

        await executor.buffer_progress_update("wf-1", progress1)
        await executor.buffer_progress_update("wf-1", progress2)

        assert state._progress_buffer["wf-1"] == progress2

    @pytest.mark.asyncio
    async def test_flush_progress_buffer(self):
        """Test flushing progress buffer."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        progress1 = MagicMock(spec=WorkflowProgress)
        progress2 = MagicMock(spec=WorkflowProgress)

        await executor.buffer_progress_update("wf-1", progress1)
        await executor.buffer_progress_update("wf-2", progress2)

        send_progress = AsyncMock()
        await executor.flush_progress_buffer(send_progress)

        assert len(state._progress_buffer) == 0
        assert send_progress.await_count == 2

    @pytest.mark.asyncio
    async def test_flush_progress_buffer_empty(self):
        """Test flushing empty buffer."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        send_progress = AsyncMock()
        await executor.flush_progress_buffer(send_progress)

        send_progress.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flush_progress_buffer_handles_exceptions(self):
        """Test flush handles exceptions gracefully."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        progress = MagicMock(spec=WorkflowProgress)
        await executor.buffer_progress_update("wf-1", progress)

        send_progress = AsyncMock(side_effect=RuntimeError("Send failed"))
        await executor.flush_progress_buffer(send_progress)

        # Should have cleared buffer despite error
        assert len(state._progress_buffer) == 0


class TestWorkerExecutorProgressFlushLoop:
    """Test progress flush loop (AD-37)."""

    @pytest.mark.asyncio
    async def test_run_progress_flush_loop_starts_running(self):
        """Test that flush loop starts running."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(
            allocator,
            logger,
            state,
            progress_flush_interval=0.01,
        )

        send_progress = AsyncMock()
        task = asyncio.create_task(executor.run_progress_flush_loop(send_progress))

        await asyncio.sleep(0.05)

        assert executor._running is True

        executor.stop()
        await asyncio.sleep(0.02)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_stop_stops_loop(self):
        """Test that stop() stops the loop."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(
            allocator,
            logger,
            state,
            progress_flush_interval=0.01,
        )

        send_progress = AsyncMock()
        task = asyncio.create_task(executor.run_progress_flush_loop(send_progress))

        await asyncio.sleep(0.03)
        executor.stop()

        assert executor._running is False

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_flush_loop_respects_reject_backpressure(self):
        """Test flush loop respects REJECT backpressure (AD-37)."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        bp_manager = MockBackpressureManager(BackpressureLevel.REJECT)
        executor = WorkerExecutor(
            allocator,
            logger,
            state,
            progress_flush_interval=0.01,
            backpressure_manager=bp_manager,
        )

        # Buffer some progress
        progress = MagicMock(spec=WorkflowProgress)
        await executor.buffer_progress_update("wf-1", progress)

        send_progress = AsyncMock()
        task = asyncio.create_task(executor.run_progress_flush_loop(send_progress))

        await asyncio.sleep(0.05)
        executor.stop()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Buffer should be cleared (updates dropped)
        assert len(state._progress_buffer) == 0
        # But nothing should have been sent
        send_progress.assert_not_awaited()


class TestWorkerExecutorMetrics:
    """Test execution metrics."""

    def test_get_execution_metrics(self):
        """Test getting execution metrics."""
        allocator = MockCoreAllocator(total_cores=16)
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        executor.record_throughput_event(1.0)
        executor.record_throughput_event(2.0)

        metrics = executor.get_execution_metrics()

        assert metrics["available_cores"] == 16
        assert metrics["total_cores"] == 16
        assert metrics["completion_samples"] == 2
        assert metrics["buffered_updates"] == 0

    @pytest.mark.asyncio
    async def test_get_execution_metrics_with_buffered(self):
        """Test metrics with buffered updates."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        progress1 = MagicMock(spec=WorkflowProgress)
        progress2 = MagicMock(spec=WorkflowProgress)
        await executor.buffer_progress_update("wf-1", progress1)
        await executor.buffer_progress_update("wf-2", progress2)

        metrics = executor.get_execution_metrics()

        assert metrics["buffered_updates"] == 2


class TestWorkerExecutorCreateInitialProgress:
    """Test create_initial_progress static method."""

    def test_create_initial_progress(self):
        """Test creating initial progress."""
        progress = WorkerExecutor.create_initial_progress(
            job_id="job-123",
            workflow_id="wf-456",
            allocated_cores=[0, 1, 2, 3],
            available_cores=8,
            cores_requested=4,
        )

        assert progress.job_id == "job-123"
        assert progress.workflow_id == "wf-456"
        assert progress.status == WorkflowStatus.RUNNING.value
        assert progress.assigned_cores == [0, 1, 2, 3]
        assert progress.worker_available_cores == 8
        assert progress.worker_workflow_assigned_cores == 4
        assert progress.completed_count == 0
        assert progress.failed_count == 0

    def test_create_initial_progress_empty_cores(self):
        """Test creating initial progress with no cores."""
        progress = WorkerExecutor.create_initial_progress(
            job_id="job-1",
            workflow_id="wf-1",
            allocated_cores=[],
            available_cores=0,
            cores_requested=0,
        )

        assert progress.assigned_cores == []
        assert progress.worker_available_cores == 0


class TestWorkerExecutorConcurrency:
    """Test concurrency aspects of WorkerExecutor."""

    @pytest.mark.asyncio
    async def test_concurrent_progress_buffering(self):
        """Test concurrent progress buffering."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        async def buffer_progress(workflow_id: str):
            progress = MagicMock(spec=WorkflowProgress)
            await executor.buffer_progress_update(workflow_id, progress)

        await asyncio.gather(*[
            buffer_progress(f"wf-{i}") for i in range(10)
        ])

        assert len(state._progress_buffer) == 10

    @pytest.mark.asyncio
    async def test_concurrent_allocation_and_free(self):
        """Test concurrent core allocation and freeing."""
        allocator = MockCoreAllocator(total_cores=16)
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        async def allocate_and_free(workflow_id: str):
            success, cores, error = await executor.allocate_cores(workflow_id, 2)
            await asyncio.sleep(0.01)
            await executor.free_cores(workflow_id)

        await asyncio.gather(*[
            allocate_and_free(f"wf-{i}") for i in range(4)
        ])

        assert executor.available_cores == 16


class TestWorkerExecutorEdgeCases:
    """Test edge cases for WorkerExecutor."""

    @pytest.mark.asyncio
    async def test_allocate_all_cores(self):
        """Test allocating all cores."""
        allocator = MockCoreAllocator(total_cores=8)
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        success, cores, error = await executor.allocate_cores("wf-1", 8)

        assert success is True
        assert len(cores) == 8
        assert executor.available_cores == 0

    @pytest.mark.asyncio
    async def test_free_nonexistent_workflow(self):
        """Test freeing cores for non-existent workflow."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        # Should not raise
        await executor.free_cores("non-existent")

    def test_many_throughput_samples(self):
        """Test with many throughput samples."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        for i in range(1000):
            executor.record_throughput_event(float(i % 10 + 1))

        assert len(state._completion_times) == 50

    def test_throughput_negative_time(self):
        """Test throughput with negative completion time."""
        allocator = MockCoreAllocator()
        logger = MagicMock()
        state = MockWorkerState()
        executor = WorkerExecutor(allocator, logger, state)

        executor.record_throughput_event(-1.0)

        assert len(state._completion_times) == 1
        # Negative values are allowed (edge case)
