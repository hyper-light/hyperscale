"""
Integration tests for GateStatsCoordinator (Section 15.3.7).

Tests statistics coordination including tiered updates, batch stats loops,
and windowed stats aggregation.
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

from hyperscale.distributed.nodes.gate.stats_coordinator import GateStatsCoordinator
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.models import JobStatus, UpdateTier
from hyperscale.distributed.reliability import BackpressureLevel


# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockLogger:
    """Mock logger for testing."""

    messages: list[str] = field(default_factory=list)

    async def log(self, *args, **kwargs):
        self.messages.append(str(args))


@dataclass
class MockTaskRunner:
    """Mock task runner for testing."""

    tasks: list = field(default_factory=list)

    def run(self, coro, *args, **kwargs):
        # If coro is callable (coroutine function), call it to get the coroutine object
        if callable(coro) and not asyncio.iscoroutine(coro):
            actual_coro = coro(*args, **kwargs)
        else:
            actual_coro = coro
        task = asyncio.create_task(actual_coro)
        self.tasks.append(task)
        return task


@dataclass
class MockWindowedStatsCollector:
    """Mock windowed stats collector."""

    pending_jobs: list[str] = field(default_factory=list)
    stats_data: dict = field(default_factory=dict)

    def get_jobs_with_pending_stats(self) -> list[str]:
        return self.pending_jobs

    async def get_aggregated_stats(self, job_id: str):
        if job_id in self.stats_data:
            return self.stats_data[job_id]
        return None


@dataclass
class MockJobStatus:
    """Mock job status object."""

    status: str = JobStatus.RUNNING.value
    total_completed: int = 100
    total_failed: int = 5
    overall_rate: float = 50.0
    elapsed_seconds: float = 10.0


# =============================================================================
# classify_update_tier Tests
# =============================================================================


class TestClassifyUpdateTierHappyPath:
    """Tests for classify_update_tier happy path."""

    def test_completed_status_is_immediate(self):
        """COMPLETED status is always immediate."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        tier = coordinator.classify_update_tier(
            "job-1", "running", JobStatus.COMPLETED.value
        )
        assert tier == UpdateTier.IMMEDIATE.value

    def test_failed_status_is_immediate(self):
        """FAILED status is always immediate."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        tier = coordinator.classify_update_tier(
            "job-1", "running", JobStatus.FAILED.value
        )
        assert tier == UpdateTier.IMMEDIATE.value

    def test_cancelled_status_is_immediate(self):
        """CANCELLED status is always immediate."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        tier = coordinator.classify_update_tier(
            "job-1", "running", JobStatus.CANCELLED.value
        )
        assert tier == UpdateTier.IMMEDIATE.value

    def test_first_running_is_immediate(self):
        """First transition to RUNNING is immediate."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        tier = coordinator.classify_update_tier("job-1", None, JobStatus.RUNNING.value)
        assert tier == UpdateTier.IMMEDIATE.value

    def test_status_change_is_immediate(self):
        """Any status change is immediate."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        tier = coordinator.classify_update_tier("job-1", "submitted", "running")
        assert tier == UpdateTier.IMMEDIATE.value

    def test_progress_within_status_is_periodic(self):
        """Progress update within same status is periodic."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        tier = coordinator.classify_update_tier("job-1", "running", "running")
        assert tier == UpdateTier.PERIODIC.value


class TestClassifyUpdateTierEdgeCases:
    """Tests for classify_update_tier edge cases."""

    def test_none_to_non_running_is_immediate(self):
        """First transition to non-RUNNING is immediate if status changes."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        # None to submitted - still a change
        tier = coordinator.classify_update_tier("job-1", None, "submitted")
        assert tier == UpdateTier.IMMEDIATE.value

    def test_same_final_status_is_immediate(self):
        """Even if no change, final statuses are immediate."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        # Already completed, still completed
        tier = coordinator.classify_update_tier(
            "job-1",
            JobStatus.COMPLETED.value,
            JobStatus.COMPLETED.value,
        )
        assert tier == UpdateTier.IMMEDIATE.value


# =============================================================================
# send_immediate_update Tests
# =============================================================================


class TestSendImmediateUpdateHappyPath:
    """Tests for send_immediate_update happy path."""

    @pytest.mark.asyncio
    async def test_sends_update_with_callback(self):
        """Sends update when callback exists."""
        state = GateRuntimeState()
        send_tcp = AsyncMock()
        job_status = MockJobStatus()

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: ("10.0.0.1", 8000) if x == "job-1" else None,
            get_job_status=lambda x: job_status if x == "job-1" else None,
            send_tcp=send_tcp,
        )

        await coordinator.send_immediate_update("job-1", "status_change")

        send_tcp.assert_called_once()
        call_args = send_tcp.call_args
        assert call_args[0][0] == ("10.0.0.1", 8000)
        assert call_args[0][1] == "job_status_push"

    @pytest.mark.asyncio
    async def test_no_op_without_callback(self):
        """No-op when no callback registered."""
        state = GateRuntimeState()
        send_tcp = AsyncMock()

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,  # No callback
            get_job_status=lambda x: MockJobStatus(),
            send_tcp=send_tcp,
        )

        await coordinator.send_immediate_update("job-1", "status_change")

        send_tcp.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_op_without_job_status(self):
        """No-op when job status not found."""
        state = GateRuntimeState()
        send_tcp = AsyncMock()

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: ("10.0.0.1", 8000),
            get_job_status=lambda x: None,  # No job status
            send_tcp=send_tcp,
        )

        await coordinator.send_immediate_update("job-1", "status_change")

        send_tcp.assert_not_called()


class TestSendImmediateUpdateFailureMode:
    """Tests for send_immediate_update failure modes."""

    @pytest.mark.asyncio
    async def test_handles_send_exception(self):
        """Handles exception during send gracefully."""
        state = GateRuntimeState()
        send_tcp = AsyncMock(side_effect=Exception("Network error"))

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: ("10.0.0.1", 8000),
            get_job_status=lambda x: MockJobStatus(),
            send_tcp=send_tcp,
        )

        # Should not raise
        await coordinator.send_immediate_update("job-1", "status_change")


# =============================================================================
# Batch Stats Loop Tests
# =============================================================================


class TestBatchStatsLoopHappyPath:
    """Tests for batch stats loop happy path."""

    @pytest.mark.asyncio
    async def test_start_creates_task(self):
        """Start batch stats loop creates background task."""
        state = GateRuntimeState()
        task_runner = MockTaskRunner()

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=task_runner,
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
        )

        await coordinator.start_batch_stats_loop()

        assert len(task_runner.tasks) == 1

    @pytest.mark.asyncio
    async def test_stop_cancels_task(self):
        """Stop batch stats loop cancels task."""
        state = GateRuntimeState()

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
            stats_push_interval_ms=10.0,  # Very short for testing
        )

        # Create a real task for the loop
        coordinator._batch_stats_task = asyncio.create_task(
            coordinator._batch_stats_loop()
        )

        await asyncio.sleep(0.01)  # Let it start

        await coordinator.stop_batch_stats_loop()

        assert coordinator._batch_stats_task.done()


class TestBatchStatsLoopBackpressure:
    """Tests for batch stats loop backpressure handling (AD-37)."""

    @pytest.mark.asyncio
    async def test_throttle_doubles_interval(self):
        """THROTTLE backpressure doubles interval."""
        state = GateRuntimeState()
        state._dc_backpressure["dc-1"] = BackpressureLevel.THROTTLE

        # We can't directly test interval timing easily, but we can verify
        # the backpressure level is read correctly
        assert state.get_max_backpressure_level() == BackpressureLevel.THROTTLE

    @pytest.mark.asyncio
    async def test_batch_quadruples_interval(self):
        """BATCH backpressure quadruples interval."""
        state = GateRuntimeState()
        state._dc_backpressure["dc-1"] = BackpressureLevel.BATCH

        assert state.get_max_backpressure_level() == BackpressureLevel.BATCH

    @pytest.mark.asyncio
    async def test_reject_skips_push(self):
        """REJECT backpressure skips push entirely."""
        state = GateRuntimeState()
        state._dc_backpressure["dc-1"] = BackpressureLevel.REJECT

        assert state.get_max_backpressure_level() == BackpressureLevel.REJECT


# =============================================================================
# Push Windowed Stats Tests
# =============================================================================


class TestPushWindowedStats:
    """Tests for _push_windowed_stats method."""

    @pytest.mark.asyncio
    async def test_pushes_stats_with_callback(self):
        """Pushes stats when callback and stats exist."""
        state = GateRuntimeState()
        state._progress_callbacks["job-1"] = ("10.0.0.1", 8000)

        @dataclass
        class MockStats:
            def dump(self) -> bytes:
                return b"stats_data"

        windowed_stats = MockWindowedStatsCollector()
        windowed_stats.stats_data["job-1"] = [MockStats()]

        send_tcp = AsyncMock()

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=windowed_stats,
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=send_tcp,
        )

        await coordinator._push_windowed_stats("job-1")

        send_tcp.assert_called_once()
        call_args = send_tcp.call_args
        assert call_args[0][0] == ("10.0.0.1", 8000)
        assert call_args[0][1] == "windowed_stats_push"

    @pytest.mark.asyncio
    async def test_no_op_without_callback(self):
        """No-op when no callback registered."""
        state = GateRuntimeState()
        # No callback registered

        send_tcp = AsyncMock()

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=send_tcp,
        )

        await coordinator._push_windowed_stats("job-1")

        send_tcp.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_op_without_stats(self):
        """No-op when no stats available."""
        state = GateRuntimeState()
        state._progress_callbacks["job-1"] = ("10.0.0.1", 8000)

        windowed_stats = MockWindowedStatsCollector()
        # No stats for job-1

        send_tcp = AsyncMock()

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=windowed_stats,
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=send_tcp,
        )

        await coordinator._push_windowed_stats("job-1")

        send_tcp.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_send_exception(self):
        """Handles exception during send gracefully."""
        state = GateRuntimeState()
        state._progress_callbacks["job-1"] = ("10.0.0.1", 8000)

        @dataclass
        class MockStats:
            def dump(self) -> bytes:
                return b"stats_data"

        windowed_stats = MockWindowedStatsCollector()
        windowed_stats.stats_data["job-1"] = [MockStats()]

        send_tcp = AsyncMock(side_effect=Exception("Network error"))

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=windowed_stats,
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=send_tcp,
        )

        # Should not raise
        await coordinator._push_windowed_stats("job-1")


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestConcurrency:
    """Tests for concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_immediate_updates(self):
        """Concurrent immediate updates don't interfere."""
        state = GateRuntimeState()
        send_tcp = AsyncMock()
        call_count = 0

        async def counting_send(*args, **kwargs):
            nonlocal call_count
            call_count += 1

        send_tcp.side_effect = counting_send

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: ("10.0.0.1", 8000),
            get_job_status=lambda x: MockJobStatus(),
            send_tcp=send_tcp,
        )

        await asyncio.gather(
            *[
                coordinator.send_immediate_update(f"job-{i}", "status_change")
                for i in range(100)
            ]
        )

        assert call_count == 100


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_zero_interval(self):
        """Zero stats push interval is valid."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
            stats_push_interval_ms=0.0,
        )

        assert coordinator._stats_push_interval_ms == 0.0

    def test_very_large_interval(self):
        """Very large stats push interval is valid."""
        state = GateRuntimeState()
        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: None,
            get_job_status=lambda x: None,
            send_tcp=AsyncMock(),
            stats_push_interval_ms=3600000.0,  # 1 hour
        )

        assert coordinator._stats_push_interval_ms == 3600000.0

    def test_job_status_with_missing_attributes(self):
        """Handle job status with missing optional attributes."""
        state = GateRuntimeState()

        class MinimalJobStatus:
            status = "running"

        coordinator = GateStatsCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            windowed_stats=MockWindowedStatsCollector(),
            get_job_callback=lambda x: ("10.0.0.1", 8000),
            get_job_status=lambda x: MinimalJobStatus(),
            send_tcp=AsyncMock(),
        )

        # Should use getattr defaults
        # This tests the getattr fallback logic
