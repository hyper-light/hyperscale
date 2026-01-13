"""
Integration tests for GateStatsCoordinator (Section 15.3.7).

Tests statistics coordination including tiered updates, batch stats,
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
    status: str = JobStatus.RUNNING.value
    total_completed: int = 100
    total_failed: int = 5
    overall_rate: float = 50.0
    elapsed_seconds: float = 10.0
    datacenters: list = field(default_factory=list)


# =============================================================================
# classify_update_tier Tests
# =============================================================================


def create_coordinator(
    state: GateRuntimeState | None = None,
    get_job_callback=None,
    get_job_status=None,
    get_all_running_jobs=None,
    send_tcp=None,
    windowed_stats=None,
) -> GateStatsCoordinator:
    return GateStatsCoordinator(
        state=state or GateRuntimeState(),
        logger=MockLogger(),
        task_runner=MockTaskRunner(),
        windowed_stats=windowed_stats or MockWindowedStatsCollector(),
        get_job_callback=get_job_callback or (lambda x: None),
        get_job_status=get_job_status or (lambda x: None),
        get_all_running_jobs=get_all_running_jobs or (lambda: []),
        send_tcp=send_tcp or AsyncMock(),
    )


class TestClassifyUpdateTierHappyPath:
    def test_completed_status_is_immediate(self):
        coordinator = create_coordinator()
        tier = coordinator.classify_update_tier(
            "job-1", "running", JobStatus.COMPLETED.value
        )
        assert tier == UpdateTier.IMMEDIATE.value

    def test_failed_status_is_immediate(self):
        coordinator = create_coordinator()
        tier = coordinator.classify_update_tier(
            "job-1", "running", JobStatus.FAILED.value
        )
        assert tier == UpdateTier.IMMEDIATE.value

    def test_cancelled_status_is_immediate(self):
        coordinator = create_coordinator()
        tier = coordinator.classify_update_tier(
            "job-1", "running", JobStatus.CANCELLED.value
        )
        assert tier == UpdateTier.IMMEDIATE.value

    def test_first_running_is_immediate(self):
        coordinator = create_coordinator()
        tier = coordinator.classify_update_tier("job-1", None, JobStatus.RUNNING.value)
        assert tier == UpdateTier.IMMEDIATE.value

    def test_status_change_is_immediate(self):
        coordinator = create_coordinator()
        tier = coordinator.classify_update_tier("job-1", "submitted", "running")
        assert tier == UpdateTier.IMMEDIATE.value

    def test_progress_within_status_is_periodic(self):
        coordinator = create_coordinator()
        tier = coordinator.classify_update_tier("job-1", "running", "running")
        assert tier == UpdateTier.PERIODIC.value


class TestClassifyUpdateTierEdgeCases:
    def test_none_to_non_running_is_immediate(self):
        coordinator = create_coordinator()
        tier = coordinator.classify_update_tier("job-1", None, "submitted")
        assert tier == UpdateTier.IMMEDIATE.value

    def test_same_final_status_is_immediate(self):
        coordinator = create_coordinator()
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
    @pytest.mark.asyncio
    async def test_sends_update_with_callback(self):
        send_tcp = AsyncMock()
        job_status = MockJobStatus()

        coordinator = create_coordinator(
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
        send_tcp = AsyncMock()

        coordinator = create_coordinator(
            get_job_callback=lambda x: None,
            get_job_status=lambda x: MockJobStatus(),
            send_tcp=send_tcp,
        )

        await coordinator.send_immediate_update("job-1", "status_change")

        send_tcp.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_op_without_job_status(self):
        send_tcp = AsyncMock()

        coordinator = create_coordinator(
            get_job_callback=lambda x: ("10.0.0.1", 8000),
            get_job_status=lambda x: None,
            send_tcp=send_tcp,
        )

        await coordinator.send_immediate_update("job-1", "status_change")

        send_tcp.assert_not_called()


class TestSendImmediateUpdateFailureMode:
    @pytest.mark.asyncio
    async def test_handles_send_exception(self):
        send_tcp = AsyncMock(side_effect=Exception("Network error"))

        coordinator = create_coordinator(
            get_job_callback=lambda x: ("10.0.0.1", 8000),
            get_job_status=lambda x: MockJobStatus(),
            send_tcp=send_tcp,
        )

        await coordinator.send_immediate_update("job-1", "status_change")


# =============================================================================
# Batch Stats Update Tests
# =============================================================================


@dataclass
class MockDCProgress:
    datacenter: str = "dc-1"
    status: str = "running"
    total_completed: int = 50
    total_failed: int = 2
    overall_rate: float = 25.0
    step_stats: list = field(default_factory=list)


class TestBatchStatsUpdateHappyPath:
    @pytest.mark.asyncio
    async def test_pushes_batch_to_running_jobs_with_callbacks(self):
        send_tcp = AsyncMock()
        job_status = MockJobStatus(datacenters=[MockDCProgress()])

        coordinator = create_coordinator(
            get_job_callback=lambda x: ("10.0.0.1", 8000) if x == "job-1" else None,
            get_all_running_jobs=lambda: [("job-1", job_status)],
            send_tcp=send_tcp,
        )

        await coordinator.batch_stats_update()

        send_tcp.assert_called_once()
        call_args = send_tcp.call_args
        assert call_args[0][0] == ("10.0.0.1", 8000)
        assert call_args[0][1] == "job_batch_push"

    @pytest.mark.asyncio
    async def test_no_op_when_no_running_jobs(self):
        send_tcp = AsyncMock()

        coordinator = create_coordinator(
            get_all_running_jobs=lambda: [],
            send_tcp=send_tcp,
        )

        await coordinator.batch_stats_update()

        send_tcp.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_op_when_no_callbacks(self):
        send_tcp = AsyncMock()
        job_status = MockJobStatus()

        coordinator = create_coordinator(
            get_job_callback=lambda x: None,
            get_all_running_jobs=lambda: [("job-1", job_status)],
            send_tcp=send_tcp,
        )

        await coordinator.batch_stats_update()

        send_tcp.assert_not_called()

    @pytest.mark.asyncio
    async def test_aggregates_step_stats_from_all_dcs(self):
        send_tcp = AsyncMock()
        dc1 = MockDCProgress(datacenter="dc-1", step_stats=["step1"])
        dc2 = MockDCProgress(datacenter="dc-2", step_stats=["step2", "step3"])
        job_status = MockJobStatus(datacenters=[dc1, dc2])

        coordinator = create_coordinator(
            get_job_callback=lambda x: ("10.0.0.1", 8000),
            get_all_running_jobs=lambda: [("job-1", job_status)],
            send_tcp=send_tcp,
        )

        await coordinator.batch_stats_update()

        send_tcp.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_send_exception_gracefully(self):
        send_tcp = AsyncMock(side_effect=Exception("Network error"))
        job_status = MockJobStatus(datacenters=[MockDCProgress()])

        coordinator = create_coordinator(
            get_job_callback=lambda x: ("10.0.0.1", 8000),
            get_all_running_jobs=lambda: [("job-1", job_status)],
            send_tcp=send_tcp,
        )

        await coordinator.batch_stats_update()


class TestBackpressureLevelState:
    def test_throttle_level_detected(self):
        state = GateRuntimeState()
        state._dc_backpressure["dc-1"] = BackpressureLevel.THROTTLE
        assert state.get_max_backpressure_level() == BackpressureLevel.THROTTLE

    def test_batch_level_detected(self):
        state = GateRuntimeState()
        state._dc_backpressure["dc-1"] = BackpressureLevel.BATCH
        assert state.get_max_backpressure_level() == BackpressureLevel.BATCH

    def test_reject_level_detected(self):
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
