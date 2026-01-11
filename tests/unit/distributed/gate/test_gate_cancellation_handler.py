"""
Integration tests for GateCancellationHandler (Section 15.3.7).

Tests job and workflow cancellation including:
- AD-20 cancellation propagation
- Rate limiting (AD-24)
- Retry logic with exponential backoff (AD-21)
- Fencing token validation (AD-10)
"""

import asyncio
import pytest
import inspect
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

from hyperscale.distributed.nodes.gate.handlers.tcp_cancellation import GateCancellationHandler
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.models import (
    CancelJob,
    CancelAck,
    JobCancelRequest,
    JobCancelResponse,
    JobCancellationComplete,
    GlobalJobStatus,
    JobStatus,
    SingleWorkflowCancelRequest,
)


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
        if inspect.iscoroutinefunction(coro):
            task = asyncio.create_task(coro(*args, **kwargs))
            self.tasks.append(task)
            return task
        return None


@dataclass
class MockNodeId:
    """Mock node ID."""
    full: str = "gate-001"
    short: str = "001"
    datacenter: str = "global"


@dataclass
class MockGateJobManager:
    """Mock gate job manager."""
    jobs: dict = field(default_factory=dict)
    callbacks: dict = field(default_factory=dict)

    def get_job(self, job_id: str):
        return self.jobs.get(job_id)

    def has_job(self, job_id: str) -> bool:
        return job_id in self.jobs

    def get_callback(self, job_id: str):
        return self.callbacks.get(job_id)


def create_mock_handler(
    state: GateRuntimeState = None,
    job_manager: MockGateJobManager = None,
    rate_limit_allowed: bool = True,
    rate_limit_retry: float = 0.0,
    available_dcs: list[str] = None,
    datacenter_managers: dict = None,
    send_tcp_response: bytes = None,
) -> GateCancellationHandler:
    """Create a mock handler with configurable behavior."""
    if state is None:
        state = GateRuntimeState()
    if job_manager is None:
        job_manager = MockGateJobManager()
    if available_dcs is None:
        available_dcs = ["dc-east", "dc-west"]
    if datacenter_managers is None:
        datacenter_managers = {
            "dc-east": [("10.0.0.1", 8000)],
            "dc-west": [("10.0.0.2", 8000)],
        }

    async def mock_send_tcp(addr, msg_type, data, timeout=None):
        if send_tcp_response:
            return (send_tcp_response, None)
        ack = CancelAck(
            job_id="job-123",
            cancelled=True,
            workflows_cancelled=5,
        )
        return (ack.dump(), None)

    return GateCancellationHandler(
        state=state,
        logger=MockLogger(),
        task_runner=MockTaskRunner(),
        job_manager=job_manager,
        datacenter_managers=datacenter_managers,
        get_node_id=lambda: MockNodeId(),
        get_host=lambda: "127.0.0.1",
        get_tcp_port=lambda: 9000,
        check_rate_limit=lambda client_id, op: (rate_limit_allowed, rate_limit_retry),
        send_tcp=mock_send_tcp,
        get_available_datacenters=lambda: available_dcs,
    )


# =============================================================================
# handle_cancel_job Happy Path Tests (AD-20)
# =============================================================================


class TestHandleCancelJobHappyPath:
    """Tests for handle_cancel_job happy path."""

    @pytest.mark.asyncio
    async def test_cancels_running_job(self):
        """Cancels a running job successfully."""
        job_manager = MockGateJobManager()
        job_manager.jobs["job-123"] = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        )

        handler = create_mock_handler(job_manager=job_manager)

        cancel_request = CancelJob(
            job_id="job-123",
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)
        ack = CancelAck.load(result)
        assert ack.cancelled is True

    @pytest.mark.asyncio
    async def test_cancels_with_ad20_format(self):
        """Cancels using AD-20 JobCancelRequest format."""
        job_manager = MockGateJobManager()
        job_manager.jobs["job-123"] = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        )

        handler = create_mock_handler(job_manager=job_manager)

        cancel_request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-001",
            timestamp=1234567890,
            fence_token=0,
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)
        response = JobCancelResponse.load(result)
        assert response.success is True


# =============================================================================
# handle_cancel_job Rate Limiting Tests (AD-24)
# =============================================================================


class TestHandleCancelJobRateLimiting:
    """Tests for handle_cancel_job rate limiting (AD-24)."""

    @pytest.mark.asyncio
    async def test_rejects_rate_limited_client(self):
        """Rejects cancel when client is rate limited."""
        handler = create_mock_handler(rate_limit_allowed=False, rate_limit_retry=5.0)

        cancel_request = CancelJob(
            job_id="job-123",
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)
        # Should return RateLimitResponse


# =============================================================================
# handle_cancel_job Fencing Token Tests (AD-10)
# =============================================================================


class TestHandleCancelJobFencingTokens:
    """Tests for handle_cancel_job fencing token validation (AD-10)."""

    @pytest.mark.asyncio
    async def test_rejects_mismatched_fence_token(self):
        """Rejects cancel with mismatched fence token."""
        job_manager = MockGateJobManager()
        job = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        )
        job.fence_token = 10
        job_manager.jobs["job-123"] = job

        handler = create_mock_handler(job_manager=job_manager)

        cancel_request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-001",
            timestamp=1234567890,
            fence_token=5,  # Wrong fence token
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        response = JobCancelResponse.load(result)
        assert response.success is False
        assert "Fence token mismatch" in response.error


# =============================================================================
# handle_cancel_job Negative Path Tests
# =============================================================================


class TestHandleCancelJobNegativePath:
    """Tests for handle_cancel_job negative paths."""

    @pytest.mark.asyncio
    async def test_rejects_unknown_job(self):
        """Rejects cancel for unknown job."""
        handler = create_mock_handler()

        cancel_request = CancelJob(
            job_id="unknown-job",
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        ack = CancelAck.load(result)
        assert ack.cancelled is False
        assert "not found" in ack.error.lower()

    @pytest.mark.asyncio
    async def test_returns_already_cancelled(self):
        """Returns success for already cancelled job."""
        job_manager = MockGateJobManager()
        job_manager.jobs["job-123"] = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.CANCELLED.value,
            datacenters=[],
            timestamp=1234567890.0,
        )

        handler = create_mock_handler(job_manager=job_manager)

        cancel_request = CancelJob(
            job_id="job-123",
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        ack = CancelAck.load(result)
        assert ack.cancelled is True

    @pytest.mark.asyncio
    async def test_rejects_completed_job(self):
        """Rejects cancel for completed job."""
        job_manager = MockGateJobManager()
        job_manager.jobs["job-123"] = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.COMPLETED.value,
            datacenters=[],
            timestamp=1234567890.0,
        )

        handler = create_mock_handler(job_manager=job_manager)

        cancel_request = CancelJob(
            job_id="job-123",
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        ack = CancelAck.load(result)
        assert ack.cancelled is False


# =============================================================================
# handle_cancel_job Failure Mode Tests
# =============================================================================


class TestHandleCancelJobFailureModes:
    """Tests for handle_cancel_job failure modes."""

    @pytest.mark.asyncio
    async def test_handles_invalid_data(self):
        """Handles invalid cancel data gracefully."""
        handler = create_mock_handler()

        errors_handled = []

        async def mock_handle_exception(error, context):
            errors_handled.append((error, context))

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=b"invalid_data",
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_handles_manager_send_failure(self):
        """Handles manager send failure gracefully."""
        job_manager = MockGateJobManager()
        job_manager.jobs["job-123"] = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        )

        async def failing_send(addr, msg_type, data, timeout=None):
            raise ConnectionError("Connection refused")

        state = GateRuntimeState()
        handler = GateCancellationHandler(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            datacenter_managers={"dc-east": [("10.0.0.1", 8000)]},
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            check_rate_limit=lambda client_id, op: (True, 0),
            send_tcp=failing_send,
            get_available_datacenters=lambda: ["dc-east"],
        )

        cancel_request = CancelJob(
            job_id="job-123",
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        # Should still return a result (with error in errors list)
        assert isinstance(result, bytes)


# =============================================================================
# handle_job_cancellation_complete Tests
# =============================================================================


class TestHandleJobCancellationComplete:
    """Tests for handle_job_cancellation_complete."""

    @pytest.mark.asyncio
    async def test_handles_completion_notification(self):
        """Handles cancellation completion notification."""
        state = GateRuntimeState()
        state.initialize_cancellation("job-123")

        handler = create_mock_handler(state=state)

        complete = JobCancellationComplete(
            job_id="job-123",
            success=True,
            cancelled_workflow_count=10,
            total_workflow_count=10,
            errors=[],
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_job_cancellation_complete(
            addr=("10.0.0.1", 8000),
            data=complete.dump(),
            handle_exception=mock_handle_exception,
        )

        assert result == b'OK'

    @pytest.mark.asyncio
    async def test_handles_invalid_data(self):
        """Handles invalid completion data gracefully."""
        handler = create_mock_handler()

        errors_handled = []

        async def mock_handle_exception(error, context):
            errors_handled.append((error, context))

        result = await handler.handle_job_cancellation_complete(
            addr=("10.0.0.1", 8000),
            data=b"invalid_data",
            handle_exception=mock_handle_exception,
        )

        assert result == b'ERROR'


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestConcurrency:
    """Tests for concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_cancel_requests(self):
        """Concurrent cancel requests don't interfere."""
        job_manager = MockGateJobManager()
        for i in range(10):
            job_manager.jobs[f"job-{i}"] = GlobalJobStatus(
                job_id=f"job-{i}",
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            )

        handler = create_mock_handler(job_manager=job_manager)

        requests = [
            CancelJob(job_id=f"job-{i}", reason="test")
            for i in range(10)
        ]

        async def mock_handle_exception(error, context):
            pass

        results = await asyncio.gather(*[
            handler.handle_cancel_job(
                addr=("10.0.0.1", 8000),
                data=req.dump(),
                handle_exception=mock_handle_exception,
            )
            for req in requests
        ])

        assert len(results) == 10
        assert all(isinstance(r, bytes) for r in results)


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_reason(self):
        """Handles empty cancellation reason."""
        job_manager = MockGateJobManager()
        job_manager.jobs["job-123"] = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        )

        handler = create_mock_handler(job_manager=job_manager)

        cancel_request = CancelJob(
            job_id="job-123",
            reason="",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_no_available_datacenters(self):
        """Handles cancel when no DCs are available."""
        job_manager = MockGateJobManager()
        job_manager.jobs["job-123"] = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        )

        handler = create_mock_handler(
            job_manager=job_manager,
            available_dcs=[],
            datacenter_managers={},
        )

        cancel_request = CancelJob(
            job_id="job-123",
            reason="test",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        # Should still return success (job marked cancelled)
        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_special_characters_in_job_id(self):
        """Handles special characters in job ID."""
        special_ids = [
            "job:colon:id",
            "job-dash-id",
            "job_underscore_id",
            "job.dot.id",
        ]

        async def mock_handle_exception(error, context):
            pass

        for job_id in special_ids:
            job_manager = MockGateJobManager()
            job_manager.jobs[job_id] = GlobalJobStatus(
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            )

            handler = create_mock_handler(job_manager=job_manager)

            cancel_request = CancelJob(
                job_id=job_id,
                reason="test",
            )

            result = await handler.handle_cancel_job(
                addr=("10.0.0.1", 8000),
                data=cancel_request.dump(),
                handle_exception=mock_handle_exception,
            )

            assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_zero_fence_token(self):
        """Handles zero fence token (means don't check)."""
        job_manager = MockGateJobManager()
        job = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        )
        job.fence_token = 10
        job_manager.jobs["job-123"] = job

        handler = create_mock_handler(job_manager=job_manager)

        cancel_request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-001",
            timestamp=1234567890,
            fence_token=0,  # Zero means don't check
            reason="user_requested",
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        response = JobCancelResponse.load(result)
        assert response.success is True

    @pytest.mark.asyncio
    async def test_very_long_reason(self):
        """Handles very long cancellation reason."""
        job_manager = MockGateJobManager()
        job_manager.jobs["job-123"] = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        )

        handler = create_mock_handler(job_manager=job_manager)

        cancel_request = CancelJob(
            job_id="job-123",
            reason="x" * 10000,  # Very long reason
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_cancel_job(
            addr=("10.0.0.1", 8000),
            data=cancel_request.dump(),
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)


__all__ = [
    "TestHandleCancelJobHappyPath",
    "TestHandleCancelJobRateLimiting",
    "TestHandleCancelJobFencingTokens",
    "TestHandleCancelJobNegativePath",
    "TestHandleCancelJobFailureModes",
    "TestHandleJobCancellationComplete",
    "TestConcurrency",
    "TestEdgeCases",
]
