"""
Integration tests for ClientJobSubmitter and ClientCancellationManager (Sections 15.1.9, 15.1.10).

Tests job submission with retry logic, leader redirection, protocol negotiation,
and job cancellation with completion tracking.

Covers:
- Happy path: Successful submission/cancellation
- Negative path: No targets, invalid workflows, rejection
- Failure mode: Network errors, timeouts, leader redirects
- Concurrency: Concurrent submissions/cancellations
- Edge cases: Large workflows, rate limiting, transient errors
"""

import asyncio
import secrets
from unittest.mock import Mock, AsyncMock, patch

import pytest
import cloudpickle

from hyperscale.distributed.nodes.client.submission import ClientJobSubmitter
from hyperscale.distributed.nodes.client.cancellation import ClientCancellationManager
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.targets import ClientTargetSelector
from hyperscale.distributed.nodes.client.protocol import ClientProtocol
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker
from hyperscale.distributed.models import (
    JobAck,
    JobCancelResponse,
    RateLimitResponse,
)
from hyperscale.distributed.errors import MessageTooLargeError
from hyperscale.logging import Logger


class TestClientJobSubmitter:
    """Test ClientJobSubmitter class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000), ("m2", 7001)],
            gates=[("g1", 9000)],
        )
        self.state = ClientState()
        self.logger = Mock(spec=Logger)
        self.logger.log = AsyncMock()
        self.targets = ClientTargetSelector(self.config, self.state)
        self.tracker = ClientJobTracker(self.state, self.logger)
        self.protocol = ClientProtocol(self.state, self.logger)

    @pytest.mark.asyncio
    async def test_happy_path_successful_submission(self):
        """Test successful job submission."""
        send_tcp = AsyncMock()

        # Mock successful acceptance
        ack = JobAck(
            job_id="job-123",
            accepted=True,
            error=None,
            queued_position=0,
            protocol_version_major=1,
            protocol_version_minor=0,
            capabilities="feature1,feature2",
        )
        send_tcp.return_value = (ack.dump(), None)

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        # Simple workflow
        workflow = Mock()
        workflow.reporting = None
        workflows = [([], workflow)]

        job_id = await submitter.submit_job(workflows)

        assert job_id.startswith("job-")
        assert send_tcp.called
        # Should have stored negotiated capabilities
        assert len(self.state._server_negotiated_caps) > 0

    @pytest.mark.asyncio
    async def test_submission_with_callbacks(self):
        """Test submission with all callbacks."""
        send_tcp = AsyncMock()
        ack = JobAck(job_id="job-callbacks", accepted=True)
        send_tcp.return_value = (ack.dump(), None)

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        status_callback = Mock()
        progress_callback = Mock()
        workflow_callback = Mock()
        reporter_callback = Mock()

        workflow = Mock()
        workflow.reporting = None

        job_id = await submitter.submit_job(
            [([], workflow)],
            on_status_update=status_callback,
            on_progress_update=progress_callback,
            on_workflow_result=workflow_callback,
            on_reporter_result=reporter_callback,
        )

        # Should have registered callbacks
        assert job_id in self.state._job_callbacks
        assert job_id in self.state._progress_callbacks

    @pytest.mark.asyncio
    async def test_submission_with_leader_redirect(self):
        """Test submission with leader redirect."""
        send_tcp = AsyncMock()

        # First response: redirect
        redirect_ack = JobAck(
            job_id="job-redirect",
            accepted=False,
            leader_addr=("leader", 8000),
        )

        # Second response: accepted
        accept_ack = JobAck(
            job_id="job-redirect",
            accepted=True,
        )

        send_tcp.side_effect = [
            (redirect_ack.dump(), None),
            (accept_ack.dump(), None),
        ]

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        workflow = Mock()
        workflow.reporting = None

        job_id = await submitter.submit_job([([], workflow)])

        # Should have followed redirect (2 calls)
        assert send_tcp.call_count == 2
        assert job_id.startswith("job-")

    @pytest.mark.asyncio
    async def test_submission_with_transient_error_retry(self):
        """Test retry on transient error."""
        send_tcp = AsyncMock()

        # First: transient error
        error_ack = JobAck(
            job_id="job-transient",
            accepted=False,
            error="syncing",  # Transient error
        )

        # Second: success
        success_ack = JobAck(
            job_id="job-transient",
            accepted=True,
        )

        send_tcp.side_effect = [
            (error_ack.dump(), None),
            (success_ack.dump(), None),
        ]

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        workflow = Mock()
        workflow.reporting = None

        job_id = await submitter.submit_job([([], workflow)])

        # Should have retried
        assert send_tcp.call_count == 2

    @pytest.mark.asyncio
    async def test_submission_failure_permanent_error(self):
        """Test permanent error causes immediate failure."""
        send_tcp = AsyncMock()

        # Permanent rejection
        reject_ack = JobAck(
            job_id="job-reject",
            accepted=False,
            error="Invalid workflow",  # Permanent error
        )

        send_tcp.return_value = (reject_ack.dump(), None)

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        workflow = Mock()
        workflow.reporting = None

        with pytest.raises(RuntimeError, match="Job rejected"):
            await submitter.submit_job([([], workflow)])

    @pytest.mark.asyncio
    async def test_submission_with_rate_limiting(self):
        """Test handling of rate limit response (AD-32)."""
        send_tcp = AsyncMock()

        # First: rate limited
        rate_limit = RateLimitResponse(
            operation="job_submission",
            retry_after_seconds=0.01,
            error="Rate limit exceeded",
        )

        # Second: success
        success_ack = JobAck(job_id="job-rate", accepted=True)

        send_tcp.side_effect = [
            (rate_limit.dump(), None),
            (success_ack.dump(), None),
        ]

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        workflow = Mock()
        workflow.reporting = None

        job_id = await submitter.submit_job([([], workflow)])

        # Should have retried after rate limit
        assert send_tcp.call_count == 2

    @pytest.mark.asyncio
    async def test_submission_size_validation(self):
        """Test pre-submission size validation."""
        send_tcp = AsyncMock()

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        # Create huge workflow that exceeds 5MB
        huge_data = "x" * (6 * 1024 * 1024)  # 6MB
        workflow = Mock()
        workflow.reporting = None
        workflow.huge_field = huge_data

        with pytest.raises(MessageTooLargeError):
            await submitter.submit_job([([], workflow)])

    @pytest.mark.asyncio
    async def test_no_targets_configured(self):
        """Test failure when no targets available."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],
            gates=[],
        )
        state = ClientState()
        targets = ClientTargetSelector(config, state)
        send_tcp = AsyncMock()

        submitter = ClientJobSubmitter(
            state,
            config,
            self.logger,
            targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        workflow = Mock()
        workflow.reporting = None

        with pytest.raises(RuntimeError, match="No managers or gates"):
            await submitter.submit_job([([], workflow)])

    @pytest.mark.asyncio
    async def test_edge_case_many_workflows(self):
        """Test submission with many workflows."""
        send_tcp = AsyncMock()
        ack = JobAck(job_id="many-workflows", accepted=True)
        send_tcp.return_value = (ack.dump(), None)

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        # 100 workflows
        workflows = []
        for i in range(100):
            workflow = Mock()
            workflow.reporting = None
            workflows.append(([], workflow))

        job_id = await submitter.submit_job(workflows)

        assert job_id.startswith("job-")

    @pytest.mark.asyncio
    async def test_concurrent_submissions(self):
        """Test concurrent job submissions."""
        send_tcp = AsyncMock()

        def create_ack(job_id):
            return JobAck(job_id=job_id, accepted=True).dump()

        send_tcp.side_effect = [
            (create_ack(f"job-{i}"), None) for i in range(10)
        ]

        submitter = ClientJobSubmitter(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            self.protocol,
            send_tcp,
        )

        async def submit_job():
            workflow = Mock()
            workflow.reporting = None
            return await submitter.submit_job([([], workflow)])

        job_ids = await asyncio.gather(*[submit_job() for _ in range(10)])

        assert len(job_ids) == 10
        assert all(jid.startswith("job-") for jid in job_ids)


class TestClientCancellationManager:
    """Test ClientCancellationManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000)],
            gates=[("g1", 9000)],
        )
        self.state = ClientState()
        self.logger = Mock(spec=Logger)
        self.logger.log = AsyncMock()
        self.targets = ClientTargetSelector(self.config, self.state)
        self.tracker = ClientJobTracker(self.state, self.logger)

    @pytest.mark.asyncio
    async def test_happy_path_successful_cancellation(self):
        """Test successful job cancellation."""
        send_tcp = AsyncMock()

        # Successful cancellation
        response = JobCancelResponse(
            job_id="cancel-job-123",
            success=True,
            cancelled_workflow_count=5,
        )
        send_tcp.return_value = (response.dump(), None)

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        job_id = "cancel-job-123"
        self.tracker.initialize_job_tracking(job_id)

        result = await manager.cancel_job(job_id, reason="User requested")

        assert result.success is True
        assert result.cancelled_workflow_count == 5
        assert send_tcp.called

    @pytest.mark.asyncio
    async def test_cancellation_with_retry(self):
        """Test cancellation retry on transient error."""
        send_tcp = AsyncMock()

        # First: transient error
        error_response = JobCancelResponse(
            job_id="retry-cancel",
            success=False,
            error="syncing",  # Transient
        )

        # Second: success
        success_response = JobCancelResponse(
            job_id="retry-cancel",
            success=True,
            cancelled_workflow_count=3,
        )

        send_tcp.side_effect = [
            (error_response.dump(), None),
            (success_response.dump(), None),
        ]

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        job_id = "retry-cancel"
        self.tracker.initialize_job_tracking(job_id)

        result = await manager.cancel_job(job_id)

        assert result.success is True
        assert send_tcp.call_count == 2

    @pytest.mark.asyncio
    async def test_cancellation_already_cancelled(self):
        """Test cancelling already cancelled job."""
        send_tcp = AsyncMock()

        response = JobCancelResponse(
            job_id="already-cancelled",
            success=False,
            already_cancelled=True,
        )
        send_tcp.return_value = (response.dump(), None)

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        job_id = "already-cancelled"
        self.tracker.initialize_job_tracking(job_id)

        result = await manager.cancel_job(job_id)

        assert result.already_cancelled is True
        # Should update status to cancelled
        assert self.state._jobs[job_id].status == "cancelled"

    @pytest.mark.asyncio
    async def test_cancellation_already_completed(self):
        """Test cancelling already completed job."""
        send_tcp = AsyncMock()

        response = JobCancelResponse(
            job_id="already-done",
            success=False,
            already_completed=True,
        )
        send_tcp.return_value = (response.dump(), None)

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        job_id = "already-done"
        self.tracker.initialize_job_tracking(job_id)

        result = await manager.cancel_job(job_id)

        assert result.already_completed is True
        assert self.state._jobs[job_id].status == "completed"

    @pytest.mark.asyncio
    async def test_cancellation_with_rate_limiting(self):
        """Test rate limit handling in cancellation (AD-32)."""
        send_tcp = AsyncMock()

        # Rate limited
        rate_limit = RateLimitResponse(
            operation="cancel_job",
            retry_after_seconds=0.01,
        )

        # Success
        success = JobCancelResponse(
            job_id="rate-cancel",
            success=True,
        )

        send_tcp.side_effect = [
            (rate_limit.dump(), None),
            (success.dump(), None),
        ]

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        job_id = "rate-cancel"
        self.tracker.initialize_job_tracking(job_id)

        result = await manager.cancel_job(job_id)

        assert result.success is True
        assert send_tcp.call_count == 2

    @pytest.mark.asyncio
    async def test_cancellation_permanent_failure(self):
        """Test permanent cancellation failure."""
        send_tcp = AsyncMock()

        response = JobCancelResponse(
            job_id="fail-cancel",
            success=False,
            error="Job not found",  # Permanent error
        )
        send_tcp.return_value = (response.dump(), None)

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        job_id = "fail-cancel"
        self.tracker.initialize_job_tracking(job_id)

        with pytest.raises(RuntimeError, match="Job cancellation failed"):
            await manager.cancel_job(job_id)

    @pytest.mark.asyncio
    async def test_await_job_cancellation_success(self):
        """Test waiting for cancellation completion."""
        send_tcp = AsyncMock()
        response = JobCancelResponse(job_id="wait-cancel", success=True)
        send_tcp.return_value = (response.dump(), None)

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        job_id = "wait-cancel"
        self.tracker.initialize_job_tracking(job_id)
        self.state.initialize_cancellation_tracking(job_id)

        async def complete_cancellation():
            await manager.cancel_job(job_id)
            # Signal completion
            self.state._cancellation_success[job_id] = True
            self.state._cancellation_events[job_id].set()

        success, errors = await asyncio.gather(
            manager.await_job_cancellation(job_id),
            complete_cancellation(),
        )

        assert success[0] is True
        assert success[1] == []

    @pytest.mark.asyncio
    async def test_await_job_cancellation_timeout(self):
        """Test cancellation wait timeout."""
        send_tcp = AsyncMock()

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        job_id = "timeout-cancel"
        self.state.initialize_cancellation_tracking(job_id)

        success, errors = await manager.await_job_cancellation(
            job_id,
            timeout=0.05
        )

        assert success is False
        assert "Timeout" in errors[0]

    @pytest.mark.asyncio
    async def test_no_targets_configured(self):
        """Test cancellation with no targets."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],
            gates=[],
        )
        state = ClientState()
        targets = ClientTargetSelector(config, state)
        send_tcp = AsyncMock()

        manager = ClientCancellationManager(
            state,
            config,
            self.logger,
            targets,
            self.tracker,
            send_tcp,
        )

        with pytest.raises(RuntimeError, match="No managers or gates"):
            await manager.cancel_job("no-targets-job")

    @pytest.mark.asyncio
    async def test_concurrent_cancellations(self):
        """Test concurrent cancellation requests."""
        send_tcp = AsyncMock()

        def create_response(job_id):
            return JobCancelResponse(job_id=job_id, success=True).dump()

        send_tcp.side_effect = [
            (create_response(f"job-{i}"), None) for i in range(10)
        ]

        manager = ClientCancellationManager(
            self.state,
            self.config,
            self.logger,
            self.targets,
            self.tracker,
            send_tcp,
        )

        # Initialize jobs
        for i in range(10):
            self.tracker.initialize_job_tracking(f"job-{i}")

        async def cancel_job(job_id):
            return await manager.cancel_job(job_id)

        results = await asyncio.gather(*[
            cancel_job(f"job-{i}") for i in range(10)
        ])

        assert all(r.success for r in results)
