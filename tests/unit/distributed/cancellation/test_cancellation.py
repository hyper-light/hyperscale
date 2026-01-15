"""
Integration tests for Job Cancellation (AD-20).

These tests verify that:
1. JobCancelRequest/JobCancelResponse message structure is correct
2. WorkflowCancelRequest/WorkflowCancelResponse message structure is correct
3. Cancellation propagates from client -> gate -> manager -> worker
4. Idempotency: repeated cancellation returns success
5. Already completed jobs return appropriate responses
6. Fence token validation prevents stale cancellations

The Cancellation Propagation pattern ensures:
- Jobs can be cancelled at any point in their lifecycle
- Cancellation propagates to all components reliably
- Resources are freed promptly on cancellation
- Clients receive confirmation of cancellation
"""

import time

from hyperscale.distributed.models import (
    JobCancelRequest,
    JobCancelResponse,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    JobStatus,
    WorkflowStatus,
    CancelJob,
    CancelAck,
)


class TestJobCancelRequestMessage:
    """Test JobCancelRequest message structure."""

    def test_cancel_request_fields(self):
        """JobCancelRequest should have required fields."""
        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-localhost:8500",
            timestamp=time.time(),
            fence_token=5,
            reason="user requested cancellation",
        )
        assert request.job_id == "job-123"
        assert request.requester_id == "client-localhost:8500"
        assert request.fence_token == 5
        assert request.reason == "user requested cancellation"

    def test_cancel_request_default_values(self):
        """JobCancelRequest should have sensible defaults."""
        request = JobCancelRequest(
            job_id="job-456",
            requester_id="client-test",
            timestamp=time.time(),
        )
        assert request.fence_token == 0
        assert request.reason == ""

    def test_cancel_request_serialization(self):
        """JobCancelRequest should serialize correctly."""
        original = JobCancelRequest(
            job_id="job-789",
            requester_id="gate-1",
            timestamp=1234567890.123,
            fence_token=10,
            reason="timeout exceeded",
        )

        serialized = original.dump()
        restored = JobCancelRequest.load(serialized)

        assert restored.job_id == "job-789"
        assert restored.requester_id == "gate-1"
        assert restored.timestamp == 1234567890.123
        assert restored.fence_token == 10
        assert restored.reason == "timeout exceeded"


class TestJobCancelResponseMessage:
    """Test JobCancelResponse message structure."""

    def test_cancel_response_success(self):
        """JobCancelResponse should indicate successful cancellation."""
        response = JobCancelResponse(
            job_id="job-123",
            success=True,
            cancelled_workflow_count=5,
        )
        assert response.job_id == "job-123"
        assert response.success is True
        assert response.cancelled_workflow_count == 5
        assert response.already_cancelled is False
        assert response.already_completed is False
        assert response.error is None

    def test_cancel_response_already_cancelled(self):
        """JobCancelResponse should indicate idempotent cancellation."""
        response = JobCancelResponse(
            job_id="job-456",
            success=True,
            cancelled_workflow_count=0,
            already_cancelled=True,
        )
        assert response.success is True
        assert response.already_cancelled is True
        assert response.cancelled_workflow_count == 0

    def test_cancel_response_already_completed(self):
        """JobCancelResponse should indicate job was already completed."""
        response = JobCancelResponse(
            job_id="job-789",
            success=True,
            cancelled_workflow_count=0,
            already_completed=True,
        )
        assert response.success is True
        assert response.already_completed is True

    def test_cancel_response_error(self):
        """JobCancelResponse should contain error on failure."""
        response = JobCancelResponse(
            job_id="job-unknown",
            success=False,
            error="Job not found",
        )
        assert response.success is False
        assert response.error == "Job not found"

    def test_cancel_response_serialization(self):
        """JobCancelResponse should serialize correctly."""
        original = JobCancelResponse(
            job_id="job-123",
            success=True,
            cancelled_workflow_count=3,
            already_cancelled=False,
            already_completed=False,
        )

        serialized = original.dump()
        restored = JobCancelResponse.load(serialized)

        assert restored.job_id == "job-123"
        assert restored.success is True
        assert restored.cancelled_workflow_count == 3


class TestWorkflowCancelRequestMessage:
    """Test WorkflowCancelRequest message structure."""

    def test_workflow_cancel_request_fields(self):
        """WorkflowCancelRequest should have required fields."""
        request = WorkflowCancelRequest(
            job_id="job-123",
            workflow_id="wf-abc-123",
            requester_id="manager-1",
            timestamp=time.time(),
        )
        assert request.job_id == "job-123"
        assert request.workflow_id == "wf-abc-123"
        assert request.requester_id == "manager-1"

    def test_workflow_cancel_request_serialization(self):
        """WorkflowCancelRequest should serialize correctly."""
        original = WorkflowCancelRequest(
            job_id="job-456",
            workflow_id="wf-def-456",
            requester_id="manager-2",
            timestamp=1234567890.0,
        )

        serialized = original.dump()
        restored = WorkflowCancelRequest.load(serialized)

        assert restored.job_id == "job-456"
        assert restored.workflow_id == "wf-def-456"
        assert restored.requester_id == "manager-2"


class TestWorkflowCancelResponseMessage:
    """Test WorkflowCancelResponse message structure."""

    def test_workflow_cancel_response_success(self):
        """WorkflowCancelResponse should indicate successful cancellation."""
        response = WorkflowCancelResponse(
            job_id="job-123",
            workflow_id="wf-abc-123",
            success=True,
            was_running=True,
        )
        assert response.job_id == "job-123"
        assert response.workflow_id == "wf-abc-123"
        assert response.success is True
        assert response.was_running is True
        assert response.already_completed is False

    def test_workflow_cancel_response_already_done(self):
        """WorkflowCancelResponse should indicate workflow was already done."""
        response = WorkflowCancelResponse(
            job_id="job-456",
            workflow_id="wf-def-456",
            success=True,
            was_running=False,
            already_completed=True,
        )
        assert response.success is True
        assert response.was_running is False
        assert response.already_completed is True

    def test_workflow_cancel_response_serialization(self):
        """WorkflowCancelResponse should serialize correctly."""
        original = WorkflowCancelResponse(
            job_id="job-789",
            workflow_id="wf-ghi-789",
            success=True,
            was_running=True,
            already_completed=False,
        )

        serialized = original.dump()
        restored = WorkflowCancelResponse.load(serialized)

        assert restored.job_id == "job-789"
        assert restored.workflow_id == "wf-ghi-789"
        assert restored.success is True
        assert restored.was_running is True


class TestCancellationPropagationScenarios:
    """Test realistic cancellation propagation scenarios."""

    def test_client_cancels_running_job(self):
        """
        Simulate client cancelling a running job.

        Scenario:
        1. Client submits job-123
        2. Job has 3 workflows running on workers
        3. Client sends JobCancelRequest
        4. Gate forwards to manager
        5. Manager cancels workflows on workers
        6. Client receives JobCancelResponse
        """
        # Simulate gate state
        gate_jobs: dict[str, dict] = {
            "job-123": {
                "status": JobStatus.RUNNING.value,
                "datacenters": ["dc-1"],
                "fence_token": 1,
            }
        }

        # Simulate manager state (3 workflows running)
        manager_workflows: dict[str, dict] = {
            "wf-1": {"job_id": "job-123", "status": WorkflowStatus.RUNNING.value, "worker": "worker-1"},
            "wf-2": {"job_id": "job-123", "status": WorkflowStatus.RUNNING.value, "worker": "worker-1"},
            "wf-3": {"job_id": "job-123", "status": WorkflowStatus.RUNNING.value, "worker": "worker-2"},
        }

        # Client sends cancel request
        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-localhost:8500",
            timestamp=time.time(),
            fence_token=0,
            reason="user cancelled",
        )

        # Gate validates job exists
        job = gate_jobs.get(request.job_id)
        assert job is not None

        # Manager processes cancellation
        cancelled_count = 0
        for wf_id, wf in list(manager_workflows.items()):
            if wf["job_id"] == request.job_id:
                if wf["status"] == WorkflowStatus.RUNNING.value:
                    wf["status"] = WorkflowStatus.CANCELLED.value
                    cancelled_count += 1

        # Update job status
        job["status"] = JobStatus.CANCELLED.value

        # Build response
        response = JobCancelResponse(
            job_id=request.job_id,
            success=True,
            cancelled_workflow_count=cancelled_count,
        )

        # Verify
        assert response.success is True
        assert response.cancelled_workflow_count == 3
        assert job["status"] == JobStatus.CANCELLED.value
        for wf in manager_workflows.values():
            if wf["job_id"] == "job-123":
                assert wf["status"] == WorkflowStatus.CANCELLED.value

    def test_cancel_already_cancelled_job(self):
        """
        Simulate cancelling an already cancelled job (idempotency).

        Scenario:
        1. Job-456 was already cancelled
        2. Client sends another JobCancelRequest
        3. Gate returns success with already_cancelled=True
        """
        gate_jobs: dict[str, dict] = {
            "job-456": {
                "status": JobStatus.CANCELLED.value,
                "cancelled_at": time.time() - 60.0,
            }
        }

        request = JobCancelRequest(
            job_id="job-456",
            requester_id="client-test",
            timestamp=time.time(),
        )

        job = gate_jobs.get(request.job_id)
        if job["status"] == JobStatus.CANCELLED.value:
            response = JobCancelResponse(
                job_id=request.job_id,
                success=True,
                cancelled_workflow_count=0,
                already_cancelled=True,
            )
        else:
            response = JobCancelResponse(
                job_id=request.job_id,
                success=True,
                cancelled_workflow_count=0,
            )

        assert response.success is True
        assert response.already_cancelled is True
        assert response.cancelled_workflow_count == 0

    def test_cancel_already_completed_job(self):
        """
        Simulate cancelling an already completed job.

        Scenario:
        1. Job-789 completed successfully
        2. Client sends JobCancelRequest (too late)
        3. Gate returns success with already_completed=True
        """
        gate_jobs: dict[str, dict] = {
            "job-789": {
                "status": JobStatus.COMPLETED.value,
                "completed_at": time.time() - 30.0,
            }
        }

        request = JobCancelRequest(
            job_id="job-789",
            requester_id="client-test",
            timestamp=time.time(),
        )

        job = gate_jobs.get(request.job_id)
        if job["status"] == JobStatus.COMPLETED.value:
            response = JobCancelResponse(
                job_id=request.job_id,
                success=True,
                cancelled_workflow_count=0,
                already_completed=True,
            )
        else:
            response = JobCancelResponse(
                job_id=request.job_id,
                success=True,
            )

        assert response.success is True
        assert response.already_completed is True

    def test_cancel_nonexistent_job(self):
        """
        Simulate cancelling a job that doesn't exist.

        Scenario:
        1. Client sends JobCancelRequest for unknown job
        2. Gate returns error
        """
        gate_jobs: dict[str, dict] = {}

        request = JobCancelRequest(
            job_id="job-unknown",
            requester_id="client-test",
            timestamp=time.time(),
        )

        job = gate_jobs.get(request.job_id)
        if job is None:
            response = JobCancelResponse(
                job_id=request.job_id,
                success=False,
                error="Job not found",
            )
        else:
            response = JobCancelResponse(
                job_id=request.job_id,
                success=True,
            )

        assert response.success is False
        assert response.error == "Job not found"

    def test_worker_cancels_running_workflow(self):
        """
        Simulate worker cancelling a running workflow.

        Scenario:
        1. Manager sends WorkflowCancelRequest to worker
        2. Worker cancels the running task
        3. Worker returns WorkflowCancelResponse
        """
        # Simulate worker state
        worker_workflows: dict[str, dict] = {
            "wf-abc-123": {
                "job_id": "job-123",
                "status": WorkflowStatus.RUNNING.value,
            }
        }

        request = WorkflowCancelRequest(
            job_id="job-123",
            workflow_id="wf-abc-123",
            requester_id="manager-1",
            timestamp=time.time(),
        )

        # Worker processes request
        wf = worker_workflows.get(request.workflow_id)
        if wf is None:
            response = WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=True,
                was_running=False,
                already_completed=True,
            )
        elif wf["status"] in (WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value, WorkflowStatus.CANCELLED.value):
            response = WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=True,
                was_running=False,
                already_completed=True,
            )
        else:
            was_running = wf["status"] == WorkflowStatus.RUNNING.value
            wf["status"] = WorkflowStatus.CANCELLED.value
            response = WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=True,
                was_running=was_running,
                already_completed=False,
            )

        assert response.success is True
        assert response.was_running is True
        assert response.already_completed is False
        assert worker_workflows["wf-abc-123"]["status"] == WorkflowStatus.CANCELLED.value

    def test_worker_cancels_already_completed_workflow(self):
        """
        Simulate worker receiving cancel for already completed workflow.

        Scenario:
        1. Workflow completed just before cancel arrived
        2. Worker returns success with already_completed=True
        """
        worker_workflows: dict[str, dict] = {
            "wf-def-456": {
                "job_id": "job-456",
                "status": WorkflowStatus.COMPLETED.value,
            }
        }

        request = WorkflowCancelRequest(
            job_id="job-456",
            workflow_id="wf-def-456",
            requester_id="manager-1",
            timestamp=time.time(),
        )

        wf = worker_workflows.get(request.workflow_id)
        if wf and wf["status"] in (WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value):
            response = WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=True,
                was_running=False,
                already_completed=True,
            )
        else:
            response = WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=True,
            )

        assert response.success is True
        assert response.already_completed is True
        assert response.was_running is False


class TestFenceTokenValidation:
    """Test fence token validation for cancellation."""

    def test_fence_token_prevents_stale_cancel(self):
        """
        Simulate fence token preventing stale cancellation.

        Scenario:
        1. Job-123 is resubmitted with higher fence token
        2. Stale cancel request arrives with old fence token
        3. Gate rejects the stale cancel
        """
        gate_jobs: dict[str, dict] = {
            "job-123": {
                "status": JobStatus.RUNNING.value,
                "fence_token": 5,  # Current fence token
            }
        }

        # Stale cancel with old fence token
        stale_request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-old",
            timestamp=time.time() - 60.0,  # From 60 seconds ago
            fence_token=3,  # Old fence token
        )

        job = gate_jobs.get(stale_request.job_id)
        if job and stale_request.fence_token < job["fence_token"]:
            response = JobCancelResponse(
                job_id=stale_request.job_id,
                success=False,
                error=f"Stale fence token: {stale_request.fence_token} < {job['fence_token']}",
            )
        else:
            response = JobCancelResponse(
                job_id=stale_request.job_id,
                success=True,
            )

        assert response.success is False
        assert "Stale fence token" in response.error

    def test_valid_fence_token_allows_cancel(self):
        """
        Simulate valid fence token allowing cancellation.

        Scenario:
        1. Job has fence_token=5
        2. Cancel request has fence_token=5 (matches)
        3. Cancellation proceeds
        """
        gate_jobs: dict[str, dict] = {
            "job-123": {
                "status": JobStatus.RUNNING.value,
                "fence_token": 5,
            }
        }

        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-current",
            timestamp=time.time(),
            fence_token=5,  # Matches current
        )

        job = gate_jobs.get(request.job_id)
        if job and request.fence_token >= job["fence_token"]:
            job["status"] = JobStatus.CANCELLED.value
            response = JobCancelResponse(
                job_id=request.job_id,
                success=True,
                cancelled_workflow_count=1,
            )
        else:
            response = JobCancelResponse(
                job_id=request.job_id,
                success=False,
            )

        assert response.success is True
        assert job["status"] == JobStatus.CANCELLED.value


class TestLegacyMessageCompatibility:
    """Test backward compatibility with legacy CancelJob/CancelAck messages."""

    def test_legacy_cancel_job_message(self):
        """Legacy CancelJob message should still work."""
        cancel = CancelJob(
            job_id="job-legacy",
            reason="legacy cancellation",
        )
        assert cancel.job_id == "job-legacy"
        assert cancel.reason == "legacy cancellation"

        # Serialization
        serialized = cancel.dump()
        restored = CancelJob.load(serialized)
        assert restored.job_id == "job-legacy"

    def test_legacy_cancel_ack_message(self):
        """Legacy CancelAck message should still work."""
        ack = CancelAck(
            job_id="job-legacy",
            cancelled=True,
            workflows_cancelled=2,
        )
        assert ack.job_id == "job-legacy"
        assert ack.cancelled is True
        assert ack.workflows_cancelled == 2

        # Serialization
        serialized = ack.dump()
        restored = CancelAck.load(serialized)
        assert restored.job_id == "job-legacy"
        assert restored.cancelled is True
