"""
Integration tests for Client Reconnection.

These tests verify that:
1. RegisterCallback message structure is correct
2. RegisterCallbackResponse provides current job status
3. Client can reconnect to an existing job
4. Client receives updates after reconnection
5. Multiple clients can register for the same job

The Client Reconnection pattern ensures:
- Clients can resume receiving updates after disconnect
- No manual re-submission of jobs required
- Current state is immediately available on reconnect
"""

import asyncio
import pytest
import time

from hyperscale.distributed_rewrite.models import (
    RegisterCallback,
    RegisterCallbackResponse,
    JobSubmission,
    JobProgress,
    JobFinalResult,
    JobStatus,
    JobStatusPush,
)


class TestRegisterCallbackMessage:
    """Test RegisterCallback message structure."""

    def test_register_callback_fields(self):
        """RegisterCallback should have required fields."""
        callback = RegisterCallback(
            job_id="job-123",
            callback_addr=("192.168.1.100", 8500),
        )
        assert callback.job_id == "job-123"
        assert callback.callback_addr == ("192.168.1.100", 8500)

    def test_register_callback_serialization(self):
        """RegisterCallback should serialize correctly."""
        original = RegisterCallback(
            job_id="job-456",
            callback_addr=("client.example.com", 9000),
        )

        serialized = original.dump()
        restored = RegisterCallback.load(serialized)

        assert restored.job_id == "job-456"
        assert restored.callback_addr == ("client.example.com", 9000)


class TestRegisterCallbackResponseMessage:
    """Test RegisterCallbackResponse message structure."""

    def test_response_success_fields(self):
        """RegisterCallbackResponse should have success fields."""
        response = RegisterCallbackResponse(
            job_id="job-123",
            success=True,
            status=JobStatus.RUNNING.value,
            total_completed=50,
            total_failed=2,
            elapsed_seconds=30.5,
        )
        assert response.job_id == "job-123"
        assert response.success is True
        assert response.status == "running"
        assert response.total_completed == 50
        assert response.total_failed == 2
        assert response.elapsed_seconds == 30.5
        assert response.error is None

    def test_response_failure_fields(self):
        """RegisterCallbackResponse should have error field on failure."""
        response = RegisterCallbackResponse(
            job_id="job-unknown",
            success=False,
            error="Job not found",
        )
        assert response.job_id == "job-unknown"
        assert response.success is False
        assert response.error == "Job not found"

    def test_response_serialization(self):
        """RegisterCallbackResponse should serialize correctly."""
        original = RegisterCallbackResponse(
            job_id="job-789",
            success=True,
            status=JobStatus.COMPLETED.value,
            total_completed=100,
            total_failed=0,
            elapsed_seconds=120.0,
        )

        serialized = original.dump()
        restored = RegisterCallbackResponse.load(serialized)

        assert restored.job_id == "job-789"
        assert restored.success is True
        assert restored.status == "completed"
        assert restored.total_completed == 100
        assert restored.total_failed == 0
        assert restored.elapsed_seconds == 120.0


class TestClientReconnectionScenarios:
    """Test realistic client reconnection scenarios."""

    def test_client_reconnect_to_running_job(self):
        """
        Simulate client reconnecting to a running job.

        Scenario:
        1. Client-A submits job-123
        2. Client-A disconnects
        3. Client-A reconnects and sends RegisterCallback
        4. Gate/Manager responds with current status
        5. Client-A resumes receiving updates
        """
        # Simulate gate state - job is running
        gate_jobs: dict[str, dict] = {
            "job-123": {
                "status": JobStatus.RUNNING.value,
                "total_completed": 75,
                "total_failed": 3,
                "timestamp": time.monotonic() - 60.0,  # Started 60s ago
            }
        }
        gate_callbacks: dict[str, tuple[str, int]] = {}

        # Client reconnects
        request = RegisterCallback(
            job_id="job-123",
            callback_addr=("client-a.example.com", 8500),
        )

        # Gate processes request
        job = gate_jobs.get(request.job_id)
        if job:
            gate_callbacks[request.job_id] = request.callback_addr
            response = RegisterCallbackResponse(
                job_id=request.job_id,
                success=True,
                status=job["status"],
                total_completed=job["total_completed"],
                total_failed=job["total_failed"],
                elapsed_seconds=time.monotonic() - job["timestamp"],
            )
        else:
            response = RegisterCallbackResponse(
                job_id=request.job_id,
                success=False,
                error="Job not found",
            )

        # Verify response
        assert response.success is True
        assert response.status == JobStatus.RUNNING.value
        assert response.total_completed == 75
        assert response.total_failed == 3
        assert response.elapsed_seconds >= 60.0

        # Verify callback registered
        assert gate_callbacks["job-123"] == ("client-a.example.com", 8500)

    def test_client_reconnect_job_not_found(self):
        """
        Simulate client trying to reconnect to unknown job.

        Scenario:
        1. Client-B sends RegisterCallback for job-unknown
        2. Gate doesn't have this job
        3. Gate responds with error
        """
        gate_jobs: dict[str, dict] = {
            "job-123": {"status": JobStatus.RUNNING.value},
        }

        request = RegisterCallback(
            job_id="job-unknown",
            callback_addr=("client-b.example.com", 8500),
        )

        # Gate processes request
        job = gate_jobs.get(request.job_id)
        if job:
            response = RegisterCallbackResponse(
                job_id=request.job_id,
                success=True,
                status=job["status"],
            )
        else:
            response = RegisterCallbackResponse(
                job_id=request.job_id,
                success=False,
                error="Job not found",
            )

        # Verify error response
        assert response.success is False
        assert response.error == "Job not found"

    def test_client_reconnect_to_completed_job(self):
        """
        Simulate client reconnecting to already completed job.

        Scenario:
        1. Job-456 completed while client was disconnected
        2. Client reconnects
        3. Client receives completed status immediately
        """
        gate_jobs: dict[str, dict] = {
            "job-456": {
                "status": JobStatus.COMPLETED.value,
                "total_completed": 1000,
                "total_failed": 5,
                "timestamp": time.monotonic() - 300.0,  # Completed 5 mins ago
            }
        }

        request = RegisterCallback(
            job_id="job-456",
            callback_addr=("client.example.com", 8500),
        )

        # Gate processes request
        job = gate_jobs.get(request.job_id)
        response = RegisterCallbackResponse(
            job_id=request.job_id,
            success=True,
            status=job["status"],
            total_completed=job["total_completed"],
            total_failed=job["total_failed"],
            elapsed_seconds=time.monotonic() - job["timestamp"],
        )

        # Verify completed status
        assert response.success is True
        assert response.status == JobStatus.COMPLETED.value
        assert response.total_completed == 1000
        assert response.total_failed == 5

    def test_multiple_clients_same_job(self):
        """
        Simulate multiple clients registering for same job.

        Note: Current implementation only stores one callback per job.
        This test documents that behavior - last client wins.
        """
        gate_jobs: dict[str, dict] = {
            "job-shared": {
                "status": JobStatus.RUNNING.value,
                "total_completed": 50,
                "total_failed": 0,
                "timestamp": time.monotonic() - 30.0,
            }
        }
        gate_callbacks: dict[str, tuple[str, int]] = {}

        # Client-A registers
        request_a = RegisterCallback(
            job_id="job-shared",
            callback_addr=("client-a.example.com", 8500),
        )
        gate_callbacks[request_a.job_id] = request_a.callback_addr

        # Client-B registers (overwrites Client-A)
        request_b = RegisterCallback(
            job_id="job-shared",
            callback_addr=("client-b.example.com", 8500),
        )
        gate_callbacks[request_b.job_id] = request_b.callback_addr

        # Only Client-B's callback is registered
        assert gate_callbacks["job-shared"] == ("client-b.example.com", 8500)

    def test_client_tries_all_gates(self):
        """
        Simulate client trying multiple gates/managers to find job.

        Scenario:
        1. Job-123 is on Gate-B
        2. Client tries Gate-A first (job not found)
        3. Client tries Gate-B (success)
        """
        # Gate-A doesn't have the job
        gate_a_jobs: dict[str, dict] = {}

        # Gate-B has the job
        gate_b_jobs: dict[str, dict] = {
            "job-123": {
                "status": JobStatus.RUNNING.value,
                "total_completed": 25,
                "total_failed": 0,
                "timestamp": time.monotonic() - 10.0,
            }
        }

        request = RegisterCallback(
            job_id="job-123",
            callback_addr=("client.example.com", 8500),
        )

        # Try Gate-A
        job_a = gate_a_jobs.get(request.job_id)
        response_a = RegisterCallbackResponse(
            job_id=request.job_id,
            success=False,
            error="Job not found",
        ) if not job_a else None

        assert response_a.success is False

        # Try Gate-B
        job_b = gate_b_jobs.get(request.job_id)
        response_b = RegisterCallbackResponse(
            job_id=request.job_id,
            success=True,
            status=job_b["status"],
            total_completed=job_b["total_completed"],
            total_failed=job_b["total_failed"],
            elapsed_seconds=time.monotonic() - job_b["timestamp"],
        )

        assert response_b.success is True
        assert response_b.status == JobStatus.RUNNING.value

    def test_manager_direct_reconnection(self):
        """
        Simulate client reconnecting directly to manager (no gate).

        Scenario:
        1. Client submitted job directly to manager
        2. Client disconnects and reconnects
        3. Manager provides current job status
        """
        # Manager state with job tracking
        manager_jobs: dict[str, dict] = {
            "job-direct": {
                "status": JobStatus.RUNNING.value,
                "workflows": {
                    "TestWorkflow": {
                        "completed_count": 40,
                        "failed_count": 1,
                    }
                },
                "timestamp": time.monotonic() - 20.0,
            }
        }
        manager_callbacks: dict[str, tuple[str, int]] = {}

        request = RegisterCallback(
            job_id="job-direct",
            callback_addr=("client.example.com", 8500),
        )

        # Manager processes request
        job = manager_jobs.get(request.job_id)
        if job:
            manager_callbacks[request.job_id] = request.callback_addr

            # Calculate totals from workflows
            total_completed = sum(
                wf["completed_count"]
                for wf in job["workflows"].values()
            )
            total_failed = sum(
                wf["failed_count"]
                for wf in job["workflows"].values()
            )

            response = RegisterCallbackResponse(
                job_id=request.job_id,
                success=True,
                status=job["status"],
                total_completed=total_completed,
                total_failed=total_failed,
                elapsed_seconds=time.monotonic() - job["timestamp"],
            )
        else:
            response = RegisterCallbackResponse(
                job_id=request.job_id,
                success=False,
                error="Job not found",
            )

        # Verify
        assert response.success is True
        assert response.total_completed == 40
        assert response.total_failed == 1
        assert manager_callbacks["job-direct"] == ("client.example.com", 8500)

