"""
Server integration tests for Cancellation Propagation (AD-20).

Tests cancellation flows in realistic server scenarios with:
- Async cancellation propagation through node hierarchy (client -> gate -> manager -> worker)
- Concurrent cancellations for multiple jobs
- Race conditions between cancellation and completion
- Failure paths (node unavailable, timeout, partial failures)
- Idempotency and retry behavior
- Fence token validation across scenarios
- State consistency after cancellation
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

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


class NodeState(Enum):
    """State of a simulated node."""

    HEALTHY = "healthy"
    UNAVAILABLE = "unavailable"
    SLOW = "slow"


@dataclass
class WorkflowInfo:
    """Information about a workflow."""

    workflow_id: str
    job_id: str
    worker_id: str
    status: WorkflowStatus
    started_at: float = field(default_factory=time.time)


@dataclass
class JobInfo:
    """Information about a job."""

    job_id: str
    status: JobStatus
    workflows: list[str]
    fence_token: int = 1
    datacenter: str = "dc-1"
    created_at: float = field(default_factory=time.time)


class SimulatedWorker:
    """Simulated worker node for cancellation testing."""

    def __init__(self, worker_id: str):
        self._worker_id = worker_id
        self._workflows: dict[str, WorkflowInfo] = {}
        self._state = NodeState.HEALTHY
        self._response_delay = 0.0
        self._fail_next_request = False

    def add_workflow(self, workflow_info: WorkflowInfo) -> None:
        """Add a workflow to this worker."""
        self._workflows[workflow_info.workflow_id] = workflow_info

    def set_state(self, state: NodeState) -> None:
        """Set worker state."""
        self._state = state

    def set_response_delay(self, delay_seconds: float) -> None:
        """Set artificial delay for responses."""
        self._response_delay = delay_seconds

    def set_fail_next(self, should_fail: bool) -> None:
        """Set whether next request should fail."""
        self._fail_next_request = should_fail

    async def handle_cancel_request(
        self,
        request: WorkflowCancelRequest,
    ) -> WorkflowCancelResponse:
        """Handle a workflow cancellation request."""
        if self._state == NodeState.UNAVAILABLE:
            raise ConnectionError(f"Worker {self._worker_id} unavailable")

        if self._response_delay > 0:
            await asyncio.sleep(self._response_delay)

        if self._fail_next_request:
            self._fail_next_request = False
            return WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=False,
                error="Internal worker error",
            )

        workflow = self._workflows.get(request.workflow_id)
        if workflow is None:
            return WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=True,
                was_running=False,
                already_completed=True,
            )

        was_running = workflow.status == WorkflowStatus.RUNNING
        if workflow.status in (
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        ):
            return WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=True,
                was_running=False,
                already_completed=True,
            )

        workflow.status = WorkflowStatus.CANCELLED
        return WorkflowCancelResponse(
            job_id=request.job_id,
            workflow_id=request.workflow_id,
            success=True,
            was_running=was_running,
            already_completed=False,
        )

    def get_workflow(self, workflow_id: str) -> WorkflowInfo | None:
        """Get workflow info."""
        return self._workflows.get(workflow_id)


class SimulatedManager:
    """Simulated manager node for cancellation testing."""

    def __init__(self, manager_id: str):
        self._manager_id = manager_id
        self._workers: dict[str, SimulatedWorker] = {}
        self._workflow_assignments: dict[str, str] = {}  # workflow_id -> worker_id
        self._state = NodeState.HEALTHY
        self._response_delay = 0.0

    def register_worker(self, worker: SimulatedWorker, worker_id: str) -> None:
        """Register a worker with this manager."""
        self._workers[worker_id] = worker

    def assign_workflow(self, workflow_id: str, worker_id: str) -> None:
        """Assign a workflow to a worker."""
        self._workflow_assignments[workflow_id] = worker_id

    def set_state(self, state: NodeState) -> None:
        """Set manager state."""
        self._state = state

    def set_response_delay(self, delay_seconds: float) -> None:
        """Set artificial delay for responses."""
        self._response_delay = delay_seconds

    async def handle_job_cancel_request(
        self,
        request: JobCancelRequest,
        workflow_ids: list[str],
    ) -> JobCancelResponse:
        """Handle a job cancellation request by cancelling all workflows."""
        if self._state == NodeState.UNAVAILABLE:
            raise ConnectionError(f"Manager {self._manager_id} unavailable")

        if self._response_delay > 0:
            await asyncio.sleep(self._response_delay)

        cancelled_count = 0
        errors = []

        for workflow_id in workflow_ids:
            worker_id = self._workflow_assignments.get(workflow_id)
            if worker_id is None:
                continue

            worker = self._workers.get(worker_id)
            if worker is None:
                errors.append(f"Worker {worker_id} not found for workflow {workflow_id}")
                continue

            try:
                wf_request = WorkflowCancelRequest(
                    job_id=request.job_id,
                    workflow_id=workflow_id,
                    requester_id=self._manager_id,
                    timestamp=time.time(),
                )
                response = await worker.handle_cancel_request(wf_request)
                if response.success and not response.already_completed:
                    cancelled_count += 1
                elif not response.success and response.error:
                    errors.append(response.error)
            except ConnectionError as connection_error:
                errors.append(str(connection_error))

        if errors:
            return JobCancelResponse(
                job_id=request.job_id,
                success=False,
                cancelled_workflow_count=cancelled_count,
                error="; ".join(errors),
            )

        return JobCancelResponse(
            job_id=request.job_id,
            success=True,
            cancelled_workflow_count=cancelled_count,
        )


class SimulatedGate:
    """Simulated gate node for cancellation testing."""

    def __init__(self, gate_id: str):
        self._gate_id = gate_id
        self._jobs: dict[str, JobInfo] = {}
        self._managers: dict[str, SimulatedManager] = {}
        self._job_datacenter_map: dict[str, list[str]] = {}  # job_id -> datacenter_ids
        self._state = NodeState.HEALTHY

    def register_job(self, job_info: JobInfo) -> None:
        """Register a job with this gate."""
        self._jobs[job_info.job_id] = job_info
        if job_info.job_id not in self._job_datacenter_map:
            self._job_datacenter_map[job_info.job_id] = []
        self._job_datacenter_map[job_info.job_id].append(job_info.datacenter)

    def register_manager(
        self,
        manager: SimulatedManager,
        manager_id: str,
        datacenter: str,
    ) -> None:
        """Register a manager with this gate."""
        self._managers[f"{datacenter}:{manager_id}"] = manager

    def set_state(self, state: NodeState) -> None:
        """Set gate state."""
        self._state = state

    async def handle_cancel_request(
        self,
        request: JobCancelRequest,
    ) -> JobCancelResponse:
        """Handle a job cancellation request."""
        if self._state == NodeState.UNAVAILABLE:
            raise ConnectionError(f"Gate {self._gate_id} unavailable")

        job = self._jobs.get(request.job_id)
        if job is None:
            return JobCancelResponse(
                job_id=request.job_id,
                success=False,
                error="Job not found",
            )

        # Fence token validation
        if request.fence_token > 0 and request.fence_token < job.fence_token:
            return JobCancelResponse(
                job_id=request.job_id,
                success=False,
                error=f"Stale fence token: {request.fence_token} < {job.fence_token}",
            )

        # Check if already in terminal state
        if job.status == JobStatus.CANCELLED:
            return JobCancelResponse(
                job_id=request.job_id,
                success=True,
                cancelled_workflow_count=0,
                already_cancelled=True,
            )

        if job.status == JobStatus.COMPLETED:
            return JobCancelResponse(
                job_id=request.job_id,
                success=True,
                cancelled_workflow_count=0,
                already_completed=True,
            )

        # Forward to managers in all datacenters
        total_cancelled = 0
        errors = []

        datacenters = self._job_datacenter_map.get(request.job_id, [])
        for datacenter in datacenters:
            for manager_key, manager in self._managers.items():
                if manager_key.startswith(datacenter):
                    try:
                        response = await manager.handle_job_cancel_request(
                            request,
                            job.workflows,
                        )
                        total_cancelled += response.cancelled_workflow_count
                        if not response.success and response.error:
                            errors.append(response.error)
                    except ConnectionError as connection_error:
                        errors.append(str(connection_error))

        # Update job status
        job.status = JobStatus.CANCELLED

        if errors:
            return JobCancelResponse(
                job_id=request.job_id,
                success=True,  # Partial success
                cancelled_workflow_count=total_cancelled,
                error="; ".join(errors),
            )

        return JobCancelResponse(
            job_id=request.job_id,
            success=True,
            cancelled_workflow_count=total_cancelled,
        )

    def get_job(self, job_id: str) -> JobInfo | None:
        """Get job info."""
        return self._jobs.get(job_id)


class TestCancellationBasicFlow:
    """Test basic cancellation flow through node hierarchy."""

    @pytest.mark.asyncio
    async def test_simple_job_cancellation(self) -> None:
        """Test simple job cancellation flow: client -> gate -> manager -> worker."""
        # Setup infrastructure
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        # Create job with 2 workflows
        job = JobInfo(
            job_id="job-123",
            status=JobStatus.RUNNING,
            workflows=["wf-1", "wf-2"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        # Assign workflows to worker
        for workflow_id in job.workflows:
            workflow_info = WorkflowInfo(
                workflow_id=workflow_id,
                job_id=job.job_id,
                worker_id="worker-1",
                status=WorkflowStatus.RUNNING,
            )
            worker.add_workflow(workflow_info)
            manager.assign_workflow(workflow_id, "worker-1")

        # Send cancellation request
        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-1",
            timestamp=time.time(),
            reason="user requested",
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is True
        assert response.cancelled_workflow_count == 2
        assert response.already_cancelled is False
        assert response.already_completed is False

        # Verify job and workflows are cancelled
        assert gate.get_job("job-123").status == JobStatus.CANCELLED
        for workflow_id in job.workflows:
            assert worker.get_workflow(workflow_id).status == WorkflowStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_multi_worker_cancellation(self) -> None:
        """Test cancellation across multiple workers."""
        worker_1 = SimulatedWorker("worker-1")
        worker_2 = SimulatedWorker("worker-2")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker_1, "worker-1")
        manager.register_worker(worker_2, "worker-2")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-456",
            status=JobStatus.RUNNING,
            workflows=["wf-1", "wf-2", "wf-3", "wf-4"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        # Distribute workflows across workers
        for idx, workflow_id in enumerate(job.workflows):
            worker = worker_1 if idx % 2 == 0 else worker_2
            worker_id = "worker-1" if idx % 2 == 0 else "worker-2"
            workflow_info = WorkflowInfo(
                workflow_id=workflow_id,
                job_id=job.job_id,
                worker_id=worker_id,
                status=WorkflowStatus.RUNNING,
            )
            worker.add_workflow(workflow_info)
            manager.assign_workflow(workflow_id, worker_id)

        request = JobCancelRequest(
            job_id="job-456",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is True
        assert response.cancelled_workflow_count == 4

        # Verify all workflows cancelled on both workers
        for workflow_id in ["wf-1", "wf-3"]:
            assert worker_1.get_workflow(workflow_id).status == WorkflowStatus.CANCELLED
        for workflow_id in ["wf-2", "wf-4"]:
            assert worker_2.get_workflow(workflow_id).status == WorkflowStatus.CANCELLED


class TestCancellationIdempotency:
    """Test idempotent cancellation behavior."""

    @pytest.mark.asyncio
    async def test_cancel_already_cancelled_job(self) -> None:
        """Test that cancelling an already cancelled job returns success with flag."""
        gate = SimulatedGate("gate-1")

        job = JobInfo(
            job_id="job-123",
            status=JobStatus.CANCELLED,
            workflows=[],
            datacenter="dc-1",
        )
        gate.register_job(job)

        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is True
        assert response.already_cancelled is True
        assert response.cancelled_workflow_count == 0

    @pytest.mark.asyncio
    async def test_cancel_completed_job(self) -> None:
        """Test that cancelling a completed job returns success with flag."""
        gate = SimulatedGate("gate-1")

        job = JobInfo(
            job_id="job-456",
            status=JobStatus.COMPLETED,
            workflows=[],
            datacenter="dc-1",
        )
        gate.register_job(job)

        request = JobCancelRequest(
            job_id="job-456",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is True
        assert response.already_completed is True
        assert response.cancelled_workflow_count == 0

    @pytest.mark.asyncio
    async def test_repeated_cancellation_is_idempotent(self) -> None:
        """Test that repeated cancellation requests are idempotent."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-789",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        workflow_info = WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        )
        worker.add_workflow(workflow_info)
        manager.assign_workflow("wf-1", "worker-1")

        request = JobCancelRequest(
            job_id="job-789",
            requester_id="client-1",
            timestamp=time.time(),
        )

        # First cancellation
        response_1 = await gate.handle_cancel_request(request)
        assert response_1.success is True
        assert response_1.cancelled_workflow_count == 1

        # Second cancellation (idempotent)
        response_2 = await gate.handle_cancel_request(request)
        assert response_2.success is True
        assert response_2.already_cancelled is True
        assert response_2.cancelled_workflow_count == 0


class TestCancellationFailurePaths:
    """Test failure paths in cancellation flow."""

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_job(self) -> None:
        """Test cancelling a job that doesn't exist."""
        gate = SimulatedGate("gate-1")

        request = JobCancelRequest(
            job_id="job-nonexistent",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is False
        assert response.error == "Job not found"

    @pytest.mark.asyncio
    async def test_cancel_with_unavailable_worker(self) -> None:
        """Test cancellation when worker is unavailable."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-123",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        workflow_info = WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        )
        worker.add_workflow(workflow_info)
        manager.assign_workflow("wf-1", "worker-1")

        # Make worker unavailable
        worker.set_state(NodeState.UNAVAILABLE)

        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        # Gate returns partial success (success=True) even when workers are unavailable
        # The job is still marked as cancelled, but the error field captures the failure
        assert response.success is True  # Partial success semantics
        assert response.error is not None
        assert "unavailable" in response.error.lower()

    @pytest.mark.asyncio
    async def test_cancel_with_unavailable_manager(self) -> None:
        """Test cancellation when manager is unavailable."""
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-456",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        # Make manager unavailable
        manager.set_state(NodeState.UNAVAILABLE)

        request = JobCancelRequest(
            job_id="job-456",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        # Job status still gets updated even if propagation fails
        assert gate.get_job("job-456").status == JobStatus.CANCELLED
        assert "unavailable" in response.error.lower()

    @pytest.mark.asyncio
    async def test_cancel_with_worker_internal_error(self) -> None:
        """Test cancellation when worker returns internal error."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-789",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        workflow_info = WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        )
        worker.add_workflow(workflow_info)
        manager.assign_workflow("wf-1", "worker-1")

        # Make worker fail next request
        worker.set_fail_next(True)

        request = JobCancelRequest(
            job_id="job-789",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        # Gate returns partial success (success=True) even when worker returns error
        # The job is still marked cancelled, but error field captures the internal error
        assert response.success is True  # Partial success semantics
        assert response.error is not None
        assert "error" in response.error.lower()

    @pytest.mark.asyncio
    async def test_partial_cancellation_failure(self) -> None:
        """Test partial cancellation when some workers fail."""
        worker_1 = SimulatedWorker("worker-1")
        worker_2 = SimulatedWorker("worker-2")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker_1, "worker-1")
        manager.register_worker(worker_2, "worker-2")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-partial",
            status=JobStatus.RUNNING,
            workflows=["wf-1", "wf-2"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        # wf-1 on worker-1, wf-2 on worker-2
        worker_1.add_workflow(WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        ))
        worker_2.add_workflow(WorkflowInfo(
            workflow_id="wf-2",
            job_id=job.job_id,
            worker_id="worker-2",
            status=WorkflowStatus.RUNNING,
        ))
        manager.assign_workflow("wf-1", "worker-1")
        manager.assign_workflow("wf-2", "worker-2")

        # Make worker-2 unavailable
        worker_2.set_state(NodeState.UNAVAILABLE)

        request = JobCancelRequest(
            job_id="job-partial",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        # Partial success: wf-1 cancelled, wf-2 failed
        assert response.cancelled_workflow_count == 1
        assert worker_1.get_workflow("wf-1").status == WorkflowStatus.CANCELLED
        assert worker_2.get_workflow("wf-2").status == WorkflowStatus.RUNNING


class TestFenceTokenValidation:
    """Test fence token validation in cancellation."""

    @pytest.mark.asyncio
    async def test_stale_fence_token_rejected(self) -> None:
        """Test that stale fence tokens are rejected."""
        gate = SimulatedGate("gate-1")

        job = JobInfo(
            job_id="job-123",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            fence_token=5,
            datacenter="dc-1",
        )
        gate.register_job(job)

        # Request with old fence token
        request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-old",
            timestamp=time.time(),
            fence_token=3,  # Less than job's fence token
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is False
        assert "Stale fence token" in response.error
        # Job should NOT be cancelled
        assert gate.get_job("job-123").status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_valid_fence_token_accepted(self) -> None:
        """Test that valid fence tokens are accepted."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-456",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            fence_token=5,
            datacenter="dc-1",
        )
        gate.register_job(job)

        worker.add_workflow(WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        ))
        manager.assign_workflow("wf-1", "worker-1")

        # Request with matching fence token
        request = JobCancelRequest(
            job_id="job-456",
            requester_id="client-current",
            timestamp=time.time(),
            fence_token=5,  # Matches job's fence token
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is True
        assert gate.get_job("job-456").status == JobStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_higher_fence_token_accepted(self) -> None:
        """Test that higher fence tokens are accepted."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-789",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            fence_token=5,
            datacenter="dc-1",
        )
        gate.register_job(job)

        worker.add_workflow(WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        ))
        manager.assign_workflow("wf-1", "worker-1")

        # Request with higher fence token (e.g., from newer client)
        request = JobCancelRequest(
            job_id="job-789",
            requester_id="client-new",
            timestamp=time.time(),
            fence_token=7,  # Higher than job's fence token
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is True
        assert gate.get_job("job-789").status == JobStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_zero_fence_token_bypasses_check(self) -> None:
        """Test that zero fence token bypasses validation."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-bypass",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            fence_token=10,
            datacenter="dc-1",
        )
        gate.register_job(job)

        worker.add_workflow(WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        ))
        manager.assign_workflow("wf-1", "worker-1")

        # Request with zero fence token (bypass)
        request = JobCancelRequest(
            job_id="job-bypass",
            requester_id="admin",
            timestamp=time.time(),
            fence_token=0,  # Zero means ignore fence token
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is True


class TestConcurrentCancellation:
    """Test concurrent cancellation scenarios."""

    @pytest.mark.asyncio
    async def test_concurrent_cancel_requests_for_same_job(self) -> None:
        """Test multiple concurrent cancellation requests for same job."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-concurrent",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        worker.add_workflow(WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        ))
        manager.assign_workflow("wf-1", "worker-1")

        # Send 5 concurrent cancellation requests
        requests = [
            JobCancelRequest(
                job_id="job-concurrent",
                requester_id=f"client-{i}",
                timestamp=time.time(),
            )
            for i in range(5)
        ]

        responses = await asyncio.gather(*[
            gate.handle_cancel_request(req) for req in requests
        ])

        # All should succeed (idempotent)
        assert all(r.success for r in responses)

        # Only one should have actually cancelled workflows
        total_cancelled = sum(r.cancelled_workflow_count for r in responses)
        already_cancelled_count = sum(1 for r in responses if r.already_cancelled)

        assert total_cancelled == 1
        assert already_cancelled_count >= 4  # Most should see already cancelled

    @pytest.mark.asyncio
    async def test_concurrent_cancellation_for_different_jobs(self) -> None:
        """Test concurrent cancellation for different jobs."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        # Create 3 jobs
        for idx in range(3):
            job = JobInfo(
                job_id=f"job-{idx}",
                status=JobStatus.RUNNING,
                workflows=[f"wf-{idx}"],
                datacenter="dc-1",
            )
            gate.register_job(job)

            worker.add_workflow(WorkflowInfo(
                workflow_id=f"wf-{idx}",
                job_id=f"job-{idx}",
                worker_id="worker-1",
                status=WorkflowStatus.RUNNING,
            ))
            manager.assign_workflow(f"wf-{idx}", "worker-1")

        # Cancel all jobs concurrently
        requests = [
            JobCancelRequest(
                job_id=f"job-{idx}",
                requester_id="client-1",
                timestamp=time.time(),
            )
            for idx in range(3)
        ]

        responses = await asyncio.gather(*[
            gate.handle_cancel_request(req) for req in requests
        ])

        # All should succeed
        assert all(r.success for r in responses)
        assert all(r.cancelled_workflow_count == 1 for r in responses)

        # All jobs should be cancelled
        for idx in range(3):
            assert gate.get_job(f"job-{idx}").status == JobStatus.CANCELLED


class TestCancellationRaceConditions:
    """Test race conditions between cancellation and other operations."""

    @pytest.mark.asyncio
    async def test_cancel_during_workflow_completion(self) -> None:
        """Test cancellation arriving while workflow is completing."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-race",
            status=JobStatus.RUNNING,
            workflows=["wf-completing"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        # Workflow is already completed (race condition)
        worker.add_workflow(WorkflowInfo(
            workflow_id="wf-completing",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.COMPLETED,  # Already completed
        ))
        manager.assign_workflow("wf-completing", "worker-1")

        request = JobCancelRequest(
            job_id="job-race",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel_request(request)

        assert response.success is True
        # Workflow was already completed, so count is 0
        assert response.cancelled_workflow_count == 0

    @pytest.mark.asyncio
    async def test_cancel_with_slow_worker(self) -> None:
        """Test cancellation with slow worker response."""
        worker = SimulatedWorker("worker-1")
        manager = SimulatedManager("manager-1")
        gate = SimulatedGate("gate-1")

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1", "dc-1")

        job = JobInfo(
            job_id="job-slow",
            status=JobStatus.RUNNING,
            workflows=["wf-1"],
            datacenter="dc-1",
        )
        gate.register_job(job)

        worker.add_workflow(WorkflowInfo(
            workflow_id="wf-1",
            job_id=job.job_id,
            worker_id="worker-1",
            status=WorkflowStatus.RUNNING,
        ))
        manager.assign_workflow("wf-1", "worker-1")

        # Make worker slow
        worker.set_response_delay(0.1)  # 100ms delay

        request = JobCancelRequest(
            job_id="job-slow",
            requester_id="client-1",
            timestamp=time.time(),
        )

        start_time = time.time()
        response = await gate.handle_cancel_request(request)
        elapsed_time = time.time() - start_time

        assert response.success is True
        assert response.cancelled_workflow_count == 1
        assert elapsed_time >= 0.1  # Should take at least worker delay


class TestLegacyMessageCompatibility:
    """Test compatibility with legacy cancellation messages."""

    @pytest.mark.asyncio
    async def test_legacy_cancel_job_serialization(self) -> None:
        """Test legacy CancelJob message serialization."""
        original = CancelJob(
            job_id="job-legacy",
            reason="timeout",
            fence_token=5,
        )

        serialized = original.dump()
        restored = CancelJob.load(serialized)

        assert restored.job_id == "job-legacy"
        assert restored.reason == "timeout"
        assert restored.fence_token == 5

    @pytest.mark.asyncio
    async def test_legacy_cancel_ack_serialization(self) -> None:
        """Test legacy CancelAck message serialization."""
        original = CancelAck(
            job_id="job-legacy",
            cancelled=True,
            workflows_cancelled=3,
        )

        serialized = original.dump()
        restored = CancelAck.load(serialized)

        assert restored.job_id == "job-legacy"
        assert restored.cancelled is True
        assert restored.workflows_cancelled == 3

    @pytest.mark.asyncio
    async def test_new_and_legacy_message_equivalence(self) -> None:
        """Test that new and legacy messages carry same information."""
        # New format request
        new_request = JobCancelRequest(
            job_id="job-123",
            requester_id="client-1",
            timestamp=time.time(),
            fence_token=5,
            reason="user cancelled",
        )

        # Legacy format request
        legacy_request = CancelJob(
            job_id="job-123",
            reason="user cancelled",
            fence_token=5,
        )

        # Should carry same essential information
        assert new_request.job_id == legacy_request.job_id
        assert new_request.reason == legacy_request.reason
        assert new_request.fence_token == legacy_request.fence_token

        # New format response
        new_response = JobCancelResponse(
            job_id="job-123",
            success=True,
            cancelled_workflow_count=3,
        )

        # Legacy format response
        legacy_response = CancelAck(
            job_id="job-123",
            cancelled=True,
            workflows_cancelled=3,
        )

        # Should carry same essential information
        assert new_response.job_id == legacy_response.job_id
        assert new_response.success == legacy_response.cancelled
        assert new_response.cancelled_workflow_count == legacy_response.workflows_cancelled
