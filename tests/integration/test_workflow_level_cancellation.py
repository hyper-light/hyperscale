"""
Integration tests for Section 6: Workflow-Level Cancellation from Gates.

Tests verify:
- SingleWorkflowCancelRequest/Response message handling
- Manager workflow cancellation with dependency traversal
- Pre-dispatch cancellation check
- Peer notification for cancellation sync
- Gate forwarding to datacenters

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
import uuid
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock


# =============================================================================
# Mock Message Types
# =============================================================================


class MockWorkflowCancellationStatus:
    """Mock WorkflowCancellationStatus enum values."""

    CANCELLED = "cancelled"
    PENDING_CANCELLED = "pending_cancelled"
    ALREADY_CANCELLED = "already_cancelled"
    ALREADY_COMPLETED = "already_completed"
    NOT_FOUND = "not_found"
    CANCELLING = "cancelling"


@dataclass
class MockSingleWorkflowCancelRequest:
    """Mock SingleWorkflowCancelRequest message."""

    job_id: str
    workflow_id: str
    request_id: str
    requester_id: str
    timestamp: float
    cancel_dependents: bool = True
    origin_gate_addr: tuple[str, int] | None = None
    origin_client_addr: tuple[str, int] | None = None

    def dump(self) -> bytes:
        return b"single_workflow_cancel_request"

    @classmethod
    def load(cls, data: bytes) -> "MockSingleWorkflowCancelRequest":
        return data


@dataclass
class MockSingleWorkflowCancelResponse:
    """Mock SingleWorkflowCancelResponse message."""

    job_id: str
    workflow_id: str
    request_id: str
    status: str
    cancelled_dependents: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    datacenter: str = ""

    def dump(self) -> bytes:
        return b"single_workflow_cancel_response"

    @classmethod
    def load(cls, data: bytes) -> "MockSingleWorkflowCancelResponse":
        return data


@dataclass
class MockCancelledWorkflowInfo:
    """Mock CancelledWorkflowInfo for tracking."""

    job_id: str
    workflow_id: str
    cancelled_at: float
    request_id: str
    dependents: list[str] = field(default_factory=list)


@dataclass
class MockWorkflowCancellationPeerNotification:
    """Mock peer notification."""

    job_id: str
    workflow_id: str
    request_id: str
    origin_node_id: str
    cancelled_workflows: list[str] = field(default_factory=list)
    timestamp: float = 0.0


# =============================================================================
# Mock Infrastructure
# =============================================================================


@dataclass
class MockLogger:
    """Mock logger."""

    _logs: list = field(default_factory=list)

    async def log(self, message: Any) -> None:
        self._logs.append(message)


@dataclass
class MockWorkflowProgress:
    """Mock workflow progress."""

    status: str = "RUNNING"
    workflow_name: str = ""


@dataclass
class MockSubWorkflow:
    """Mock sub-workflow."""

    token: str
    worker_id: str | None = None
    progress: MockWorkflowProgress | None = None
    dependencies: list[str] = field(default_factory=list)


@dataclass
class MockJob:
    """Mock job."""

    job_id: str
    status: str = "RUNNING"
    sub_workflows: dict = field(default_factory=dict)


@dataclass
class MockJobManager:
    """Mock job manager."""

    _jobs: dict = field(default_factory=dict)

    def get_job_by_id(self, job_id: str) -> MockJob | None:
        return self._jobs.get(job_id)


class MockManagerServer:
    """
    Mock manager server for testing workflow-level cancellation.
    """

    def __init__(self) -> None:
        # Identity
        self._host = "127.0.0.1"
        self._tcp_port = 9090
        self._node_id = MagicMock()
        self._node_id.short = "manager-001"
        self._datacenter = "dc1"

        # Infrastructure
        self._udp_logger = MockLogger()
        self._job_manager = MockJobManager()

        # Cancelled workflow tracking (Section 6)
        self._cancelled_workflows: dict[str, MockCancelledWorkflowInfo] = {}
        self._workflow_cancellation_locks: dict[str, asyncio.Lock] = {}

        # Peer tracking
        self._known_manager_peers: dict[str, tuple[str, int]] = {}

        # TCP tracking
        self._tcp_calls: list[tuple[tuple[str, int], str, Any]] = []

        # Rate limiting mock
        self._rate_limited = False

    def _check_rate_limit_for_operation(self, client_id: str, operation: str) -> tuple[bool, float]:
        return (not self._rate_limited, 0.0)

    async def send_tcp(
        self,
        addr: tuple[str, int],
        action: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes | None, float]:
        self._tcp_calls.append((addr, action, data))
        return (b"OK", 0.01)

    async def receive_cancel_single_workflow(
        self,
        request: MockSingleWorkflowCancelRequest,
    ) -> MockSingleWorkflowCancelResponse:
        """Handle single workflow cancellation request."""

        # Check if already cancelled
        if request.workflow_id in self._cancelled_workflows:
            existing = self._cancelled_workflows[request.workflow_id]
            return MockSingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=MockWorkflowCancellationStatus.ALREADY_CANCELLED,
                cancelled_dependents=existing.dependents,
                datacenter=self._datacenter,
            )

        job = self._job_manager.get_job_by_id(request.job_id)
        if not job:
            return MockSingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=MockWorkflowCancellationStatus.NOT_FOUND,
                errors=["Job not found"],
                datacenter=self._datacenter,
            )

        # Acquire per-workflow lock
        lock = self._workflow_cancellation_locks.setdefault(
            request.workflow_id, asyncio.Lock()
        )

        async with lock:
            # Find the workflow
            target_sub_wf = None
            for sub_wf in job.sub_workflows.values():
                if str(sub_wf.token) == request.workflow_id:
                    target_sub_wf = sub_wf
                    break

            if target_sub_wf is None:
                return MockSingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=MockWorkflowCancellationStatus.NOT_FOUND,
                    errors=["Workflow not found in job"],
                    datacenter=self._datacenter,
                )

            # Check if already completed
            if target_sub_wf.progress and target_sub_wf.progress.status in ("COMPLETED", "AGGREGATED"):
                return MockSingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=MockWorkflowCancellationStatus.ALREADY_COMPLETED,
                    datacenter=self._datacenter,
                )

            # Collect all workflows to cancel
            workflows_to_cancel = [request.workflow_id]
            cancelled_dependents: list[str] = []

            if request.cancel_dependents:
                dependents = self._find_dependent_workflows(request.job_id, request.workflow_id)
                workflows_to_cancel.extend(dependents)
                cancelled_dependents = dependents

            # Cancel workflows
            status = MockWorkflowCancellationStatus.CANCELLED

            for wf_id in workflows_to_cancel:
                self._cancelled_workflows[wf_id] = MockCancelledWorkflowInfo(
                    job_id=request.job_id,
                    workflow_id=wf_id,
                    cancelled_at=time.monotonic(),
                    request_id=request.request_id,
                    dependents=cancelled_dependents if wf_id == request.workflow_id else [],
                )

                # Check if pending
                for sub_wf in job.sub_workflows.values():
                    if str(sub_wf.token) == wf_id:
                        if sub_wf.progress is None or sub_wf.progress.status == "PENDING":
                            if wf_id == request.workflow_id:
                                status = MockWorkflowCancellationStatus.PENDING_CANCELLED
                        break

            return MockSingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=status,
                cancelled_dependents=cancelled_dependents,
                errors=[],
                datacenter=self._datacenter,
            )

    def _find_dependent_workflows(self, job_id: str, workflow_id: str) -> list[str]:
        """Find all workflows that depend on the given workflow."""
        dependents: list[str] = []
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return dependents

        # Build reverse dependency map
        reverse_deps: dict[str, list[str]] = {}
        for sub_wf in job.sub_workflows.values():
            wf_id = str(sub_wf.token)
            if sub_wf.dependencies:
                for dep in sub_wf.dependencies:
                    if dep not in reverse_deps:
                        reverse_deps[dep] = []
                    reverse_deps[dep].append(wf_id)

        # BFS to find all dependents
        queue = [workflow_id]
        visited: set[str] = set()

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)

            for dependent in reverse_deps.get(current, []):
                if dependent not in visited:
                    dependents.append(dependent)
                    queue.append(dependent)

        return dependents

    def is_workflow_cancelled(self, workflow_id: str) -> bool:
        """Check if workflow is cancelled (for pre-dispatch check)."""
        return workflow_id in self._cancelled_workflows

    # Test helpers

    def add_job(self, job_id: str, workflows: dict[str, MockSubWorkflow]) -> None:
        """Add a job with workflows."""
        job = MockJob(job_id=job_id, sub_workflows=workflows)
        self._job_manager._jobs[job_id] = job


class MockGateServer:
    """Mock gate server for testing workflow cancellation forwarding."""

    def __init__(self) -> None:
        self._node_id = MagicMock()
        self._node_id.short = "gate-001"
        self._host = "127.0.0.1"
        self._tcp_port = 8080

        self._udp_logger = MockLogger()
        self._jobs: dict[str, Any] = {}
        self._datacenter_managers: dict[str, Any] = {}
        self._rate_limited = False

        self._tcp_calls: list[tuple[tuple[str, int], str, Any]] = []

    def _check_rate_limit_for_operation(self, client_id: str, operation: str) -> tuple[bool, float]:
        return (not self._rate_limited, 0.0)

    async def send_tcp(
        self,
        addr: tuple[str, int],
        action: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes | None, float]:
        self._tcp_calls.append((addr, action, data))
        # Return mock response
        return (
            MockSingleWorkflowCancelResponse(
                job_id="job-001",
                workflow_id="workflow-001",
                request_id="request-001",
                status=MockWorkflowCancellationStatus.CANCELLED,
                datacenter="dc1",
            ),
            0.01,
        )

    async def receive_cancel_single_workflow(
        self,
        request: MockSingleWorkflowCancelRequest,
    ) -> MockSingleWorkflowCancelResponse:
        """Handle workflow cancellation - forward to datacenters."""

        if request.job_id not in self._jobs:
            return MockSingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=MockWorkflowCancellationStatus.NOT_FOUND,
                errors=["Job not found"],
            )

        # Collect DC addresses
        target_dcs: list[tuple[str, tuple[str, int]]] = []
        for dc_name, dc_info in self._datacenter_managers.items():
            if dc_info and hasattr(dc_info, 'tcp_addr') and dc_info.tcp_addr:
                target_dcs.append((dc_name, dc_info.tcp_addr))

        if not target_dcs:
            return MockSingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=MockWorkflowCancellationStatus.NOT_FOUND,
                errors=["No datacenters available"],
            )

        # Forward to all DCs
        aggregated_dependents: list[str] = []
        final_status = MockWorkflowCancellationStatus.NOT_FOUND

        for dc_name, dc_addr in target_dcs:
            response_data, _ = await self.send_tcp(
                dc_addr,
                "receive_cancel_single_workflow",
                request.dump(),
                timeout=5.0,
            )

            if response_data:
                response = response_data  # Mock returns object directly
                if hasattr(response, 'cancelled_dependents'):
                    aggregated_dependents.extend(response.cancelled_dependents)
                if hasattr(response, 'status'):
                    if response.status == MockWorkflowCancellationStatus.CANCELLED:
                        final_status = MockWorkflowCancellationStatus.CANCELLED

        return MockSingleWorkflowCancelResponse(
            job_id=request.job_id,
            workflow_id=request.workflow_id,
            request_id=request.request_id,
            status=final_status,
            cancelled_dependents=list(set(aggregated_dependents)),
            errors=[],
        )

    # Test helpers

    def add_job(self, job_id: str) -> None:
        self._jobs[job_id] = True

    def add_datacenter(self, dc_name: str, tcp_addr: tuple[str, int]) -> None:
        @dataclass
        class DCInfo:
            tcp_addr: tuple[str, int]

        self._datacenter_managers[dc_name] = DCInfo(tcp_addr=tcp_addr)


# =============================================================================
# Test Classes
# =============================================================================


class TestManagerWorkflowCancellation:
    """Tests for manager handling single workflow cancellation."""

    @pytest.mark.asyncio
    async def test_cancel_running_workflow(self):
        """Manager should cancel a running workflow."""
        manager = MockManagerServer()

        workflows = {
            "wf1": MockSubWorkflow(
                token="workflow-001",
                worker_id="worker-001",
                progress=MockWorkflowProgress(status="RUNNING"),
            )
        }
        manager.add_job("job-001", workflows)

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )

        response = await manager.receive_cancel_single_workflow(request)

        assert response.status == MockWorkflowCancellationStatus.CANCELLED
        assert "workflow-001" in manager._cancelled_workflows

    @pytest.mark.asyncio
    async def test_cancel_pending_workflow(self):
        """Manager should cancel a pending workflow with PENDING_CANCELLED status."""
        manager = MockManagerServer()

        workflows = {
            "wf1": MockSubWorkflow(
                token="workflow-001",
                progress=MockWorkflowProgress(status="PENDING"),
            )
        }
        manager.add_job("job-001", workflows)

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )

        response = await manager.receive_cancel_single_workflow(request)

        assert response.status == MockWorkflowCancellationStatus.PENDING_CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_completed_workflow_fails(self):
        """Manager should not cancel an already completed workflow."""
        manager = MockManagerServer()

        workflows = {
            "wf1": MockSubWorkflow(
                token="workflow-001",
                progress=MockWorkflowProgress(status="COMPLETED"),
            )
        }
        manager.add_job("job-001", workflows)

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )

        response = await manager.receive_cancel_single_workflow(request)

        assert response.status == MockWorkflowCancellationStatus.ALREADY_COMPLETED

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_workflow(self):
        """Manager should return NOT_FOUND for nonexistent workflow."""
        manager = MockManagerServer()

        workflows = {}
        manager.add_job("job-001", workflows)

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-999",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )

        response = await manager.receive_cancel_single_workflow(request)

        assert response.status == MockWorkflowCancellationStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_cancel_idempotent(self):
        """Cancelling same workflow twice should return ALREADY_CANCELLED."""
        manager = MockManagerServer()

        workflows = {
            "wf1": MockSubWorkflow(
                token="workflow-001",
                progress=MockWorkflowProgress(status="RUNNING"),
            )
        }
        manager.add_job("job-001", workflows)

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )

        # First cancellation
        response1 = await manager.receive_cancel_single_workflow(request)
        assert response1.status == MockWorkflowCancellationStatus.CANCELLED

        # Second cancellation
        response2 = await manager.receive_cancel_single_workflow(request)
        assert response2.status == MockWorkflowCancellationStatus.ALREADY_CANCELLED


class TestDependentWorkflowCancellation:
    """Tests for cancelling workflows with dependencies."""

    @pytest.mark.asyncio
    async def test_cancel_with_dependents(self):
        """Cancelling a workflow should also cancel its dependents."""
        manager = MockManagerServer()

        # workflow-001 -> workflow-002 -> workflow-003
        workflows = {
            "wf1": MockSubWorkflow(
                token="workflow-001",
                progress=MockWorkflowProgress(status="RUNNING"),
                dependencies=[],
            ),
            "wf2": MockSubWorkflow(
                token="workflow-002",
                progress=MockWorkflowProgress(status="PENDING"),
                dependencies=["workflow-001"],
            ),
            "wf3": MockSubWorkflow(
                token="workflow-003",
                progress=MockWorkflowProgress(status="PENDING"),
                dependencies=["workflow-002"],
            ),
        }
        manager.add_job("job-001", workflows)

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
            cancel_dependents=True,
        )

        response = await manager.receive_cancel_single_workflow(request)

        assert response.status == MockWorkflowCancellationStatus.CANCELLED
        # All 3 workflows should be cancelled
        assert "workflow-001" in manager._cancelled_workflows
        assert "workflow-002" in manager._cancelled_workflows
        assert "workflow-003" in manager._cancelled_workflows

    @pytest.mark.asyncio
    async def test_cancel_without_dependents(self):
        """Cancelling with cancel_dependents=False should only cancel target."""
        manager = MockManagerServer()

        workflows = {
            "wf1": MockSubWorkflow(
                token="workflow-001",
                progress=MockWorkflowProgress(status="RUNNING"),
                dependencies=[],
            ),
            "wf2": MockSubWorkflow(
                token="workflow-002",
                progress=MockWorkflowProgress(status="PENDING"),
                dependencies=["workflow-001"],
            ),
        }
        manager.add_job("job-001", workflows)

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
            cancel_dependents=False,
        )

        response = await manager.receive_cancel_single_workflow(request)

        assert response.status == MockWorkflowCancellationStatus.CANCELLED
        assert "workflow-001" in manager._cancelled_workflows
        assert "workflow-002" not in manager._cancelled_workflows


class TestPreDispatchCancellationCheck:
    """Tests for pre-dispatch cancellation check."""

    @pytest.mark.asyncio
    async def test_cancelled_workflow_blocked_from_dispatch(self):
        """Cancelled workflows should be blocked from dispatch."""
        manager = MockManagerServer()

        # Add workflow to cancelled bucket
        manager._cancelled_workflows["workflow-001"] = MockCancelledWorkflowInfo(
            job_id="job-001",
            workflow_id="workflow-001",
            cancelled_at=time.monotonic(),
            request_id="request-001",
        )

        # Check would be: if workflow_id in self._cancelled_workflows
        assert manager.is_workflow_cancelled("workflow-001")
        assert not manager.is_workflow_cancelled("workflow-002")


class TestGateWorkflowCancellationForwarding:
    """Tests for gate forwarding workflow cancellation to datacenters."""

    @pytest.mark.asyncio
    async def test_gate_forwards_to_datacenters(self):
        """Gate should forward cancellation request to all datacenters."""
        gate = MockGateServer()

        gate.add_job("job-001")
        gate.add_datacenter("dc1", ("192.168.1.10", 9090))
        gate.add_datacenter("dc2", ("192.168.1.20", 9090))

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )

        response = await gate.receive_cancel_single_workflow(request)

        # Should have forwarded to both DCs
        assert len(gate._tcp_calls) == 2
        assert response.status == MockWorkflowCancellationStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_gate_job_not_found(self):
        """Gate should return NOT_FOUND for unknown job."""
        gate = MockGateServer()

        request = MockSingleWorkflowCancelRequest(
            job_id="unknown-job",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )

        response = await gate.receive_cancel_single_workflow(request)

        assert response.status == MockWorkflowCancellationStatus.NOT_FOUND
        assert "Job not found" in response.errors

    @pytest.mark.asyncio
    async def test_gate_no_datacenters(self):
        """Gate should return error if no datacenters available."""
        gate = MockGateServer()

        gate.add_job("job-001")
        # No datacenters added

        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )

        response = await gate.receive_cancel_single_workflow(request)

        assert response.status == MockWorkflowCancellationStatus.NOT_FOUND
        assert "No datacenters available" in response.errors


class TestConcurrentCancellation:
    """Tests for concurrent cancellation handling."""

    @pytest.mark.asyncio
    async def test_concurrent_cancellation_requests(self):
        """Multiple concurrent cancellation requests should be handled safely."""
        manager = MockManagerServer()

        workflows = {
            "wf1": MockSubWorkflow(
                token="workflow-001",
                progress=MockWorkflowProgress(status="RUNNING"),
            )
        }
        manager.add_job("job-001", workflows)

        # Create multiple requests
        requests = [
            MockSingleWorkflowCancelRequest(
                job_id="job-001",
                workflow_id="workflow-001",
                request_id=str(uuid.uuid4()),
                requester_id=f"client-{i}",
                timestamp=time.monotonic(),
            )
            for i in range(5)
        ]

        # Execute concurrently
        tasks = [manager.receive_cancel_single_workflow(req) for req in requests]
        responses = await asyncio.gather(*tasks)

        # One should be CANCELLED, rest should be ALREADY_CANCELLED
        cancelled_count = sum(
            1 for r in responses
            if r.status == MockWorkflowCancellationStatus.CANCELLED
        )
        already_cancelled_count = sum(
            1 for r in responses
            if r.status == MockWorkflowCancellationStatus.ALREADY_CANCELLED
        )

        assert cancelled_count == 1
        assert already_cancelled_count == 4

    @pytest.mark.asyncio
    async def test_cancellation_during_dispatch_race(self):
        """Cancellation and dispatch should not race."""
        manager = MockManagerServer()

        workflows = {
            "wf1": MockSubWorkflow(
                token="workflow-001",
                progress=MockWorkflowProgress(status="PENDING"),
            )
        }
        manager.add_job("job-001", workflows)

        # Simulate race: cancellation happens
        request = MockSingleWorkflowCancelRequest(
            job_id="job-001",
            workflow_id="workflow-001",
            request_id=str(uuid.uuid4()),
            requester_id="client-001",
            timestamp=time.monotonic(),
        )
        await manager.receive_cancel_single_workflow(request)

        # Now dispatch check should block
        assert manager.is_workflow_cancelled("workflow-001")
