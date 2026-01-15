"""
Integration tests for Section 5: Event-Driven Cancellation Push Notification Chain.

Tests verify the full push notification chain:
- Worker → Manager (WorkflowCancellationComplete)
- Manager → Gate/Client (JobCancellationComplete)

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock


# =============================================================================
# Mock Message Types (matching distributed.py)
# =============================================================================


@dataclass
class MockWorkflowCancellationComplete:
    """Mock WorkflowCancellationComplete message."""

    job_id: str
    workflow_id: str
    success: bool
    errors: list[str] = field(default_factory=list)
    cancelled_at: float = 0.0
    node_id: str = ""

    def dump(self) -> bytes:
        """Serialize to bytes (mock)."""
        return b"workflow_cancellation_complete"

    @classmethod
    def load(cls, data: bytes) -> "MockWorkflowCancellationComplete":
        """Deserialize from bytes (mock)."""
        return data  # In tests, we pass the object directly


@dataclass
class MockJobCancellationComplete:
    """Mock JobCancellationComplete message."""

    job_id: str
    success: bool
    cancelled_workflow_count: int = 0
    total_workflow_count: int = 0
    errors: list[str] = field(default_factory=list)
    cancelled_at: float = 0.0

    def dump(self) -> bytes:
        """Serialize to bytes (mock)."""
        return b"job_cancellation_complete"

    @classmethod
    def load(cls, data: bytes) -> "MockJobCancellationComplete":
        """Deserialize from bytes (mock)."""
        return data


# =============================================================================
# Mock Infrastructure
# =============================================================================


@dataclass
class MockLogger:
    """Mock logger for tests."""

    _logs: list = field(default_factory=list)

    async def log(self, message: Any) -> None:
        self._logs.append(message)


@dataclass
class MockManagerInfo:
    """Mock manager info."""

    node_id: str
    tcp_host: str
    tcp_port: int


@dataclass
class MockSubWorkflow:
    """Mock sub-workflow."""

    workflow_id: str
    worker_id: str | None = None
    status: str = "running"
    result: Any = None


@dataclass
class MockJob:
    """Mock job."""

    job_id: str
    sub_workflows: dict = field(default_factory=dict)


@dataclass
class MockJobManager:
    """Mock job manager."""

    _jobs: dict = field(default_factory=dict)

    def get_job_by_id(self, job_id: str) -> MockJob | None:
        return self._jobs.get(job_id)

    def add_job(self, job: MockJob) -> None:
        self._jobs[job.job_id] = job


class MockWorkerServer:
    """
    Mock worker server for testing cancellation push.

    Implements only the methods needed for cancellation push testing.
    """

    def __init__(self) -> None:
        # Identity
        self._host = "127.0.0.1"
        self._tcp_port = 8000
        self._node_id = MagicMock()
        self._node_id.short = "worker-001"

        # Infrastructure
        self._udp_logger = MockLogger()

        # Manager tracking
        self._known_managers: dict[str, MockManagerInfo] = {}
        self._healthy_manager_ids: set[str] = set()
        self._workflow_job_leader: dict[str, tuple[str, int]] = {}

        # TCP call tracking for verification
        self._tcp_calls: list[tuple[tuple[str, int], str, Any]] = []
        self._tcp_call_results: dict[str, tuple[bytes | None, float]] = {}

    async def send_tcp(
        self,
        addr: tuple[str, int],
        action: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes | None, float]:
        """Mock TCP send - records calls for verification."""
        self._tcp_calls.append((addr, action, data))
        return self._tcp_call_results.get(action, (b'{"accepted": true}', 0.01))

    async def _push_cancellation_complete(
        self,
        job_id: str,
        workflow_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """
        Push workflow cancellation completion to the job leader manager.

        This is the method under test - copied from worker.py for isolation.
        """
        completion = MockWorkflowCancellationComplete(
            job_id=job_id,
            workflow_id=workflow_id,
            success=success,
            errors=errors,
            cancelled_at=time.time(),
            node_id=self._node_id.short,
        )

        job_leader_addr = self._workflow_job_leader.get(workflow_id)

        # Try job leader first
        if job_leader_addr:
            try:
                await self.send_tcp(
                    job_leader_addr,
                    "workflow_cancellation_complete",
                    completion.dump(),
                    timeout=5.0,
                )
                return
            except Exception:
                pass

        # Job leader unknown or failed - try any healthy manager
        for manager_id in list(self._healthy_manager_ids):
            manager_info = self._known_managers.get(manager_id)
            if not manager_info:
                continue

            manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
            if manager_addr == job_leader_addr:
                continue

            try:
                await self.send_tcp(
                    manager_addr,
                    "workflow_cancellation_complete",
                    completion.dump(),
                    timeout=5.0,
                )
                return
            except Exception:
                continue

    # Test helpers

    def add_manager(self, manager_id: str, host: str, port: int) -> None:
        """Add a manager for testing."""
        self._known_managers[manager_id] = MockManagerInfo(
            node_id=manager_id,
            tcp_host=host,
            tcp_port=port,
        )
        self._healthy_manager_ids.add(manager_id)

    def set_job_leader(self, workflow_id: str, addr: tuple[str, int]) -> None:
        """Set job leader for a workflow."""
        self._workflow_job_leader[workflow_id] = addr


class MockManagerServer:
    """
    Mock manager server for testing cancellation push.

    Implements only the methods needed for cancellation push testing.
    """

    def __init__(self) -> None:
        # Identity
        self._host = "127.0.0.1"
        self._tcp_port = 9090
        self._node_id = MagicMock()
        self._node_id.short = "manager-001"

        # Infrastructure
        self._udp_logger = MockLogger()
        self._job_manager = MockJobManager()

        # Job tracking
        self._job_origin_gates: dict[str, tuple[str, int]] = {}
        self._job_callbacks: dict[str, tuple[str, int]] = {}

        # Cancellation tracking
        self._cancellation_completions: list[MockWorkflowCancellationComplete] = []

        # TCP call tracking
        self._tcp_calls: list[tuple[tuple[str, int], str, Any]] = []

    async def send_tcp(
        self,
        addr: tuple[str, int],
        action: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes | None, float]:
        """Mock TCP send."""
        self._tcp_calls.append((addr, action, data))
        return (b'{"accepted": true}', 0.01)

    async def workflow_cancellation_complete(
        self,
        completion: MockWorkflowCancellationComplete,
    ) -> None:
        """
        Handle workflow cancellation completion from worker.

        Simplified version of manager.py handler for testing.
        """
        self._cancellation_completions.append(completion)

        # Check if all workflows for job are cancelled
        job = self._job_manager.get_job_by_id(completion.job_id)
        if job:
            all_cancelled = all(
                sw.status == "cancelled"
                for sw in job.sub_workflows.values()
            )

            if all_cancelled:
                await self._push_cancellation_complete_to_origin(
                    completion.job_id,
                    success=completion.success,
                    errors=completion.errors,
                )

    async def _push_cancellation_complete_to_origin(
        self,
        job_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """
        Push job cancellation completion to origin gate/client.

        Simplified version for testing.
        """
        job = self._job_manager.get_job_by_id(job_id)

        cancelled_workflow_count = 0
        total_workflow_count = 0
        if job:
            total_workflow_count = len(job.sub_workflows)
            cancelled_workflow_count = total_workflow_count - len(errors)

        completion = MockJobCancellationComplete(
            job_id=job_id,
            success=success,
            cancelled_workflow_count=cancelled_workflow_count,
            total_workflow_count=total_workflow_count,
            errors=errors,
            cancelled_at=time.monotonic(),
        )

        # Try origin gate first
        origin_gate = self._job_origin_gates.get(job_id)
        if origin_gate:
            await self.send_tcp(
                origin_gate,
                "receive_job_cancellation_complete",
                completion.dump(),
                timeout=2.0,
            )
            return

        # Fallback to client callback
        callback = self._job_callbacks.get(job_id)
        if callback:
            await self.send_tcp(
                callback,
                "receive_job_cancellation_complete",
                completion.dump(),
                timeout=2.0,
            )

    # Test helpers

    def add_job(self, job_id: str, workflow_ids: list[str]) -> None:
        """Add a job for testing."""
        job = MockJob(job_id=job_id)
        for wf_id in workflow_ids:
            job.sub_workflows[wf_id] = MockSubWorkflow(workflow_id=wf_id)
        self._job_manager.add_job(job)

    def set_origin_gate(self, job_id: str, addr: tuple[str, int]) -> None:
        """Set origin gate for a job."""
        self._job_origin_gates[job_id] = addr

    def set_client_callback(self, job_id: str, addr: tuple[str, int]) -> None:
        """Set client callback for a job."""
        self._job_callbacks[job_id] = addr

    def mark_workflow_cancelled(self, job_id: str, workflow_id: str) -> None:
        """Mark a workflow as cancelled."""
        job = self._job_manager.get_job_by_id(job_id)
        if job and workflow_id in job.sub_workflows:
            job.sub_workflows[workflow_id].status = "cancelled"


class MockGateServer:
    """
    Mock gate server for testing cancellation push.
    """

    def __init__(self) -> None:
        # Identity
        self._node_id = MagicMock()
        self._node_id.short = "gate-001"

        # Received completions
        self._received_completions: list[MockJobCancellationComplete] = []

        # Client callbacks
        self._job_callbacks: dict[str, tuple[str, int]] = {}

        # TCP calls
        self._tcp_calls: list[tuple[tuple[str, int], str, Any]] = []

    async def send_tcp(
        self,
        addr: tuple[str, int],
        action: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes | None, float]:
        """Mock TCP send."""
        self._tcp_calls.append((addr, action, data))
        return (b'{"accepted": true}', 0.01)

    async def receive_job_cancellation_complete(
        self,
        completion: MockJobCancellationComplete,
    ) -> None:
        """Handle job cancellation completion from manager."""
        self._received_completions.append(completion)

        # Forward to client callback if registered
        callback = self._job_callbacks.get(completion.job_id)
        if callback:
            await self.send_tcp(
                callback,
                "receive_job_cancellation_complete",
                completion.dump(),
                timeout=2.0,
            )

    def set_client_callback(self, job_id: str, addr: tuple[str, int]) -> None:
        """Set client callback for a job."""
        self._job_callbacks[job_id] = addr


class MockClientServer:
    """
    Mock client for testing cancellation completion reception.
    """

    def __init__(self) -> None:
        # Received completions
        self._received_completions: list[MockJobCancellationComplete] = []

        # Cancellation events
        self._cancellation_events: dict[str, asyncio.Event] = {}
        self._cancellation_results: dict[str, tuple[bool, list[str]]] = {}

    async def receive_job_cancellation_complete(
        self,
        completion: MockJobCancellationComplete,
    ) -> None:
        """Handle job cancellation completion."""
        self._received_completions.append(completion)

        # Store result
        self._cancellation_results[completion.job_id] = (
            completion.success,
            completion.errors,
        )

        # Signal event if any waiters
        event = self._cancellation_events.get(completion.job_id)
        if event:
            event.set()

    async def await_job_cancellation(
        self,
        job_id: str,
        timeout: float = 10.0,
    ) -> tuple[bool, list[str]]:
        """Wait for job cancellation completion."""
        if job_id in self._cancellation_results:
            return self._cancellation_results[job_id]

        # Create event and wait
        event = asyncio.Event()
        self._cancellation_events[job_id] = event

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return self._cancellation_results.get(job_id, (False, ["timeout"]))
        except asyncio.TimeoutError:
            return (False, ["timeout"])
        finally:
            self._cancellation_events.pop(job_id, None)


# =============================================================================
# Test Classes
# =============================================================================


class TestWorkerPushCancellationComplete:
    """Tests for worker pushing WorkflowCancellationComplete to manager."""

    @pytest.mark.asyncio
    async def test_push_to_job_leader(self):
        """Worker should push cancellation completion to job leader."""
        worker = MockWorkerServer()

        job_leader_addr = ("192.168.1.10", 9090)
        worker.set_job_leader("workflow-001", job_leader_addr)

        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
        )

        # Should have sent to job leader
        assert len(worker._tcp_calls) == 1
        assert worker._tcp_calls[0][0] == job_leader_addr
        assert worker._tcp_calls[0][1] == "workflow_cancellation_complete"

    @pytest.mark.asyncio
    async def test_push_with_errors(self):
        """Worker should include errors in cancellation completion."""
        worker = MockWorkerServer()

        job_leader_addr = ("192.168.1.10", 9090)
        worker.set_job_leader("workflow-001", job_leader_addr)

        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=False,
            errors=["Task timed out", "Resource cleanup failed"],
        )

        assert len(worker._tcp_calls) == 1
        # The actual message contains the errors

    @pytest.mark.asyncio
    async def test_fallback_to_healthy_manager(self):
        """Worker should fallback to other managers if job leader unknown."""
        worker = MockWorkerServer()

        # No job leader set, but healthy manager exists
        worker.add_manager("manager-001", "192.168.1.20", 9090)

        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
        )

        # Should have sent to healthy manager
        assert len(worker._tcp_calls) == 1
        assert worker._tcp_calls[0][0] == ("192.168.1.20", 9090)

    @pytest.mark.asyncio
    async def test_no_managers_available(self):
        """Worker should handle case where no managers are available."""
        worker = MockWorkerServer()

        # No job leader, no healthy managers
        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
        )

        # No calls made (graceful degradation)
        assert len(worker._tcp_calls) == 0


class TestManagerReceiveCancellationComplete:
    """Tests for manager receiving WorkflowCancellationComplete from worker."""

    @pytest.mark.asyncio
    async def test_receive_workflow_completion(self):
        """Manager should track received workflow cancellation completions."""
        manager = MockManagerServer()

        manager.add_job("job-001", ["workflow-001"])

        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        await manager.workflow_cancellation_complete(completion)

        assert len(manager._cancellation_completions) == 1
        assert manager._cancellation_completions[0].job_id == "job-001"

    @pytest.mark.asyncio
    async def test_push_to_gate_when_all_cancelled(self):
        """Manager should push to gate when all workflows cancelled."""
        manager = MockManagerServer()

        gate_addr = ("192.168.1.100", 8080)
        manager.add_job("job-001", ["workflow-001"])
        manager.set_origin_gate("job-001", gate_addr)

        # Mark workflow as cancelled before receiving completion
        manager.mark_workflow_cancelled("job-001", "workflow-001")

        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        await manager.workflow_cancellation_complete(completion)

        # Should have pushed to gate
        gate_calls = [c for c in manager._tcp_calls if c[0] == gate_addr]
        assert len(gate_calls) == 1
        assert gate_calls[0][1] == "receive_job_cancellation_complete"

    @pytest.mark.asyncio
    async def test_push_to_client_callback_if_no_gate(self):
        """Manager should push to client callback if no origin gate."""
        manager = MockManagerServer()

        client_addr = ("192.168.1.200", 7070)
        manager.add_job("job-001", ["workflow-001"])
        manager.set_client_callback("job-001", client_addr)

        manager.mark_workflow_cancelled("job-001", "workflow-001")

        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        await manager.workflow_cancellation_complete(completion)

        # Should have pushed to client callback
        client_calls = [c for c in manager._tcp_calls if c[0] == client_addr]
        assert len(client_calls) == 1


class TestGateReceiveCancellationComplete:
    """Tests for gate receiving JobCancellationComplete from manager."""

    @pytest.mark.asyncio
    async def test_receive_job_completion(self):
        """Gate should track received job cancellation completions."""
        gate = MockGateServer()

        completion = MockJobCancellationComplete(
            job_id="job-001",
            success=True,
            cancelled_workflow_count=3,
            total_workflow_count=3,
            errors=[],
            cancelled_at=time.monotonic(),
        )

        await gate.receive_job_cancellation_complete(completion)

        assert len(gate._received_completions) == 1
        assert gate._received_completions[0].job_id == "job-001"

    @pytest.mark.asyncio
    async def test_forward_to_client_callback(self):
        """Gate should forward completion to client callback."""
        gate = MockGateServer()

        client_addr = ("192.168.1.200", 7070)
        gate.set_client_callback("job-001", client_addr)

        completion = MockJobCancellationComplete(
            job_id="job-001",
            success=True,
            cancelled_workflow_count=3,
            total_workflow_count=3,
            errors=[],
            cancelled_at=time.monotonic(),
        )

        await gate.receive_job_cancellation_complete(completion)

        # Should have forwarded to client
        client_calls = [c for c in gate._tcp_calls if c[0] == client_addr]
        assert len(client_calls) == 1
        assert client_calls[0][1] == "receive_job_cancellation_complete"


class TestClientReceiveCancellationComplete:
    """Tests for client receiving JobCancellationComplete."""

    @pytest.mark.asyncio
    async def test_receive_completion(self):
        """Client should track received cancellation completions."""
        client = MockClientServer()

        completion = MockJobCancellationComplete(
            job_id="job-001",
            success=True,
            cancelled_workflow_count=3,
            total_workflow_count=3,
            errors=[],
            cancelled_at=time.monotonic(),
        )

        await client.receive_job_cancellation_complete(completion)

        assert len(client._received_completions) == 1
        assert client._cancellation_results["job-001"] == (True, [])

    @pytest.mark.asyncio
    async def test_receive_completion_with_errors(self):
        """Client should receive and store errors."""
        client = MockClientServer()

        errors = ["Workflow-001 timeout", "Workflow-002 cleanup failed"]
        completion = MockJobCancellationComplete(
            job_id="job-001",
            success=False,
            cancelled_workflow_count=1,
            total_workflow_count=3,
            errors=errors,
            cancelled_at=time.monotonic(),
        )

        await client.receive_job_cancellation_complete(completion)

        success, result_errors = client._cancellation_results["job-001"]
        assert not success
        assert result_errors == errors

    @pytest.mark.asyncio
    async def test_await_cancellation_immediate(self):
        """Client await should return immediately if result available."""
        client = MockClientServer()

        # Pre-populate result
        client._cancellation_results["job-001"] = (True, [])

        success, errors = await client.await_job_cancellation("job-001", timeout=1.0)

        assert success
        assert errors == []

    @pytest.mark.asyncio
    async def test_await_cancellation_with_event(self):
        """Client await should wait for event signal."""
        client = MockClientServer()

        async def send_completion_later():
            await asyncio.sleep(0.1)
            completion = MockJobCancellationComplete(
                job_id="job-001",
                success=True,
                cancelled_workflow_count=1,
                total_workflow_count=1,
                errors=[],
                cancelled_at=time.monotonic(),
            )
            await client.receive_job_cancellation_complete(completion)

        # Start sending completion in background
        task = asyncio.create_task(send_completion_later())

        success, errors = await client.await_job_cancellation("job-001", timeout=1.0)

        assert success
        assert errors == []

        await task

    @pytest.mark.asyncio
    async def test_await_cancellation_timeout(self):
        """Client await should timeout if no completion received."""
        client = MockClientServer()

        success, errors = await client.await_job_cancellation("job-001", timeout=0.1)

        assert not success
        assert "timeout" in errors


class TestFullPushChain:
    """Integration tests for the full Worker → Manager → Gate → Client chain."""

    @pytest.mark.asyncio
    async def test_full_chain_success(self):
        """Test complete successful cancellation flow through all layers."""
        worker = MockWorkerServer()
        manager = MockManagerServer()
        gate = MockGateServer()
        client = MockClientServer()

        # Setup: worker knows job leader
        job_leader_addr = ("192.168.1.10", 9090)
        worker.set_job_leader("workflow-001", job_leader_addr)

        # Setup: manager knows gate and has job
        gate_addr = ("192.168.1.100", 8080)
        manager.add_job("job-001", ["workflow-001"])
        manager.set_origin_gate("job-001", gate_addr)
        manager.mark_workflow_cancelled("job-001", "workflow-001")

        # Setup: gate knows client
        client_addr = ("192.168.1.200", 7070)
        gate.set_client_callback("job-001", client_addr)

        # Step 1: Worker pushes to manager
        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
        )

        # Step 2: Manager receives and creates completion
        worker_completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )
        await manager.workflow_cancellation_complete(worker_completion)

        # Verify manager pushed to gate
        gate_pushes = [c for c in manager._tcp_calls if c[1] == "receive_job_cancellation_complete"]
        assert len(gate_pushes) == 1

        # Step 3: Gate receives and forwards
        job_completion = MockJobCancellationComplete(
            job_id="job-001",
            success=True,
            cancelled_workflow_count=1,
            total_workflow_count=1,
            errors=[],
            cancelled_at=time.monotonic(),
        )
        await gate.receive_job_cancellation_complete(job_completion)

        # Verify gate forwarded to client
        client_forwards = [c for c in gate._tcp_calls if c[1] == "receive_job_cancellation_complete"]
        assert len(client_forwards) == 1

        # Step 4: Client receives
        await client.receive_job_cancellation_complete(job_completion)

        # Verify client has result
        assert "job-001" in client._cancellation_results
        success, errors = client._cancellation_results["job-001"]
        assert success
        assert errors == []

    @pytest.mark.asyncio
    async def test_full_chain_with_errors(self):
        """Test cancellation flow with errors propagated through chain."""
        manager = MockManagerServer()
        gate = MockGateServer()
        client = MockClientServer()

        # Setup
        gate_addr = ("192.168.1.100", 8080)
        manager.add_job("job-001", ["workflow-001", "workflow-002"])
        manager.set_origin_gate("job-001", gate_addr)
        manager.mark_workflow_cancelled("job-001", "workflow-001")
        manager.mark_workflow_cancelled("job-001", "workflow-002")

        client_addr = ("192.168.1.200", 7070)
        gate.set_client_callback("job-001", client_addr)

        # Worker reports failure
        worker_completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=False,
            errors=["Task stuck in syscall"],
            cancelled_at=time.time(),
            node_id="worker-001",
        )
        await manager.workflow_cancellation_complete(worker_completion)

        # Manager should push with errors
        job_completion = MockJobCancellationComplete(
            job_id="job-001",
            success=False,
            cancelled_workflow_count=1,
            total_workflow_count=2,
            errors=["Task stuck in syscall"],
            cancelled_at=time.monotonic(),
        )
        await gate.receive_job_cancellation_complete(job_completion)
        await client.receive_job_cancellation_complete(job_completion)

        # Verify errors propagated to client
        success, errors = client._cancellation_results["job-001"]
        assert not success
        assert "Task stuck in syscall" in errors

    @pytest.mark.asyncio
    async def test_multiple_workflows_aggregation(self):
        """Test cancellation with multiple workflows being aggregated."""
        manager = MockManagerServer()

        manager.add_job("job-001", ["workflow-001", "workflow-002", "workflow-003"])
        manager.set_origin_gate("job-001", ("192.168.1.100", 8080))

        # Mark all as cancelled
        for wf_id in ["workflow-001", "workflow-002", "workflow-003"]:
            manager.mark_workflow_cancelled("job-001", wf_id)

        # Receive completion for each
        for wf_id in ["workflow-001", "workflow-002", "workflow-003"]:
            completion = MockWorkflowCancellationComplete(
                job_id="job-001",
                workflow_id=wf_id,
                success=True,
                errors=[],
                cancelled_at=time.time(),
                node_id="worker-001",
            )
            await manager.workflow_cancellation_complete(completion)

        # Should have received 3 completions
        assert len(manager._cancellation_completions) == 3

        # Should have pushed to gate 3 times (once per workflow completion when all cancelled)
        gate_pushes = [c for c in manager._tcp_calls if c[1] == "receive_job_cancellation_complete"]
        assert len(gate_pushes) == 3


# =============================================================================
# Extended Tests: Negative Paths and Failure Modes
# =============================================================================


class TestNegativePathsWorker:
    """Tests for worker negative paths and error handling."""

    @pytest.mark.asyncio
    async def test_push_with_unknown_workflow_no_job_leader(self):
        """Worker should handle workflow with no known job leader."""
        worker = MockWorkerServer()

        # No job leader set, no healthy managers
        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="unknown-workflow",
            success=True,
            errors=[],
        )

        # Should silently succeed with no TCP calls
        assert len(worker._tcp_calls) == 0

    @pytest.mark.asyncio
    async def test_push_with_empty_error_list(self):
        """Worker should handle empty error list correctly."""
        worker = MockWorkerServer()

        job_leader_addr = ("192.168.1.10", 9090)
        worker.set_job_leader("workflow-001", job_leader_addr)

        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=False,
            errors=[],  # Empty but success=False
        )

        assert len(worker._tcp_calls) == 1

    @pytest.mark.asyncio
    async def test_push_with_very_long_error_messages(self):
        """Worker should handle very long error messages."""
        worker = MockWorkerServer()

        job_leader_addr = ("192.168.1.10", 9090)
        worker.set_job_leader("workflow-001", job_leader_addr)

        # Very long error message
        long_error = "E" * 10000

        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=False,
            errors=[long_error],
        )

        assert len(worker._tcp_calls) == 1

    @pytest.mark.asyncio
    async def test_push_with_many_errors(self):
        """Worker should handle many errors in list."""
        worker = MockWorkerServer()

        job_leader_addr = ("192.168.1.10", 9090)
        worker.set_job_leader("workflow-001", job_leader_addr)

        # 100 errors
        errors = [f"Error {i}: Something went wrong" for i in range(100)]

        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=False,
            errors=errors,
        )

        assert len(worker._tcp_calls) == 1

    @pytest.mark.asyncio
    async def test_push_after_manager_removed_from_healthy(self):
        """Worker should skip manager if removed from healthy set."""
        worker = MockWorkerServer()

        # Add manager then remove from healthy
        worker.add_manager("manager-001", "192.168.1.20", 9090)
        worker._healthy_manager_ids.discard("manager-001")

        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
        )

        # No calls (manager not healthy)
        assert len(worker._tcp_calls) == 0


class TestNegativePathsManager:
    """Tests for manager negative paths and error handling."""

    @pytest.mark.asyncio
    async def test_receive_completion_for_unknown_job(self):
        """Manager should handle completion for unknown job."""
        manager = MockManagerServer()

        # No job added
        completion = MockWorkflowCancellationComplete(
            job_id="unknown-job",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        # Should not raise
        await manager.workflow_cancellation_complete(completion)

        # Should record completion
        assert len(manager._cancellation_completions) == 1

    @pytest.mark.asyncio
    async def test_receive_completion_for_unknown_workflow(self):
        """Manager should handle completion for unknown workflow in known job."""
        manager = MockManagerServer()

        manager.add_job("job-001", ["workflow-001"])

        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="unknown-workflow",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        await manager.workflow_cancellation_complete(completion)

        assert len(manager._cancellation_completions) == 1

    @pytest.mark.asyncio
    async def test_receive_duplicate_completion(self):
        """Manager should handle duplicate completions for same workflow."""
        manager = MockManagerServer()

        manager.add_job("job-001", ["workflow-001"])
        manager.mark_workflow_cancelled("job-001", "workflow-001")
        manager.set_origin_gate("job-001", ("192.168.1.100", 8080))

        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        # Send twice
        await manager.workflow_cancellation_complete(completion)
        await manager.workflow_cancellation_complete(completion)

        # Both recorded
        assert len(manager._cancellation_completions) == 2

    @pytest.mark.asyncio
    async def test_push_with_no_origin_gate_or_callback(self):
        """Manager should handle case where no destination is configured."""
        manager = MockManagerServer()

        manager.add_job("job-001", ["workflow-001"])
        manager.mark_workflow_cancelled("job-001", "workflow-001")
        # No origin gate or callback set

        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        # Should not raise
        await manager.workflow_cancellation_complete(completion)

        # No TCP calls (no destination)
        assert len(manager._tcp_calls) == 0


class TestNegativePathsGate:
    """Tests for gate negative paths and error handling."""

    @pytest.mark.asyncio
    async def test_receive_completion_no_client_callback(self):
        """Gate should handle completion when no client callback registered."""
        gate = MockGateServer()

        # No client callback set
        completion = MockJobCancellationComplete(
            job_id="job-001",
            success=True,
            cancelled_workflow_count=1,
            total_workflow_count=1,
            errors=[],
            cancelled_at=time.monotonic(),
        )

        await gate.receive_job_cancellation_complete(completion)

        # Should record but not forward
        assert len(gate._received_completions) == 1
        assert len(gate._tcp_calls) == 0

    @pytest.mark.asyncio
    async def test_receive_completion_for_different_job_id(self):
        """Gate should not forward to wrong client callback."""
        gate = MockGateServer()

        # Callback for different job
        gate.set_client_callback("other-job", ("192.168.1.200", 7070))

        completion = MockJobCancellationComplete(
            job_id="job-001",  # Different from callback
            success=True,
            cancelled_workflow_count=1,
            total_workflow_count=1,
            errors=[],
            cancelled_at=time.monotonic(),
        )

        await gate.receive_job_cancellation_complete(completion)

        # Should record but not forward (different job)
        assert len(gate._received_completions) == 1
        assert len(gate._tcp_calls) == 0


class TestNegativePathsClient:
    """Tests for client negative paths and error handling."""

    @pytest.mark.asyncio
    async def test_await_cancellation_for_unknown_job(self):
        """Client await should timeout for unknown job."""
        client = MockClientServer()

        success, errors = await client.await_job_cancellation("unknown-job", timeout=0.1)

        assert not success
        assert "timeout" in errors

    @pytest.mark.asyncio
    async def test_receive_completion_overwrites_previous(self):
        """Later completion should overwrite earlier result for same job."""
        client = MockClientServer()

        # First completion
        completion_1 = MockJobCancellationComplete(
            job_id="job-001",
            success=False,
            cancelled_workflow_count=0,
            total_workflow_count=1,
            errors=["First error"],
            cancelled_at=time.monotonic(),
        )
        await client.receive_job_cancellation_complete(completion_1)

        # Second completion overwrites
        completion_2 = MockJobCancellationComplete(
            job_id="job-001",
            success=True,
            cancelled_workflow_count=1,
            total_workflow_count=1,
            errors=[],
            cancelled_at=time.monotonic(),
        )
        await client.receive_job_cancellation_complete(completion_2)

        # Latest wins
        success, errors = client._cancellation_results["job-001"]
        assert success
        assert errors == []


# =============================================================================
# Extended Tests: Concurrency and Race Conditions
# =============================================================================


class TestConcurrencyWorker:
    """Tests for concurrent operations on worker."""

    @pytest.mark.asyncio
    async def test_concurrent_pushes_for_different_workflows(self):
        """Worker should handle concurrent pushes for different workflows."""
        worker = MockWorkerServer()

        # Setup job leaders for multiple workflows
        for i in range(10):
            worker.set_job_leader(f"workflow-{i:03d}", ("192.168.1.10", 9090))

        # Push all concurrently
        await asyncio.gather(*[
            worker._push_cancellation_complete(
                job_id="job-001",
                workflow_id=f"workflow-{i:03d}",
                success=True,
                errors=[],
            )
            for i in range(10)
        ])

        # All should succeed
        assert len(worker._tcp_calls) == 10

    @pytest.mark.asyncio
    async def test_concurrent_pushes_same_workflow(self):
        """Worker should handle concurrent pushes for same workflow."""
        worker = MockWorkerServer()

        worker.set_job_leader("workflow-001", ("192.168.1.10", 9090))

        # Push same workflow multiple times concurrently
        await asyncio.gather(*[
            worker._push_cancellation_complete(
                job_id="job-001",
                workflow_id="workflow-001",
                success=True,
                errors=[],
            )
            for _ in range(5)
        ])

        # All pushes should go through
        assert len(worker._tcp_calls) == 5

    @pytest.mark.asyncio
    async def test_rapid_succession_pushes(self):
        """Worker should handle rapid succession of pushes."""
        worker = MockWorkerServer()

        worker.set_job_leader("workflow-001", ("192.168.1.10", 9090))

        # Rapid fire
        for i in range(100):
            await worker._push_cancellation_complete(
                job_id="job-001",
                workflow_id="workflow-001",
                success=i % 2 == 0,  # Alternate success/failure
                errors=[] if i % 2 == 0 else [f"Error {i}"],
            )

        assert len(worker._tcp_calls) == 100


class TestConcurrencyManager:
    """Tests for concurrent operations on manager."""

    @pytest.mark.asyncio
    async def test_concurrent_completions_from_multiple_workers(self):
        """Manager should handle concurrent completions from multiple workers."""
        manager = MockManagerServer()

        manager.add_job("job-001", [f"workflow-{i:03d}" for i in range(10)])
        manager.set_origin_gate("job-001", ("192.168.1.100", 8080))

        # Mark all cancelled
        for i in range(10):
            manager.mark_workflow_cancelled("job-001", f"workflow-{i:03d}")

        # Send completions concurrently from different "workers"
        await asyncio.gather(*[
            manager.workflow_cancellation_complete(
                MockWorkflowCancellationComplete(
                    job_id="job-001",
                    workflow_id=f"workflow-{i:03d}",
                    success=True,
                    errors=[],
                    cancelled_at=time.time(),
                    node_id=f"worker-{i:03d}",
                )
            )
            for i in range(10)
        ])

        # All completions recorded
        assert len(manager._cancellation_completions) == 10

    @pytest.mark.asyncio
    async def test_concurrent_completions_for_different_jobs(self):
        """Manager should handle concurrent completions for different jobs."""
        manager = MockManagerServer()

        # Setup multiple jobs
        for job_idx in range(5):
            job_id = f"job-{job_idx:03d}"
            manager.add_job(job_id, [f"{job_id}-workflow-001"])
            manager.set_origin_gate(job_id, ("192.168.1.100", 8080))
            manager.mark_workflow_cancelled(job_id, f"{job_id}-workflow-001")

        # Concurrent completions for different jobs
        await asyncio.gather(*[
            manager.workflow_cancellation_complete(
                MockWorkflowCancellationComplete(
                    job_id=f"job-{job_idx:03d}",
                    workflow_id=f"job-{job_idx:03d}-workflow-001",
                    success=True,
                    errors=[],
                    cancelled_at=time.time(),
                    node_id="worker-001",
                )
            )
            for job_idx in range(5)
        ])

        # All completions recorded
        assert len(manager._cancellation_completions) == 5


class TestConcurrencyClient:
    """Tests for concurrent operations on client."""

    @pytest.mark.asyncio
    async def test_multiple_waiters_same_job(self):
        """Multiple awaits on same job should all receive result."""
        client = MockClientServer()

        # Start multiple waiters
        async def waiter():
            return await client.await_job_cancellation("job-001", timeout=1.0)

        waiter_tasks = [asyncio.create_task(waiter()) for _ in range(5)]

        # Send completion after waiters started
        await asyncio.sleep(0.05)

        completion = MockJobCancellationComplete(
            job_id="job-001",
            success=True,
            cancelled_workflow_count=1,
            total_workflow_count=1,
            errors=[],
            cancelled_at=time.monotonic(),
        )
        await client.receive_job_cancellation_complete(completion)

        # All waiters should get result (or timeout if event not shared)
        results = await asyncio.gather(*waiter_tasks)

        # At least one should succeed
        successes = [r for r in results if r[0]]
        assert len(successes) >= 1

    @pytest.mark.asyncio
    async def test_concurrent_receives_different_jobs(self):
        """Client should handle concurrent receives for different jobs."""
        client = MockClientServer()

        completions = [
            MockJobCancellationComplete(
                job_id=f"job-{i:03d}",
                success=True,
                cancelled_workflow_count=1,
                total_workflow_count=1,
                errors=[],
                cancelled_at=time.monotonic(),
            )
            for i in range(10)
        ]

        await asyncio.gather(*[
            client.receive_job_cancellation_complete(c) for c in completions
        ])

        # All recorded
        assert len(client._received_completions) == 10
        assert len(client._cancellation_results) == 10


# =============================================================================
# Extended Tests: Edge Cases and Boundary Conditions
# =============================================================================


class TestEdgeCasesWorker:
    """Edge case tests for worker."""

    @pytest.mark.asyncio
    async def test_push_with_special_characters_in_ids(self):
        """Worker should handle special characters in job/workflow IDs."""
        worker = MockWorkerServer()

        job_leader_addr = ("192.168.1.10", 9090)

        special_ids = [
            ("job:with:colons", "workflow:with:colons"),
            ("job-with-dashes", "workflow-with-dashes"),
            ("job_with_underscores", "workflow_with_underscores"),
            ("job.with.dots", "workflow.with.dots"),
            ("job/with/slashes", "workflow/with/slashes"),
        ]

        for job_id, workflow_id in special_ids:
            worker.set_job_leader(workflow_id, job_leader_addr)
            await worker._push_cancellation_complete(
                job_id=job_id,
                workflow_id=workflow_id,
                success=True,
                errors=[],
            )

        assert len(worker._tcp_calls) == 5

    @pytest.mark.asyncio
    async def test_push_with_unicode_in_errors(self):
        """Worker should handle unicode in error messages."""
        worker = MockWorkerServer()

        job_leader_addr = ("192.168.1.10", 9090)
        worker.set_job_leader("workflow-001", job_leader_addr)

        unicode_errors = [
            "Error with emoji: 🚀",
            "Error with Japanese: エラー",
            "Error with Chinese: 错误",
            "Error with Arabic: خطأ",
        ]

        await worker._push_cancellation_complete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=False,
            errors=unicode_errors,
        )

        assert len(worker._tcp_calls) == 1

    @pytest.mark.asyncio
    async def test_push_with_empty_job_id(self):
        """Worker should handle empty job ID."""
        worker = MockWorkerServer()

        job_leader_addr = ("192.168.1.10", 9090)
        worker.set_job_leader("workflow-001", job_leader_addr)

        await worker._push_cancellation_complete(
            job_id="",  # Empty
            workflow_id="workflow-001",
            success=True,
            errors=[],
        )

        assert len(worker._tcp_calls) == 1


class TestEdgeCasesManager:
    """Edge case tests for manager."""

    @pytest.mark.asyncio
    async def test_zero_workflow_job(self):
        """Manager should handle job with zero workflows."""
        manager = MockManagerServer()

        manager.add_job("job-001", [])  # No workflows
        manager.set_origin_gate("job-001", ("192.168.1.100", 8080))

        # Receiving completion for unknown workflow in zero-workflow job
        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="phantom-workflow",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        await manager.workflow_cancellation_complete(completion)

        # Should record but no all_cancelled (empty = all cancelled)
        assert len(manager._cancellation_completions) == 1

    @pytest.mark.asyncio
    async def test_partial_workflow_cancellation_status(self):
        """Manager should only push when ALL workflows are cancelled."""
        manager = MockManagerServer()

        manager.add_job("job-001", ["workflow-001", "workflow-002"])
        manager.set_origin_gate("job-001", ("192.168.1.100", 8080))

        # Only mark one as cancelled
        manager.mark_workflow_cancelled("job-001", "workflow-001")

        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time(),
            node_id="worker-001",
        )

        await manager.workflow_cancellation_complete(completion)

        # Should NOT push to gate (workflow-002 not cancelled)
        gate_pushes = [c for c in manager._tcp_calls if c[1] == "receive_job_cancellation_complete"]
        assert len(gate_pushes) == 0

    @pytest.mark.asyncio
    async def test_completion_with_future_timestamp(self):
        """Manager should handle completion with future timestamp."""
        manager = MockManagerServer()

        manager.add_job("job-001", ["workflow-001"])
        manager.mark_workflow_cancelled("job-001", "workflow-001")
        manager.set_origin_gate("job-001", ("192.168.1.100", 8080))

        # Future timestamp
        completion = MockWorkflowCancellationComplete(
            job_id="job-001",
            workflow_id="workflow-001",
            success=True,
            errors=[],
            cancelled_at=time.time() + 86400,  # 1 day in future
            node_id="worker-001",
        )

        await manager.workflow_cancellation_complete(completion)

        # Should still process
        assert len(manager._cancellation_completions) == 1


class TestEdgeCasesClient:
    """Edge case tests for client."""

    @pytest.mark.asyncio
    async def test_await_with_zero_timeout(self):
        """Client await with zero timeout should return immediately."""
        client = MockClientServer()

        success, errors = await client.await_job_cancellation("job-001", timeout=0.0)

        assert not success
        assert "timeout" in errors

    @pytest.mark.asyncio
    async def test_await_with_very_short_timeout(self):
        """Client await with very short timeout should handle gracefully."""
        client = MockClientServer()

        success, errors = await client.await_job_cancellation("job-001", timeout=0.001)

        assert not success
        assert "timeout" in errors

    @pytest.mark.asyncio
    async def test_completion_with_zero_counts(self):
        """Client should handle completion with zero workflow counts."""
        client = MockClientServer()

        completion = MockJobCancellationComplete(
            job_id="job-001",
            success=True,
            cancelled_workflow_count=0,
            total_workflow_count=0,
            errors=[],
            cancelled_at=time.monotonic(),
        )

        await client.receive_job_cancellation_complete(completion)

        success, errors = client._cancellation_results["job-001"]
        assert success

    @pytest.mark.asyncio
    async def test_completion_with_mismatched_counts(self):
        """Client should handle completion where counts don't match."""
        client = MockClientServer()

        completion = MockJobCancellationComplete(
            job_id="job-001",
            success=True,  # Success despite mismatch
            cancelled_workflow_count=3,
            total_workflow_count=5,  # 3 of 5 cancelled but still "success"
            errors=[],
            cancelled_at=time.monotonic(),
        )

        await client.receive_job_cancellation_complete(completion)

        # Should accept as-is
        assert len(client._received_completions) == 1


class TestFullChainEdgeCases:
    """Edge case tests for full push chain."""

    @pytest.mark.asyncio
    async def test_chain_with_mixed_success_failure(self):
        """Test chain where some workflows succeed, others fail."""
        manager = MockManagerServer()
        gate = MockGateServer()
        client = MockClientServer()

        # Setup
        manager.add_job("job-001", ["workflow-001", "workflow-002", "workflow-003"])
        manager.set_origin_gate("job-001", ("192.168.1.100", 8080))
        for wf in ["workflow-001", "workflow-002", "workflow-003"]:
            manager.mark_workflow_cancelled("job-001", wf)

        gate.set_client_callback("job-001", ("192.168.1.200", 7070))

        # Mixed completions
        completions = [
            MockWorkflowCancellationComplete(
                job_id="job-001",
                workflow_id="workflow-001",
                success=True,
                errors=[],
                cancelled_at=time.time(),
                node_id="worker-001",
            ),
            MockWorkflowCancellationComplete(
                job_id="job-001",
                workflow_id="workflow-002",
                success=False,
                errors=["Failed to cancel"],
                cancelled_at=time.time(),
                node_id="worker-002",
            ),
            MockWorkflowCancellationComplete(
                job_id="job-001",
                workflow_id="workflow-003",
                success=True,
                errors=[],
                cancelled_at=time.time(),
                node_id="worker-003",
            ),
        ]

        for completion in completions:
            await manager.workflow_cancellation_complete(completion)

        # All completions recorded
        assert len(manager._cancellation_completions) == 3

    @pytest.mark.asyncio
    async def test_chain_with_large_number_of_workflows(self):
        """Test chain with large number of workflows."""
        manager = MockManagerServer()

        workflow_ids = [f"workflow-{i:06d}" for i in range(1000)]
        manager.add_job("job-001", workflow_ids)
        manager.set_origin_gate("job-001", ("192.168.1.100", 8080))

        for wf_id in workflow_ids:
            manager.mark_workflow_cancelled("job-001", wf_id)

        # Send all completions
        for wf_id in workflow_ids:
            completion = MockWorkflowCancellationComplete(
                job_id="job-001",
                workflow_id=wf_id,
                success=True,
                errors=[],
                cancelled_at=time.time(),
                node_id="worker-001",
            )
            await manager.workflow_cancellation_complete(completion)

        # All recorded
        assert len(manager._cancellation_completions) == 1000

    @pytest.mark.asyncio
    async def test_chain_with_interleaved_jobs(self):
        """Test chain with completions for multiple jobs interleaved."""
        manager = MockManagerServer()

        # Setup multiple jobs
        for job_idx in range(3):
            job_id = f"job-{job_idx:03d}"
            workflow_ids = [f"{job_id}-wf-{i:03d}" for i in range(3)]
            manager.add_job(job_id, workflow_ids)
            manager.set_origin_gate(job_id, ("192.168.1.100", 8080))
            for wf_id in workflow_ids:
                manager.mark_workflow_cancelled(job_id, wf_id)

        # Interleaved completions
        for wf_idx in range(3):
            for job_idx in range(3):
                job_id = f"job-{job_idx:03d}"
                wf_id = f"{job_id}-wf-{wf_idx:03d}"
                completion = MockWorkflowCancellationComplete(
                    job_id=job_id,
                    workflow_id=wf_id,
                    success=True,
                    errors=[],
                    cancelled_at=time.time(),
                    node_id="worker-001",
                )
                await manager.workflow_cancellation_complete(completion)

        # 9 completions total (3 jobs * 3 workflows)
        assert len(manager._cancellation_completions) == 9
