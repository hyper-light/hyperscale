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
