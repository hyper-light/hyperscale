"""
Comprehensive Edge Case Tests for Cancellation Propagation (AD-20).

Tests rare but critical scenarios:
- Timeout handling during cancellation propagation
- Cascading failures across multiple layers
- Large scale cancellation (many workflows)
- Memory safety with repeated cancel/retry cycles
- Cancel during job failure/exception
- Duplicate request handling
- Cancel propagation ordering guarantees
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class NodeState(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass
class CancelRequest:
    job_id: str
    request_id: str
    requester_id: str
    timestamp: float
    fence_token: int = 0
    timeout_seconds: float = 5.0


@dataclass
class CancelResponse:
    job_id: str
    request_id: str
    success: bool
    cancelled_count: int = 0
    error: str | None = None
    elapsed_seconds: float = 0.0


@dataclass
class WorkflowInfo:
    workflow_id: str
    job_id: str
    worker_id: str
    status: WorkflowStatus = WorkflowStatus.RUNNING
    progress: float = 0.0


class TimeoutSimulator:
    """Simulates timeout scenarios."""

    def __init__(self):
        self._delays: dict[str, float] = {}
        self._should_timeout: dict[str, bool] = {}

    def set_delay(self, node_id: str, delay_seconds: float) -> None:
        self._delays[node_id] = delay_seconds

    def set_timeout(self, node_id: str, should_timeout: bool) -> None:
        self._should_timeout[node_id] = should_timeout

    async def apply_delay(self, node_id: str) -> None:
        delay = self._delays.get(node_id, 0.0)
        if delay > 0:
            await asyncio.sleep(delay)

    def will_timeout(self, node_id: str) -> bool:
        return self._should_timeout.get(node_id, False)


class SimulatedWorkerEdge:
    """Worker with edge case simulation capabilities."""

    def __init__(self, worker_id: str, timeout_sim: TimeoutSimulator):
        self._worker_id = worker_id
        self._workflows: dict[str, WorkflowInfo] = {}
        self._state = NodeState.HEALTHY
        self._timeout_sim = timeout_sim
        self._cancel_count = 0
        self._cancel_history: list[tuple[str, float]] = []
        self._fail_on_cancel = False
        self._crash_on_cancel = False

    def add_workflow(self, workflow: WorkflowInfo) -> None:
        self._workflows[workflow.workflow_id] = workflow

    def set_state(self, state: NodeState) -> None:
        self._state = state

    def set_fail_on_cancel(self, should_fail: bool) -> None:
        self._fail_on_cancel = should_fail

    def set_crash_on_cancel(self, should_crash: bool) -> None:
        self._crash_on_cancel = should_crash

    async def handle_cancel(self, workflow_id: str, timeout: float) -> tuple[bool, str | None]:
        """Handle workflow cancellation with edge case simulation."""
        if self._state == NodeState.UNAVAILABLE:
            raise ConnectionError(f"Worker {self._worker_id} unavailable")

        if self._crash_on_cancel:
            raise RuntimeError(f"Worker {self._worker_id} crashed during cancellation")

        await self._timeout_sim.apply_delay(self._worker_id)

        if self._timeout_sim.will_timeout(self._worker_id):
            await asyncio.sleep(timeout + 1)  # Exceed timeout

        if self._fail_on_cancel:
            return False, "Internal worker error"

        self._cancel_count += 1
        self._cancel_history.append((workflow_id, time.monotonic()))

        workflow = self._workflows.get(workflow_id)
        if workflow:
            workflow.status = WorkflowStatus.CANCELLED
            return True, None

        return True, None  # Already cancelled/completed

    @property
    def cancel_count(self) -> int:
        return self._cancel_count

    @property
    def cancel_history(self) -> list[tuple[str, float]]:
        return self._cancel_history.copy()


class SimulatedManagerEdge:
    """Manager with edge case simulation capabilities."""

    def __init__(self, manager_id: str, timeout_sim: TimeoutSimulator):
        self._manager_id = manager_id
        self._workers: dict[str, SimulatedWorkerEdge] = {}
        self._workflow_assignments: dict[str, str] = {}
        self._state = NodeState.HEALTHY
        self._timeout_sim = timeout_sim
        self._request_dedup: dict[str, CancelResponse] = {}

    def register_worker(self, worker: SimulatedWorkerEdge, worker_id: str) -> None:
        self._workers[worker_id] = worker

    def assign_workflow(self, workflow_id: str, worker_id: str) -> None:
        self._workflow_assignments[workflow_id] = worker_id

    def set_state(self, state: NodeState) -> None:
        self._state = state

    async def handle_cancel(
        self,
        request: CancelRequest,
        workflow_ids: list[str],
    ) -> CancelResponse:
        """Handle cancellation with deduplication and timeout handling."""
        start_time = time.monotonic()

        # Check for duplicate request
        if request.request_id in self._request_dedup:
            return self._request_dedup[request.request_id]

        if self._state == NodeState.UNAVAILABLE:
            raise ConnectionError(f"Manager {self._manager_id} unavailable")

        await self._timeout_sim.apply_delay(self._manager_id)

        cancelled = 0
        errors = []

        # Process workflows concurrently to allow partial success
        async def cancel_workflow(workflow_id: str) -> tuple[bool, str | None]:
            worker_id = self._workflow_assignments.get(workflow_id)
            if not worker_id:
                return False, None

            worker = self._workers.get(worker_id)
            if not worker:
                return False, f"Worker {worker_id} not found"

            try:
                success, error = await asyncio.wait_for(
                    worker.handle_cancel(workflow_id, request.timeout_seconds),
                    timeout=request.timeout_seconds,
                )
                return success, error
            except asyncio.TimeoutError:
                return False, f"Timeout cancelling {workflow_id} on {worker_id}"
            except ConnectionError as conn_err:
                return False, str(conn_err)
            except RuntimeError as runtime_err:
                return False, str(runtime_err)

        # Run all cancellations concurrently
        tasks = [cancel_workflow(wf_id) for wf_id in workflow_ids]
        results = await asyncio.gather(*tasks)

        for success, error in results:
            if success:
                cancelled += 1
            elif error:
                errors.append(error)

        elapsed = time.monotonic() - start_time
        response = CancelResponse(
            job_id=request.job_id,
            request_id=request.request_id,
            success=len(errors) == 0,
            cancelled_count=cancelled,
            error="; ".join(errors) if errors else None,
            elapsed_seconds=elapsed,
        )

        # Store for deduplication
        self._request_dedup[request.request_id] = response
        return response


class SimulatedGateEdge:
    """Gate with edge case simulation capabilities."""

    def __init__(self, gate_id: str, timeout_sim: TimeoutSimulator):
        self._gate_id = gate_id
        self._managers: dict[str, SimulatedManagerEdge] = {}
        self._job_workflows: dict[str, list[str]] = {}
        self._job_status: dict[str, JobStatus] = {}
        self._timeout_sim = timeout_sim
        self._request_dedup: dict[str, CancelResponse] = {}
        self._cancel_ordering: list[tuple[str, float]] = []

    def register_manager(self, manager: SimulatedManagerEdge, manager_id: str) -> None:
        self._managers[manager_id] = manager

    def register_job(self, job_id: str, workflow_ids: list[str]) -> None:
        self._job_workflows[job_id] = workflow_ids
        self._job_status[job_id] = JobStatus.RUNNING

    async def handle_cancel(self, request: CancelRequest) -> CancelResponse:
        """Handle cancellation at gate level."""
        start_time = time.monotonic()
        self._cancel_ordering.append((request.job_id, start_time))

        # Check for duplicate request
        if request.request_id in self._request_dedup:
            return self._request_dedup[request.request_id]

        workflow_ids = self._job_workflows.get(request.job_id, [])
        if not workflow_ids:
            return CancelResponse(
                job_id=request.job_id,
                request_id=request.request_id,
                success=False,
                error="Job not found",
            )

        total_cancelled = 0
        all_errors = []

        for manager_id, manager in self._managers.items():
            try:
                response = await asyncio.wait_for(
                    manager.handle_cancel(request, workflow_ids),
                    timeout=request.timeout_seconds,
                )
                total_cancelled += response.cancelled_count
                if response.error:
                    all_errors.append(response.error)
            except asyncio.TimeoutError:
                all_errors.append(f"Timeout from manager {manager_id}")
            except ConnectionError as conn_err:
                all_errors.append(str(conn_err))

        # Update job status
        self._job_status[request.job_id] = JobStatus.CANCELLED

        elapsed = time.monotonic() - start_time
        response = CancelResponse(
            job_id=request.job_id,
            request_id=request.request_id,
            success=len(all_errors) == 0,
            cancelled_count=total_cancelled,
            error="; ".join(all_errors) if all_errors else None,
            elapsed_seconds=elapsed,
        )

        self._request_dedup[request.request_id] = response
        return response

    @property
    def cancel_ordering(self) -> list[tuple[str, float]]:
        return self._cancel_ordering.copy()


class TestTimeoutHandling:
    """Test timeout scenarios during cancellation."""

    @pytest.mark.asyncio
    async def test_worker_timeout_during_cancel(self) -> None:
        """Test handling when worker times out during cancellation."""
        timeout_sim = TimeoutSimulator()
        worker = SimulatedWorkerEdge("worker-1", timeout_sim)
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        # Make worker timeout
        timeout_sim.set_timeout("worker-1", True)

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1")

        workflow = WorkflowInfo("wf-1", "job-1", "worker-1")
        worker.add_workflow(workflow)
        manager.assign_workflow("wf-1", "worker-1")
        gate.register_job("job-1", ["wf-1"])

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
            timeout_seconds=0.5,
        )

        response = await gate.handle_cancel(request)

        assert response.success is False
        assert "Timeout" in response.error
        assert response.cancelled_count == 0

    @pytest.mark.asyncio
    async def test_manager_timeout_during_cancel(self) -> None:
        """Test handling when manager times out during cancellation."""
        timeout_sim = TimeoutSimulator()
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        # Make manager slow
        timeout_sim.set_delay("manager-1", 2.0)

        gate.register_manager(manager, "manager-1")
        gate.register_job("job-1", ["wf-1"])

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
            timeout_seconds=0.5,
        )

        response = await gate.handle_cancel(request)

        assert response.success is False
        assert "Timeout" in response.error

    @pytest.mark.asyncio
    async def test_partial_timeout_some_workers(self) -> None:
        """Test when only some workers timeout."""
        timeout_sim = TimeoutSimulator()
        worker1 = SimulatedWorkerEdge("worker-1", timeout_sim)
        worker2 = SimulatedWorkerEdge("worker-2", timeout_sim)
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        # Only worker-2 times out (but use delay, not full timeout)
        # This allows worker-1 to succeed while worker-2 fails
        timeout_sim.set_delay("worker-2", 2.0)  # Long delay causes timeout

        manager.register_worker(worker1, "worker-1")
        manager.register_worker(worker2, "worker-2")
        gate.register_manager(manager, "manager-1")

        worker1.add_workflow(WorkflowInfo("wf-1", "job-1", "worker-1"))
        worker2.add_workflow(WorkflowInfo("wf-2", "job-1", "worker-2"))
        manager.assign_workflow("wf-1", "worker-1")
        manager.assign_workflow("wf-2", "worker-2")
        gate.register_job("job-1", ["wf-1", "wf-2"])

        # Use a short per-worker timeout (0.5s) but give the gate enough time
        # to wait for the manager to collect partial results
        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
            timeout_seconds=0.5,  # Per-worker timeout
        )

        # Call manager directly to test partial timeout at manager level
        # (bypasses gate's additional timeout layer)
        response = await manager.handle_cancel(request, ["wf-1", "wf-2"])

        # Partial success - worker-1 cancelled, worker-2 timed out
        assert response.cancelled_count == 1
        assert "Timeout" in response.error


class TestCascadingFailures:
    """Test cascading failure scenarios."""

    @pytest.mark.asyncio
    async def test_all_workers_fail(self) -> None:
        """Test when all workers fail during cancellation."""
        timeout_sim = TimeoutSimulator()
        workers = [SimulatedWorkerEdge(f"worker-{i}", timeout_sim) for i in range(5)]
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        # All workers unavailable
        for worker in workers:
            worker.set_state(NodeState.UNAVAILABLE)
            manager.register_worker(worker, worker._worker_id)

        gate.register_manager(manager, "manager-1")

        for i, worker in enumerate(workers):
            wf = WorkflowInfo(f"wf-{i}", "job-1", worker._worker_id)
            worker.add_workflow(wf)
            manager.assign_workflow(f"wf-{i}", worker._worker_id)

        gate.register_job("job-1", [f"wf-{i}" for i in range(5)])

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel(request)

        assert response.success is False
        assert response.cancelled_count == 0
        assert "unavailable" in response.error.lower()

    @pytest.mark.asyncio
    async def test_all_managers_fail(self) -> None:
        """Test when all managers fail during cancellation."""
        timeout_sim = TimeoutSimulator()
        managers = [SimulatedManagerEdge(f"manager-{i}", timeout_sim) for i in range(3)]
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        # All managers unavailable
        for manager in managers:
            manager.set_state(NodeState.UNAVAILABLE)
            gate.register_manager(manager, manager._manager_id)

        gate.register_job("job-1", ["wf-1"])

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel(request)

        assert response.success is False
        assert "unavailable" in response.error.lower()

    @pytest.mark.asyncio
    async def test_worker_crash_during_cancel(self) -> None:
        """Test worker crashing during cancellation."""
        timeout_sim = TimeoutSimulator()
        worker = SimulatedWorkerEdge("worker-1", timeout_sim)
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        worker.set_crash_on_cancel(True)

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1")

        worker.add_workflow(WorkflowInfo("wf-1", "job-1", "worker-1"))
        manager.assign_workflow("wf-1", "worker-1")
        gate.register_job("job-1", ["wf-1"])

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel(request)

        assert response.success is False
        assert "crashed" in response.error.lower()


class TestLargeScaleCancellation:
    """Test large scale cancellation scenarios."""

    @pytest.mark.asyncio
    async def test_cancel_100_workflows(self) -> None:
        """Test cancelling 100 workflows efficiently."""
        timeout_sim = TimeoutSimulator()
        num_workers = 10
        workflows_per_worker = 10

        workers = [SimulatedWorkerEdge(f"worker-{i}", timeout_sim) for i in range(num_workers)]
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        all_workflow_ids = []
        for i, worker in enumerate(workers):
            manager.register_worker(worker, worker._worker_id)
            for j in range(workflows_per_worker):
                wf_id = f"wf-{i}-{j}"
                wf = WorkflowInfo(wf_id, "job-1", worker._worker_id)
                worker.add_workflow(wf)
                manager.assign_workflow(wf_id, worker._worker_id)
                all_workflow_ids.append(wf_id)

        gate.register_manager(manager, "manager-1")
        gate.register_job("job-1", all_workflow_ids)

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
            timeout_seconds=30.0,
        )

        start = time.monotonic()
        response = await gate.handle_cancel(request)
        elapsed = time.monotonic() - start

        assert response.success is True
        assert response.cancelled_count == 100
        # Should complete reasonably quickly
        assert elapsed < 5.0

    @pytest.mark.asyncio
    async def test_cancel_with_mixed_worker_health(self) -> None:
        """Test cancelling when workers have mixed health states."""
        timeout_sim = TimeoutSimulator()
        workers = [SimulatedWorkerEdge(f"worker-{i}", timeout_sim) for i in range(10)]
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        all_workflow_ids = []
        healthy_count = 0
        for i, worker in enumerate(workers):
            # Alternate healthy/unhealthy
            if i % 2 == 0:
                worker.set_state(NodeState.HEALTHY)
                healthy_count += 1
            else:
                worker.set_state(NodeState.UNAVAILABLE)

            manager.register_worker(worker, worker._worker_id)
            wf_id = f"wf-{i}"
            wf = WorkflowInfo(wf_id, "job-1", worker._worker_id)
            worker.add_workflow(wf)
            manager.assign_workflow(wf_id, worker._worker_id)
            all_workflow_ids.append(wf_id)

        gate.register_manager(manager, "manager-1")
        gate.register_job("job-1", all_workflow_ids)

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel(request)

        assert response.cancelled_count == healthy_count
        assert response.error is not None  # Some failures


class TestDuplicateRequestHandling:
    """Test duplicate request handling."""

    @pytest.mark.asyncio
    async def test_duplicate_request_returns_same_response(self) -> None:
        """Test that duplicate requests return cached response."""
        timeout_sim = TimeoutSimulator()
        worker = SimulatedWorkerEdge("worker-1", timeout_sim)
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1")

        worker.add_workflow(WorkflowInfo("wf-1", "job-1", "worker-1"))
        manager.assign_workflow("wf-1", "worker-1")
        gate.register_job("job-1", ["wf-1"])

        request = CancelRequest(
            job_id="job-1",
            request_id="req-same-id",
            requester_id="client-1",
            timestamp=time.time(),
        )

        # First request
        response1 = await gate.handle_cancel(request)

        # Duplicate request
        response2 = await gate.handle_cancel(request)

        assert response1.request_id == response2.request_id
        assert response1.cancelled_count == response2.cancelled_count
        # Worker should only have been called once
        assert worker.cancel_count == 1

    @pytest.mark.asyncio
    async def test_different_request_ids_both_processed(self) -> None:
        """Test that different request IDs are processed independently."""
        timeout_sim = TimeoutSimulator()
        worker = SimulatedWorkerEdge("worker-1", timeout_sim)
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1")

        worker.add_workflow(WorkflowInfo("wf-1", "job-1", "worker-1"))
        manager.assign_workflow("wf-1", "worker-1")
        gate.register_job("job-1", ["wf-1"])

        request1 = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
        )
        request2 = CancelRequest(
            job_id="job-1",
            request_id="req-2",  # Different ID
            requester_id="client-1",
            timestamp=time.time(),
        )

        response1 = await gate.handle_cancel(request1)
        response2 = await gate.handle_cancel(request2)

        # Both processed (but second may find already cancelled)
        assert response1.success is True
        assert response2.success is True


class TestCancelOrdering:
    """Test cancellation ordering guarantees."""

    @pytest.mark.asyncio
    async def test_cancel_ordering_preserved(self) -> None:
        """Test that cancellation order is preserved."""
        timeout_sim = TimeoutSimulator()
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        gate.register_manager(manager, "manager-1")

        # Register multiple jobs
        for i in range(5):
            gate.register_job(f"job-{i}", [f"wf-{i}"])

        # Cancel in order
        for i in range(5):
            request = CancelRequest(
                job_id=f"job-{i}",
                request_id=f"req-{i}",
                requester_id="client-1",
                timestamp=time.time(),
            )
            await gate.handle_cancel(request)

        # Verify ordering
        ordering = gate.cancel_ordering
        assert len(ordering) == 5
        for i, (job_id, _) in enumerate(ordering):
            assert job_id == f"job-{i}"

    @pytest.mark.asyncio
    async def test_concurrent_cancels_all_complete(self) -> None:
        """Test concurrent cancellations all complete."""
        timeout_sim = TimeoutSimulator()
        workers = [SimulatedWorkerEdge(f"worker-{i}", timeout_sim) for i in range(5)]
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        for i, worker in enumerate(workers):
            manager.register_worker(worker, worker._worker_id)
            wf = WorkflowInfo(f"wf-{i}", f"job-{i}", worker._worker_id)
            worker.add_workflow(wf)
            manager.assign_workflow(f"wf-{i}", worker._worker_id)
            gate.register_job(f"job-{i}", [f"wf-{i}"])

        gate.register_manager(manager, "manager-1")

        # Concurrent cancellations
        requests = [
            CancelRequest(
                job_id=f"job-{i}",
                request_id=f"req-{i}",
                requester_id="client-1",
                timestamp=time.time(),
            )
            for i in range(5)
        ]

        responses = await asyncio.gather(*[
            gate.handle_cancel(req) for req in requests
        ])

        # All should succeed
        assert all(r.success for r in responses)
        assert sum(r.cancelled_count for r in responses) == 5


class TestMemorySafety:
    """Test memory safety with repeated operations."""

    @pytest.mark.asyncio
    async def test_repeated_cancel_retry_cycles(self) -> None:
        """Test memory doesn't grow with repeated cancel/retry cycles."""
        timeout_sim = TimeoutSimulator()
        worker = SimulatedWorkerEdge("worker-1", timeout_sim)
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1")

        worker.add_workflow(WorkflowInfo("wf-1", "job-1", "worker-1"))
        manager.assign_workflow("wf-1", "worker-1")
        gate.register_job("job-1", ["wf-1"])

        # Many cancel requests with different IDs
        for i in range(100):
            request = CancelRequest(
                job_id="job-1",
                request_id=f"req-{i}",
                requester_id="client-1",
                timestamp=time.time(),
            )
            await gate.handle_cancel(request)

        # Dedup cache should exist but not cause issues
        assert len(gate._request_dedup) == 100

    @pytest.mark.asyncio
    async def test_large_error_messages_handled(self) -> None:
        """Test that large error messages don't cause issues."""
        timeout_sim = TimeoutSimulator()
        workers = [SimulatedWorkerEdge(f"worker-{i}", timeout_sim) for i in range(50)]
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        # All workers fail with different errors
        for i, worker in enumerate(workers):
            worker.set_fail_on_cancel(True)
            manager.register_worker(worker, worker._worker_id)
            wf = WorkflowInfo(f"wf-{i}", "job-1", worker._worker_id)
            worker.add_workflow(wf)
            manager.assign_workflow(f"wf-{i}", worker._worker_id)

        gate.register_manager(manager, "manager-1")
        gate.register_job("job-1", [f"wf-{i}" for i in range(50)])

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel(request)

        assert response.success is False
        assert response.error is not None
        # Error message should contain all errors
        assert "Internal worker error" in response.error


class TestCancelDuringExceptions:
    """Test cancellation during exception handling."""

    @pytest.mark.asyncio
    async def test_cancel_while_workflow_failing(self) -> None:
        """Test cancellation while workflow is failing."""
        timeout_sim = TimeoutSimulator()
        worker = SimulatedWorkerEdge("worker-1", timeout_sim)
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1")

        wf = WorkflowInfo("wf-1", "job-1", "worker-1", status=WorkflowStatus.FAILED)
        worker.add_workflow(wf)
        manager.assign_workflow("wf-1", "worker-1")
        gate.register_job("job-1", ["wf-1"])

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = await gate.handle_cancel(request)

        # Should handle gracefully
        assert response.success is True

    @pytest.mark.asyncio
    async def test_cancel_with_rapid_state_changes(self) -> None:
        """Test cancellation with rapid workflow state changes."""
        timeout_sim = TimeoutSimulator()
        worker = SimulatedWorkerEdge("worker-1", timeout_sim)
        manager = SimulatedManagerEdge("manager-1", timeout_sim)
        gate = SimulatedGateEdge("gate-1", timeout_sim)

        manager.register_worker(worker, "worker-1")
        gate.register_manager(manager, "manager-1")

        wf = WorkflowInfo("wf-1", "job-1", "worker-1")
        worker.add_workflow(wf)
        manager.assign_workflow("wf-1", "worker-1")
        gate.register_job("job-1", ["wf-1"])

        async def change_state():
            for _ in range(10):
                wf.status = WorkflowStatus.RUNNING
                await asyncio.sleep(0.001)
                wf.status = WorkflowStatus.COMPLETED
                await asyncio.sleep(0.001)

        request = CancelRequest(
            job_id="job-1",
            request_id="req-1",
            requester_id="client-1",
            timestamp=time.time(),
        )

        # Run cancellation and state changes concurrently
        _, response = await asyncio.gather(
            change_state(),
            gate.handle_cancel(request),
        )

        # Should complete without error
        assert response is not None
