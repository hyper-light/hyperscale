"""
Unit tests for Worker-Side Job Leader Failure Handling (Section 3).

These tests verify the orphan workflow handling when a job leader manager fails:
1. Workflows are marked as orphaned when their job leader manager fails
2. Orphaned workflows are rescued when JobLeaderWorkerTransfer arrives before grace period
3. Orphaned workflows are cancelled when grace period expires without transfer
4. Configuration of grace period and check interval

All networking I/O is mocked to enable pure asyncio unit testing.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hyperscale.distributed.models import (
    JobLeaderWorkerTransfer,
    JobLeaderWorkerTransferAck,
    ManagerInfo,
    WorkflowProgress,
    WorkflowStatus,
)


@dataclass
class MockEnv:
    """Mock environment configuration for testing."""
    
    WORKER_MAX_CORES: int | None = 4
    WORKER_PROGRESS_UPDATE_INTERVAL: float = 0.05
    WORKER_PROGRESS_FLUSH_INTERVAL: float = 0.05
    WORKER_DEAD_MANAGER_REAP_INTERVAL: float = 900.0
    WORKER_DEAD_MANAGER_CHECK_INTERVAL: float = 60.0
    WORKER_CANCELLATION_POLL_INTERVAL: float = 5.0
    WORKER_TCP_TIMEOUT_SHORT: float = 2.0
    WORKER_TCP_TIMEOUT_STANDARD: float = 5.0
    WORKER_ORPHAN_GRACE_PERIOD: float = 0.5
    WORKER_ORPHAN_CHECK_INTERVAL: float = 0.1
    RECOVERY_JITTER_MIN: float = 0.0
    RECOVERY_JITTER_MAX: float = 0.0
    RECOVERY_SEMAPHORE_SIZE: int = 5
    MERCURY_SYNC_MAX_PENDING_WORKFLOWS: int = 100
    DISCOVERY_PROBE_INTERVAL: float = 30.0
    DISCOVERY_FAILURE_DECAY_INTERVAL: float = 60.0
    
    def get_discovery_config(self, **kwargs) -> MagicMock:
        mock_config = MagicMock()
        mock_config.dns_names = []
        return mock_config


class MockTaskRunner:
    """Mock TaskRunner that executes coroutines immediately."""
    
    def __init__(self):
        self.tasks: list[asyncio.Task] = []
        self._cancelled_tokens: set[str] = set()
    
    def run(self, coro_or_func, *args, **kwargs) -> str:
        token = f"task-{len(self.tasks)}"
        if asyncio.iscoroutinefunction(coro_or_func):
            coro = coro_or_func(*args, **kwargs)
            try:
                loop = asyncio.get_running_loop()
                task = loop.create_task(coro)
                self.tasks.append(task)
            except RuntimeError:
                pass
        return token
    
    async def cancel(self, token: str) -> None:
        self._cancelled_tokens.add(token)


class MockLogger:
    """Mock async logger."""
    
    def __init__(self):
        self.logs: list[Any] = []
    
    async def log(self, message: Any) -> None:
        self.logs.append(message)


class MockDiscoveryService:
    """Mock discovery service."""
    
    def __init__(self, config: Any):
        self.config = config
        self.peers: dict[str, tuple[str, int]] = {}
    
    def add_peer(self, peer_id: str, host: str, port: int, **kwargs) -> None:
        self.peers[peer_id] = (host, port)
    
    def decay_failures(self) -> None:
        pass
    
    def cleanup_expired_dns(self) -> None:
        pass


class MockCoreAllocator:
    """Mock core allocator."""
    
    def __init__(self, total_cores: int):
        self.total_cores = total_cores
        self.available_cores = total_cores
    
    async def get_core_assignments(self) -> dict[int, str | None]:
        return {}


class MockNodeId:
    """Mock node identifier."""
    
    def __init__(self):
        self.full = "worker-test-node-12345678"
        self.short = "worker-test"


class WorkerOrphanTestHarness:
    """
    Test harness that simulates WorkerServer orphan handling behavior.
    
    Isolates the orphan-related logic for unit testing without
    requiring full server initialization.
    """
    
    def __init__(self, orphan_grace_period: float = 0.5, orphan_check_interval: float = 0.1):
        self._running = True
        self._host = "127.0.0.1"
        self._tcp_port = 9000
        self._node_id = MockNodeId()
        
        self._orphan_grace_period = orphan_grace_period
        self._orphan_check_interval = orphan_check_interval
        
        self._orphaned_workflows: dict[str, float] = {}
        self._active_workflows: dict[str, WorkflowProgress] = {}
        self._workflow_job_leader: dict[str, tuple[str, int]] = {}
        self._known_managers: dict[str, ManagerInfo] = {}
        self._healthy_manager_ids: set[str] = set()
        self._manager_unhealthy_since: dict[str, float] = {}
        self._manager_state_epoch: dict[str, int] = {}
        self._manager_state_locks: dict[str, asyncio.Lock] = {}
        self._primary_manager_id: str | None = None
        self._workflow_tokens: dict[str, str] = {}
        self._workflow_cancel_events: dict[str, asyncio.Event] = {}
        self._workflow_id_to_name: dict[str, str] = {}
        
        self._task_runner = MockTaskRunner()
        self._udp_logger = MockLogger()
        self._recovery_semaphore = asyncio.Semaphore(5)
        
        self._cancelled_workflows: list[str] = []
        self._orphan_check_task: asyncio.Task | None = None
    
    def _get_manager_state_lock(self, manager_id: str) -> asyncio.Lock:
        if manager_id not in self._manager_state_locks:
            self._manager_state_locks[manager_id] = asyncio.Lock()
        return self._manager_state_locks[manager_id]
    
    def add_manager(
        self,
        manager_id: str,
        tcp_host: str,
        tcp_port: int,
        udp_host: str,
        udp_port: int,
        is_leader: bool = False,
    ) -> ManagerInfo:
        manager_info = ManagerInfo(
            node_id=manager_id,
            tcp_host=tcp_host,
            tcp_port=tcp_port,
            udp_host=udp_host,
            udp_port=udp_port,
            datacenter="default",
            is_leader=is_leader,
        )
        self._known_managers[manager_id] = manager_info
        self._healthy_manager_ids.add(manager_id)
        if is_leader:
            self._primary_manager_id = manager_id
        return manager_info
    
    def add_workflow(
        self,
        workflow_id: str,
        job_id: str,
        job_leader_addr: tuple[str, int],
        workflow_name: str = "TestWorkflow",
    ) -> WorkflowProgress:
        progress = WorkflowProgress(
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
        )
        self._active_workflows[workflow_id] = progress
        self._workflow_job_leader[workflow_id] = job_leader_addr
        self._workflow_tokens[workflow_id] = f"token-{workflow_id}"
        self._workflow_cancel_events[workflow_id] = asyncio.Event()
        self._workflow_id_to_name[workflow_id] = workflow_name
        return progress
    
    async def _mark_workflows_orphaned_for_manager(self, manager_id: str) -> None:
        manager_info = self._known_managers.get(manager_id)
        if not manager_info:
            return
        
        dead_manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
        orphaned_count = 0
        current_time = time.monotonic()
        
        for workflow_id, job_leader_addr in list(self._workflow_job_leader.items()):
            if job_leader_addr == dead_manager_addr:
                if workflow_id in self._active_workflows:
                    if workflow_id not in self._orphaned_workflows:
                        self._orphaned_workflows[workflow_id] = current_time
                        orphaned_count += 1
        
        if orphaned_count > 0:
            await self._udp_logger.log(
                f"Marked {orphaned_count} workflow(s) as orphaned after manager {manager_id} failure"
            )
    
    async def _handle_manager_failure(self, manager_id: str) -> None:
        manager_lock = self._get_manager_state_lock(manager_id)
        async with manager_lock:
            self._manager_state_epoch[manager_id] = self._manager_state_epoch.get(manager_id, 0) + 1
            self._healthy_manager_ids.discard(manager_id)
            if manager_id not in self._manager_unhealthy_since:
                self._manager_unhealthy_since[manager_id] = time.monotonic()
        
        await self._udp_logger.log(f"Manager {manager_id} marked unhealthy (SWIM DEAD)")
        await self._mark_workflows_orphaned_for_manager(manager_id)
        
        if manager_id == self._primary_manager_id:
            self._primary_manager_id = None
    
    async def _cancel_workflow(self, workflow_id: str, reason: str) -> tuple[bool, list[str]]:
        if workflow_id not in self._workflow_tokens:
            return (False, [f"Workflow {workflow_id} not found"])
        
        cancel_event = self._workflow_cancel_events.get(workflow_id)
        if cancel_event:
            cancel_event.set()
        
        await self._task_runner.cancel(self._workflow_tokens[workflow_id])
        
        if workflow_id in self._active_workflows:
            self._active_workflows[workflow_id].status = WorkflowStatus.CANCELLED.value
        
        self._cancelled_workflows.append(workflow_id)
        return (True, [])
    
    async def _orphan_check_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._orphan_check_interval)
                
                current_time = time.monotonic()
                workflows_to_cancel: list[str] = []
                
                for workflow_id, orphan_timestamp in list(self._orphaned_workflows.items()):
                    elapsed = current_time - orphan_timestamp
                    if elapsed >= self._orphan_grace_period:
                        workflows_to_cancel.append(workflow_id)
                
                for workflow_id in workflows_to_cancel:
                    self._orphaned_workflows.pop(workflow_id, None)
                    
                    if workflow_id not in self._active_workflows:
                        continue
                    
                    await self._udp_logger.log(
                        f"Cancelling orphaned workflow {workflow_id[:8]}... - "
                        f"grace period ({self._orphan_grace_period}s) expired"
                    )
                    
                    success, errors = await self._cancel_workflow(workflow_id, "orphan_grace_period_expired")
                    
                    if not success or errors:
                        await self._udp_logger.log(f"Error cancelling orphaned workflow: {errors}")
            
            except asyncio.CancelledError:
                break
            except Exception:
                pass
    
    async def job_leader_worker_transfer(self, transfer: JobLeaderWorkerTransfer) -> JobLeaderWorkerTransferAck:
        workflows_updated = 0
        workflows_rescued_from_orphan = 0
        
        for workflow_id in transfer.workflow_ids:
            if workflow_id in self._active_workflows:
                current_leader = self._workflow_job_leader.get(workflow_id)
                new_leader = transfer.new_manager_addr
                
                if current_leader != new_leader:
                    self._workflow_job_leader[workflow_id] = new_leader
                    workflows_updated += 1
                
                if workflow_id in self._orphaned_workflows:
                    del self._orphaned_workflows[workflow_id]
                    workflows_rescued_from_orphan += 1
        
        if workflows_updated > 0:
            rescue_message = ""
            if workflows_rescued_from_orphan > 0:
                rescue_message = f" ({workflows_rescued_from_orphan} rescued from orphan state)"
            
            await self._udp_logger.log(
                f"Job {transfer.job_id[:8]}... leadership transfer: "
                f"updated {workflows_updated} workflow(s){rescue_message}"
            )
        
        return JobLeaderWorkerTransferAck(
            job_id=transfer.job_id,
            worker_id=self._node_id.full,
            workflows_updated=workflows_updated,
            accepted=True,
        )
    
    def start_orphan_check_loop(self) -> None:
        self._orphan_check_task = asyncio.create_task(self._orphan_check_loop())
    
    async def stop(self) -> None:
        self._running = False
        if self._orphan_check_task:
            self._orphan_check_task.cancel()
            try:
                await self._orphan_check_task
            except asyncio.CancelledError:
                pass


class TestOrphanedWorkflowTracking:
    """Test orphaned workflow tracking data structure (3.1)."""
    
    @pytest.mark.asyncio
    async def test_orphaned_workflows_dict_exists(self) -> None:
        harness = WorkerOrphanTestHarness()
        assert isinstance(harness._orphaned_workflows, dict)
        assert len(harness._orphaned_workflows) == 0
    
    @pytest.mark.asyncio
    async def test_orphaned_workflows_stores_timestamp(self) -> None:
        harness = WorkerOrphanTestHarness()
        current_time = time.monotonic()
        
        harness._orphaned_workflows["wf-123"] = current_time
        
        assert "wf-123" in harness._orphaned_workflows
        assert harness._orphaned_workflows["wf-123"] == current_time


class TestMarkWorkflowsOrphaned:
    """Test _on_node_dead marks workflows as orphaned (3.2)."""
    
    @pytest.mark.asyncio
    async def test_marks_workflows_orphaned_on_manager_failure(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        manager_info = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
            is_leader=True,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
        )
        harness.add_workflow(
            workflow_id="wf-2",
            job_id="job-1",
            job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        
        assert "wf-1" in harness._orphaned_workflows
        assert "wf-2" in harness._orphaned_workflows
        assert len(harness._orphaned_workflows) == 2
    
    @pytest.mark.asyncio
    async def test_does_not_mark_workflows_for_other_managers(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        manager_1 = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        harness.add_manager(
            manager_id="manager-2",
            tcp_host="192.168.1.20",
            tcp_port=8000,
            udp_host="192.168.1.20",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_1.tcp_host, manager_1.tcp_port),
        )
        harness.add_workflow(
            workflow_id="wf-2",
            job_id="job-2",
            job_leader_addr=("192.168.1.20", 8000),
        )
        
        await harness._handle_manager_failure("manager-1")
        
        assert "wf-1" in harness._orphaned_workflows
        assert "wf-2" not in harness._orphaned_workflows
    
    @pytest.mark.asyncio
    async def test_does_not_immediately_cancel_orphaned_workflows(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        manager_info = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        
        assert harness._active_workflows["wf-1"].status == WorkflowStatus.RUNNING.value
        assert len(harness._cancelled_workflows) == 0
    
    @pytest.mark.asyncio
    async def test_manager_marked_unhealthy_on_failure(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        await harness._handle_manager_failure("manager-1")
        
        assert "manager-1" not in harness._healthy_manager_ids
        assert "manager-1" in harness._manager_unhealthy_since


class TestJobLeaderWorkerTransfer:
    """Test job_leader_worker_transfer handler clears orphaned workflows (3.3)."""
    
    @pytest.mark.asyncio
    async def test_clears_workflow_from_orphaned_on_transfer(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        manager_1 = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        harness.add_manager(
            manager_id="manager-2",
            tcp_host="192.168.1.20",
            tcp_port=8000,
            udp_host="192.168.1.20",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_1.tcp_host, manager_1.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        assert "wf-1" in harness._orphaned_workflows
        
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-2",
            new_manager_addr=("192.168.1.20", 8000),
            fence_token=1,
            old_manager_id="manager-1",
        )
        
        ack = await harness.job_leader_worker_transfer(transfer)
        
        assert "wf-1" not in harness._orphaned_workflows
        assert ack.accepted is True
        assert ack.workflows_updated == 1
    
    @pytest.mark.asyncio
    async def test_updates_workflow_job_leader_mapping(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        manager_1 = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_1.tcp_host, manager_1.tcp_port),
        )
        
        new_leader_addr = ("192.168.1.20", 8000)
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-2",
            new_manager_addr=new_leader_addr,
            fence_token=1,
        )
        
        await harness.job_leader_worker_transfer(transfer)
        
        assert harness._workflow_job_leader["wf-1"] == new_leader_addr
    
    @pytest.mark.asyncio
    async def test_logs_successful_transfer_with_rescue_count(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        manager_1 = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_1.tcp_host, manager_1.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-2",
            new_manager_addr=("192.168.1.20", 8000),
            fence_token=1,
        )
        
        await harness.job_leader_worker_transfer(transfer)
        
        log_messages = [str(log) for log in harness._udp_logger.logs]
        assert any("rescued from orphan state" in msg for msg in log_messages)
    
    @pytest.mark.asyncio
    async def test_handles_transfer_for_unknown_workflow(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-unknown"],
            new_manager_id="manager-2",
            new_manager_addr=("192.168.1.20", 8000),
            fence_token=1,
        )
        
        ack = await harness.job_leader_worker_transfer(transfer)
        
        assert ack.accepted is True
        assert ack.workflows_updated == 0


class TestOrphanGracePeriodChecker:
    """Test orphan grace period checker loop (3.4)."""
    
    @pytest.mark.asyncio
    async def test_cancels_workflow_after_grace_period_expires(self) -> None:
        harness = WorkerOrphanTestHarness(
            orphan_grace_period=0.2,
            orphan_check_interval=0.05,
        )
        
        manager_info = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        assert "wf-1" in harness._orphaned_workflows
        
        harness.start_orphan_check_loop()
        
        await asyncio.sleep(0.35)
        
        await harness.stop()
        
        assert "wf-1" not in harness._orphaned_workflows
        assert "wf-1" in harness._cancelled_workflows
        assert harness._active_workflows["wf-1"].status == WorkflowStatus.CANCELLED.value
    
    @pytest.mark.asyncio
    async def test_does_not_cancel_if_transfer_arrives_before_grace_period(self) -> None:
        harness = WorkerOrphanTestHarness(
            orphan_grace_period=0.5,
            orphan_check_interval=0.05,
        )
        
        manager_1 = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_1.tcp_host, manager_1.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        harness.start_orphan_check_loop()
        
        await asyncio.sleep(0.1)
        
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-2",
            new_manager_addr=("192.168.1.20", 8000),
            fence_token=1,
        )
        await harness.job_leader_worker_transfer(transfer)
        
        await asyncio.sleep(0.5)
        
        await harness.stop()
        
        assert "wf-1" not in harness._cancelled_workflows
        assert harness._active_workflows["wf-1"].status == WorkflowStatus.RUNNING.value
    
    @pytest.mark.asyncio
    async def test_removes_workflow_from_orphaned_after_cancellation(self) -> None:
        harness = WorkerOrphanTestHarness(
            orphan_grace_period=0.1,
            orphan_check_interval=0.05,
        )
        
        manager_info = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        harness.start_orphan_check_loop()
        
        await asyncio.sleep(0.25)
        
        await harness.stop()
        
        assert "wf-1" not in harness._orphaned_workflows
    
    @pytest.mark.asyncio
    async def test_handles_multiple_orphaned_workflows(self) -> None:
        harness = WorkerOrphanTestHarness(
            orphan_grace_period=0.15,
            orphan_check_interval=0.05,
        )
        
        manager_info = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        for index in range(5):
            harness.add_workflow(
                workflow_id=f"wf-{index}",
                job_id="job-1",
                job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
            )
        
        await harness._handle_manager_failure("manager-1")
        assert len(harness._orphaned_workflows) == 5
        
        harness.start_orphan_check_loop()
        
        await asyncio.sleep(0.3)
        
        await harness.stop()
        
        assert len(harness._orphaned_workflows) == 0
        assert len(harness._cancelled_workflows) == 5
    
    @pytest.mark.asyncio
    async def test_does_not_cancel_already_completed_workflow(self) -> None:
        harness = WorkerOrphanTestHarness(
            orphan_grace_period=0.15,
            orphan_check_interval=0.05,
        )
        
        manager_info = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        
        del harness._active_workflows["wf-1"]
        
        harness.start_orphan_check_loop()
        
        await asyncio.sleep(0.25)
        
        await harness.stop()
        
        assert "wf-1" not in harness._cancelled_workflows


class TestOrphanConfiguration:
    """Test configuration options for orphan handling (3.5)."""
    
    @pytest.mark.asyncio
    async def test_default_grace_period(self) -> None:
        from hyperscale.distributed.env import Env
        
        env = Env()
        assert env.WORKER_ORPHAN_GRACE_PERIOD == 5.0
    
    @pytest.mark.asyncio
    async def test_default_check_interval(self) -> None:
        from hyperscale.distributed.env import Env
        
        env = Env()
        assert env.WORKER_ORPHAN_CHECK_INTERVAL == 1.0
    
    @pytest.mark.asyncio
    async def test_custom_grace_period_affects_cancellation_timing(self) -> None:
        short_grace_harness = WorkerOrphanTestHarness(
            orphan_grace_period=0.1,
            orphan_check_interval=0.03,
        )
        long_grace_harness = WorkerOrphanTestHarness(
            orphan_grace_period=0.4,
            orphan_check_interval=0.03,
        )
        
        for harness in [short_grace_harness, long_grace_harness]:
            manager_info = harness.add_manager(
                manager_id="manager-1",
                tcp_host="192.168.1.10",
                tcp_port=8000,
                udp_host="192.168.1.10",
                udp_port=8001,
            )
            harness.add_workflow(
                workflow_id="wf-1",
                job_id="job-1",
                job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
            )
            await harness._handle_manager_failure("manager-1")
            harness.start_orphan_check_loop()
        
        await asyncio.sleep(0.2)
        
        assert "wf-1" in short_grace_harness._cancelled_workflows
        assert "wf-1" not in long_grace_harness._cancelled_workflows
        
        await short_grace_harness.stop()
        await long_grace_harness.stop()


class TestEdgeCases:
    """Test edge cases in orphan handling."""
    
    @pytest.mark.asyncio
    async def test_workflow_orphaned_then_transferred_then_manager_fails_again(self) -> None:
        harness = WorkerOrphanTestHarness(
            orphan_grace_period=0.5,
            orphan_check_interval=0.05,
        )
        
        manager_1 = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        manager_2 = harness.add_manager(
            manager_id="manager-2",
            tcp_host="192.168.1.20",
            tcp_port=8000,
            udp_host="192.168.1.20",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_1.tcp_host, manager_1.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        assert "wf-1" in harness._orphaned_workflows
        
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-2",
            new_manager_addr=(manager_2.tcp_host, manager_2.tcp_port),
            fence_token=1,
        )
        await harness.job_leader_worker_transfer(transfer)
        
        assert "wf-1" not in harness._orphaned_workflows
        assert harness._workflow_job_leader["wf-1"] == (manager_2.tcp_host, manager_2.tcp_port)
        
        await harness._handle_manager_failure("manager-2")
        
        assert "wf-1" in harness._orphaned_workflows
    
    @pytest.mark.asyncio
    async def test_concurrent_manager_failures(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        manager_1 = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        manager_2 = harness.add_manager(
            manager_id="manager-2",
            tcp_host="192.168.1.20",
            tcp_port=8000,
            udp_host="192.168.1.20",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_1.tcp_host, manager_1.tcp_port),
        )
        harness.add_workflow(
            workflow_id="wf-2",
            job_id="job-2",
            job_leader_addr=(manager_2.tcp_host, manager_2.tcp_port),
        )
        
        await asyncio.gather(
            harness._handle_manager_failure("manager-1"),
            harness._handle_manager_failure("manager-2"),
        )
        
        assert "wf-1" in harness._orphaned_workflows
        assert "wf-2" in harness._orphaned_workflows
        assert len(harness._orphaned_workflows) == 2
    
    @pytest.mark.asyncio
    async def test_idempotent_orphan_marking(self) -> None:
        harness = WorkerOrphanTestHarness()
        
        manager_info = harness.add_manager(
            manager_id="manager-1",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
        )
        
        harness.add_workflow(
            workflow_id="wf-1",
            job_id="job-1",
            job_leader_addr=(manager_info.tcp_host, manager_info.tcp_port),
        )
        
        await harness._handle_manager_failure("manager-1")
        first_timestamp = harness._orphaned_workflows["wf-1"]
        
        await harness._mark_workflows_orphaned_for_manager("manager-1")
        
        assert harness._orphaned_workflows["wf-1"] == first_timestamp
    
    @pytest.mark.asyncio
    async def test_graceful_loop_shutdown(self) -> None:
        harness = WorkerOrphanTestHarness(
            orphan_grace_period=10.0,
            orphan_check_interval=0.05,
        )
        
        harness.start_orphan_check_loop()
        
        await asyncio.sleep(0.1)
        
        await harness.stop()
        
        assert harness._orphan_check_task is not None
        assert harness._orphan_check_task.done()
