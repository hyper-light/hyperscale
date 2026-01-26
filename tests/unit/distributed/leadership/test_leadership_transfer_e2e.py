"""
End-to-end simulation tests for leadership transfer scenarios.

These tests simulate complete leadership transfer scenarios across multiple
managers and workers, verifying:
1. Leader fails, new leader is elected, workers receive transfer notifications
2. Split-brain recovery where two managers think they're leader, fence tokens resolve conflict
3. Cascading failures: leader fails, new leader fails immediately, third takes over
4. Network partition heals and stale leader attempts to reclaim jobs

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock


# =============================================================================
# Shared Mock Infrastructure
# =============================================================================


@dataclass
class MockNodeId:
    """Mock node ID with full and short representations."""

    full: str
    short: str
    datacenter: str = "dc1"


@dataclass
class MockEnv:
    """Mock environment configuration."""

    RECOVERY_JITTER_MIN: float = 0.0
    RECOVERY_JITTER_MAX: float = 0.0
    DATACENTER_ID: str = "dc1"
    WORKER_ORPHAN_GRACE_PERIOD: float = 2.0
    WORKER_ORPHAN_CHECK_INTERVAL: float = 0.1


@dataclass
class MockTaskRunner:
    """Mock task runner that records scheduled tasks."""

    _tasks: list = field(default_factory=list)

    def run(self, coro_or_func, *args, **kwargs) -> None:
        self._tasks.append((coro_or_func, args, kwargs))

    def clear(self) -> None:
        self._tasks.clear()


@dataclass
class MockLogger:
    """Mock logger that records log calls."""

    _logs: list = field(default_factory=list)

    async def log(self, message: Any) -> None:
        self._logs.append(message)


@dataclass
class MockManagerInfo:
    """Mock manager peer info."""

    node_id: str
    tcp_host: str
    tcp_port: int
    udp_host: str
    udp_port: int


@dataclass
class MockWorkerRegistration:
    """Mock worker registration."""

    node: "MockWorkerNode"


@dataclass
class MockWorkerNode:
    """Mock worker node info."""

    host: str
    port: int


@dataclass
class MockSubWorkflow:
    """Mock sub-workflow for job manager."""

    worker_id: str | None = None
    result: Any = None


@dataclass
class MockJob:
    """Mock job for job manager."""

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


@dataclass
class MockJobLeaderWorkerTransfer:
    """Mock job leader worker transfer message."""

    job_id: str
    workflow_ids: list[str]
    new_manager_addr: tuple[str, int]
    new_manager_id: str
    old_manager_id: str | None
    fence_token: int


@dataclass
class MockJobLeaderWorkerTransferAck:
    """Mock transfer acknowledgment."""

    job_id: str
    workflows_updated: int
    accepted: bool
    fence_token: int


# =============================================================================
# Simulated Manager Server
# =============================================================================


class SimulatedManager:
    """
    Simulated manager server for end-to-end testing.

    Implements leader election, job leadership tracking, and transfer logic.
    """

    def __init__(self, node_id: str, tcp_port: int, udp_port: int) -> None:
        self._node_id = MockNodeId(full=node_id, short=node_id[:8])
        self._host = "127.0.0.1"
        self._tcp_port = tcp_port
        self._udp_port = udp_port

        self.env = MockEnv()
        self._task_runner = MockTaskRunner()
        self._udp_logger = MockLogger()
        self._job_manager = MockJobManager()

        self._state_version = 0
        self._is_leader = False
        self._dead_managers: set[tuple[str, int]] = set()

        self._job_leaders: dict[str, str] = {}
        self._job_leader_addrs: dict[str, tuple[str, int]] = {}
        self._job_fencing_tokens: dict[str, int] = {}
        self._job_origin_gates: dict[str, tuple[str, int]] = {}

        self._workers: dict[str, MockWorkerRegistration] = {}
        self._known_manager_peers: dict[str, MockManagerInfo] = {}
        self._manager_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}

        # Network simulation
        self._tcp_calls: list[tuple[str, tuple[str, int], Any]] = []
        self._received_transfers: list[MockJobLeaderWorkerTransfer] = []

        # Cluster reference (set after creation)
        self._cluster: "SimulatedCluster | None" = None
        self._is_alive = True

    def is_leader(self) -> bool:
        return self._is_leader

    def become_leader(self) -> None:
        """Become the SWIM cluster leader."""
        self._is_leader = True
        self._task_runner.run(self._scan_for_orphaned_jobs)

    def step_down(self) -> None:
        """Step down from leadership."""
        self._is_leader = False

    def mark_dead(self) -> None:
        """Simulate this manager dying."""
        self._is_alive = False

    def mark_alive(self) -> None:
        """Simulate this manager recovering."""
        self._is_alive = True

    def _increment_version(self) -> None:
        self._state_version += 1

    def add_manager_peer(
        self,
        manager_id: str,
        tcp_host: str,
        tcp_port: int,
        udp_host: str,
        udp_port: int,
    ) -> None:
        self._known_manager_peers[manager_id] = MockManagerInfo(
            node_id=manager_id,
            tcp_host=tcp_host,
            tcp_port=tcp_port,
            udp_host=udp_host,
            udp_port=udp_port,
        )
        self._manager_udp_to_tcp[(udp_host, udp_port)] = (tcp_host, tcp_port)

    def add_job(
        self,
        job_id: str,
        leader_node_id: str,
        leader_addr: tuple[str, int],
        fencing_token: int = 1,
        origin_gate: tuple[str, int] | None = None,
    ) -> None:
        self._job_leaders[job_id] = leader_node_id
        self._job_leader_addrs[job_id] = leader_addr
        self._job_fencing_tokens[job_id] = fencing_token
        if origin_gate:
            self._job_origin_gates[job_id] = origin_gate
        self._job_manager.add_job(MockJob(job_id=job_id))

    def add_worker(self, worker_id: str, host: str, port: int) -> None:
        self._workers[worker_id] = MockWorkerRegistration(
            node=MockWorkerNode(host=host, port=port)
        )

    def add_sub_workflow_to_job(
        self,
        job_id: str,
        sub_workflow_id: str,
        worker_id: str,
        completed: bool = False,
    ) -> None:
        job = self._job_manager.get_job_by_id(job_id)
        if job:
            job.sub_workflows[sub_workflow_id] = MockSubWorkflow(
                worker_id=worker_id,
                result="done" if completed else None,
            )

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        manager_tcp_addr = self._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            self._dead_managers.add(manager_tcp_addr)

    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        manager_tcp_addr = self._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            self._dead_managers.discard(manager_tcp_addr)

    async def _scan_for_orphaned_jobs(self) -> None:
        if not self._dead_managers:
            return

        orphaned_jobs: list[tuple[str, tuple[str, int]]] = []
        for job_id, leader_addr in list(self._job_leader_addrs.items()):
            if leader_addr in self._dead_managers:
                orphaned_jobs.append((job_id, leader_addr))

        if not orphaned_jobs:
            self._dead_managers.clear()
            return

        processed_dead_managers: set[tuple[str, int]] = set()

        for job_id, dead_leader_addr in orphaned_jobs:
            old_token = self._job_fencing_tokens.get(job_id, 0)
            new_token = old_token + 1

            self._job_leaders[job_id] = self._node_id.full
            self._job_leader_addrs[job_id] = (self._host, self._tcp_port)
            self._job_fencing_tokens[job_id] = new_token

            self._increment_version()

            await self._notify_workers_of_leadership_transfer(job_id, new_token)
            processed_dead_managers.add(dead_leader_addr)

        self._dead_managers -= processed_dead_managers

    async def _handle_job_leader_failure(
        self,
        failed_manager_addr: tuple[str, int],
    ) -> None:
        if not self.is_leader():
            return

        orphaned_jobs: list[str] = []
        for job_id, leader_addr in list(self._job_leader_addrs.items()):
            if leader_addr == failed_manager_addr:
                orphaned_jobs.append(job_id)

        if not orphaned_jobs:
            return

        for job_id in orphaned_jobs:
            old_token = self._job_fencing_tokens.get(job_id, 0)
            new_token = old_token + 1

            self._job_leaders[job_id] = self._node_id.full
            self._job_leader_addrs[job_id] = (self._host, self._tcp_port)
            self._job_fencing_tokens[job_id] = new_token

            self._increment_version()

            await self._notify_workers_of_leadership_transfer(job_id, new_token)

    async def _notify_workers_of_leadership_transfer(
        self,
        job_id: str,
        fence_token: int,
    ) -> None:
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return

        worker_workflows: dict[str, list[str]] = {}
        for sub_wf_id, sub_wf in job.sub_workflows.items():
            if sub_wf.result is None and sub_wf.worker_id:
                if sub_wf.worker_id not in worker_workflows:
                    worker_workflows[sub_wf.worker_id] = []
                worker_workflows[sub_wf.worker_id].append(sub_wf_id)

        for worker_id, workflow_ids in worker_workflows.items():
            worker_reg = self._workers.get(worker_id)
            if worker_reg and self._cluster:
                worker_addr = (worker_reg.node.host, worker_reg.node.port)
                transfer = MockJobLeaderWorkerTransfer(
                    job_id=job_id,
                    workflow_ids=workflow_ids,
                    new_manager_addr=(self._host, self._tcp_port),
                    new_manager_id=self._node_id.full,
                    old_manager_id=None,
                    fence_token=fence_token,
                )
                self._tcp_calls.append(("job_leader_worker_transfer", worker_addr, transfer))

                # Deliver to simulated worker
                worker = self._cluster.get_worker_by_addr(worker_addr)
                if worker and worker._is_alive:
                    await worker.job_leader_worker_transfer(transfer)


# =============================================================================
# Simulated Worker Server
# =============================================================================


class SimulatedWorker:
    """
    Simulated worker server for end-to-end testing.

    Implements orphan handling and transfer acceptance logic.
    """

    def __init__(self, worker_id: str, tcp_port: int) -> None:
        self._node_id = MagicMock()
        self._node_id.short = worker_id
        self._host = "127.0.0.1"
        self._tcp_port = tcp_port

        self.env = MockEnv()
        self._udp_logger = MockLogger()
        self._running = True
        self._is_alive = True

        # Manager tracking
        self._known_managers: dict[str, MockManagerInfo] = {}
        self._primary_manager_id: str | None = None

        # Workflow tracking
        self._active_workflows: dict[str, "WorkflowState"] = {}
        self._workflow_job_leader: dict[str, tuple[str, int]] = {}

        # Orphan handling
        self._orphaned_workflows: dict[str, float] = {}
        self._orphan_grace_period: float = self.env.WORKER_ORPHAN_GRACE_PERIOD
        self._orphan_check_task: asyncio.Task | None = None

        # Transfer tracking
        self._cancelled_workflows: list[tuple[str, str]] = []
        self._transfer_notifications: list[MockJobLeaderWorkerTransfer] = []
        self._fence_tokens: dict[str, int] = {}

    def mark_dead(self) -> None:
        self._is_alive = False

    def mark_alive(self) -> None:
        self._is_alive = True

    def add_manager(
        self,
        manager_id: str,
        tcp_host: str,
        tcp_port: int,
    ) -> None:
        self._known_managers[manager_id] = MockManagerInfo(
            node_id=manager_id,
            tcp_host=tcp_host,
            tcp_port=tcp_port,
            udp_host=tcp_host,
            udp_port=tcp_port + 1,
        )

    def add_workflow(
        self,
        workflow_id: str,
        job_id: str,
        job_leader_addr: tuple[str, int],
    ) -> None:
        self._active_workflows[workflow_id] = WorkflowState(
            workflow_id=workflow_id,
            job_id=job_id,
            status="running",
        )
        self._workflow_job_leader[workflow_id] = job_leader_addr

    async def _mark_workflows_orphaned_for_manager_addr(
        self,
        dead_manager_addr: tuple[str, int],
    ) -> None:
        current_time = time.monotonic()

        for workflow_id, job_leader_addr in list(self._workflow_job_leader.items()):
            if job_leader_addr == dead_manager_addr:
                if workflow_id in self._active_workflows:
                    if workflow_id not in self._orphaned_workflows:
                        self._orphaned_workflows[workflow_id] = current_time

    async def job_leader_worker_transfer(
        self,
        data: MockJobLeaderWorkerTransfer,
    ) -> MockJobLeaderWorkerTransferAck:
        self._transfer_notifications.append(data)

        # Validate fence token
        current_token = self._fence_tokens.get(data.job_id, -1)
        if data.fence_token <= current_token:
            return MockJobLeaderWorkerTransferAck(
                job_id=data.job_id,
                workflows_updated=0,
                accepted=False,
                fence_token=current_token,
            )

        # Accept the new token
        self._fence_tokens[data.job_id] = data.fence_token
        workflows_updated = 0

        for workflow_id in data.workflow_ids:
            if workflow_id in self._active_workflows:
                current_leader = self._workflow_job_leader.get(workflow_id)
                new_leader = data.new_manager_addr

                if current_leader != new_leader:
                    self._workflow_job_leader[workflow_id] = new_leader
                    workflows_updated += 1

                # Clear from orphaned workflows if present
                if workflow_id in self._orphaned_workflows:
                    del self._orphaned_workflows[workflow_id]

        return MockJobLeaderWorkerTransferAck(
            job_id=data.job_id,
            workflows_updated=workflows_updated,
            accepted=True,
            fence_token=data.fence_token,
        )

    async def _cancel_workflow(self, workflow_id: str, reason: str) -> None:
        self._cancelled_workflows.append((workflow_id, reason))
        self._active_workflows.pop(workflow_id, None)
        self._workflow_job_leader.pop(workflow_id, None)


@dataclass
class WorkflowState:
    """Workflow execution state."""

    workflow_id: str
    job_id: str
    status: str


# =============================================================================
# Simulated Cluster
# =============================================================================


class SimulatedCluster:
    """
    Simulated cluster containing multiple managers and workers.

    Coordinates failure injection, leader election, and message routing.
    """

    def __init__(self) -> None:
        self.managers: dict[str, SimulatedManager] = {}
        self.workers: dict[str, SimulatedWorker] = {}
        self._current_leader_id: str | None = None
        self._election_history: list[tuple[float, str]] = []

    def add_manager(self, manager: SimulatedManager) -> None:
        self.managers[manager._node_id.full] = manager
        manager._cluster = self

        # Register with other managers
        for other_id, other_mgr in self.managers.items():
            if other_id != manager._node_id.full:
                manager.add_manager_peer(
                    other_id,
                    other_mgr._host,
                    other_mgr._tcp_port,
                    other_mgr._host,
                    other_mgr._udp_port,
                )
                other_mgr.add_manager_peer(
                    manager._node_id.full,
                    manager._host,
                    manager._tcp_port,
                    manager._host,
                    manager._udp_port,
                )

    def add_worker(self, worker: SimulatedWorker) -> None:
        self.workers[worker._node_id.short] = worker

    def get_worker_by_addr(self, addr: tuple[str, int]) -> SimulatedWorker | None:
        for worker in self.workers.values():
            if (worker._host, worker._tcp_port) == addr:
                return worker
        return None

    def elect_leader(self, manager_id: str) -> None:
        """Elect a specific manager as leader."""
        if self._current_leader_id:
            old_leader = self.managers.get(self._current_leader_id)
            if old_leader:
                old_leader.step_down()

        self._current_leader_id = manager_id
        new_leader = self.managers[manager_id]
        new_leader.become_leader()
        self._election_history.append((time.monotonic(), manager_id))

    def simulate_manager_failure(self, manager_id: str) -> None:
        """Simulate a manager failure."""
        failed_manager = self.managers[manager_id]
        failed_manager.mark_dead()

        # Notify all other managers
        failed_udp_addr = (failed_manager._host, failed_manager._udp_port)
        for other_id, other_mgr in self.managers.items():
            if other_id != manager_id and other_mgr._is_alive:
                other_mgr._on_node_dead(failed_udp_addr)

    def simulate_manager_recovery(self, manager_id: str) -> None:
        """Simulate a manager recovering."""
        recovered_manager = self.managers[manager_id]
        recovered_manager.mark_alive()

        # Notify all other managers
        recovered_udp_addr = (recovered_manager._host, recovered_manager._udp_port)
        for other_id, other_mgr in self.managers.items():
            if other_id != manager_id and other_mgr._is_alive:
                other_mgr._on_node_join(recovered_udp_addr)

    def get_leader(self) -> SimulatedManager | None:
        if self._current_leader_id:
            return self.managers.get(self._current_leader_id)
        return None


# =============================================================================
# Test Classes
# =============================================================================


class TestLeaderFailsNewLeaderElected:
    """
    Test scenario: Leader fails, new leader is elected, workers receive transfers.

    Flow:
    1. Manager-A is SWIM leader and job leader for job-001
    2. Workers have active workflows led by Manager-A
    3. Manager-A fails
    4. Manager-B wins election, becomes new SWIM leader
    5. Manager-B scans for orphaned jobs and takes over
    6. Workers receive transfer notifications with incremented fence token
    """

    @pytest.mark.asyncio
    async def test_basic_leader_failover(self):
        """Basic leader failover with single job and worker."""
        cluster = SimulatedCluster()

        # Create managers
        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)

        # Create worker
        worker = SimulatedWorker("worker-001", tcp_port=8000)
        cluster.add_worker(worker)

        # Register worker with managers
        manager_a.add_worker("worker-001", "127.0.0.1", 8000)
        manager_b.add_worker("worker-001", "127.0.0.1", 8000)

        # Manager-A is initial leader with job-001
        cluster.elect_leader("manager-a")
        manager_a.add_job(
            job_id="job-001",
            leader_node_id="manager-a",
            leader_addr=("127.0.0.1", 9090),
            fencing_token=1,
        )
        manager_b.add_job(
            job_id="job-001",
            leader_node_id="manager-a",
            leader_addr=("127.0.0.1", 9090),
            fencing_token=1,
        )

        # Add workflow to job
        manager_a.add_sub_workflow_to_job("job-001", "wf-001", "worker-001")
        manager_b.add_sub_workflow_to_job("job-001", "wf-001", "worker-001")

        # Worker has active workflow
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        # Step 1: Manager-A fails
        cluster.simulate_manager_failure("manager-a")

        # Verify: Manager-B tracked the dead manager
        assert ("127.0.0.1", 9090) in manager_b._dead_managers

        # Step 2: Manager-B becomes new leader
        cluster.elect_leader("manager-b")

        # Step 3: Manager-B scans for orphans
        await manager_b._scan_for_orphaned_jobs()

        # Verify: Manager-B took over job leadership
        assert manager_b._job_leaders["job-001"] == "manager-b"
        assert manager_b._job_leader_addrs["job-001"] == ("127.0.0.1", 9092)
        assert manager_b._job_fencing_tokens["job-001"] == 2  # Incremented

        # Verify: Worker received transfer notification
        assert len(worker._transfer_notifications) == 1
        transfer = worker._transfer_notifications[0]
        assert transfer.job_id == "job-001"
        assert transfer.fence_token == 2
        assert transfer.new_manager_addr == ("127.0.0.1", 9092)

        # Verify: Worker updated job leader mapping
        assert worker._workflow_job_leader["wf-001"] == ("127.0.0.1", 9092)
        assert worker._fence_tokens["job-001"] == 2

    @pytest.mark.asyncio
    async def test_leader_failover_multiple_jobs(self):
        """Leader failover with multiple jobs distributed across leader."""
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)
        manager_c = SimulatedManager("manager-c", tcp_port=9094, udp_port=9095)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)
        cluster.add_manager(manager_c)

        worker = SimulatedWorker("worker-001", tcp_port=8000)
        cluster.add_worker(worker)

        for mgr in [manager_a, manager_b, manager_c]:
            mgr.add_worker("worker-001", "127.0.0.1", 8000)

        cluster.elect_leader("manager-a")

        # Manager-A leads multiple jobs
        for job_num in range(3):
            job_id = f"job-{job_num:03d}"
            wf_id = f"wf-{job_num:03d}"

            for mgr in [manager_a, manager_b, manager_c]:
                mgr.add_job(job_id, "manager-a", ("127.0.0.1", 9090), fencing_token=1)
                mgr.add_sub_workflow_to_job(job_id, wf_id, "worker-001")

            worker.add_workflow(wf_id, job_id, ("127.0.0.1", 9090))

        # Manager-A fails
        cluster.simulate_manager_failure("manager-a")
        cluster.elect_leader("manager-b")
        await manager_b._scan_for_orphaned_jobs()

        # All jobs should be taken over
        for job_num in range(3):
            job_id = f"job-{job_num:03d}"
            assert manager_b._job_leaders[job_id] == "manager-b"
            assert manager_b._job_fencing_tokens[job_id] == 2

        # Worker should have received 3 transfers
        assert len(worker._transfer_notifications) == 3

    @pytest.mark.asyncio
    async def test_leader_failover_multiple_workers(self):
        """Leader failover with multiple workers receiving transfers."""
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)

        # Create multiple workers
        workers = []
        for worker_num in range(3):
            worker = SimulatedWorker(f"worker-{worker_num:03d}", tcp_port=8000 + worker_num)
            cluster.add_worker(worker)
            workers.append(worker)

            manager_a.add_worker(f"worker-{worker_num:03d}", "127.0.0.1", 8000 + worker_num)
            manager_b.add_worker(f"worker-{worker_num:03d}", "127.0.0.1", 8000 + worker_num)

        cluster.elect_leader("manager-a")

        # Job with workflows on different workers
        for mgr in [manager_a, manager_b]:
            mgr.add_job("job-001", "manager-a", ("127.0.0.1", 9090), fencing_token=1)
            for worker_num in range(3):
                mgr.add_sub_workflow_to_job("job-001", f"wf-{worker_num:03d}", f"worker-{worker_num:03d}")

        for worker_num, worker in enumerate(workers):
            worker.add_workflow(f"wf-{worker_num:03d}", "job-001", ("127.0.0.1", 9090))

        # Failover
        cluster.simulate_manager_failure("manager-a")
        cluster.elect_leader("manager-b")
        await manager_b._scan_for_orphaned_jobs()

        # All workers should receive transfers
        for worker in workers:
            assert len(worker._transfer_notifications) == 1
            assert worker._transfer_notifications[0].fence_token == 2


class TestSplitBrainRecovery:
    """
    Test scenario: Split-brain recovery where fence tokens resolve conflicts.

    Flow:
    1. Network partition causes two managers to think they're leader
    2. Both attempt to claim job leadership
    3. Workers use fence tokens to accept only the highest token
    4. Partition heals, fence tokens ensure consistency
    """

    @pytest.mark.asyncio
    async def test_fence_token_rejects_stale_leader(self):
        """Worker rejects transfer from stale leader with lower fence token."""
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)

        worker = SimulatedWorker("worker-001", tcp_port=8000)
        cluster.add_worker(worker)

        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        # Simulate: Worker already accepted transfer with token 5
        worker._fence_tokens["job-001"] = 5

        # Stale leader tries to send transfer with lower token
        stale_transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9090),
            new_manager_id="manager-a",
            old_manager_id=None,
            fence_token=3,  # Lower than current
        )

        ack = await worker.job_leader_worker_transfer(stale_transfer)

        # Should be rejected
        assert not ack.accepted
        assert ack.workflows_updated == 0

        # Token should remain unchanged
        assert worker._fence_tokens["job-001"] == 5

    @pytest.mark.asyncio
    async def test_fence_token_accepts_higher_token(self):
        """Worker accepts transfer from new leader with higher fence token."""
        cluster = SimulatedCluster()

        worker = SimulatedWorker("worker-001", tcp_port=8000)
        cluster.add_worker(worker)

        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))
        worker._fence_tokens["job-001"] = 5

        # New leader sends transfer with higher token
        new_transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9092),
            new_manager_id="manager-b",
            old_manager_id="manager-a",
            fence_token=6,  # Higher than current
        )

        ack = await worker.job_leader_worker_transfer(new_transfer)

        # Should be accepted
        assert ack.accepted
        assert ack.workflows_updated == 1

        # Token should be updated
        assert worker._fence_tokens["job-001"] == 6
        assert worker._workflow_job_leader["wf-001"] == ("127.0.0.1", 9092)

    @pytest.mark.asyncio
    async def test_split_brain_dual_leader_scenario(self):
        """
        Both managers think they're leader during partition.

        After partition heals, the manager with higher election term wins.
        """
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)

        worker = SimulatedWorker("worker-001", tcp_port=8000)
        cluster.add_worker(worker)

        for mgr in [manager_a, manager_b]:
            mgr.add_worker("worker-001", "127.0.0.1", 8000)

        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        # Initial state: Manager-A is leader
        cluster.elect_leader("manager-a")
        for mgr in [manager_a, manager_b]:
            mgr.add_job("job-001", "manager-a", ("127.0.0.1", 9090), fencing_token=1)
            mgr.add_sub_workflow_to_job("job-001", "wf-001", "worker-001")

        # Partition: Manager-B thinks Manager-A is dead
        manager_b._dead_managers.add(("127.0.0.1", 9090))
        manager_b._is_leader = True  # Thinks it's leader

        # Manager-B takes over with token 2
        await manager_b._scan_for_orphaned_jobs()

        # Worker now has token 2 pointing to Manager-B
        assert worker._fence_tokens["job-001"] == 2
        assert worker._workflow_job_leader["wf-001"] == ("127.0.0.1", 9092)

        # Partition heals, Manager-A is actually still alive
        # Manager-A tries to reclaim with token 1 (stale)
        stale_transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9090),
            new_manager_id="manager-a",
            old_manager_id=None,
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(stale_transfer)

        # Should be rejected - token 1 < current token 2
        assert not ack.accepted
        assert worker._workflow_job_leader["wf-001"] == ("127.0.0.1", 9092)

    @pytest.mark.asyncio
    async def test_equal_fence_token_rejected(self):
        """Transfer with equal fence token (not greater) should be rejected."""
        worker = SimulatedWorker("worker-001", tcp_port=8000)
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))
        worker._fence_tokens["job-001"] = 5

        # Try transfer with EQUAL token
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9092),
            new_manager_id="manager-b",
            old_manager_id="manager-a",
            fence_token=5,  # Equal to current
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert not ack.accepted
        assert worker._fence_tokens["job-001"] == 5


class TestCascadingFailures:
    """
    Test scenario: Cascading failures where multiple leaders fail in sequence.

    Flow:
    1. Manager-A is leader, fails
    2. Manager-B becomes leader, immediately fails
    3. Manager-C becomes leader, takes over all orphaned jobs
    4. Workers receive final transfer with correct cumulative fence token
    """

    @pytest.mark.asyncio
    async def test_double_leader_failure(self):
        """Two consecutive leader failures, third manager takes over."""
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)
        manager_c = SimulatedManager("manager-c", tcp_port=9094, udp_port=9095)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)
        cluster.add_manager(manager_c)

        worker = SimulatedWorker("worker-001", tcp_port=8000)
        cluster.add_worker(worker)

        for mgr in [manager_a, manager_b, manager_c]:
            mgr.add_worker("worker-001", "127.0.0.1", 8000)
            mgr.add_job("job-001", "manager-a", ("127.0.0.1", 9090), fencing_token=1)
            mgr.add_sub_workflow_to_job("job-001", "wf-001", "worker-001")

        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        # Initial leader
        cluster.elect_leader("manager-a")

        # Manager-A fails
        cluster.simulate_manager_failure("manager-a")

        # Manager-B becomes leader and takes over
        cluster.elect_leader("manager-b")
        await manager_b._scan_for_orphaned_jobs()

        assert manager_b._job_fencing_tokens["job-001"] == 2
        assert worker._fence_tokens["job-001"] == 2

        # Manager-B immediately fails too
        cluster.simulate_manager_failure("manager-b")

        # Manager-C now also tracks Manager-B as dead
        assert ("127.0.0.1", 9092) in manager_c._dead_managers

        # Update Manager-C's view of job leadership
        manager_c._job_leaders["job-001"] = "manager-b"
        manager_c._job_leader_addrs["job-001"] = ("127.0.0.1", 9092)
        manager_c._job_fencing_tokens["job-001"] = 2

        # Manager-C becomes leader
        cluster.elect_leader("manager-c")
        await manager_c._scan_for_orphaned_jobs()

        # Token should be 3 now
        assert manager_c._job_fencing_tokens["job-001"] == 3
        assert worker._fence_tokens["job-001"] == 3
        assert worker._workflow_job_leader["wf-001"] == ("127.0.0.1", 9094)

    @pytest.mark.asyncio
    async def test_multiple_jobs_across_cascading_failures(self):
        """Multiple jobs handled correctly during cascading failures."""
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)
        manager_c = SimulatedManager("manager-c", tcp_port=9094, udp_port=9095)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)
        cluster.add_manager(manager_c)

        workers = [
            SimulatedWorker(f"worker-{i}", tcp_port=8000 + i)
            for i in range(3)
        ]
        for worker in workers:
            cluster.add_worker(worker)

        # Setup: Manager-A leads job-001, Manager-B leads job-002
        for mgr in [manager_a, manager_b, manager_c]:
            for i, worker in enumerate(workers):
                mgr.add_worker(f"worker-{i}", "127.0.0.1", 8000 + i)

            mgr.add_job("job-001", "manager-a", ("127.0.0.1", 9090), fencing_token=1)
            mgr.add_job("job-002", "manager-b", ("127.0.0.1", 9092), fencing_token=1)

            mgr.add_sub_workflow_to_job("job-001", "wf-001", "worker-0")
            mgr.add_sub_workflow_to_job("job-002", "wf-002", "worker-1")

        workers[0].add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))
        workers[1].add_workflow("wf-002", "job-002", ("127.0.0.1", 9092))

        # Both Manager-A and Manager-B fail
        cluster.simulate_manager_failure("manager-a")
        cluster.simulate_manager_failure("manager-b")

        # Manager-C becomes leader and takes over both jobs
        cluster.elect_leader("manager-c")
        await manager_c._scan_for_orphaned_jobs()

        # Both jobs should be taken over by Manager-C
        assert manager_c._job_leaders["job-001"] == "manager-c"
        assert manager_c._job_leaders["job-002"] == "manager-c"
        assert manager_c._job_fencing_tokens["job-001"] == 2
        assert manager_c._job_fencing_tokens["job-002"] == 2

        # Workers should have correct mappings
        assert workers[0]._workflow_job_leader["wf-001"] == ("127.0.0.1", 9094)
        assert workers[1]._workflow_job_leader["wf-002"] == ("127.0.0.1", 9094)


class TestNetworkPartitionHeal:
    """
    Test scenario: Network partition heals and stale leader attempts to reclaim.

    Flow:
    1. Manager-A is leader during partition
    2. Partition: Manager-B elected leader on other side
    3. Manager-B takes over jobs
    4. Partition heals
    5. Manager-A attempts to reclaim - rejected due to lower fence token
    """

    @pytest.mark.asyncio
    async def test_stale_leader_after_partition_heal(self):
        """Stale leader's transfers are rejected after partition heals."""
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)

        worker = SimulatedWorker("worker-001", tcp_port=8000)
        cluster.add_worker(worker)

        for mgr in [manager_a, manager_b]:
            mgr.add_worker("worker-001", "127.0.0.1", 8000)
            mgr.add_job("job-001", "manager-a", ("127.0.0.1", 9090), fencing_token=1)
            mgr.add_sub_workflow_to_job("job-001", "wf-001", "worker-001")

        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        # Initial: Manager-A is leader
        cluster.elect_leader("manager-a")

        # Partition: Manager-B's side thinks Manager-A is dead
        manager_b._dead_managers.add(("127.0.0.1", 9090))
        cluster.elect_leader("manager-b")
        await manager_b._scan_for_orphaned_jobs()

        # Worker now points to Manager-B with token 2
        assert worker._fence_tokens["job-001"] == 2

        # Partition heals: Manager-A tries to assert leadership
        # But it still has the old token
        stale_transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9090),
            new_manager_id="manager-a",
            old_manager_id=None,
            fence_token=1,  # Old token
        )

        ack = await worker.job_leader_worker_transfer(stale_transfer)

        # Rejected
        assert not ack.accepted
        assert worker._workflow_job_leader["wf-001"] == ("127.0.0.1", 9092)

    @pytest.mark.asyncio
    async def test_recovered_manager_gets_updated_state(self):
        """
        After partition heals, the stale leader should eventually sync.

        In real system, state sync would update Manager-A's tokens.
        Here we verify that even with manual update, higher token wins.
        """
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)

        worker = SimulatedWorker("worker-001", tcp_port=8000)
        cluster.add_worker(worker)

        for mgr in [manager_a, manager_b]:
            mgr.add_worker("worker-001", "127.0.0.1", 8000)
            mgr.add_job("job-001", "manager-a", ("127.0.0.1", 9090), fencing_token=1)
            mgr.add_sub_workflow_to_job("job-001", "wf-001", "worker-001")

        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))
        worker._fence_tokens["job-001"] = 1

        # Manager-B takes over with token 5 (simulating multiple elections)
        manager_b._job_fencing_tokens["job-001"] = 5
        transfer_b = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9092),
            new_manager_id="manager-b",
            old_manager_id="manager-a",
            fence_token=5,
        )
        await worker.job_leader_worker_transfer(transfer_b)

        # Manager-A learns the new token and tries to take back with token 6
        manager_a._job_fencing_tokens["job-001"] = 6
        transfer_a = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9090),
            new_manager_id="manager-a",
            old_manager_id="manager-b",
            fence_token=6,
        )
        ack = await worker.job_leader_worker_transfer(transfer_a)

        # Now Manager-A wins because it has the higher token
        assert ack.accepted
        assert worker._fence_tokens["job-001"] == 6
        assert worker._workflow_job_leader["wf-001"] == ("127.0.0.1", 9090)


class TestEdgeCasesAndRobustness:
    """Edge cases and robustness tests for leadership transfers."""

    @pytest.mark.asyncio
    async def test_worker_not_found_during_transfer(self):
        """Manager handles missing worker gracefully during transfer notification."""
        cluster = SimulatedCluster()

        manager = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        cluster.add_manager(manager)

        # Register worker that won't be in cluster
        manager.add_worker("worker-ghost", "127.0.0.1", 8000)
        manager.add_job("job-001", "old-leader", ("10.0.0.1", 9090), fencing_token=1)
        manager.add_sub_workflow_to_job("job-001", "wf-001", "worker-ghost")

        manager._dead_managers.add(("10.0.0.1", 9090))
        manager.become_leader()

        # Should not raise even though worker isn't in cluster
        await manager._scan_for_orphaned_jobs()

        assert manager._job_leaders["job-001"] == "manager-a"

    @pytest.mark.asyncio
    async def test_empty_job_takeover(self):
        """Job with no active workflows can still be taken over."""
        cluster = SimulatedCluster()

        manager = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        cluster.add_manager(manager)

        # Job with no sub-workflows
        manager.add_job("job-empty", "old-leader", ("10.0.0.1", 9090), fencing_token=1)
        manager._dead_managers.add(("10.0.0.1", 9090))
        manager.become_leader()

        await manager._scan_for_orphaned_jobs()

        assert manager._job_leaders["job-empty"] == "manager-a"
        assert manager._job_fencing_tokens["job-empty"] == 2

    @pytest.mark.asyncio
    async def test_idempotent_scan(self):
        """Running scan multiple times is idempotent."""
        cluster = SimulatedCluster()

        manager = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        cluster.add_manager(manager)

        manager.add_job("job-001", "old-leader", ("10.0.0.1", 9090), fencing_token=1)
        manager._dead_managers.add(("10.0.0.1", 9090))
        manager.become_leader()

        # First scan
        await manager._scan_for_orphaned_jobs()
        first_token = manager._job_fencing_tokens["job-001"]
        first_version = manager._state_version

        # Second scan (dead_managers should be cleared)
        await manager._scan_for_orphaned_jobs()

        # Should not increment again
        assert manager._job_fencing_tokens["job-001"] == first_token
        assert manager._state_version == first_version

    @pytest.mark.asyncio
    async def test_manager_recovery_clears_dead_tracking(self):
        """Recovered manager is removed from dead tracking."""
        cluster = SimulatedCluster()

        manager_a = SimulatedManager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = SimulatedManager("manager-b", tcp_port=9092, udp_port=9093)

        cluster.add_manager(manager_a)
        cluster.add_manager(manager_b)

        # Manager-A fails
        cluster.simulate_manager_failure("manager-a")
        assert ("127.0.0.1", 9090) in manager_b._dead_managers

        # Manager-A recovers
        cluster.simulate_manager_recovery("manager-a")
        assert ("127.0.0.1", 9090) not in manager_b._dead_managers

    @pytest.mark.asyncio
    async def test_very_large_fence_token(self):
        """System handles very large fence tokens correctly."""
        worker = SimulatedWorker("worker-001", tcp_port=8000)
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        large_token = 2**62

        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9092),
            new_manager_id="manager-b",
            old_manager_id="manager-a",
            fence_token=large_token,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted
        assert worker._fence_tokens["job-001"] == large_token

        # Even larger token should still work
        larger_transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["wf-001"],
            new_manager_addr=("127.0.0.1", 9094),
            new_manager_id="manager-c",
            old_manager_id="manager-b",
            fence_token=large_token + 1,
        )

        ack2 = await worker.job_leader_worker_transfer(larger_transfer)
        assert ack2.accepted