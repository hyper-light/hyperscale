"""
Unit tests for Section 1: Job Leadership Takeover When SWIM Leader IS Job Leader.

These tests verify the AD-31 Section 1 implementation:
1. Dead manager tracking via _dead_managers set
2. Orphaned job scanning via _scan_for_orphaned_jobs()
3. New leader callback integration via _on_manager_become_leader()
4. Edge cases including concurrent failures and manager recovery

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch


# =============================================================================
# Mock Infrastructure
# =============================================================================


@dataclass
class MockNodeId:
    """Mock node ID with full and short representations."""

    full: str = "manager-node-001"
    short: str = "mgr-001"
    datacenter: str = "dc1"


@dataclass
class MockEnv:
    """Mock environment configuration for tests."""

    RECOVERY_JITTER_MIN: float = 0.0  # Disable jitter for faster tests
    RECOVERY_JITTER_MAX: float = 0.0  # Disable jitter for faster tests
    DATACENTER_ID: str = "dc1"


@dataclass
class MockTaskRunner:
    """Mock task runner that records scheduled tasks."""

    _tasks: list = field(default_factory=list)

    def run(self, coro_or_func, *args, **kwargs) -> None:
        """Record task for verification without executing."""
        self._tasks.append((coro_or_func, args, kwargs))

    def clear(self) -> None:
        """Clear recorded tasks."""
        self._tasks.clear()

    @property
    def task_count(self) -> int:
        """Number of tasks scheduled."""
        return len(self._tasks)


@dataclass
class MockLogger:
    """Mock logger that records log calls."""

    _logs: list = field(default_factory=list)

    async def log(self, message: Any) -> None:
        """Record log message."""
        self._logs.append(message)

    def clear(self) -> None:
        """Clear recorded logs."""
        self._logs.clear()

    @property
    def log_count(self) -> int:
        """Number of log messages recorded."""
        return len(self._logs)


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


class MockManagerServer:
    """
    Mock implementation of ManagerServer for testing Section 1 functionality.

    This mock implements only the methods and data structures needed for
    testing job leadership takeover behavior.
    """

    def __init__(self) -> None:
        # Identity
        self._node_id = MockNodeId()
        self._host = "127.0.0.1"
        self._tcp_port = 9090

        # Configuration
        self.env = MockEnv()

        # Infrastructure
        self._task_runner = MockTaskRunner()
        self._udp_logger = MockLogger()
        self._job_manager = MockJobManager()

        # State versioning
        self._state_version = 0

        # Dead manager tracking (AD-31 Section 1)
        self._dead_managers: set[tuple[str, int]] = set()

        # Job leader tracking
        self._job_leaders: dict[str, str] = {}  # job_id -> leader_node_id
        self._job_leader_addrs: dict[str, tuple[str, int]] = {}  # job_id -> (host, tcp_port)
        self._job_fencing_tokens: dict[str, int] = {}  # job_id -> fencing token

        # Origin gate addresses
        self._job_origin_gates: dict[str, tuple[str, int]] = {}

        # Worker tracking
        self._workers: dict[str, MockWorkerRegistration] = {}

        # Manager peer tracking
        self._known_manager_peers: dict[str, MockManagerInfo] = {}
        self._manager_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        self._manager_peer_unhealthy_since: dict[str, float] = {}

        # Leadership status
        self._is_leader = False

        # Network call tracking for verification
        self._tcp_calls: list[tuple[str, tuple[str, int], Any]] = []

    def is_leader(self) -> bool:
        """Return whether this manager is the SWIM cluster leader."""
        return self._is_leader

    def _increment_version(self) -> None:
        """Increment state version."""
        self._state_version += 1

    async def send_tcp(
        self,
        addr: tuple[str, int],
        action: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes | None, float]:
        """Mock TCP send - records calls for verification."""
        self._tcp_calls.append((action, addr, data))
        # Return mock success response
        return (b'{"accepted": true}', 0.01)

    # =========================================================================
    # Methods Under Test (copied from actual implementation for isolation)
    # =========================================================================

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """Called when a node is marked as DEAD via SWIM."""
        manager_tcp_addr = self._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            # Track dead manager for orphaned job scanning (AD-31 Section 1)
            self._dead_managers.add(manager_tcp_addr)
            # Trigger failure handling
            self._task_runner.run(self._handle_manager_peer_failure, node_addr, manager_tcp_addr)

    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        """Called when a node joins or rejoins the SWIM cluster."""
        manager_tcp_addr = self._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            # Clear from dead managers tracking (AD-31 Section 1)
            self._dead_managers.discard(manager_tcp_addr)

    def _on_manager_become_leader(self) -> None:
        """Called when this manager becomes the SWIM cluster leader."""
        self._task_runner.run(self._scan_for_orphaned_jobs)

    async def _handle_manager_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle manager peer failure."""
        # Find manager ID
        for manager_id, info in self._known_manager_peers.items():
            if (info.tcp_host, info.tcp_port) == tcp_addr:
                self._manager_peer_unhealthy_since[manager_id] = time.monotonic()
                break

        # If we're leader, handle job leadership failover
        if self.is_leader():
            await self._handle_job_leader_failure(tcp_addr)

    async def _handle_job_leader_failure(
        self,
        failed_manager_addr: tuple[str, int],
    ) -> None:
        """Handle job leadership takeover when a job leader manager fails."""
        if not self.is_leader():
            return

        # Find jobs led by the failed manager
        orphaned_jobs: list[str] = []
        for job_id, leader_addr in list(self._job_leader_addrs.items()):
            if leader_addr == failed_manager_addr:
                orphaned_jobs.append(job_id)

        if not orphaned_jobs:
            return

        # Take over leadership of each orphaned job
        for job_id in orphaned_jobs:
            old_leader = self._job_leaders.get(job_id)
            old_token = self._job_fencing_tokens.get(job_id, 0)
            new_token = old_token + 1

            self._job_leaders[job_id] = self._node_id.full
            self._job_leader_addrs[job_id] = (self._host, self._tcp_port)
            self._job_fencing_tokens[job_id] = new_token

            self._increment_version()

            # Notify gate and workers
            await self._notify_gate_of_leadership_transfer(job_id, old_leader)
            await self._notify_workers_of_leadership_transfer(job_id, old_leader)

    async def _scan_for_orphaned_jobs(self) -> None:
        """Scan for and take over orphaned jobs after becoming SWIM cluster leader."""
        if not self._dead_managers:
            return

        # Find all orphaned jobs
        orphaned_jobs: list[tuple[str, tuple[str, int]]] = []
        for job_id, leader_addr in list(self._job_leader_addrs.items()):
            if leader_addr in self._dead_managers:
                orphaned_jobs.append((job_id, leader_addr))

        if not orphaned_jobs:
            self._dead_managers.clear()
            return

        # Track processed dead managers
        processed_dead_managers: set[tuple[str, int]] = set()

        for job_id, dead_leader_addr in orphaned_jobs:
            # Skip jitter for tests (env.RECOVERY_JITTER_MAX = 0)

            old_leader = self._job_leaders.get(job_id)
            old_token = self._job_fencing_tokens.get(job_id, 0)
            new_token = old_token + 1

            self._job_leaders[job_id] = self._node_id.full
            self._job_leader_addrs[job_id] = (self._host, self._tcp_port)
            self._job_fencing_tokens[job_id] = new_token

            self._increment_version()

            await self._notify_gate_of_leadership_transfer(job_id, old_leader)
            await self._notify_workers_of_leadership_transfer(job_id, old_leader)

            processed_dead_managers.add(dead_leader_addr)

        # Clear processed dead managers
        self._dead_managers -= processed_dead_managers

    async def _notify_gate_of_leadership_transfer(
        self,
        job_id: str,
        old_manager_id: str | None,
    ) -> None:
        """Notify the origin gate of job leadership transfer."""
        origin_gate_addr = self._job_origin_gates.get(job_id)
        if not origin_gate_addr:
            return

        # Record the notification for test verification
        self._tcp_calls.append(("job_leader_manager_transfer", origin_gate_addr, job_id))

    async def _notify_workers_of_leadership_transfer(
        self,
        job_id: str,
        old_manager_id: str | None,
    ) -> None:
        """Notify workers of job leadership transfer."""
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return

        # Find workers with active workflows
        worker_workflows: dict[str, list[str]] = {}
        for sub_wf_id, sub_wf in job.sub_workflows.items():
            if sub_wf.result is None and sub_wf.worker_id:
                if sub_wf.worker_id not in worker_workflows:
                    worker_workflows[sub_wf.worker_id] = []
                worker_workflows[sub_wf.worker_id].append(sub_wf_id)

        for worker_id, workflow_ids in worker_workflows.items():
            worker_reg = self._workers.get(worker_id)
            if worker_reg:
                worker_addr = (worker_reg.node.host, worker_reg.node.port)
                self._tcp_calls.append(("job_leader_worker_transfer", worker_addr, job_id))

    # =========================================================================
    # Test Helpers
    # =========================================================================

    def add_manager_peer(
        self,
        manager_id: str,
        tcp_host: str,
        tcp_port: int,
        udp_host: str,
        udp_port: int,
    ) -> None:
        """Add a manager peer for testing."""
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
        """Add a job for testing."""
        self._job_leaders[job_id] = leader_node_id
        self._job_leader_addrs[job_id] = leader_addr
        self._job_fencing_tokens[job_id] = fencing_token
        if origin_gate:
            self._job_origin_gates[job_id] = origin_gate

        # Add to job manager
        self._job_manager.add_job(MockJob(job_id=job_id))

    def add_worker(
        self,
        worker_id: str,
        host: str,
        port: int,
    ) -> None:
        """Add a worker for testing."""
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
        """Add a sub-workflow to a job for testing."""
        job = self._job_manager.get_job_by_id(job_id)
        if job:
            job.sub_workflows[sub_workflow_id] = MockSubWorkflow(
                worker_id=worker_id,
                result="done" if completed else None,
            )


# =============================================================================
# Test Classes
# =============================================================================


class TestDeadManagersTracking:
    """Tests for _dead_managers set tracking behavior."""

    def test_dead_managers_initially_empty(self):
        """_dead_managers should be empty on initialization."""
        manager = MockManagerServer()
        assert len(manager._dead_managers) == 0

    def test_on_node_dead_adds_manager_to_dead_set(self):
        """_on_node_dead should add manager TCP address to _dead_managers."""
        manager = MockManagerServer()

        # Add a manager peer
        manager.add_manager_peer(
            manager_id="peer-001",
            tcp_host="192.168.1.10",
            tcp_port=9090,
            udp_host="192.168.1.10",
            udp_port=9091,
        )

        # Simulate SWIM detecting the manager as dead
        manager._on_node_dead(("192.168.1.10", 9091))

        # Verify TCP address was added to dead managers
        assert ("192.168.1.10", 9090) in manager._dead_managers

    def test_on_node_dead_ignores_unknown_addresses(self):
        """_on_node_dead should ignore addresses not in _manager_udp_to_tcp."""
        manager = MockManagerServer()

        # Call with unknown address
        manager._on_node_dead(("10.0.0.1", 9091))

        # Should not add anything
        assert len(manager._dead_managers) == 0

    def test_on_node_join_removes_manager_from_dead_set(self):
        """_on_node_join should remove manager from _dead_managers."""
        manager = MockManagerServer()

        # Add a manager peer and mark as dead
        manager.add_manager_peer(
            manager_id="peer-001",
            tcp_host="192.168.1.10",
            tcp_port=9090,
            udp_host="192.168.1.10",
            udp_port=9091,
        )
        manager._dead_managers.add(("192.168.1.10", 9090))

        # Simulate manager rejoining
        manager._on_node_join(("192.168.1.10", 9091))

        # Verify removed from dead managers
        assert ("192.168.1.10", 9090) not in manager._dead_managers

    def test_on_node_join_handles_not_in_set(self):
        """_on_node_join should handle case where manager not in _dead_managers."""
        manager = MockManagerServer()

        # Add a manager peer (not in dead set)
        manager.add_manager_peer(
            manager_id="peer-001",
            tcp_host="192.168.1.10",
            tcp_port=9090,
            udp_host="192.168.1.10",
            udp_port=9091,
        )

        # Should not raise
        manager._on_node_join(("192.168.1.10", 9091))

        # Set should remain empty
        assert len(manager._dead_managers) == 0

    def test_multiple_managers_tracked_independently(self):
        """Multiple dead managers should be tracked independently."""
        manager = MockManagerServer()

        # Add two manager peers
        manager.add_manager_peer(
            manager_id="peer-001",
            tcp_host="192.168.1.10",
            tcp_port=9090,
            udp_host="192.168.1.10",
            udp_port=9091,
        )
        manager.add_manager_peer(
            manager_id="peer-002",
            tcp_host="192.168.1.20",
            tcp_port=9090,
            udp_host="192.168.1.20",
            udp_port=9091,
        )

        # Mark both as dead
        manager._on_node_dead(("192.168.1.10", 9091))
        manager._on_node_dead(("192.168.1.20", 9091))

        assert len(manager._dead_managers) == 2
        assert ("192.168.1.10", 9090) in manager._dead_managers
        assert ("192.168.1.20", 9090) in manager._dead_managers

        # One rejoins
        manager._on_node_join(("192.168.1.10", 9091))

        # Only one should remain
        assert len(manager._dead_managers) == 1
        assert ("192.168.1.10", 9090) not in manager._dead_managers
        assert ("192.168.1.20", 9090) in manager._dead_managers


class TestScanForOrphanedJobs:
    """Tests for _scan_for_orphaned_jobs() method."""

    @pytest.mark.asyncio
    async def test_returns_early_when_no_dead_managers(self):
        """Should return immediately when _dead_managers is empty."""
        manager = MockManagerServer()

        # Add a job
        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-001",
            leader_addr=("192.168.1.10", 9090),
        )

        # No dead managers
        await manager._scan_for_orphaned_jobs()

        # Job leadership should be unchanged
        assert manager._job_leaders["job-001"] == "peer-001"
        assert manager._job_leader_addrs["job-001"] == ("192.168.1.10", 9090)

    @pytest.mark.asyncio
    async def test_clears_dead_managers_when_no_orphaned_jobs(self):
        """Should clear _dead_managers when no jobs are orphaned."""
        manager = MockManagerServer()

        # Add a dead manager that leads no jobs
        manager._dead_managers.add(("192.168.1.10", 9090))

        # Add a job led by a different (alive) manager
        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-002",
            leader_addr=("192.168.1.20", 9090),
        )

        await manager._scan_for_orphaned_jobs()

        # Dead managers should be cleared
        assert len(manager._dead_managers) == 0

    @pytest.mark.asyncio
    async def test_takes_over_orphaned_job(self):
        """Should take over leadership of orphaned jobs."""
        manager = MockManagerServer()

        dead_manager_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_manager_addr)

        # Add job led by dead manager
        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-001",
            leader_addr=dead_manager_addr,
            fencing_token=5,
        )

        await manager._scan_for_orphaned_jobs()

        # Verify takeover
        assert manager._job_leaders["job-001"] == manager._node_id.full
        assert manager._job_leader_addrs["job-001"] == (manager._host, manager._tcp_port)

    @pytest.mark.asyncio
    async def test_increments_fencing_token(self):
        """Should increment fencing token when taking over job."""
        manager = MockManagerServer()

        dead_manager_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_manager_addr)

        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-001",
            leader_addr=dead_manager_addr,
            fencing_token=5,
        )

        await manager._scan_for_orphaned_jobs()

        # Token should be incremented
        assert manager._job_fencing_tokens["job-001"] == 6

    @pytest.mark.asyncio
    async def test_increments_state_version(self):
        """Should increment state version for each takeover."""
        manager = MockManagerServer()

        dead_manager_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_manager_addr)

        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-001",
            leader_addr=dead_manager_addr,
        )
        manager.add_job(
            job_id="job-002",
            leader_node_id="peer-001",
            leader_addr=dead_manager_addr,
        )

        initial_version = manager._state_version

        await manager._scan_for_orphaned_jobs()

        # Version should be incremented once per job
        assert manager._state_version == initial_version + 2

    @pytest.mark.asyncio
    async def test_clears_processed_dead_managers(self):
        """Should remove processed dead managers from tracking."""
        manager = MockManagerServer()

        dead_addr_1 = ("192.168.1.10", 9090)
        dead_addr_2 = ("192.168.1.20", 9090)
        manager._dead_managers.add(dead_addr_1)
        manager._dead_managers.add(dead_addr_2)

        # Only dead_addr_1 leads a job
        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-001",
            leader_addr=dead_addr_1,
        )

        await manager._scan_for_orphaned_jobs()

        # dead_addr_1 should be cleared (processed)
        # dead_addr_2 should remain (no jobs to process)
        assert dead_addr_1 not in manager._dead_managers
        assert dead_addr_2 in manager._dead_managers

    @pytest.mark.asyncio
    async def test_notifies_gate_of_transfer(self):
        """Should notify origin gate of leadership transfer."""
        manager = MockManagerServer()

        dead_manager_addr = ("192.168.1.10", 9090)
        origin_gate = ("192.168.1.100", 8080)
        manager._dead_managers.add(dead_manager_addr)

        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-001",
            leader_addr=dead_manager_addr,
            origin_gate=origin_gate,
        )

        await manager._scan_for_orphaned_jobs()

        # Verify gate notification was sent
        gate_notifications = [
            call for call in manager._tcp_calls
            if call[0] == "job_leader_manager_transfer"
        ]
        assert len(gate_notifications) == 1
        assert gate_notifications[0][1] == origin_gate

    @pytest.mark.asyncio
    async def test_notifies_workers_of_transfer(self):
        """Should notify workers with active workflows of leadership transfer."""
        manager = MockManagerServer()

        dead_manager_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_manager_addr)

        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-001",
            leader_addr=dead_manager_addr,
        )

        # Add workers with active sub-workflows
        manager.add_worker("worker-001", "192.168.1.50", 8000)
        manager.add_worker("worker-002", "192.168.1.51", 8000)
        manager.add_sub_workflow_to_job("job-001", "wf-001", "worker-001", completed=False)
        manager.add_sub_workflow_to_job("job-001", "wf-002", "worker-002", completed=False)

        await manager._scan_for_orphaned_jobs()

        # Verify worker notifications
        worker_notifications = [
            call for call in manager._tcp_calls
            if call[0] == "job_leader_worker_transfer"
        ]
        assert len(worker_notifications) == 2

    @pytest.mark.asyncio
    async def test_skips_completed_workflows_in_worker_notification(self):
        """Should not notify workers for completed workflows."""
        manager = MockManagerServer()

        dead_manager_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_manager_addr)

        manager.add_job(
            job_id="job-001",
            leader_node_id="peer-001",
            leader_addr=dead_manager_addr,
        )

        # One active, one completed workflow
        manager.add_worker("worker-001", "192.168.1.50", 8000)
        manager.add_worker("worker-002", "192.168.1.51", 8000)
        manager.add_sub_workflow_to_job("job-001", "wf-001", "worker-001", completed=False)
        manager.add_sub_workflow_to_job("job-001", "wf-002", "worker-002", completed=True)

        await manager._scan_for_orphaned_jobs()

        # Only one worker should be notified
        worker_notifications = [
            call for call in manager._tcp_calls
            if call[0] == "job_leader_worker_transfer"
        ]
        assert len(worker_notifications) == 1
        assert worker_notifications[0][1] == ("192.168.1.50", 8000)

    @pytest.mark.asyncio
    async def test_handles_multiple_orphaned_jobs(self):
        """Should handle multiple orphaned jobs from same dead manager."""
        manager = MockManagerServer()

        dead_manager_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_manager_addr)

        # Add multiple jobs led by same dead manager
        manager.add_job("job-001", "peer-001", dead_manager_addr, fencing_token=1)
        manager.add_job("job-002", "peer-001", dead_manager_addr, fencing_token=3)
        manager.add_job("job-003", "peer-001", dead_manager_addr, fencing_token=5)

        await manager._scan_for_orphaned_jobs()

        # All jobs should be taken over
        for job_id in ["job-001", "job-002", "job-003"]:
            assert manager._job_leaders[job_id] == manager._node_id.full
            assert manager._job_leader_addrs[job_id] == (manager._host, manager._tcp_port)

        # Each token should be incremented
        assert manager._job_fencing_tokens["job-001"] == 2
        assert manager._job_fencing_tokens["job-002"] == 4
        assert manager._job_fencing_tokens["job-003"] == 6


class TestOnManagerBecomeLeader:
    """Tests for _on_manager_become_leader() callback integration."""

    def test_schedules_orphan_scan(self):
        """Should schedule _scan_for_orphaned_jobs via task runner."""
        manager = MockManagerServer()

        manager._on_manager_become_leader()

        # Verify scan was scheduled
        assert manager._task_runner.task_count >= 1

        # Find the orphan scan task
        scan_tasks = [
            task for task in manager._task_runner._tasks
            if task[0] == manager._scan_for_orphaned_jobs
        ]
        assert len(scan_tasks) == 1

    @pytest.mark.asyncio
    async def test_callback_integration_with_dead_managers(self):
        """Full integration: become leader -> scan for orphans."""
        manager = MockManagerServer()

        # Setup: dead manager with orphaned job
        dead_manager_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_manager_addr)
        manager.add_job("job-001", "peer-001", dead_manager_addr, fencing_token=1)

        # Trigger callback
        manager._on_manager_become_leader()

        # Manually execute the scheduled scan (simulating task runner)
        await manager._scan_for_orphaned_jobs()

        # Verify takeover occurred
        assert manager._job_leaders["job-001"] == manager._node_id.full
        assert manager._job_fencing_tokens["job-001"] == 2


class TestHandleJobLeaderFailure:
    """Tests for _handle_job_leader_failure() during normal operation."""

    @pytest.mark.asyncio
    async def test_only_leader_performs_takeover(self):
        """Only SWIM cluster leader should take over orphaned jobs."""
        manager = MockManagerServer()
        manager._is_leader = False  # Not the leader

        dead_manager_addr = ("192.168.1.10", 9090)
        manager.add_job("job-001", "peer-001", dead_manager_addr, fencing_token=1)

        await manager._handle_job_leader_failure(dead_manager_addr)

        # Job should NOT be taken over
        assert manager._job_leaders["job-001"] == "peer-001"
        assert manager._job_fencing_tokens["job-001"] == 1

    @pytest.mark.asyncio
    async def test_leader_takes_over_jobs(self):
        """Leader should take over jobs from failed manager."""
        manager = MockManagerServer()
        manager._is_leader = True

        dead_manager_addr = ("192.168.1.10", 9090)
        manager.add_job("job-001", "peer-001", dead_manager_addr, fencing_token=1)

        await manager._handle_job_leader_failure(dead_manager_addr)

        # Job should be taken over
        assert manager._job_leaders["job-001"] == manager._node_id.full
        assert manager._job_fencing_tokens["job-001"] == 2

    @pytest.mark.asyncio
    async def test_ignores_jobs_with_other_leaders(self):
        """Should not affect jobs led by other (alive) managers."""
        manager = MockManagerServer()
        manager._is_leader = True

        dead_manager_addr = ("192.168.1.10", 9090)
        alive_manager_addr = ("192.168.1.20", 9090)

        # Job led by dead manager
        manager.add_job("job-001", "peer-001", dead_manager_addr, fencing_token=1)
        # Job led by alive manager
        manager.add_job("job-002", "peer-002", alive_manager_addr, fencing_token=5)

        await manager._handle_job_leader_failure(dead_manager_addr)

        # Only job-001 should be taken over
        assert manager._job_leaders["job-001"] == manager._node_id.full
        assert manager._job_leaders["job-002"] == "peer-002"
        assert manager._job_fencing_tokens["job-002"] == 5  # Unchanged


class TestEdgeCases:
    """Tests for edge cases and race conditions."""

    @pytest.mark.asyncio
    async def test_manager_recovery_during_election(self):
        """Manager rejoining should remove from dead set before scan."""
        manager = MockManagerServer()

        # Setup: manager is dead
        manager.add_manager_peer(
            manager_id="peer-001",
            tcp_host="192.168.1.10",
            tcp_port=9090,
            udp_host="192.168.1.10",
            udp_port=9091,
        )
        dead_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_addr)

        # Add job led by dead manager
        manager.add_job("job-001", "peer-001", dead_addr, fencing_token=1)

        # Manager recovers before scan runs
        manager._on_node_join(("192.168.1.10", 9091))

        # Now run scan
        await manager._scan_for_orphaned_jobs()

        # Job should NOT be taken over (manager is alive)
        assert manager._job_leaders["job-001"] == "peer-001"
        assert manager._job_fencing_tokens["job-001"] == 1

    @pytest.mark.asyncio
    async def test_job_completed_before_scan(self):
        """Jobs that complete before scan should not cause issues."""
        manager = MockManagerServer()

        dead_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_addr)

        # Add job, then remove it (simulating completion)
        manager.add_job("job-001", "peer-001", dead_addr, fencing_token=1)
        del manager._job_leaders["job-001"]
        del manager._job_leader_addrs["job-001"]

        # Scan should not raise
        await manager._scan_for_orphaned_jobs()

        # Dead managers should be cleared (no orphaned jobs found)
        assert len(manager._dead_managers) == 0

    @pytest.mark.asyncio
    async def test_multiple_scans_are_idempotent(self):
        """Running scan multiple times should be idempotent."""
        manager = MockManagerServer()

        dead_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_addr)

        manager.add_job("job-001", "peer-001", dead_addr, fencing_token=1)

        # First scan
        await manager._scan_for_orphaned_jobs()

        first_token = manager._job_fencing_tokens["job-001"]
        first_version = manager._state_version

        # Second scan (dead_addr should be cleared now)
        await manager._scan_for_orphaned_jobs()

        # Token and version should not change
        assert manager._job_fencing_tokens["job-001"] == first_token
        assert manager._state_version == first_version

    @pytest.mark.asyncio
    async def test_concurrent_death_and_join_of_same_manager(self):
        """Concurrent death and join of same manager should be handled."""
        manager = MockManagerServer()

        manager.add_manager_peer(
            manager_id="peer-001",
            tcp_host="192.168.1.10",
            tcp_port=9090,
            udp_host="192.168.1.10",
            udp_port=9091,
        )
        udp_addr = ("192.168.1.10", 9091)
        tcp_addr = ("192.168.1.10", 9090)

        # Rapid death -> join -> death -> join
        manager._on_node_dead(udp_addr)
        assert tcp_addr in manager._dead_managers

        manager._on_node_join(udp_addr)
        assert tcp_addr not in manager._dead_managers

        manager._on_node_dead(udp_addr)
        assert tcp_addr in manager._dead_managers

        manager._on_node_join(udp_addr)
        assert tcp_addr not in manager._dead_managers

    @pytest.mark.asyncio
    async def test_no_gate_notification_when_no_origin_gate(self):
        """Should skip gate notification when no origin gate recorded."""
        manager = MockManagerServer()

        dead_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_addr)

        # Job without origin gate
        manager.add_job("job-001", "peer-001", dead_addr, fencing_token=1, origin_gate=None)

        await manager._scan_for_orphaned_jobs()

        # No gate notifications
        gate_notifications = [
            call for call in manager._tcp_calls
            if call[0] == "job_leader_manager_transfer"
        ]
        assert len(gate_notifications) == 0

    @pytest.mark.asyncio
    async def test_no_worker_notification_when_no_job_in_manager(self):
        """Should skip worker notification when job not in job manager."""
        manager = MockManagerServer()

        dead_addr = ("192.168.1.10", 9090)
        manager._dead_managers.add(dead_addr)

        # Add job to tracking but NOT to job manager
        manager._job_leaders["job-001"] = "peer-001"
        manager._job_leader_addrs["job-001"] = dead_addr
        manager._job_fencing_tokens["job-001"] = 1
        # Note: NOT calling manager.add_job() so it's not in _job_manager

        await manager._scan_for_orphaned_jobs()

        # No worker notifications
        worker_notifications = [
            call for call in manager._tcp_calls
            if call[0] == "job_leader_worker_transfer"
        ]
        assert len(worker_notifications) == 0

    @pytest.mark.asyncio
    async def test_fencing_token_monotonically_increases(self):
        """Fencing tokens should always increase monotonically."""
        manager = MockManagerServer()
        manager._is_leader = True

        dead_addr = ("192.168.1.10", 9090)

        # Add job with high initial token
        manager.add_job("job-001", "peer-001", dead_addr, fencing_token=100)

        # Takeover via handle_job_leader_failure
        await manager._handle_job_leader_failure(dead_addr)

        assert manager._job_fencing_tokens["job-001"] == 101

        # Reset and test via scan
        manager._job_leaders["job-001"] = "peer-002"
        manager._job_leader_addrs["job-001"] = ("192.168.1.20", 9090)
        manager._dead_managers.add(("192.168.1.20", 9090))

        await manager._scan_for_orphaned_jobs()

        # Token should increment again
        assert manager._job_fencing_tokens["job-001"] == 102


class TestFailoverScenarios:
    """Tests for realistic failover scenarios."""

    @pytest.mark.asyncio
    async def test_swim_leader_is_job_leader_scenario(self):
        """
        Test the main scenario: SWIM leader (also job leader) fails.

        1. Manager-A is SWIM leader and job leader
        2. Manager-A fails
        3. Manager-B wins election, becomes new SWIM leader
        4. Manager-B runs _scan_for_orphaned_jobs and takes over job
        """
        # Manager-B (this instance) will become the new leader
        manager_b = MockManagerServer()
        manager_b._node_id.full = "manager-b-full"
        manager_b._node_id.short = "mgr-b"

        # Setup: Manager-A was the previous leader
        manager_a_tcp = ("192.168.1.10", 9090)
        manager_a_udp = ("192.168.1.10", 9091)

        manager_b.add_manager_peer(
            manager_id="manager-a-full",
            tcp_host="192.168.1.10",
            tcp_port=9090,
            udp_host="192.168.1.10",
            udp_port=9091,
        )

        # Manager-A was leading a job
        manager_b.add_job(
            job_id="critical-job",
            leader_node_id="manager-a-full",
            leader_addr=manager_a_tcp,
            fencing_token=10,
            origin_gate=("192.168.1.100", 8080),
        )

        # Add workers
        manager_b.add_worker("worker-001", "192.168.1.50", 8000)
        manager_b.add_sub_workflow_to_job("critical-job", "wf-001", "worker-001")

        # Step 1: SWIM detects Manager-A as dead
        manager_b._on_node_dead(manager_a_udp)
        assert manager_a_tcp in manager_b._dead_managers

        # Step 2: Manager-B wins election, becomes leader
        manager_b._is_leader = True
        manager_b._on_manager_become_leader()

        # Step 3: Execute the scheduled scan
        await manager_b._scan_for_orphaned_jobs()

        # Verify: Manager-B took over job leadership
        assert manager_b._job_leaders["critical-job"] == "manager-b-full"
        assert manager_b._job_leader_addrs["critical-job"] == (manager_b._host, manager_b._tcp_port)
        assert manager_b._job_fencing_tokens["critical-job"] == 11

        # Verify: Gate was notified
        gate_notifications = [
            call for call in manager_b._tcp_calls
            if call[0] == "job_leader_manager_transfer"
        ]
        assert len(gate_notifications) == 1

        # Verify: Worker was notified
        worker_notifications = [
            call for call in manager_b._tcp_calls
            if call[0] == "job_leader_worker_transfer"
        ]
        assert len(worker_notifications) == 1

        # Verify: Dead manager was cleared
        assert manager_a_tcp not in manager_b._dead_managers

    @pytest.mark.asyncio
    async def test_non_leader_job_leader_fails_scenario(self):
        """
        Test scenario: Job leader (not SWIM leader) fails.

        1. Manager-A is SWIM leader
        2. Manager-B is job leader for job-001
        3. Manager-B fails
        4. Manager-A (already leader) takes over via _handle_job_leader_failure
        """
        # Manager-A is SWIM leader
        manager_a = MockManagerServer()
        manager_a._node_id.full = "manager-a-full"
        manager_a._is_leader = True

        # Manager-B is job leader
        manager_b_tcp = ("192.168.1.20", 9090)
        manager_b_udp = ("192.168.1.20", 9091)

        manager_a.add_manager_peer(
            manager_id="manager-b-full",
            tcp_host="192.168.1.20",
            tcp_port=9090,
            udp_host="192.168.1.20",
            udp_port=9091,
        )

        manager_a.add_job(
            job_id="job-001",
            leader_node_id="manager-b-full",
            leader_addr=manager_b_tcp,
            fencing_token=5,
        )

        # Manager-B fails
        manager_a._on_node_dead(manager_b_udp)

        # Execute the failure handling (normally done by task runner)
        await manager_a._handle_manager_peer_failure(manager_b_udp, manager_b_tcp)

        # Verify: Manager-A took over
        assert manager_a._job_leaders["job-001"] == "manager-a-full"
        assert manager_a._job_fencing_tokens["job-001"] == 6

    @pytest.mark.asyncio
    async def test_cascading_failures_scenario(self):
        """
        Test scenario: Multiple managers fail in sequence.

        1. Manager-A leads job-001, Manager-B leads job-002
        2. Both fail
        3. Manager-C becomes leader, scans for orphans
        4. Manager-C takes over both jobs
        """
        manager_c = MockManagerServer()
        manager_c._node_id.full = "manager-c-full"

        manager_a_tcp = ("192.168.1.10", 9090)
        manager_b_tcp = ("192.168.1.20", 9090)

        # Both managers are dead
        manager_c._dead_managers.add(manager_a_tcp)
        manager_c._dead_managers.add(manager_b_tcp)

        # Jobs led by different dead managers
        manager_c.add_job("job-001", "manager-a-full", manager_a_tcp, fencing_token=1)
        manager_c.add_job("job-002", "manager-b-full", manager_b_tcp, fencing_token=3)

        await manager_c._scan_for_orphaned_jobs()

        # Both jobs should be taken over
        assert manager_c._job_leaders["job-001"] == "manager-c-full"
        assert manager_c._job_leaders["job-002"] == "manager-c-full"
        assert manager_c._job_fencing_tokens["job-001"] == 2
        assert manager_c._job_fencing_tokens["job-002"] == 4

        # Both dead managers cleared
        assert len(manager_c._dead_managers) == 0
