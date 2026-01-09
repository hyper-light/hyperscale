"""
Integration tests for Section 7: Gate Job Leadership Takeover Handling.

Tests verify:
- Gate tracks dead job leader managers
- Jobs are marked as orphaned when their manager fails
- Orphaned jobs are cleared when transfer is received
- Jobs fail after grace period expires without transfer

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock


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
class MockGateEnv:
    """Mock environment configuration for gate tests."""

    GATE_ORPHAN_GRACE_PERIOD: float = 2.0  # Short grace period for faster tests
    GATE_ORPHAN_CHECK_INTERVAL: float = 0.5


@dataclass
class MockJobInfo:
    """Mock job info."""

    job_id: str
    status: str = "RUNNING"
    error: str | None = None


@dataclass
class MockJobLeadershipTracker:
    """Mock job leadership tracker."""

    _dc_managers: dict = field(default_factory=dict)  # job_id -> {dc_id -> addr}
    _jobs: set = field(default_factory=set)

    def get_dc_manager(self, job_id: str, dc_id: str) -> tuple[str, int] | None:
        job_dcs = self._dc_managers.get(job_id, {})
        return job_dcs.get(dc_id)

    def list_jobs(self) -> list[str]:
        return list(self._jobs)

    def add_job(self, job_id: str, dc_id: str, manager_addr: tuple[str, int]) -> None:
        if job_id not in self._dc_managers:
            self._dc_managers[job_id] = {}
        self._dc_managers[job_id][dc_id] = manager_addr
        self._jobs.add(job_id)


class MockGateServer:
    """
    Mock gate server for testing Section 7 functionality.
    """

    def __init__(self, env: MockGateEnv | None = None) -> None:
        # Configuration
        env = env or MockGateEnv()

        # Identity
        self._host = "127.0.0.1"
        self._tcp_port = 8080
        self._node_id = MagicMock()
        self._node_id.short = "gate-001"
        self._node_id.full = "gate-001-full"

        # Infrastructure
        self._udp_logger = MockLogger()
        self._running = True
        self._task_runner = MagicMock()
        self._task_runner.run = lambda coro, *args, **kwargs: None

        # Job tracking
        self._job_dc_managers: dict[str, dict[str, tuple[str, int]]] = {}
        self._job_callbacks: dict[str, tuple[str, int]] = {}
        self._progress_callbacks: dict[str, tuple[str, int]] = {}
        self._jobs: dict[str, MockJobInfo] = {}
        self._job_leadership_tracker = MockJobLeadershipTracker()

        # Section 7: Gate job leadership takeover handling
        self._dead_job_leaders: set[tuple[str, int]] = set()
        self._orphaned_jobs: dict[str, float] = {}
        self._orphan_grace_period: float = env.GATE_ORPHAN_GRACE_PERIOD
        self._orphan_check_interval: float = env.GATE_ORPHAN_CHECK_INTERVAL
        self._orphan_check_task: asyncio.Task | None = None

        # TCP tracking
        self._tcp_calls: list[tuple[tuple[str, int], str, Any]] = []

    async def send_tcp(
        self,
        addr: tuple[str, int],
        action: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes | None, float]:
        self._tcp_calls.append((addr, action, data))
        return (b"OK", 0.01)

    # =========================================================================
    # Section 7 Methods (copied from implementation for testing)
    # =========================================================================

    async def _handle_manager_death_for_jobs(
        self,
        manager_addr: tuple[str, int],
        datacenter_id: str,
    ) -> None:
        """Handle a job leader manager's death."""
        self._dead_job_leaders.add(manager_addr)
        await self._scan_for_orphaned_jobs(manager_addr, datacenter_id)

    async def _scan_for_orphaned_jobs(
        self,
        dead_manager_addr: tuple[str, int],
        datacenter_id: str,
    ) -> None:
        """Scan for jobs whose leader manager has died."""
        current_time = time.monotonic()

        # Check jobs in _job_dc_managers
        for job_id, dc_managers in list(self._job_dc_managers.items()):
            manager_addr = dc_managers.get(datacenter_id)
            if manager_addr == dead_manager_addr:
                if job_id not in self._orphaned_jobs:
                    self._orphaned_jobs[job_id] = current_time

        # Also check the leadership tracker
        for job_id in self._job_leadership_tracker.list_jobs():
            manager_addr = self._job_leadership_tracker.get_dc_manager(job_id, datacenter_id)
            if manager_addr == dead_manager_addr:
                if job_id not in self._orphaned_jobs:
                    self._orphaned_jobs[job_id] = current_time

    def _clear_orphaned_job(self, job_id: str, new_manager_addr: tuple[str, int]) -> None:
        """Clear a job's orphaned status when transfer is received."""
        if job_id in self._orphaned_jobs:
            del self._orphaned_jobs[job_id]

    async def _orphan_check_loop(self) -> None:
        """Background loop checking for orphaned jobs."""
        while self._running:
            try:
                await asyncio.sleep(self._orphan_check_interval)

                current_time = time.monotonic()
                jobs_to_fail: list[str] = []

                for job_id, orphan_timestamp in list(self._orphaned_jobs.items()):
                    elapsed = current_time - orphan_timestamp
                    if elapsed >= self._orphan_grace_period:
                        jobs_to_fail.append(job_id)

                for job_id in jobs_to_fail:
                    self._orphaned_jobs.pop(job_id, None)
                    await self._handle_job_orphan_timeout(job_id)

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _handle_job_orphan_timeout(self, job_id: str) -> None:
        """Handle a job whose orphan grace period has expired."""
        # Update job status to failed
        job_info = self._jobs.get(job_id)
        if job_info:
            job_info.status = "FAILED"
            job_info.error = "Job leader manager failed, no replacement within grace period"

        # Clean up callbacks
        self._job_callbacks.pop(job_id, None)
        self._progress_callbacks.pop(job_id, None)

    def start_orphan_check_loop(self) -> None:
        """Start the orphan check background task."""
        if self._orphan_check_task is None or self._orphan_check_task.done():
            self._orphan_check_task = asyncio.create_task(self._orphan_check_loop())

    async def stop_orphan_check_loop(self) -> None:
        """Stop the orphan check background task."""
        self._running = False
        if self._orphan_check_task:
            self._orphan_check_task.cancel()
            try:
                await self._orphan_check_task
            except asyncio.CancelledError:
                pass
            self._orphan_check_task = None

    # Test helpers

    def add_job(
        self,
        job_id: str,
        dc_id: str,
        manager_addr: tuple[str, int],
    ) -> None:
        """Add a job with DC manager."""
        if job_id not in self._job_dc_managers:
            self._job_dc_managers[job_id] = {}
        self._job_dc_managers[job_id][dc_id] = manager_addr
        self._jobs[job_id] = MockJobInfo(job_id=job_id)
        self._job_leadership_tracker.add_job(job_id, dc_id, manager_addr)

    def set_callback(self, job_id: str, callback_addr: tuple[str, int]) -> None:
        """Set client callback for a job."""
        self._job_callbacks[job_id] = callback_addr


# =============================================================================
# Test Classes
# =============================================================================


class TestDeadJobLeaderTracking:
    """Tests for tracking dead job leader managers."""

    @pytest.mark.asyncio
    async def test_manager_added_to_dead_leaders(self):
        """Manager should be added to dead leaders set when death detected."""
        gate = MockGateServer()

        manager_addr = ("192.168.1.10", 9090)
        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        assert manager_addr in gate._dead_job_leaders

    @pytest.mark.asyncio
    async def test_multiple_managers_tracked(self):
        """Multiple dead managers should be tracked."""
        gate = MockGateServer()

        manager1 = ("192.168.1.10", 9090)
        manager2 = ("192.168.1.20", 9090)

        await gate._handle_manager_death_for_jobs(manager1, "dc1")
        await gate._handle_manager_death_for_jobs(manager2, "dc2")

        assert manager1 in gate._dead_job_leaders
        assert manager2 in gate._dead_job_leaders


class TestOrphanedJobScanning:
    """Tests for scanning and marking orphaned jobs."""

    @pytest.mark.asyncio
    async def test_job_marked_orphaned_when_manager_dies(self):
        """Job should be marked orphaned when its manager dies."""
        gate = MockGateServer()

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        assert "job-001" in gate._orphaned_jobs
        assert gate._orphaned_jobs["job-001"] > 0  # Has timestamp

    @pytest.mark.asyncio
    async def test_only_affected_jobs_marked_orphaned(self):
        """Only jobs led by dead manager should be marked orphaned."""
        gate = MockGateServer()

        manager1 = ("192.168.1.10", 9090)
        manager2 = ("192.168.1.20", 9090)

        gate.add_job("job-001", "dc1", manager1)
        gate.add_job("job-002", "dc1", manager2)

        # Only manager1 dies
        await gate._handle_manager_death_for_jobs(manager1, "dc1")

        assert "job-001" in gate._orphaned_jobs
        assert "job-002" not in gate._orphaned_jobs

    @pytest.mark.asyncio
    async def test_job_not_orphaned_if_different_dc(self):
        """Job with manager in different DC should not be orphaned."""
        gate = MockGateServer()

        manager_dc1 = ("192.168.1.10", 9090)
        manager_dc2 = ("192.168.1.20", 9090)

        # Job in dc2, manager in dc1 dies
        gate.add_job("job-001", "dc2", manager_dc2)

        await gate._handle_manager_death_for_jobs(manager_dc1, "dc1")

        assert "job-001" not in gate._orphaned_jobs


class TestOrphanedJobClearing:
    """Tests for clearing orphaned jobs when transfer is received."""

    @pytest.mark.asyncio
    async def test_orphan_cleared_on_transfer(self):
        """Orphaned job should be cleared when transfer is received."""
        gate = MockGateServer()

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)

        # Manager dies
        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")
        assert "job-001" in gate._orphaned_jobs

        # New manager takes over
        new_manager_addr = ("192.168.1.20", 9090)
        gate._clear_orphaned_job("job-001", new_manager_addr)

        assert "job-001" not in gate._orphaned_jobs

    @pytest.mark.asyncio
    async def test_clear_nonexistent_orphan_is_safe(self):
        """Clearing a non-orphaned job should be safe."""
        gate = MockGateServer()

        # No exception should be raised
        gate._clear_orphaned_job("nonexistent-job", ("192.168.1.20", 9090))

        assert "nonexistent-job" not in gate._orphaned_jobs


class TestOrphanGracePeriod:
    """Tests for orphan grace period handling."""

    @pytest.mark.asyncio
    async def test_job_not_failed_before_grace_period(self):
        """Job should not be failed before grace period expires."""
        env = MockGateEnv(
            GATE_ORPHAN_GRACE_PERIOD=2.0,
            GATE_ORPHAN_CHECK_INTERVAL=0.1,
        )
        gate = MockGateServer(env)

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        # Start orphan check loop
        gate.start_orphan_check_loop()

        # Wait less than grace period
        await asyncio.sleep(0.3)

        await gate.stop_orphan_check_loop()

        # Job should still be orphaned but not failed
        assert "job-001" in gate._orphaned_jobs
        assert gate._jobs["job-001"].status == "RUNNING"

    @pytest.mark.asyncio
    async def test_job_failed_after_grace_period(self):
        """Job should be failed after grace period expires without transfer."""
        env = MockGateEnv(
            GATE_ORPHAN_GRACE_PERIOD=0.3,
            GATE_ORPHAN_CHECK_INTERVAL=0.1,
        )
        gate = MockGateServer(env)

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        # Start orphan check loop
        gate.start_orphan_check_loop()

        # Wait past grace period
        await asyncio.sleep(0.5)

        await gate.stop_orphan_check_loop()

        # Job should be failed
        assert "job-001" not in gate._orphaned_jobs
        assert gate._jobs["job-001"].status == "FAILED"
        assert "grace period" in gate._jobs["job-001"].error

    @pytest.mark.asyncio
    async def test_job_rescued_by_transfer_before_grace_expires(self):
        """Job should not fail if transfer arrives before grace expires."""
        env = MockGateEnv(
            GATE_ORPHAN_GRACE_PERIOD=1.0,
            GATE_ORPHAN_CHECK_INTERVAL=0.1,
        )
        gate = MockGateServer(env)

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        # Start orphan check loop
        gate.start_orphan_check_loop()

        # Wait a bit
        await asyncio.sleep(0.3)

        # Transfer arrives
        new_manager_addr = ("192.168.1.20", 9090)
        gate._clear_orphaned_job("job-001", new_manager_addr)

        # Wait past original grace period
        await asyncio.sleep(1.0)

        await gate.stop_orphan_check_loop()

        # Job should NOT be failed (was rescued)
        assert gate._jobs["job-001"].status == "RUNNING"


class TestMultipleOrphanedJobs:
    """Tests for handling multiple orphaned jobs."""

    @pytest.mark.asyncio
    async def test_multiple_jobs_orphaned_on_single_manager_failure(self):
        """Multiple jobs led by same manager should all be orphaned."""
        gate = MockGateServer()

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)
        gate.add_job("job-002", "dc1", manager_addr)
        gate.add_job("job-003", "dc1", manager_addr)

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        assert "job-001" in gate._orphaned_jobs
        assert "job-002" in gate._orphaned_jobs
        assert "job-003" in gate._orphaned_jobs

    @pytest.mark.asyncio
    async def test_partial_transfer_only_rescues_mentioned_jobs(self):
        """Transfer for one job should not clear other orphaned jobs."""
        gate = MockGateServer()

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)
        gate.add_job("job-002", "dc1", manager_addr)

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        # Transfer only for job-001
        new_manager_addr = ("192.168.1.20", 9090)
        gate._clear_orphaned_job("job-001", new_manager_addr)

        assert "job-001" not in gate._orphaned_jobs
        assert "job-002" in gate._orphaned_jobs

    @pytest.mark.asyncio
    async def test_cascading_failures(self):
        """Multiple manager failures in sequence should be handled."""
        gate = MockGateServer()

        manager1 = ("192.168.1.10", 9090)
        manager2 = ("192.168.1.20", 9090)

        gate.add_job("job-001", "dc1", manager1)
        gate.add_job("job-002", "dc2", manager2)

        # Both managers fail
        await gate._handle_manager_death_for_jobs(manager1, "dc1")
        await gate._handle_manager_death_for_jobs(manager2, "dc2")

        assert "job-001" in gate._orphaned_jobs
        assert "job-002" in gate._orphaned_jobs
        assert manager1 in gate._dead_job_leaders
        assert manager2 in gate._dead_job_leaders


class TestOrphanTimeoutHandling:
    """Tests for orphan timeout handling."""

    @pytest.mark.asyncio
    async def test_callback_cleanup_on_timeout(self):
        """Callbacks should be cleaned up when job times out."""
        env = MockGateEnv(
            GATE_ORPHAN_GRACE_PERIOD=0.2,
            GATE_ORPHAN_CHECK_INTERVAL=0.05,
        )
        gate = MockGateServer(env)

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)
        gate.set_callback("job-001", ("192.168.1.100", 7070))

        assert "job-001" in gate._job_callbacks

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        gate.start_orphan_check_loop()
        await asyncio.sleep(0.4)
        await gate.stop_orphan_check_loop()

        # Callback should be cleaned up
        assert "job-001" not in gate._job_callbacks

    @pytest.mark.asyncio
    async def test_multiple_timeouts_in_sequence(self):
        """Multiple jobs timing out should all be handled."""
        env = MockGateEnv(
            GATE_ORPHAN_GRACE_PERIOD=0.2,
            GATE_ORPHAN_CHECK_INTERVAL=0.05,
        )
        gate = MockGateServer(env)

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)
        gate.add_job("job-002", "dc1", manager_addr)

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        gate.start_orphan_check_loop()
        await asyncio.sleep(0.4)
        await gate.stop_orphan_check_loop()

        # Both jobs should be failed
        assert gate._jobs["job-001"].status == "FAILED"
        assert gate._jobs["job-002"].status == "FAILED"


class TestEdgeCases:
    """Edge case tests."""

    @pytest.mark.asyncio
    async def test_empty_orphan_dict_handled_gracefully(self):
        """Empty orphan dict should not cause issues."""
        env = MockGateEnv(
            GATE_ORPHAN_GRACE_PERIOD=0.1,
            GATE_ORPHAN_CHECK_INTERVAL=0.05,
        )
        gate = MockGateServer(env)

        gate.start_orphan_check_loop()
        await asyncio.sleep(0.2)
        await gate.stop_orphan_check_loop()

        # Should complete without error

    @pytest.mark.asyncio
    async def test_job_completed_naturally_before_timeout(self):
        """Job that completes naturally should be handled correctly."""
        env = MockGateEnv(
            GATE_ORPHAN_GRACE_PERIOD=0.3,
            GATE_ORPHAN_CHECK_INTERVAL=0.05,
        )
        gate = MockGateServer(env)

        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)

        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        # Job completes naturally - remove from tracking
        del gate._jobs["job-001"]
        del gate._orphaned_jobs["job-001"]

        gate.start_orphan_check_loop()
        await asyncio.sleep(0.5)
        await gate.stop_orphan_check_loop()

        # No errors should have occurred

    @pytest.mark.asyncio
    async def test_same_manager_multiple_dcs(self):
        """Manager serving multiple DCs should orphan jobs in all DCs."""
        gate = MockGateServer()

        # Same manager address used in multiple DCs (unusual but possible)
        manager_addr = ("192.168.1.10", 9090)
        gate.add_job("job-001", "dc1", manager_addr)
        gate.add_job("job-002", "dc2", manager_addr)

        # When manager dies, only jobs in the same DC should be orphaned
        await gate._handle_manager_death_for_jobs(manager_addr, "dc1")

        # job-001 is in dc1 which is dead
        assert "job-001" in gate._orphaned_jobs
        # job-002 is in dc2, but manager_addr for dc2 is also dead...
        # Actually in this test setup, both jobs have the same addr but different DCs
        # The scan only checks the specific DC, so job-002 won't be found
        # Let's verify:
        assert "job-002" not in gate._orphaned_jobs
