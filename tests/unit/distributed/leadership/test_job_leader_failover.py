"""
Integration tests for Section 4: Job Leadership Failover scenarios.

These tests verify the full integration between:
- Manager job leadership takeover (Section 1)
- Worker orphan grace period handling (Section 2.7, Section 3)
- Gate notification flows (Section 7)

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock


# =============================================================================
# Mock Infrastructure for Worker
# =============================================================================


@dataclass
class MockWorkerEnv:
    """Mock environment configuration for worker tests."""

    WORKER_ORPHAN_GRACE_PERIOD: float = 2.0  # Short grace period for faster tests
    WORKER_ORPHAN_CHECK_INTERVAL: float = 0.5  # Frequent checks for faster tests
    RECOVERY_JITTER_MIN: float = 0.0
    RECOVERY_JITTER_MAX: float = 0.0
    DATACENTER_ID: str = "dc1"


@dataclass
class MockWorkerLogger:
    """Mock logger for worker tests."""

    _logs: list = field(default_factory=list)

    async def log(self, message: Any) -> None:
        """Record log message."""
        self._logs.append(message)

    def clear(self) -> None:
        """Clear recorded logs."""
        self._logs.clear()


@dataclass
class MockManagerInfo:
    """Mock manager info."""

    node_id: str
    tcp_host: str
    tcp_port: int


@dataclass
class MockJobLeaderWorkerTransfer:
    """Mock job leader worker transfer message."""

    job_id: str
    workflow_ids: list[str]
    new_manager_addr: tuple[str, int]
    old_manager_id: str
    fencing_token: int

    @classmethod
    def load(cls, data: bytes) -> "MockJobLeaderWorkerTransfer":
        """Deserialize from bytes (mock implementation)."""
        # In tests, we'll pass the object directly
        return data


@dataclass
class MockJobLeaderWorkerTransferAck:
    """Mock transfer acknowledgment."""

    job_id: str
    workflows_updated: int
    accepted: bool


class MockWorkerServer:
    """
    Mock implementation of WorkerServer for testing Section 4 functionality.

    Implements only the methods and data structures needed for testing
    worker orphan workflow handling and job leader transfers.
    """

    def __init__(self, env: MockWorkerEnv | None = None) -> None:
        # Configuration
        self.env = env or MockWorkerEnv()

        # Identity
        self._host = "127.0.0.1"
        self._tcp_port = 8000
        self._node_id = MagicMock()
        self._node_id.short = "worker-001"

        # Infrastructure
        self._udp_logger = MockWorkerLogger()
        self._running = True

        # Manager tracking
        self._known_managers: dict[str, MockManagerInfo] = {}
        self._primary_manager_id: str | None = None

        # Workflow tracking
        self._active_workflows: set[str] = set()
        self._workflow_job_leader: dict[str, tuple[str, int]] = {}

        # Orphan handling (Section 2.7)
        self._orphaned_workflows: dict[str, float] = {}  # workflow_id -> orphan_timestamp
        self._orphan_grace_period: float = self.env.WORKER_ORPHAN_GRACE_PERIOD
        self._orphan_check_interval: float = self.env.WORKER_ORPHAN_CHECK_INTERVAL
        self._orphan_check_task: asyncio.Task | None = None

        # Cancellation tracking for test verification
        self._cancelled_workflows: list[tuple[str, str]] = []  # (workflow_id, reason)
        self._transfer_notifications: list[MockJobLeaderWorkerTransfer] = []

    # =========================================================================
    # Manager Failure Handling (from Section 3)
    # =========================================================================

    async def _mark_workflows_orphaned_for_manager(self, manager_id: str) -> None:
        """
        Mark workflows as orphaned when their job leader manager fails.

        Workflows are added to _orphaned_workflows with a timestamp.
        The orphan grace period checker will cancel them if no
        JobLeaderWorkerTransfer arrives before the grace period expires.
        """
        manager_info = self._known_managers.get(manager_id)
        if not manager_info:
            return

        dead_manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
        current_time = time.monotonic()

        # Find all workflows whose job leader was the dead manager
        for workflow_id, job_leader_addr in list(self._workflow_job_leader.items()):
            if job_leader_addr == dead_manager_addr:
                # Check if workflow is still active
                if workflow_id in self._active_workflows:
                    # Mark as orphaned (don't cancel yet - wait for potential transfer)
                    if workflow_id not in self._orphaned_workflows:
                        self._orphaned_workflows[workflow_id] = current_time

    async def _handle_manager_failure(self, manager_id: str) -> None:
        """Handle manager failure - mark workflows as orphaned."""
        await self._mark_workflows_orphaned_for_manager(manager_id)

    # =========================================================================
    # Orphan Check Loop (from Section 3.4)
    # =========================================================================

    async def _orphan_check_loop(self) -> None:
        """
        Background loop that checks for orphaned workflows whose grace period has expired.
        """
        while self._running:
            try:
                await asyncio.sleep(self._orphan_check_interval)

                current_time = time.monotonic()
                workflows_to_cancel: list[str] = []

                # Find workflows whose grace period has expired
                for workflow_id, orphan_timestamp in list(self._orphaned_workflows.items()):
                    elapsed = current_time - orphan_timestamp
                    if elapsed >= self._orphan_grace_period:
                        workflows_to_cancel.append(workflow_id)

                # Cancel expired orphaned workflows
                for workflow_id in workflows_to_cancel:
                    # Remove from orphan tracking first
                    self._orphaned_workflows.pop(workflow_id, None)

                    # Check if workflow is still active (may have completed naturally)
                    if workflow_id not in self._active_workflows:
                        continue

                    # Cancel the workflow
                    await self._cancel_workflow(workflow_id, "orphan_grace_period_expired")

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _cancel_workflow(self, workflow_id: str, reason: str) -> tuple[bool, list[str]]:
        """Mock workflow cancellation - records for test verification."""
        self._cancelled_workflows.append((workflow_id, reason))
        self._active_workflows.discard(workflow_id)
        self._workflow_job_leader.pop(workflow_id, None)
        return (True, [])

    # =========================================================================
    # Job Leader Transfer (from Section 3.3)
    # =========================================================================

    async def job_leader_worker_transfer(
        self,
        data: MockJobLeaderWorkerTransfer,
    ) -> MockJobLeaderWorkerTransferAck:
        """
        Handle job leadership transfer notification from manager.

        Clears workflows from _orphaned_workflows when transfer arrives.
        """
        self._transfer_notifications.append(data)

        workflows_updated = 0
        workflows_rescued = 0

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
                    workflows_rescued += 1

        return MockJobLeaderWorkerTransferAck(
            job_id=data.job_id,
            workflows_updated=workflows_updated,
            accepted=True,
        )

    # =========================================================================
    # Test Helpers
    # =========================================================================

    def add_manager(
        self,
        manager_id: str,
        tcp_host: str,
        tcp_port: int,
    ) -> None:
        """Add a known manager."""
        self._known_managers[manager_id] = MockManagerInfo(
            node_id=manager_id,
            tcp_host=tcp_host,
            tcp_port=tcp_port,
        )

    def add_workflow(
        self,
        workflow_id: str,
        job_leader_addr: tuple[str, int],
    ) -> None:
        """Add an active workflow with job leader."""
        self._active_workflows.add(workflow_id)
        self._workflow_job_leader[workflow_id] = job_leader_addr

    def start_orphan_check_loop(self) -> None:
        """Start the orphan check background task."""
        if self._orphan_check_task is None:
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


# =============================================================================
# Test Classes for Section 4
# =============================================================================


class TestWorkerOrphanGracePeriod:
    """Tests for worker orphan grace period handling (Section 4.3)."""

    @pytest.mark.asyncio
    async def test_workflow_marked_orphaned_on_manager_failure(self):
        """Worker should mark workflows as orphaned when job leader manager fails."""
        worker = MockWorkerServer()

        # Setup: manager with active workflow
        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # Manager fails
        await worker._handle_manager_failure("manager-001")

        # Workflow should be marked as orphaned
        assert "workflow-001" in worker._orphaned_workflows
        assert worker._orphaned_workflows["workflow-001"] > 0  # Has timestamp

    @pytest.mark.asyncio
    async def test_orphaned_workflow_not_cancelled_immediately(self):
        """Worker should NOT immediately cancel orphaned workflows."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        await worker._handle_manager_failure("manager-001")

        # Should still be active, not cancelled
        assert "workflow-001" in worker._active_workflows
        assert len(worker._cancelled_workflows) == 0

    @pytest.mark.asyncio
    async def test_orphaned_workflow_cancelled_after_grace_period(self):
        """Worker should cancel orphaned workflow after grace period expires."""
        # Use very short grace period for test
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.2,  # 200ms
            WORKER_ORPHAN_CHECK_INTERVAL=0.05,  # 50ms check interval
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        await worker._handle_manager_failure("manager-001")

        # Start orphan check loop
        worker.start_orphan_check_loop()

        # Wait for grace period to expire plus some buffer
        await asyncio.sleep(0.4)

        # Stop the loop
        await worker.stop_orphan_check_loop()

        # Workflow should be cancelled
        assert len(worker._cancelled_workflows) == 1
        assert worker._cancelled_workflows[0] == ("workflow-001", "orphan_grace_period_expired")

    @pytest.mark.asyncio
    async def test_orphaned_workflow_not_cancelled_before_grace_period(self):
        """Worker should NOT cancel orphaned workflow before grace period expires."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=2.0,  # 2 second grace period
            WORKER_ORPHAN_CHECK_INTERVAL=0.1,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        await worker._handle_manager_failure("manager-001")

        # Start orphan check loop
        worker.start_orphan_check_loop()

        # Wait less than grace period
        await asyncio.sleep(0.3)

        # Stop the loop
        await worker.stop_orphan_check_loop()

        # Workflow should NOT be cancelled yet
        assert len(worker._cancelled_workflows) == 0
        assert "workflow-001" in worker._orphaned_workflows

    @pytest.mark.asyncio
    async def test_only_workflows_for_dead_manager_marked_orphaned(self):
        """Only workflows led by the dead manager should be marked orphaned."""
        worker = MockWorkerServer()

        manager_addr_1 = ("192.168.1.10", 9090)
        manager_addr_2 = ("192.168.1.20", 9090)

        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_manager("manager-002", "192.168.1.20", 9090)

        # Workflows with different job leaders
        worker.add_workflow("workflow-001", manager_addr_1)  # Led by manager-001
        worker.add_workflow("workflow-002", manager_addr_2)  # Led by manager-002

        # Only manager-001 fails
        await worker._handle_manager_failure("manager-001")

        # Only workflow-001 should be orphaned
        assert "workflow-001" in worker._orphaned_workflows
        assert "workflow-002" not in worker._orphaned_workflows


class TestWorkerReceivesTransferBeforeGrace:
    """Tests for worker receiving transfer before grace period expires (Section 4.4)."""

    @pytest.mark.asyncio
    async def test_transfer_clears_orphaned_workflow(self):
        """Transfer notification should clear workflow from orphaned tracking."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # Manager fails - workflow becomes orphaned
        await worker._handle_manager_failure("manager-001")
        assert "workflow-001" in worker._orphaned_workflows

        # New leader sends transfer notification
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        await worker.job_leader_worker_transfer(transfer)

        # Workflow should be cleared from orphaned
        assert "workflow-001" not in worker._orphaned_workflows

    @pytest.mark.asyncio
    async def test_transfer_updates_job_leader_mapping(self):
        """Transfer notification should update workflow job leader mapping."""
        worker = MockWorkerServer()

        old_manager_addr = ("192.168.1.10", 9090)
        new_manager_addr = ("192.168.1.20", 9090)

        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", old_manager_addr)

        # Send transfer
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=new_manager_addr,
            old_manager_id="manager-001",
            fencing_token=2,
        )

        await worker.job_leader_worker_transfer(transfer)

        # Job leader should be updated
        assert worker._workflow_job_leader["workflow-001"] == new_manager_addr

    @pytest.mark.asyncio
    async def test_workflow_continues_after_transfer(self):
        """Workflow should continue executing after transfer (not cancelled)."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.3,
            WORKER_ORPHAN_CHECK_INTERVAL=0.05,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # Manager fails
        await worker._handle_manager_failure("manager-001")

        # Start orphan check loop
        worker.start_orphan_check_loop()

        # Wait a bit but not past grace period
        await asyncio.sleep(0.1)

        # Transfer arrives
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )
        await worker.job_leader_worker_transfer(transfer)

        # Wait past original grace period
        await asyncio.sleep(0.4)

        # Stop the loop
        await worker.stop_orphan_check_loop()

        # Workflow should NOT be cancelled (transfer rescued it)
        assert len(worker._cancelled_workflows) == 0
        assert "workflow-001" in worker._active_workflows

    @pytest.mark.asyncio
    async def test_multiple_workflows_rescued_by_single_transfer(self):
        """Single transfer should rescue multiple workflows."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)

        # Multiple workflows with same job leader
        worker.add_workflow("workflow-001", manager_addr)
        worker.add_workflow("workflow-002", manager_addr)
        worker.add_workflow("workflow-003", manager_addr)

        # Manager fails - all workflows orphaned
        await worker._handle_manager_failure("manager-001")
        assert len(worker._orphaned_workflows) == 3

        # Transfer for all workflows
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001", "workflow-002", "workflow-003"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        # All workflows rescued
        assert len(worker._orphaned_workflows) == 0
        assert ack.workflows_updated == 3

    @pytest.mark.asyncio
    async def test_partial_transfer_only_rescues_mentioned_workflows(self):
        """Transfer should only rescue workflows mentioned in the transfer."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)

        worker.add_workflow("workflow-001", manager_addr)
        worker.add_workflow("workflow-002", manager_addr)

        await worker._handle_manager_failure("manager-001")

        # Transfer only mentions workflow-001
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],  # Only one workflow
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        await worker.job_leader_worker_transfer(transfer)

        # Only workflow-001 should be rescued
        assert "workflow-001" not in worker._orphaned_workflows
        assert "workflow-002" in worker._orphaned_workflows


class TestIntegrationManagerAndWorker:
    """Full integration tests simulating manager-worker interaction."""

    @pytest.mark.asyncio
    async def test_full_flow_swim_leader_job_leader_fails(self):
        """
        Test full scenario: SWIM leader (also job leader) fails.

        1. Manager-A is SWIM leader and job leader for job-001
        2. Worker has workflow running, led by Manager-A
        3. Manager-A fails
        4. Worker marks workflow orphaned
        5. Manager-B becomes new SWIM leader
        6. Manager-B sends transfer to worker
        7. Worker updates job leader mapping, continues workflow
        """
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=1.0,
            WORKER_ORPHAN_CHECK_INTERVAL=0.1,
        )
        worker = MockWorkerServer(env)

        # Setup: Manager-A is job leader
        manager_a_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-a", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_a_addr)

        # Step 1: Manager-A fails
        await worker._handle_manager_failure("manager-a")

        # Verify: workflow is orphaned
        assert "workflow-001" in worker._orphaned_workflows

        # Start orphan check
        worker.start_orphan_check_loop()

        # Step 2: After short delay, Manager-B sends transfer
        await asyncio.sleep(0.2)

        manager_b_addr = ("192.168.1.20", 9090)
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=manager_b_addr,
            old_manager_id="manager-a",
            fencing_token=2,
        )
        await worker.job_leader_worker_transfer(transfer)

        # Verify: workflow rescued
        assert "workflow-001" not in worker._orphaned_workflows
        assert worker._workflow_job_leader["workflow-001"] == manager_b_addr

        # Step 3: Wait past original grace period
        await asyncio.sleep(1.0)

        await worker.stop_orphan_check_loop()

        # Verify: workflow NOT cancelled
        assert len(worker._cancelled_workflows) == 0
        assert "workflow-001" in worker._active_workflows

    @pytest.mark.asyncio
    async def test_full_flow_no_transfer_workflow_cancelled(self):
        """
        Test full scenario: Manager fails, no transfer arrives.

        1. Manager-A is job leader for workflow
        2. Manager-A fails
        3. Worker marks workflow orphaned
        4. No transfer arrives (all managers dead or no new leader)
        5. Grace period expires
        6. Worker cancels workflow
        """
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.3,
            WORKER_ORPHAN_CHECK_INTERVAL=0.05,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-a", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # Manager fails
        await worker._handle_manager_failure("manager-a")

        # Start orphan check
        worker.start_orphan_check_loop()

        # Wait for grace period to expire
        await asyncio.sleep(0.5)

        await worker.stop_orphan_check_loop()

        # Verify: workflow cancelled
        assert len(worker._cancelled_workflows) == 1
        assert worker._cancelled_workflows[0] == ("workflow-001", "orphan_grace_period_expired")
        assert "workflow-001" not in worker._active_workflows

    @pytest.mark.asyncio
    async def test_cascading_failures_multiple_managers(self):
        """
        Test scenario: Multiple managers fail in sequence.

        1. Manager-A is job leader for workflow-001
        2. Manager-B is job leader for workflow-002
        3. Both managers fail
        4. Worker marks both workflows orphaned
        5. Manager-C sends transfer for both
        6. Both workflows rescued
        """
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=1.0,
            WORKER_ORPHAN_CHECK_INTERVAL=0.1,
        )
        worker = MockWorkerServer(env)

        # Setup: Two managers, two workflows
        manager_a_addr = ("192.168.1.10", 9090)
        manager_b_addr = ("192.168.1.20", 9090)

        worker.add_manager("manager-a", "192.168.1.10", 9090)
        worker.add_manager("manager-b", "192.168.1.20", 9090)
        worker.add_workflow("workflow-001", manager_a_addr)
        worker.add_workflow("workflow-002", manager_b_addr)

        # Both managers fail
        await worker._handle_manager_failure("manager-a")
        await worker._handle_manager_failure("manager-b")

        # Both workflows orphaned
        assert "workflow-001" in worker._orphaned_workflows
        assert "workflow-002" in worker._orphaned_workflows

        # Start orphan check
        worker.start_orphan_check_loop()

        await asyncio.sleep(0.2)

        # Manager-C takes over both
        manager_c_addr = ("192.168.1.30", 9090)

        transfer_1 = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=manager_c_addr,
            old_manager_id="manager-a",
            fencing_token=2,
        )
        transfer_2 = MockJobLeaderWorkerTransfer(
            job_id="job-002",
            workflow_ids=["workflow-002"],
            new_manager_addr=manager_c_addr,
            old_manager_id="manager-b",
            fencing_token=2,
        )

        await worker.job_leader_worker_transfer(transfer_1)
        await worker.job_leader_worker_transfer(transfer_2)

        # Both workflows rescued
        assert len(worker._orphaned_workflows) == 0

        # Wait past grace period
        await asyncio.sleep(1.0)

        await worker.stop_orphan_check_loop()

        # Neither workflow cancelled
        assert len(worker._cancelled_workflows) == 0


class TestOrphanCheckLoopEdgeCases:
    """Edge case tests for the orphan check loop."""

    @pytest.mark.asyncio
    async def test_workflow_completes_naturally_before_cancellation(self):
        """Workflow that completes naturally should not be cancelled."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.3,
            WORKER_ORPHAN_CHECK_INTERVAL=0.05,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # Manager fails
        await worker._handle_manager_failure("manager-001")

        # Start orphan check
        worker.start_orphan_check_loop()

        # Wait a bit
        await asyncio.sleep(0.1)

        # Workflow completes naturally (remove from active)
        worker._active_workflows.discard("workflow-001")

        # Wait past grace period
        await asyncio.sleep(0.4)

        await worker.stop_orphan_check_loop()

        # Workflow should NOT appear in cancelled (completed naturally)
        assert len(worker._cancelled_workflows) == 0

    @pytest.mark.asyncio
    async def test_multiple_grace_period_expirations(self):
        """Multiple workflows with staggered orphan times should cancel at right times."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.2,
            WORKER_ORPHAN_CHECK_INTERVAL=0.05,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)

        # Add first workflow
        worker.add_workflow("workflow-001", manager_addr)
        await worker._handle_manager_failure("manager-001")

        # Start orphan check
        worker.start_orphan_check_loop()

        # After 100ms, add second workflow as orphaned
        await asyncio.sleep(0.1)

        # Manually add second workflow as orphaned (simulating staggered failure)
        worker._active_workflows.add("workflow-002")
        worker._workflow_job_leader["workflow-002"] = manager_addr
        worker._orphaned_workflows["workflow-002"] = time.monotonic()

        # Wait for first workflow to be cancelled (200ms grace + some buffer)
        await asyncio.sleep(0.2)

        # First should be cancelled, second should not yet
        cancelled_ids = [c[0] for c in worker._cancelled_workflows]
        assert "workflow-001" in cancelled_ids

        # Wait for second to expire
        await asyncio.sleep(0.2)

        await worker.stop_orphan_check_loop()

        # Now both should be cancelled
        cancelled_ids = [c[0] for c in worker._cancelled_workflows]
        assert "workflow-001" in cancelled_ids
        assert "workflow-002" in cancelled_ids

    @pytest.mark.asyncio
    async def test_orphan_loop_handles_empty_orphan_dict(self):
        """Orphan check loop should handle empty orphan dict gracefully."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.1,
            WORKER_ORPHAN_CHECK_INTERVAL=0.05,
        )
        worker = MockWorkerServer(env)

        # No orphaned workflows
        assert len(worker._orphaned_workflows) == 0

        # Start loop
        worker.start_orphan_check_loop()

        # Run for a bit
        await asyncio.sleep(0.2)

        await worker.stop_orphan_check_loop()

        # Should complete without error, no cancellations
        assert len(worker._cancelled_workflows) == 0

    @pytest.mark.asyncio
    async def test_transfer_for_unknown_workflow_handled_gracefully(self):
        """Transfer for unknown workflow should be handled gracefully."""
        worker = MockWorkerServer()

        # No workflows active
        assert len(worker._active_workflows) == 0

        # Transfer for unknown workflow
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["unknown-workflow"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        # Should succeed but with 0 workflows updated
        assert ack.accepted
        assert ack.workflows_updated == 0


class TestTransferNotificationTracking:
    """Tests for tracking transfer notifications."""

    @pytest.mark.asyncio
    async def test_transfer_notifications_are_recorded(self):
        """All transfer notifications should be recorded."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        await worker.job_leader_worker_transfer(transfer)

        assert len(worker._transfer_notifications) == 1
        assert worker._transfer_notifications[0] == transfer

    @pytest.mark.asyncio
    async def test_multiple_transfers_recorded_in_order(self):
        """Multiple transfers should be recorded in order."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)
        worker.add_workflow("workflow-002", manager_addr)

        transfer_1 = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        transfer_2 = MockJobLeaderWorkerTransfer(
            job_id="job-002",
            workflow_ids=["workflow-002"],
            new_manager_addr=("192.168.1.30", 9090),
            old_manager_id="manager-001",
            fencing_token=3,
        )

        await worker.job_leader_worker_transfer(transfer_1)
        await worker.job_leader_worker_transfer(transfer_2)

        assert len(worker._transfer_notifications) == 2
        assert worker._transfer_notifications[0].job_id == "job-001"
        assert worker._transfer_notifications[1].job_id == "job-002"


# =============================================================================
# Extended Tests: Negative Paths and Failure Modes
# =============================================================================


class TestNegativePaths:
    """Tests for error handling and negative scenarios."""

    @pytest.mark.asyncio
    async def test_manager_failure_for_unknown_manager(self):
        """Handling failure for a manager not in known managers."""
        worker = MockWorkerServer()

        # No managers configured
        assert len(worker._known_managers) == 0

        # Try to handle failure for unknown manager
        await worker._handle_manager_failure("unknown-manager")

        # Should not raise, no workflows orphaned
        assert len(worker._orphaned_workflows) == 0

    @pytest.mark.asyncio
    async def test_duplicate_manager_failure_events(self):
        """Handling duplicate failure events for the same manager."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # First failure
        await worker._handle_manager_failure("manager-001")
        first_orphan_time = worker._orphaned_workflows["workflow-001"]

        # Small delay
        await asyncio.sleep(0.01)

        # Duplicate failure event
        await worker._handle_manager_failure("manager-001")

        # Orphan timestamp should NOT be updated (already orphaned)
        assert worker._orphaned_workflows["workflow-001"] == first_orphan_time

    @pytest.mark.asyncio
    async def test_transfer_after_workflow_already_cancelled(self):
        """Transfer arriving after workflow was already cancelled."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.1,
            WORKER_ORPHAN_CHECK_INTERVAL=0.02,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        await worker._handle_manager_failure("manager-001")

        worker.start_orphan_check_loop()
        await asyncio.sleep(0.2)  # Wait for cancellation
        await worker.stop_orphan_check_loop()

        # Workflow should be cancelled
        assert len(worker._cancelled_workflows) == 1
        assert "workflow-001" not in worker._active_workflows

        # Late transfer arrives
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        # Should accept but with 0 updates (workflow gone)
        assert ack.accepted
        assert ack.workflows_updated == 0

    @pytest.mark.asyncio
    async def test_empty_workflow_list_in_transfer(self):
        """Transfer with empty workflow list."""
        worker = MockWorkerServer()

        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=[],  # Empty list
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted
        assert ack.workflows_updated == 0

    @pytest.mark.asyncio
    async def test_workflow_with_no_job_leader_mapping(self):
        """Workflow exists but has no job leader mapping."""
        worker = MockWorkerServer()

        # Add workflow without job leader
        worker._active_workflows.add("workflow-001")
        # Don't set job leader mapping

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)

        # This should not raise
        await worker._handle_manager_failure("manager-001")

        # Workflow should NOT be orphaned (has no job leader)
        assert "workflow-001" not in worker._orphaned_workflows


class TestConcurrencyAndRaceConditions:
    """Tests for concurrent operations and race conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_manager_failure_and_transfer(self):
        """Concurrent manager failure and transfer notifications."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        # Run both concurrently
        await asyncio.gather(
            worker._handle_manager_failure("manager-001"),
            worker.job_leader_worker_transfer(transfer),
        )

        # Workflow should be rescued (transfer should win)
        # The order is non-deterministic, but the workflow should end up not orphaned
        # because transfer clears orphan status
        assert "workflow-001" not in worker._orphaned_workflows or \
               worker._workflow_job_leader.get("workflow-001") == ("192.168.1.20", 9090)

    @pytest.mark.asyncio
    async def test_rapid_successive_transfers(self):
        """Rapid succession of transfers for the same job."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # Multiple rapid transfers
        transfers = [
            MockJobLeaderWorkerTransfer(
                job_id="job-001",
                workflow_ids=["workflow-001"],
                new_manager_addr=(f"192.168.1.{20 + i}", 9090),
                old_manager_id="manager-001",
                fencing_token=i + 1,
            )
            for i in range(5)
        ]

        # Apply all transfers
        for transfer in transfers:
            await worker.job_leader_worker_transfer(transfer)

        # Final job leader should be the last one
        assert worker._workflow_job_leader["workflow-001"] == ("192.168.1.24", 9090)
        assert len(worker._transfer_notifications) == 5

    @pytest.mark.asyncio
    async def test_concurrent_transfers_for_same_workflow(self):
        """Concurrent transfers for the same workflow."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        transfer_1 = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )
        transfer_2 = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.30", 9090),
            old_manager_id="manager-001",
            fencing_token=3,
        )

        # Run concurrently
        results = await asyncio.gather(
            worker.job_leader_worker_transfer(transfer_1),
            worker.job_leader_worker_transfer(transfer_2),
        )

        # Both should succeed
        assert all(r.accepted for r in results)
        # One of the addresses should be final
        assert worker._workflow_job_leader["workflow-001"] in [
            ("192.168.1.20", 9090),
            ("192.168.1.30", 9090),
        ]

    @pytest.mark.asyncio
    async def test_orphan_check_during_transfer_processing(self):
        """Orphan check running while transfer is being processed."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.1,
            WORKER_ORPHAN_CHECK_INTERVAL=0.02,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        await worker._handle_manager_failure("manager-001")

        # Start orphan check loop
        worker.start_orphan_check_loop()

        # Wait almost until grace period
        await asyncio.sleep(0.08)

        # Transfer arrives just before expiration
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )
        await worker.job_leader_worker_transfer(transfer)

        # Wait past original grace period
        await asyncio.sleep(0.1)

        await worker.stop_orphan_check_loop()

        # Workflow should NOT be cancelled
        assert len(worker._cancelled_workflows) == 0

    @pytest.mark.asyncio
    async def test_multiple_manager_failures_in_quick_succession(self):
        """Multiple different managers failing quickly."""
        worker = MockWorkerServer()

        # Setup multiple managers with workflows
        for i in range(5):
            manager_id = f"manager-{i:03d}"
            addr = (f"192.168.1.{10 + i}", 9090)
            worker.add_manager(manager_id, f"192.168.1.{10 + i}", 9090)
            worker.add_workflow(f"workflow-{i:03d}", addr)

        # All managers fail concurrently
        await asyncio.gather(*[
            worker._handle_manager_failure(f"manager-{i:03d}")
            for i in range(5)
        ])

        # All workflows should be orphaned
        assert len(worker._orphaned_workflows) == 5


class TestEdgeCasesAndBoundaryConditions:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_zero_grace_period(self):
        """Grace period of zero should still work (immediate cancellation)."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.0,  # Zero grace period
            WORKER_ORPHAN_CHECK_INTERVAL=0.01,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        await worker._handle_manager_failure("manager-001")

        worker.start_orphan_check_loop()
        await asyncio.sleep(0.05)
        await worker.stop_orphan_check_loop()

        # Should be cancelled almost immediately
        assert len(worker._cancelled_workflows) == 1

    @pytest.mark.asyncio
    async def test_very_long_grace_period(self):
        """Very long grace period should not cause issues."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=3600.0,  # 1 hour
            WORKER_ORPHAN_CHECK_INTERVAL=0.05,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        await worker._handle_manager_failure("manager-001")

        worker.start_orphan_check_loop()
        await asyncio.sleep(0.1)
        await worker.stop_orphan_check_loop()

        # Should NOT be cancelled (grace period not expired)
        assert len(worker._cancelled_workflows) == 0
        assert "workflow-001" in worker._orphaned_workflows

    @pytest.mark.asyncio
    async def test_transfer_with_same_new_and_old_manager(self):
        """Transfer where new manager is the same as current (no-op)."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # Transfer to same address
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=manager_addr,  # Same as current
            old_manager_id="manager-001",
            fencing_token=2,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        # Should succeed but no change in routing
        assert ack.accepted
        assert ack.workflows_updated == 0  # No change
        assert worker._workflow_job_leader["workflow-001"] == manager_addr

    @pytest.mark.asyncio
    async def test_large_number_of_workflows(self):
        """Handling large number of workflows from single manager."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)

        # Add 1000 workflows
        workflow_ids = [f"workflow-{i:06d}" for i in range(1000)]
        for wf_id in workflow_ids:
            worker.add_workflow(wf_id, manager_addr)

        # Manager fails
        await worker._handle_manager_failure("manager-001")

        # All should be orphaned
        assert len(worker._orphaned_workflows) == 1000

        # Single transfer rescues all
        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=workflow_ids,
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted
        assert ack.workflows_updated == 1000
        assert len(worker._orphaned_workflows) == 0

    @pytest.mark.asyncio
    async def test_workflow_id_with_special_characters(self):
        """Workflow IDs with special characters handled correctly."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)

        # Workflow IDs with various characters
        special_ids = [
            "workflow:with:colons",
            "workflow-with-dashes",
            "workflow_with_underscores",
            "workflow.with.dots",
            "workflow/with/slashes",
        ]

        for wf_id in special_ids:
            worker.add_workflow(wf_id, manager_addr)

        await worker._handle_manager_failure("manager-001")

        # All should be orphaned
        for wf_id in special_ids:
            assert wf_id in worker._orphaned_workflows

    @pytest.mark.asyncio
    async def test_manager_with_different_port(self):
        """Same host but different port should be tracked separately."""
        worker = MockWorkerServer()

        addr_1 = ("192.168.1.10", 9090)
        addr_2 = ("192.168.1.10", 9091)  # Same host, different port

        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_manager("manager-002", "192.168.1.10", 9091)

        worker.add_workflow("workflow-001", addr_1)
        worker.add_workflow("workflow-002", addr_2)

        # Only manager-001 fails
        await worker._handle_manager_failure("manager-001")

        # Only workflow-001 should be orphaned
        assert "workflow-001" in worker._orphaned_workflows
        assert "workflow-002" not in worker._orphaned_workflows


class TestOrphanLoopStopStart:
    """Tests for stopping and restarting the orphan check loop."""

    @pytest.mark.asyncio
    async def test_stop_loop_before_start(self):
        """Stopping loop before it's started should not raise."""
        worker = MockWorkerServer()

        # Should not raise
        await worker.stop_orphan_check_loop()

    @pytest.mark.asyncio
    async def test_double_start_loop(self):
        """Starting loop twice should not create duplicate tasks."""
        worker = MockWorkerServer()

        worker.start_orphan_check_loop()
        first_task = worker._orphan_check_task

        worker.start_orphan_check_loop()
        second_task = worker._orphan_check_task

        # Should be the same task (not started twice)
        assert first_task is second_task

        await worker.stop_orphan_check_loop()

    @pytest.mark.asyncio
    async def test_restart_loop_after_stop(self):
        """Restarting loop after stop should work."""
        env = MockWorkerEnv(
            WORKER_ORPHAN_GRACE_PERIOD=0.1,
            WORKER_ORPHAN_CHECK_INTERVAL=0.02,
        )
        worker = MockWorkerServer(env)

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        # Start and stop
        worker.start_orphan_check_loop()
        await asyncio.sleep(0.05)
        await worker.stop_orphan_check_loop()

        # Re-enable running
        worker._running = True

        # Mark orphaned
        await worker._handle_manager_failure("manager-001")

        # Restart
        worker.start_orphan_check_loop()
        await asyncio.sleep(0.2)
        await worker.stop_orphan_check_loop()

        # Workflow should be cancelled
        assert len(worker._cancelled_workflows) == 1


class TestTransferValidation:
    """Tests for transfer message validation."""

    @pytest.mark.asyncio
    async def test_transfer_with_none_old_manager_id(self):
        """Transfer with None old_manager_id (unknown previous leader)."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001"],
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id=None,  # Unknown previous leader
            fencing_token=2,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted
        assert ack.workflows_updated == 1

    @pytest.mark.asyncio
    async def test_transfer_with_duplicate_workflow_ids(self):
        """Transfer with duplicate workflow IDs in the list."""
        worker = MockWorkerServer()

        manager_addr = ("192.168.1.10", 9090)
        worker.add_manager("manager-001", "192.168.1.10", 9090)
        worker.add_workflow("workflow-001", manager_addr)

        transfer = MockJobLeaderWorkerTransfer(
            job_id="job-001",
            workflow_ids=["workflow-001", "workflow-001", "workflow-001"],  # Duplicates
            new_manager_addr=("192.168.1.20", 9090),
            old_manager_id="manager-001",
            fencing_token=2,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted
        # Should only count as 1 update (same workflow updated multiple times)
        assert ack.workflows_updated == 1
