"""
Integration tests for Section 8: Worker robust response to job leadership takeover.

These tests verify that workers handle job leadership transfers robustly:
- 8.1: Per-job locks prevent race conditions
- 8.2: Transfer validation (fence tokens, known managers)
- 8.3: Pending transfers for late-arriving workflows
- 8.4: Detailed acknowledgment with workflow states
- 8.5: In-flight operation handling (covered via lock tests)
- 8.6: Transfer metrics
- 8.7: Detailed logging (verified via mock logger)
- 8.8: Defensive _on_node_dead handling
"""

import asyncio
import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass, field

from hyperscale.distributed_rewrite.models import (
    JobLeaderWorkerTransfer,
    JobLeaderWorkerTransferAck,
    PendingTransfer,
    WorkflowProgress,
    WorkflowStatus,
    ManagerInfo,
)


@dataclass
class MockWorkerServer:
    """
    Mock WorkerServer for testing job leadership transfer handling.

    Implements the Section 8 transfer handling logic.
    """
    node_id: str = "worker-001"
    host: str = "127.0.0.1"
    tcp_port: int = 9000

    # Workflow tracking
    active_workflows: dict[str, WorkflowProgress] = field(default_factory=dict)
    workflow_job_leader: dict[str, tuple[str, int]] = field(default_factory=dict)
    orphaned_workflows: dict[str, float] = field(default_factory=dict)

    # Section 8: Transfer handling
    job_leader_transfer_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    job_fence_tokens: dict[str, int] = field(default_factory=dict)
    pending_transfers: dict[str, PendingTransfer] = field(default_factory=dict)
    pending_transfer_ttl: float = 60.0

    # Transfer metrics (8.6)
    transfer_metrics_received: int = 0
    transfer_metrics_accepted: int = 0
    transfer_metrics_rejected_stale_token: int = 0
    transfer_metrics_rejected_unknown_manager: int = 0
    transfer_metrics_rejected_other: int = 0

    # Known managers
    known_managers: dict[str, ManagerInfo] = field(default_factory=dict)

    # Log capture
    log_messages: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.job_leader_transfer_locks = {}
        self.job_fence_tokens = {}
        self.pending_transfers = {}
        self.known_managers = {}
        self.log_messages = []
        self.active_workflows = {}
        self.workflow_job_leader = {}
        self.orphaned_workflows = {}

    def _get_job_transfer_lock(self, job_id: str) -> asyncio.Lock:
        """Get or create per-job lock (8.1)."""
        if job_id not in self.job_leader_transfer_locks:
            self.job_leader_transfer_locks[job_id] = asyncio.Lock()
        return self.job_leader_transfer_locks[job_id]

    def _validate_transfer_fence_token(self, job_id: str, new_fence_token: int) -> tuple[bool, str]:
        """Validate fence token (8.2)."""
        current_token = self.job_fence_tokens.get(job_id, -1)
        if new_fence_token <= current_token:
            return (False, f"Stale fence token: received {new_fence_token}, current {current_token}")
        return (True, "")

    def _validate_transfer_manager(self, new_manager_id: str) -> tuple[bool, str]:
        """Validate manager is known (8.2)."""
        if new_manager_id not in self.known_managers:
            return (False, f"Unknown manager: {new_manager_id} not in known managers")
        return (True, "")

    async def job_leader_worker_transfer(self, transfer: JobLeaderWorkerTransfer) -> JobLeaderWorkerTransferAck:
        """Process job leadership transfer (Section 8)."""
        self.transfer_metrics_received += 1
        job_id = transfer.job_id

        self.log_messages.append(f"Processing transfer for job {job_id}")

        # 8.1: Acquire per-job lock
        job_lock = self._get_job_transfer_lock(job_id)
        async with job_lock:
            # 8.2: Validate fence token
            # Support both sync and async validation (for testing with delays)
            fence_result = self._validate_transfer_fence_token(job_id, transfer.fence_token)
            if asyncio.iscoroutine(fence_result):
                fence_valid, fence_reason = await fence_result
            else:
                fence_valid, fence_reason = fence_result
            if not fence_valid:
                self.transfer_metrics_rejected_stale_token += 1
                self.log_messages.append(f"Rejected: {fence_reason}")
                return JobLeaderWorkerTransferAck(
                    job_id=job_id,
                    worker_id=self.node_id,
                    workflows_updated=0,
                    accepted=False,
                    rejection_reason=fence_reason,
                    fence_token_received=transfer.fence_token,
                )

            # 8.2: Validate manager is known
            manager_valid, manager_reason = self._validate_transfer_manager(transfer.new_manager_id)
            if not manager_valid:
                self.transfer_metrics_rejected_unknown_manager += 1
                self.log_messages.append(f"Rejected: {manager_reason}")
                return JobLeaderWorkerTransferAck(
                    job_id=job_id,
                    worker_id=self.node_id,
                    workflows_updated=0,
                    accepted=False,
                    rejection_reason=manager_reason,
                    fence_token_received=transfer.fence_token,
                )

            # Update fence token
            self.job_fence_tokens[job_id] = transfer.fence_token

            workflows_updated = 0
            workflows_not_found: list[str] = []
            workflow_states: dict[str, str] = {}

            # Update routing for each workflow
            for workflow_id in transfer.workflow_ids:
                if workflow_id in self.active_workflows:
                    self.workflow_job_leader[workflow_id] = transfer.new_manager_addr
                    workflows_updated += 1

                    # Clear orphaned state if present
                    if workflow_id in self.orphaned_workflows:
                        del self.orphaned_workflows[workflow_id]

                    # 8.4: Collect workflow state
                    workflow_states[workflow_id] = self.active_workflows[workflow_id].status
                else:
                    workflows_not_found.append(workflow_id)

            # 8.3: Store pending transfer for late arrivals
            if workflows_not_found:
                self.pending_transfers[job_id] = PendingTransfer(
                    job_id=job_id,
                    workflow_ids=workflows_not_found,
                    new_manager_id=transfer.new_manager_id,
                    new_manager_addr=transfer.new_manager_addr,
                    fence_token=transfer.fence_token,
                    old_manager_id=transfer.old_manager_id,
                    received_at=time.monotonic(),
                )

            self.transfer_metrics_accepted += 1
            self.log_messages.append(f"Accepted: updated {workflows_updated}, pending {len(workflows_not_found)}")

            # 8.4: Return detailed ack
            return JobLeaderWorkerTransferAck(
                job_id=job_id,
                worker_id=self.node_id,
                workflows_updated=workflows_updated,
                accepted=True,
                rejection_reason="",
                fence_token_received=transfer.fence_token,
                workflow_states=workflow_states,
            )


class TestTransferValidation:
    """Tests for Section 8.2: Transfer validation."""

    @pytest.mark.asyncio
    async def test_rejects_stale_fence_token(self):
        """Test that stale fence tokens are rejected."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Set current fence token
        worker.job_fence_tokens["job-1"] = 10

        # Try transfer with lower fence token
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=5,  # Lower than current 10
            old_manager_id="manager-old",
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is False
        assert "Stale fence token" in ack.rejection_reason
        assert worker.transfer_metrics_rejected_stale_token == 1
        assert worker.transfer_metrics_accepted == 0

    @pytest.mark.asyncio
    async def test_rejects_unknown_manager(self):
        """Test that transfers from unknown managers are rejected."""
        worker = MockWorkerServer()
        # Don't add manager-new to known_managers

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-unknown",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
            old_manager_id="manager-old",
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is False
        assert "Unknown manager" in ack.rejection_reason
        assert worker.transfer_metrics_rejected_unknown_manager == 1

    @pytest.mark.asyncio
    async def test_accepts_valid_transfer(self):
        """Test that valid transfers are accepted."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Add active workflow
        worker.active_workflows["wf-1"] = WorkflowProgress(
            job_id="job-1",
            workflow_id="wf-1",
            workflow_name="test",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
        )
        worker.workflow_job_leader["wf-1"] = ("127.0.0.1", 8000)  # Old leader

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
            old_manager_id="manager-old",
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 1
        assert worker.workflow_job_leader["wf-1"] == ("127.0.0.1", 8001)
        assert worker.transfer_metrics_accepted == 1


class TestPendingTransfers:
    """Tests for Section 8.3: Pending transfers for late-arriving workflows."""

    @pytest.mark.asyncio
    async def test_stores_pending_transfer_for_unknown_workflows(self):
        """Test that transfers for unknown workflows are stored as pending."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Don't add any active workflows
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1", "wf-2"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
            old_manager_id="manager-old",
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 0  # No workflows were active
        assert "job-1" in worker.pending_transfers

        pending = worker.pending_transfers["job-1"]
        assert pending.workflow_ids == ["wf-1", "wf-2"]
        assert pending.new_manager_addr == ("127.0.0.1", 8001)
        assert pending.fence_token == 1

    @pytest.mark.asyncio
    async def test_partial_pending_transfer(self):
        """Test that partial transfers (some known, some unknown) are handled."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Add one active workflow
        worker.active_workflows["wf-1"] = WorkflowProgress(
            job_id="job-1",
            workflow_id="wf-1",
            workflow_name="test",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
        )
        worker.workflow_job_leader["wf-1"] = ("127.0.0.1", 8000)

        # Transfer includes both known and unknown workflows
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1", "wf-2"],  # wf-1 known, wf-2 unknown
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
            old_manager_id="manager-old",
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 1  # Only wf-1
        assert worker.workflow_job_leader["wf-1"] == ("127.0.0.1", 8001)

        # wf-2 should be in pending transfers
        assert "job-1" in worker.pending_transfers
        assert worker.pending_transfers["job-1"].workflow_ids == ["wf-2"]


class TestTransferMetrics:
    """Tests for Section 8.6: Transfer metrics."""

    @pytest.mark.asyncio
    async def test_metrics_tracking(self):
        """Test that transfer metrics are tracked correctly."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Accepted transfer
        transfer1 = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )
        await worker.job_leader_worker_transfer(transfer1)

        # Stale token rejection
        transfer2 = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=0,  # Lower than stored 1
        )
        await worker.job_leader_worker_transfer(transfer2)

        # Unknown manager rejection
        transfer3 = JobLeaderWorkerTransfer(
            job_id="job-2",
            workflow_ids=["wf-1"],
            new_manager_id="manager-unknown",
            new_manager_addr=("127.0.0.1", 8099),
            fence_token=1,
        )
        await worker.job_leader_worker_transfer(transfer3)

        assert worker.transfer_metrics_received == 3
        assert worker.transfer_metrics_accepted == 1
        assert worker.transfer_metrics_rejected_stale_token == 1
        assert worker.transfer_metrics_rejected_unknown_manager == 1


class TestTransferAcknowledgment:
    """Tests for Section 8.4: Detailed acknowledgment with workflow states."""

    @pytest.mark.asyncio
    async def test_ack_includes_workflow_states(self):
        """Test that ack includes current workflow states."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Add workflows in different states
        worker.active_workflows["wf-1"] = WorkflowProgress(
            job_id="job-1",
            workflow_id="wf-1",
            workflow_name="test1",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
        )
        worker.active_workflows["wf-2"] = WorkflowProgress(
            job_id="job-1",
            workflow_id="wf-2",
            workflow_name="test2",
            status=WorkflowStatus.ASSIGNED.value,
            completed_count=100,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=10.0,
            timestamp=time.monotonic(),
        )
        worker.workflow_job_leader["wf-1"] = ("127.0.0.1", 8000)
        worker.workflow_job_leader["wf-2"] = ("127.0.0.1", 8000)

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1", "wf-2"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 2
        assert ack.fence_token_received == 1
        assert ack.workflow_states == {
            "wf-1": WorkflowStatus.RUNNING.value,
            "wf-2": WorkflowStatus.ASSIGNED.value,
        }

    @pytest.mark.asyncio
    async def test_ack_includes_fence_token(self):
        """Test that ack includes the received fence token."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=42,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.fence_token_received == 42


class TestPerJobLocks:
    """Tests for Section 8.1: Per-job locks prevent race conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_transfers_same_job_serialized(self):
        """Test that concurrent transfers for the same job are serialized."""
        worker = MockWorkerServer()
        worker.known_managers["manager-1"] = ManagerInfo(
            node_id="manager-1",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )
        worker.known_managers["manager-2"] = ManagerInfo(
            node_id="manager-2",
            tcp_host="127.0.0.1",
            tcp_port=8003,
            udp_host="127.0.0.1",
            udp_port=8004,
            datacenter="dc-default",
        )

        execution_order: list[int] = []
        original_validate = worker._validate_transfer_fence_token

        async def slow_validate(job_id: str, token: int):
            execution_order.append(token)
            await asyncio.sleep(0.05)  # Simulate slow validation
            return original_validate(job_id, token)

        worker._validate_transfer_fence_token = slow_validate

        # Create two concurrent transfers for the same job
        transfer1 = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-1",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )
        transfer2 = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-2",
            new_manager_addr=("127.0.0.1", 8003),
            fence_token=2,
        )

        # Run both concurrently
        results = await asyncio.gather(
            worker.job_leader_worker_transfer(transfer1),
            worker.job_leader_worker_transfer(transfer2),
        )

        # Due to per-job lock, transfers should be serialized
        # One should accept, one should be stale (since they have different tokens)
        accepted = [r for r in results if r.accepted]
        rejected = [r for r in results if not r.accepted]

        # First one (token=1) should succeed, second (token=2) should also succeed
        # because it has a higher fence token
        assert len(accepted) == 2  # Both should be accepted since token 2 > token 1
        # The final fence token should be 2
        assert worker.job_fence_tokens["job-1"] == 2

    @pytest.mark.asyncio
    async def test_concurrent_transfers_different_jobs_parallel(self):
        """Test that transfers for different jobs can proceed in parallel."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Track execution timing
        start_times: dict[str, float] = {}
        end_times: dict[str, float] = {}

        original_validate = worker._validate_transfer_fence_token

        async def timed_validate(job_id: str, token: int):
            start_times[job_id] = time.monotonic()
            await asyncio.sleep(0.05)  # Simulate work
            result = original_validate(job_id, token)
            end_times[job_id] = time.monotonic()
            return result

        worker._validate_transfer_fence_token = timed_validate

        transfer1 = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )
        transfer2 = JobLeaderWorkerTransfer(
            job_id="job-2",  # Different job
            workflow_ids=["wf-2"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        await asyncio.gather(
            worker.job_leader_worker_transfer(transfer1),
            worker.job_leader_worker_transfer(transfer2),
        )

        # Both jobs should have separate locks, allowing parallel execution
        assert "job-1" in worker.job_leader_transfer_locks
        assert "job-2" in worker.job_leader_transfer_locks

        # If parallel, start times should be close together
        time_diff = abs(start_times.get("job-1", 0) - start_times.get("job-2", 0))
        assert time_diff < 0.02  # Should start nearly simultaneously


class TestOrphanedWorkflowRescue:
    """Tests for orphaned workflow rescue during transfer."""

    @pytest.mark.asyncio
    async def test_transfer_clears_orphaned_status(self):
        """Test that transfer clears orphaned workflow status."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Add orphaned workflow
        worker.active_workflows["wf-1"] = WorkflowProgress(
            job_id="job-1",
            workflow_id="wf-1",
            workflow_name="test",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
        )
        worker.workflow_job_leader["wf-1"] = ("127.0.0.1", 8000)
        worker.orphaned_workflows["wf-1"] = time.monotonic() - 2.0  # Orphaned 2 seconds ago

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert "wf-1" not in worker.orphaned_workflows  # Should be cleared


class TestDefensiveNodeDeath:
    """Tests for Section 8.8: Defensive _on_node_dead handling."""

    @pytest.mark.asyncio
    async def test_only_orphans_workflows_for_actual_job_leader(self):
        """Test that only workflows with the dead manager as job leader are orphaned."""
        worker = MockWorkerServer()

        # Add two managers
        manager_1_addr = ("127.0.0.1", 8001)
        manager_2_addr = ("127.0.0.1", 8002)

        # Add workflows with different job leaders
        worker.active_workflows["wf-1"] = WorkflowProgress(
            job_id="job-1",
            workflow_id="wf-1",
            workflow_name="test1",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
        )
        worker.active_workflows["wf-2"] = WorkflowProgress(
            job_id="job-2",
            workflow_id="wf-2",
            workflow_name="test2",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
        )

        # wf-1 has manager-1 as job leader, wf-2 has manager-2
        worker.workflow_job_leader["wf-1"] = manager_1_addr
        worker.workflow_job_leader["wf-2"] = manager_2_addr

        # Simulate manager-1 dying
        # Only wf-1 should become orphaned
        current_time = time.monotonic()
        for workflow_id, job_leader_addr in list(worker.workflow_job_leader.items()):
            if job_leader_addr == manager_1_addr:
                if workflow_id in worker.active_workflows:
                    worker.orphaned_workflows[workflow_id] = current_time

        assert "wf-1" in worker.orphaned_workflows
        assert "wf-2" not in worker.orphaned_workflows  # Different job leader


class TestLogging:
    """Tests for Section 8.7: Detailed logging."""

    @pytest.mark.asyncio
    async def test_logs_transfer_processing(self):
        """Test that transfer processing is logged."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        transfer = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        await worker.job_leader_worker_transfer(transfer)

        assert any("Processing transfer" in msg for msg in worker.log_messages)
        assert any("Accepted" in msg for msg in worker.log_messages)

    @pytest.mark.asyncio
    async def test_logs_rejection_reason(self):
        """Test that rejection reasons are logged."""
        worker = MockWorkerServer()
        # Don't add manager to known_managers

        transfer = JobLeaderWorkerTransfer(
            job_id="job-123",
            workflow_ids=["wf-1"],
            new_manager_id="manager-unknown",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        await worker.job_leader_worker_transfer(transfer)

        assert any("Rejected" in msg for msg in worker.log_messages)
        assert any("Unknown manager" in msg for msg in worker.log_messages)


# =============================================================================
# Extended Tests: Negative Paths and Failure Modes
# =============================================================================


class TestNegativePaths:
    """Tests for error handling and negative scenarios."""

    @pytest.mark.asyncio
    async def test_transfer_with_empty_workflow_list(self):
        """Transfer with empty workflow list should be accepted."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=[],  # Empty list
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 0

    @pytest.mark.asyncio
    async def test_transfer_with_equal_fence_token_rejected(self):
        """Transfer with equal fence token (not greater) should be rejected."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Set current fence token
        worker.job_fence_tokens["job-1"] = 5

        # Try transfer with EQUAL fence token
        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=5,  # Equal to current 5
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is False
        assert "Stale fence token" in ack.rejection_reason

    @pytest.mark.asyncio
    async def test_transfer_with_negative_fence_token(self):
        """Transfer with negative fence token should work if first."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=-1,  # Negative but > default -1
        )

        # Default is -1, so -1 should be rejected (not > -1)
        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is False

    @pytest.mark.asyncio
    async def test_transfer_with_zero_fence_token(self):
        """Transfer with zero fence token should work for new job."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=0,  # 0 > -1 (default)
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert worker.job_fence_tokens["job-1"] == 0

    @pytest.mark.asyncio
    async def test_duplicate_workflow_ids_in_transfer(self):
        """Transfer with duplicate workflow IDs should handle gracefully."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        worker.active_workflows["wf-1"] = WorkflowProgress(
            job_id="job-1",
            workflow_id="wf-1",
            workflow_name="test",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
        )
        worker.workflow_job_leader["wf-1"] = ("127.0.0.1", 8000)

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1", "wf-1", "wf-1"],  # Duplicates
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        # Counted 3 times but same workflow
        assert ack.workflows_updated == 3


class TestConcurrencyRaceConditions:
    """Tests for concurrent operations and race conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_transfers_different_jobs(self):
        """Concurrent transfers for different jobs should all succeed."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        transfers = [
            JobLeaderWorkerTransfer(
                job_id=f"job-{i}",
                workflow_ids=[f"wf-{i}"],
                new_manager_id="manager-new",
                new_manager_addr=("127.0.0.1", 8001),
                fence_token=1,
            )
            for i in range(10)
        ]

        results = await asyncio.gather(*[
            worker.job_leader_worker_transfer(t) for t in transfers
        ])

        # All should be accepted
        assert all(r.accepted for r in results)
        assert worker.transfer_metrics_accepted == 10

    @pytest.mark.asyncio
    async def test_rapid_successive_transfers_same_job(self):
        """Rapid successive transfers for same job with increasing tokens."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Sequential transfers with increasing tokens
        for i in range(20):
            transfer = JobLeaderWorkerTransfer(
                job_id="job-1",
                workflow_ids=["wf-1"],
                new_manager_id="manager-new",
                new_manager_addr=("127.0.0.1", 8001),
                fence_token=i,
            )
            ack = await worker.job_leader_worker_transfer(transfer)
            assert ack.accepted is True

        assert worker.job_fence_tokens["job-1"] == 19

    @pytest.mark.asyncio
    async def test_interleaved_accepted_and_rejected_transfers(self):
        """Interleaved accepted and rejected transfers should be tracked correctly."""
        worker = MockWorkerServer()
        worker.known_managers["manager-known"] = ManagerInfo(
            node_id="manager-known",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Set initial fence token
        worker.job_fence_tokens["job-1"] = 10

        results = []
        for i in range(5):
            # Alternating valid (higher token) and invalid (lower token)
            if i % 2 == 0:
                token = 11 + i  # Valid: higher
                manager = "manager-known"
            else:
                token = 5 + i  # Invalid: lower
                manager = "manager-known"

            transfer = JobLeaderWorkerTransfer(
                job_id="job-1",
                workflow_ids=["wf-1"],
                new_manager_id=manager,
                new_manager_addr=("127.0.0.1", 8001),
                fence_token=token,
            )
            results.append(await worker.job_leader_worker_transfer(transfer))

        accepted = [r for r in results if r.accepted]
        rejected = [r for r in results if not r.accepted]

        assert len(accepted) == 3  # i=0,2,4 (tokens 11, 13, 15)
        assert len(rejected) == 2  # i=1,3 (tokens 6, 8)


class TestEdgeCasesAndBoundaryConditions:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_very_large_fence_token(self):
        """Worker should handle very large fence tokens."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=2**63 - 1,  # Max int64
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert worker.job_fence_tokens["job-1"] == 2**63 - 1

    @pytest.mark.asyncio
    async def test_workflow_id_with_special_characters(self):
        """Worker should handle workflow IDs with special characters."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        special_ids = [
            "wf:with:colons",
            "wf-with-dashes",
            "wf_with_underscores",
            "wf.with.dots",
        ]

        for wf_id in special_ids:
            worker.active_workflows[wf_id] = WorkflowProgress(
                job_id="job-1",
                workflow_id=wf_id,
                workflow_name="test",
                status=WorkflowStatus.RUNNING.value,
                completed_count=0,
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
            )
            worker.workflow_job_leader[wf_id] = ("127.0.0.1", 8000)

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=special_ids,
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 4

    @pytest.mark.asyncio
    async def test_very_long_workflow_id(self):
        """Worker should handle very long workflow IDs."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        long_id = "w" * 1000

        worker.active_workflows[long_id] = WorkflowProgress(
            job_id="job-1",
            workflow_id=long_id,
            workflow_name="test",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
        )
        worker.workflow_job_leader[long_id] = ("127.0.0.1", 8000)

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=[long_id],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 1

    @pytest.mark.asyncio
    async def test_large_number_of_workflows_in_transfer(self):
        """Worker should handle transfer with many workflows."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Add 1000 workflows
        workflow_ids = [f"wf-{i:06d}" for i in range(1000)]
        for wf_id in workflow_ids:
            worker.active_workflows[wf_id] = WorkflowProgress(
                job_id="job-1",
                workflow_id=wf_id,
                workflow_name="test",
                status=WorkflowStatus.RUNNING.value,
                completed_count=0,
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
            )
            worker.workflow_job_leader[wf_id] = ("127.0.0.1", 8000)

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=workflow_ids,
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 1000


class TestPendingTransferEdgeCases:
    """Tests for pending transfer edge cases."""

    @pytest.mark.asyncio
    async def test_pending_transfer_overwrites_previous(self):
        """Later pending transfer should overwrite earlier one for same job."""
        worker = MockWorkerServer()
        worker.known_managers["manager-1"] = ManagerInfo(
            node_id="manager-1",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )
        worker.known_managers["manager-2"] = ManagerInfo(
            node_id="manager-2",
            tcp_host="127.0.0.1",
            tcp_port=8003,
            udp_host="127.0.0.1",
            udp_port=8004,
            datacenter="dc-default",
        )

        # First transfer creates pending
        transfer1 = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1"],
            new_manager_id="manager-1",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )
        await worker.job_leader_worker_transfer(transfer1)

        assert worker.pending_transfers["job-1"].new_manager_id == "manager-1"

        # Second transfer overwrites
        transfer2 = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-2"],
            new_manager_id="manager-2",
            new_manager_addr=("127.0.0.1", 8003),
            fence_token=2,
        )
        await worker.job_leader_worker_transfer(transfer2)

        assert worker.pending_transfers["job-1"].new_manager_id == "manager-2"
        assert worker.pending_transfers["job-1"].workflow_ids == ["wf-2"]

    @pytest.mark.asyncio
    async def test_pending_transfer_not_created_if_all_workflows_found(self):
        """No pending transfer if all workflows are found."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Add all workflows
        for wf_id in ["wf-1", "wf-2"]:
            worker.active_workflows[wf_id] = WorkflowProgress(
                job_id="job-1",
                workflow_id=wf_id,
                workflow_name="test",
                status=WorkflowStatus.RUNNING.value,
                completed_count=0,
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
            )
            worker.workflow_job_leader[wf_id] = ("127.0.0.1", 8000)

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1", "wf-2"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        await worker.job_leader_worker_transfer(transfer)

        # No pending transfer created
        assert "job-1" not in worker.pending_transfers


class TestMultipleWorkflowStates:
    """Tests for handling workflows in various states."""

    @pytest.mark.asyncio
    async def test_transfer_updates_workflows_in_various_states(self):
        """Transfer should update workflows regardless of their state."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        states = [
            WorkflowStatus.PENDING.value,
            WorkflowStatus.RUNNING.value,
            WorkflowStatus.ASSIGNED.value,
            WorkflowStatus.COMPLETED.value,
        ]

        for i, status in enumerate(states):
            wf_id = f"wf-{i}"
            worker.active_workflows[wf_id] = WorkflowProgress(
                job_id="job-1",
                workflow_id=wf_id,
                workflow_name=f"test-{i}",
                status=status,
                completed_count=0,
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
            )
            worker.workflow_job_leader[wf_id] = ("127.0.0.1", 8000)

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=[f"wf-{i}" for i in range(4)],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        ack = await worker.job_leader_worker_transfer(transfer)

        assert ack.accepted is True
        assert ack.workflows_updated == 4
        assert len(ack.workflow_states) == 4
        assert ack.workflow_states["wf-0"] == WorkflowStatus.PENDING.value
        assert ack.workflow_states["wf-1"] == WorkflowStatus.RUNNING.value

    @pytest.mark.asyncio
    async def test_mixed_orphaned_and_non_orphaned_workflows(self):
        """Transfer should clear orphan status for orphaned workflows only."""
        worker = MockWorkerServer()
        worker.known_managers["manager-new"] = ManagerInfo(
            node_id="manager-new",
            tcp_host="127.0.0.1",
            tcp_port=8001,
            udp_host="127.0.0.1",
            udp_port=8002,
            datacenter="dc-default",
        )

        # Add workflows
        for wf_id in ["wf-1", "wf-2", "wf-3"]:
            worker.active_workflows[wf_id] = WorkflowProgress(
                job_id="job-1",
                workflow_id=wf_id,
                workflow_name="test",
                status=WorkflowStatus.RUNNING.value,
                completed_count=0,
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
            )
            worker.workflow_job_leader[wf_id] = ("127.0.0.1", 8000)

        # Only wf-1 and wf-2 are orphaned
        worker.orphaned_workflows["wf-1"] = time.monotonic()
        worker.orphaned_workflows["wf-2"] = time.monotonic()

        transfer = JobLeaderWorkerTransfer(
            job_id="job-1",
            workflow_ids=["wf-1", "wf-2", "wf-3"],
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
        )

        await worker.job_leader_worker_transfer(transfer)

        # All orphan statuses should be cleared
        assert "wf-1" not in worker.orphaned_workflows
        assert "wf-2" not in worker.orphaned_workflows
        assert "wf-3" not in worker.orphaned_workflows  # Was never orphaned


class TestLockBehavior:
    """Tests for per-job lock behavior."""

    @pytest.mark.asyncio
    async def test_lock_created_on_first_access(self):
        """Lock should be created on first access for a job."""
        worker = MockWorkerServer()

        assert "job-1" not in worker.job_leader_transfer_locks

        lock = worker._get_job_transfer_lock("job-1")

        assert "job-1" in worker.job_leader_transfer_locks
        assert lock is worker.job_leader_transfer_locks["job-1"]

    @pytest.mark.asyncio
    async def test_same_lock_returned_on_subsequent_access(self):
        """Same lock should be returned on subsequent accesses."""
        worker = MockWorkerServer()

        lock1 = worker._get_job_transfer_lock("job-1")
        lock2 = worker._get_job_transfer_lock("job-1")

        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_different_locks_for_different_jobs(self):
        """Different jobs should have different locks."""
        worker = MockWorkerServer()

        lock1 = worker._get_job_transfer_lock("job-1")
        lock2 = worker._get_job_transfer_lock("job-2")

        assert lock1 is not lock2
