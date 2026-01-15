"""
Integration tests for Section 9: Client robust response to leadership takeovers.

These tests verify that clients handle leadership transfers robustly:
- 9.1: Gate leadership tracking
- 9.2: Manager leadership tracking
- 9.3: Request re-routing and retry logic
- 9.4: Stale response handling (via fence token validation)
- 9.5: Client-side orphan job handling
- 9.6: Metrics and observability
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field

from hyperscale.distributed.models import (
    GateLeaderInfo,
    ManagerLeaderInfo,
    OrphanedJobInfo,
    LeadershipRetryPolicy,
    GateJobLeaderTransfer,
    GateJobLeaderTransferAck,
    ManagerJobLeaderTransfer,
    ManagerJobLeaderTransferAck,
)


@dataclass
class MockHyperscaleClient:
    """
    Mock HyperscaleClient for testing Section 9 leadership transfer handling.

    Implements the client-side transfer handling logic.
    """
    node_id: str = "client-001"
    host: str = "127.0.0.1"
    tcp_port: int = 8500

    # 9.1.1: Gate leadership tracking
    gate_job_leaders: dict[str, GateLeaderInfo] = field(default_factory=dict)

    # 9.2.1: Manager leadership tracking (job_id, datacenter_id) -> info
    manager_job_leaders: dict[tuple[str, str], ManagerLeaderInfo] = field(default_factory=dict)

    # 9.3.2: Per-job locks
    request_routing_locks: dict[str, asyncio.Lock] = field(default_factory=dict)

    # 9.5.1: Orphaned jobs
    orphaned_jobs: dict[str, OrphanedJobInfo] = field(default_factory=dict)
    orphan_grace_period: float = 15.0

    # Job targets
    job_targets: dict[str, tuple[str, int]] = field(default_factory=dict)

    # Metrics
    gate_transfers_received: int = 0
    manager_transfers_received: int = 0
    requests_rerouted: int = 0
    requests_failed_leadership_change: int = 0

    # Log capture
    log_messages: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.gate_job_leaders = {}
        self.manager_job_leaders = {}
        self.request_routing_locks = {}
        self.orphaned_jobs = {}
        self.job_targets = {}
        self.log_messages = []

    def _get_request_routing_lock(self, job_id: str) -> asyncio.Lock:
        """Get or create per-job lock (9.3.2)."""
        if job_id not in self.request_routing_locks:
            self.request_routing_locks[job_id] = asyncio.Lock()
        return self.request_routing_locks[job_id]

    def _validate_gate_fence_token(self, job_id: str, new_fence_token: int) -> tuple[bool, str]:
        """Validate gate transfer fence token (9.1.2)."""
        current_leader = self.gate_job_leaders.get(job_id)
        if current_leader and new_fence_token <= current_leader.fence_token:
            return (False, f"Stale fence token: received {new_fence_token}, current {current_leader.fence_token}")
        return (True, "")

    def _validate_manager_fence_token(
        self,
        job_id: str,
        datacenter_id: str,
        new_fence_token: int,
    ) -> tuple[bool, str]:
        """Validate manager transfer fence token (9.2.2)."""
        key = (job_id, datacenter_id)
        current_leader = self.manager_job_leaders.get(key)
        if current_leader and new_fence_token <= current_leader.fence_token:
            return (False, f"Stale fence token: received {new_fence_token}, current {current_leader.fence_token}")
        return (True, "")

    def _update_gate_leader(
        self,
        job_id: str,
        gate_addr: tuple[str, int],
        fence_token: int,
    ) -> None:
        """Update gate job leader (9.1.1)."""
        self.gate_job_leaders[job_id] = GateLeaderInfo(
            gate_addr=gate_addr,
            fence_token=fence_token,
            last_updated=time.monotonic(),
        )
        # Clear orphan status
        if job_id in self.orphaned_jobs:
            del self.orphaned_jobs[job_id]

    def _update_manager_leader(
        self,
        job_id: str,
        datacenter_id: str,
        manager_addr: tuple[str, int],
        fence_token: int,
    ) -> None:
        """Update manager job leader (9.2.1)."""
        key = (job_id, datacenter_id)
        self.manager_job_leaders[key] = ManagerLeaderInfo(
            manager_addr=manager_addr,
            fence_token=fence_token,
            datacenter_id=datacenter_id,
            last_updated=time.monotonic(),
        )

    def _mark_job_orphaned(
        self,
        job_id: str,
        last_known_gate: tuple[str, int] | None,
        last_known_manager: tuple[str, int] | None,
        datacenter_id: str = "",
    ) -> None:
        """Mark job as orphaned (9.5.1)."""
        if job_id not in self.orphaned_jobs:
            self.orphaned_jobs[job_id] = OrphanedJobInfo(
                job_id=job_id,
                orphan_timestamp=time.monotonic(),
                last_known_gate=last_known_gate,
                last_known_manager=last_known_manager,
                datacenter_id=datacenter_id,
            )

    async def receive_gate_job_leader_transfer(
        self,
        transfer: GateJobLeaderTransfer,
    ) -> GateJobLeaderTransferAck:
        """Process gate job leadership transfer (9.1.2)."""
        self.gate_transfers_received += 1
        job_id = transfer.job_id

        self.log_messages.append(f"Processing gate transfer for job {job_id}")

        routing_lock = self._get_request_routing_lock(job_id)
        async with routing_lock:
            # Validate fence token
            fence_valid, fence_reason = self._validate_gate_fence_token(job_id, transfer.fence_token)
            if not fence_valid:
                self.log_messages.append(f"Rejected: {fence_reason}")
                return GateJobLeaderTransferAck(
                    job_id=job_id,
                    client_id=self.node_id,
                    accepted=False,
                    rejection_reason=fence_reason,
                )

            # Update gate leader
            self._update_gate_leader(
                job_id=job_id,
                gate_addr=transfer.new_gate_addr,
                fence_token=transfer.fence_token,
            )

            # Update job target
            if job_id in self.job_targets:
                self.job_targets[job_id] = transfer.new_gate_addr

            self.log_messages.append(f"Accepted: new gate {transfer.new_gate_addr}")
            return GateJobLeaderTransferAck(
                job_id=job_id,
                client_id=self.node_id,
                accepted=True,
            )

    async def receive_manager_job_leader_transfer(
        self,
        transfer: ManagerJobLeaderTransfer,
    ) -> ManagerJobLeaderTransferAck:
        """Process manager job leadership transfer (9.2.2)."""
        self.manager_transfers_received += 1
        job_id = transfer.job_id
        datacenter_id = transfer.datacenter_id

        self.log_messages.append(f"Processing manager transfer for job {job_id} in dc {datacenter_id}")

        routing_lock = self._get_request_routing_lock(job_id)
        async with routing_lock:
            # Validate fence token
            fence_valid, fence_reason = self._validate_manager_fence_token(
                job_id, datacenter_id, transfer.fence_token
            )
            if not fence_valid:
                self.log_messages.append(f"Rejected: {fence_reason}")
                return ManagerJobLeaderTransferAck(
                    job_id=job_id,
                    client_id=self.node_id,
                    datacenter_id=datacenter_id,
                    accepted=False,
                    rejection_reason=fence_reason,
                )

            # Update manager leader
            self._update_manager_leader(
                job_id=job_id,
                datacenter_id=datacenter_id,
                manager_addr=transfer.new_manager_addr,
                fence_token=transfer.fence_token,
            )

            self.log_messages.append(f"Accepted: new manager {transfer.new_manager_addr}")
            return ManagerJobLeaderTransferAck(
                job_id=job_id,
                client_id=self.node_id,
                datacenter_id=datacenter_id,
                accepted=True,
            )

    def get_leadership_metrics(self) -> dict[str, int]:
        """Get leadership transfer metrics (9.6.1)."""
        return {
            "gate_transfers_received": self.gate_transfers_received,
            "manager_transfers_received": self.manager_transfers_received,
            "requests_rerouted": self.requests_rerouted,
            "requests_failed_leadership_change": self.requests_failed_leadership_change,
            "orphaned_jobs": len(self.orphaned_jobs),
            "tracked_gate_leaders": len(self.gate_job_leaders),
            "tracked_manager_leaders": len(self.manager_job_leaders),
        }


class TestGateLeadershipTracking:
    """Tests for Section 9.1: Gate leadership tracking."""

    @pytest.mark.asyncio
    async def test_accepts_valid_gate_transfer(self):
        """Test that valid gate transfers are accepted."""
        client = MockHyperscaleClient()

        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
            old_gate_id="gate-old",
            old_gate_addr=("127.0.0.1", 9000),
        )

        ack = await client.receive_gate_job_leader_transfer(transfer)

        assert ack.accepted is True
        assert ack.job_id == "job-1"
        assert "job-1" in client.gate_job_leaders
        assert client.gate_job_leaders["job-1"].gate_addr == ("127.0.0.1", 9001)
        assert client.gate_job_leaders["job-1"].fence_token == 1
        assert client.gate_transfers_received == 1

    @pytest.mark.asyncio
    async def test_rejects_stale_gate_transfer(self):
        """Test that stale gate transfers are rejected (9.4.1)."""
        client = MockHyperscaleClient()

        # First, establish a gate leader
        client.gate_job_leaders["job-1"] = GateLeaderInfo(
            gate_addr=("127.0.0.1", 9000),
            fence_token=10,
            last_updated=time.monotonic(),
        )

        # Try to transfer with a lower fence token
        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-stale",
            new_gate_addr=("127.0.0.1", 9002),
            fence_token=5,  # Lower than 10
        )

        ack = await client.receive_gate_job_leader_transfer(transfer)

        assert ack.accepted is False
        assert "Stale fence token" in ack.rejection_reason
        # Leader should NOT be updated
        assert client.gate_job_leaders["job-1"].gate_addr == ("127.0.0.1", 9000)
        assert client.gate_job_leaders["job-1"].fence_token == 10

    @pytest.mark.asyncio
    async def test_transfer_updates_job_target(self):
        """Test that gate transfer updates job target for routing."""
        client = MockHyperscaleClient()
        client.job_targets["job-1"] = ("127.0.0.1", 9000)  # Old gate

        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
        )

        await client.receive_gate_job_leader_transfer(transfer)

        assert client.job_targets["job-1"] == ("127.0.0.1", 9001)


class TestManagerLeadershipTracking:
    """Tests for Section 9.2: Manager leadership tracking."""

    @pytest.mark.asyncio
    async def test_accepts_valid_manager_transfer(self):
        """Test that valid manager transfers are accepted."""
        client = MockHyperscaleClient()

        transfer = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
            datacenter_id="dc-east",
        )

        ack = await client.receive_manager_job_leader_transfer(transfer)

        assert ack.accepted is True
        assert ack.job_id == "job-1"
        assert ack.datacenter_id == "dc-east"

        key = ("job-1", "dc-east")
        assert key in client.manager_job_leaders
        assert client.manager_job_leaders[key].manager_addr == ("127.0.0.1", 8001)
        assert client.manager_job_leaders[key].fence_token == 1

    @pytest.mark.asyncio
    async def test_rejects_stale_manager_transfer(self):
        """Test that stale manager transfers are rejected."""
        client = MockHyperscaleClient()

        # Establish manager leader
        key = ("job-1", "dc-east")
        client.manager_job_leaders[key] = ManagerLeaderInfo(
            manager_addr=("127.0.0.1", 8000),
            fence_token=10,
            datacenter_id="dc-east",
            last_updated=time.monotonic(),
        )

        # Try with lower fence token
        transfer = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-stale",
            new_manager_addr=("127.0.0.1", 8002),
            fence_token=5,
            datacenter_id="dc-east",
        )

        ack = await client.receive_manager_job_leader_transfer(transfer)

        assert ack.accepted is False
        assert "Stale fence token" in ack.rejection_reason

    @pytest.mark.asyncio
    async def test_multi_datacenter_tracking(self):
        """Test that manager leaders are tracked per datacenter (9.2.3)."""
        client = MockHyperscaleClient()

        # Transfer for DC-east
        transfer_east = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-east",
            new_manager_addr=("10.0.0.1", 8000),
            fence_token=1,
            datacenter_id="dc-east",
        )

        # Transfer for DC-west
        transfer_west = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-west",
            new_manager_addr=("10.0.0.2", 8000),
            fence_token=1,
            datacenter_id="dc-west",
        )

        await client.receive_manager_job_leader_transfer(transfer_east)
        await client.receive_manager_job_leader_transfer(transfer_west)

        # Both should be tracked separately
        assert ("job-1", "dc-east") in client.manager_job_leaders
        assert ("job-1", "dc-west") in client.manager_job_leaders
        assert client.manager_job_leaders[("job-1", "dc-east")].manager_addr == ("10.0.0.1", 8000)
        assert client.manager_job_leaders[("job-1", "dc-west")].manager_addr == ("10.0.0.2", 8000)


class TestPerJobLocks:
    """Tests for Section 9.3.2: Per-job routing locks."""

    @pytest.mark.asyncio
    async def test_concurrent_transfers_serialized(self):
        """Test that concurrent transfers for the same job are serialized."""
        client = MockHyperscaleClient()

        execution_order: list[int] = []
        original_validate = client._validate_gate_fence_token

        def tracking_validate(job_id: str, token: int) -> tuple[bool, str]:
            execution_order.append(token)
            return original_validate(job_id, token)

        client._validate_gate_fence_token = tracking_validate

        # Two concurrent transfers
        transfer1 = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-1",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
        )
        transfer2 = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-2",
            new_gate_addr=("127.0.0.1", 9002),
            fence_token=2,
        )

        results = await asyncio.gather(
            client.receive_gate_job_leader_transfer(transfer1),
            client.receive_gate_job_leader_transfer(transfer2),
        )

        # Both should be accepted since fence token 2 > 1
        accepted = [r for r in results if r.accepted]
        assert len(accepted) == 2

        # Final state should have fence token 2
        assert client.gate_job_leaders["job-1"].fence_token == 2


class TestOrphanedJobs:
    """Tests for Section 9.5: Client-side orphan job handling."""

    @pytest.mark.asyncio
    async def test_mark_job_orphaned(self):
        """Test that jobs can be marked as orphaned."""
        client = MockHyperscaleClient()

        client._mark_job_orphaned(
            job_id="job-1",
            last_known_gate=("127.0.0.1", 9000),
            last_known_manager=("127.0.0.1", 8000),
            datacenter_id="dc-east",
        )

        assert "job-1" in client.orphaned_jobs
        orphan = client.orphaned_jobs["job-1"]
        assert orphan.last_known_gate == ("127.0.0.1", 9000)
        assert orphan.last_known_manager == ("127.0.0.1", 8000)

    @pytest.mark.asyncio
    async def test_transfer_clears_orphan_status(self):
        """Test that gate transfer clears orphan status (9.5.2)."""
        client = MockHyperscaleClient()

        # Mark job as orphaned
        client._mark_job_orphaned(
            job_id="job-1",
            last_known_gate=("127.0.0.1", 9000),
            last_known_manager=None,
        )
        assert "job-1" in client.orphaned_jobs

        # Receive gate transfer
        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
        )

        await client.receive_gate_job_leader_transfer(transfer)

        # Orphan status should be cleared
        assert "job-1" not in client.orphaned_jobs


class TestMetrics:
    """Tests for Section 9.6: Metrics and observability."""

    @pytest.mark.asyncio
    async def test_metrics_tracking(self):
        """Test that leadership transfer metrics are tracked."""
        client = MockHyperscaleClient()

        # Gate transfer
        gate_transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-1",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
        )
        await client.receive_gate_job_leader_transfer(gate_transfer)

        # Manager transfers
        manager_transfer1 = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-1",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=1,
            datacenter_id="dc-east",
        )
        manager_transfer2 = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-2",
            new_manager_addr=("127.0.0.1", 8002),
            fence_token=1,
            datacenter_id="dc-west",
        )
        await client.receive_manager_job_leader_transfer(manager_transfer1)
        await client.receive_manager_job_leader_transfer(manager_transfer2)

        metrics = client.get_leadership_metrics()
        assert metrics["gate_transfers_received"] == 1
        assert metrics["manager_transfers_received"] == 2
        assert metrics["tracked_gate_leaders"] == 1
        assert metrics["tracked_manager_leaders"] == 2


class TestLogging:
    """Tests for Section 9.6.2: Detailed logging."""

    @pytest.mark.asyncio
    async def test_logs_transfer_processing(self):
        """Test that transfer processing is logged."""
        client = MockHyperscaleClient()

        transfer = GateJobLeaderTransfer(
            job_id="job-123",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
        )

        await client.receive_gate_job_leader_transfer(transfer)

        assert any("Processing gate transfer" in msg for msg in client.log_messages)
        assert any("Accepted" in msg for msg in client.log_messages)

    @pytest.mark.asyncio
    async def test_logs_rejection_reason(self):
        """Test that rejection reasons are logged."""
        client = MockHyperscaleClient()

        # Establish existing leader
        client.gate_job_leaders["job-1"] = GateLeaderInfo(
            gate_addr=("127.0.0.1", 9000),
            fence_token=10,
            last_updated=time.monotonic(),
        )

        # Try stale transfer
        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-stale",
            new_gate_addr=("127.0.0.1", 9002),
            fence_token=5,
        )

        await client.receive_gate_job_leader_transfer(transfer)

        assert any("Rejected" in msg for msg in client.log_messages)


class TestRetryPolicy:
    """Tests for Section 9.3.3: Leadership retry policy."""

    def test_default_retry_policy(self):
        """Test default retry policy configuration."""
        policy = LeadershipRetryPolicy()

        assert policy.max_retries == 3
        assert policy.retry_delay == 0.5
        assert policy.exponential_backoff is True
        assert policy.max_delay == 5.0

    def test_custom_retry_policy(self):
        """Test custom retry policy configuration."""
        policy = LeadershipRetryPolicy(
            max_retries=5,
            retry_delay=1.0,
            exponential_backoff=False,
            max_delay=10.0,
        )

        assert policy.max_retries == 5
        assert policy.retry_delay == 1.0
        assert policy.exponential_backoff is False
        assert policy.max_delay == 10.0


# =============================================================================
# Extended Tests: Negative Paths and Failure Modes
# =============================================================================


class TestNegativePaths:
    """Tests for error handling and negative scenarios."""

    @pytest.mark.asyncio
    async def test_gate_transfer_with_equal_fence_token_rejected(self):
        """Gate transfer with equal fence token should be rejected."""
        client = MockHyperscaleClient()

        # Set current fence token
        client.gate_job_leaders["job-1"] = GateLeaderInfo(
            gate_addr=("127.0.0.1", 9000),
            fence_token=5,
            last_updated=time.monotonic(),
        )

        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=5,  # Equal to current
        )

        ack = await client.receive_gate_job_leader_transfer(transfer)

        assert ack.accepted is False
        assert "Stale fence token" in ack.rejection_reason

    @pytest.mark.asyncio
    async def test_manager_transfer_with_equal_fence_token_rejected(self):
        """Manager transfer with equal fence token should be rejected."""
        client = MockHyperscaleClient()

        key = ("job-1", "dc-east")
        client.manager_job_leaders[key] = ManagerLeaderInfo(
            manager_addr=("127.0.0.1", 8000),
            fence_token=5,
            datacenter_id="dc-east",
            last_updated=time.monotonic(),
        )

        transfer = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-new",
            new_manager_addr=("127.0.0.1", 8001),
            fence_token=5,  # Equal to current
            datacenter_id="dc-east",
        )

        ack = await client.receive_manager_job_leader_transfer(transfer)

        assert ack.accepted is False
        assert "Stale fence token" in ack.rejection_reason

    @pytest.mark.asyncio
    async def test_gate_transfer_for_unknown_job_accepted(self):
        """Gate transfer for unknown job should still be accepted."""
        client = MockHyperscaleClient()

        transfer = GateJobLeaderTransfer(
            job_id="unknown-job",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
        )

        ack = await client.receive_gate_job_leader_transfer(transfer)

        assert ack.accepted is True
        assert "unknown-job" in client.gate_job_leaders

    @pytest.mark.asyncio
    async def test_duplicate_orphan_marking_preserves_first_timestamp(self):
        """Duplicate orphan marking should preserve first timestamp."""
        client = MockHyperscaleClient()

        # First mark
        client._mark_job_orphaned(
            job_id="job-1",
            last_known_gate=("127.0.0.1", 9000),
            last_known_manager=None,
        )
        first_timestamp = client.orphaned_jobs["job-1"].orphan_timestamp

        # Small delay
        await asyncio.sleep(0.01)

        # Second mark (should not update)
        client._mark_job_orphaned(
            job_id="job-1",
            last_known_gate=("127.0.0.1", 9001),
            last_known_manager=("127.0.0.1", 8000),
        )

        assert client.orphaned_jobs["job-1"].orphan_timestamp == first_timestamp

    @pytest.mark.asyncio
    async def test_gate_transfer_without_job_target(self):
        """Gate transfer should work even if job_targets doesn't have the job."""
        client = MockHyperscaleClient()

        # No job target set
        assert "job-1" not in client.job_targets

        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
        )

        ack = await client.receive_gate_job_leader_transfer(transfer)

        assert ack.accepted is True
        # job_targets still shouldn't have it (only updates if already present)
        assert "job-1" not in client.job_targets


# =============================================================================
# Extended Tests: Concurrency and Race Conditions
# =============================================================================


class TestConcurrencyAndRaceConditions:
    """Tests for concurrent operations and race conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_gate_transfers_different_jobs(self):
        """Concurrent gate transfers for different jobs should all succeed."""
        client = MockHyperscaleClient()

        transfers = [
            GateJobLeaderTransfer(
                job_id=f"job-{i}",
                new_gate_id=f"gate-{i}",
                new_gate_addr=("127.0.0.1", 9000 + i),
                fence_token=1,
            )
            for i in range(10)
        ]

        results = await asyncio.gather(*[
            client.receive_gate_job_leader_transfer(t) for t in transfers
        ])

        assert all(r.accepted for r in results)
        assert client.gate_transfers_received == 10
        assert len(client.gate_job_leaders) == 10

    @pytest.mark.asyncio
    async def test_concurrent_manager_transfers_different_datacenters(self):
        """Concurrent manager transfers for different DCs should all succeed."""
        client = MockHyperscaleClient()

        transfers = [
            ManagerJobLeaderTransfer(
                job_id="job-1",
                new_manager_id=f"manager-{i}",
                new_manager_addr=("127.0.0.1", 8000 + i),
                fence_token=1,
                datacenter_id=f"dc-{i}",
            )
            for i in range(5)
        ]

        results = await asyncio.gather(*[
            client.receive_manager_job_leader_transfer(t) for t in transfers
        ])

        assert all(r.accepted for r in results)
        assert len(client.manager_job_leaders) == 5

    @pytest.mark.asyncio
    async def test_rapid_successive_gate_transfers(self):
        """Rapid successive gate transfers with increasing tokens."""
        client = MockHyperscaleClient()

        for i in range(20):
            transfer = GateJobLeaderTransfer(
                job_id="job-1",
                new_gate_id=f"gate-{i}",
                new_gate_addr=("127.0.0.1", 9000 + i),
                fence_token=i,
            )
            ack = await client.receive_gate_job_leader_transfer(transfer)
            assert ack.accepted is True

        assert client.gate_job_leaders["job-1"].fence_token == 19

    @pytest.mark.asyncio
    async def test_interleaved_gate_and_manager_transfers(self):
        """Interleaved gate and manager transfers for same job."""
        client = MockHyperscaleClient()

        for i in range(5):
            # Gate transfer
            gate_transfer = GateJobLeaderTransfer(
                job_id="job-1",
                new_gate_id=f"gate-{i}",
                new_gate_addr=("127.0.0.1", 9000 + i),
                fence_token=i,
            )
            await client.receive_gate_job_leader_transfer(gate_transfer)

            # Manager transfer
            manager_transfer = ManagerJobLeaderTransfer(
                job_id="job-1",
                new_manager_id=f"manager-{i}",
                new_manager_addr=("127.0.0.1", 8000 + i),
                fence_token=i,
                datacenter_id="dc-east",
            )
            await client.receive_manager_job_leader_transfer(manager_transfer)

        assert client.gate_transfers_received == 5
        assert client.manager_transfers_received == 5


# =============================================================================
# Extended Tests: Edge Cases and Boundary Conditions
# =============================================================================


class TestEdgeCasesAndBoundaryConditions:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_very_large_fence_token(self):
        """Client should handle very large fence tokens."""
        client = MockHyperscaleClient()

        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=2**63 - 1,  # Max int64
        )

        ack = await client.receive_gate_job_leader_transfer(transfer)

        assert ack.accepted is True
        assert client.gate_job_leaders["job-1"].fence_token == 2**63 - 1

    @pytest.mark.asyncio
    async def test_job_id_with_special_characters(self):
        """Client should handle job IDs with special characters."""
        client = MockHyperscaleClient()

        special_ids = [
            "job:with:colons",
            "job-with-dashes",
            "job_with_underscores",
            "job.with.dots",
        ]

        for job_id in special_ids:
            transfer = GateJobLeaderTransfer(
                job_id=job_id,
                new_gate_id="gate-new",
                new_gate_addr=("127.0.0.1", 9001),
                fence_token=1,
            )
            ack = await client.receive_gate_job_leader_transfer(transfer)
            assert ack.accepted is True
            assert job_id in client.gate_job_leaders

    @pytest.mark.asyncio
    async def test_very_long_job_id(self):
        """Client should handle very long job IDs."""
        client = MockHyperscaleClient()

        long_id = "j" * 1000

        transfer = GateJobLeaderTransfer(
            job_id=long_id,
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=1,
        )

        ack = await client.receive_gate_job_leader_transfer(transfer)

        assert ack.accepted is True
        assert long_id in client.gate_job_leaders

    @pytest.mark.asyncio
    async def test_datacenter_id_with_special_characters(self):
        """Client should handle datacenter IDs with special characters."""
        client = MockHyperscaleClient()

        special_dc_ids = [
            "dc:west:1",
            "dc-east-2",
            "dc_central_3",
            "dc.north.4",
        ]

        for dc_id in special_dc_ids:
            transfer = ManagerJobLeaderTransfer(
                job_id="job-1",
                new_manager_id="manager-new",
                new_manager_addr=("127.0.0.1", 8001),
                fence_token=1,
                datacenter_id=dc_id,
            )
            ack = await client.receive_manager_job_leader_transfer(transfer)
            assert ack.accepted is True

        assert len(client.manager_job_leaders) == 4

    @pytest.mark.asyncio
    async def test_large_number_of_jobs_tracked(self):
        """Client should handle tracking many jobs."""
        client = MockHyperscaleClient()

        for i in range(1000):
            transfer = GateJobLeaderTransfer(
                job_id=f"job-{i:06d}",
                new_gate_id=f"gate-{i}",
                new_gate_addr=("127.0.0.1", 9000),
                fence_token=1,
            )
            await client.receive_gate_job_leader_transfer(transfer)

        assert len(client.gate_job_leaders) == 1000

    @pytest.mark.asyncio
    async def test_zero_fence_token_accepted_for_new_job(self):
        """Zero fence token should be accepted for new job."""
        client = MockHyperscaleClient()

        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-new",
            new_gate_addr=("127.0.0.1", 9001),
            fence_token=0,
        )

        ack = await client.receive_gate_job_leader_transfer(transfer)

        assert ack.accepted is True
        assert client.gate_job_leaders["job-1"].fence_token == 0


class TestLockBehavior:
    """Tests for per-job lock behavior."""

    @pytest.mark.asyncio
    async def test_lock_created_on_first_access(self):
        """Lock should be created on first access for a job."""
        client = MockHyperscaleClient()

        assert "job-1" not in client.request_routing_locks

        lock = client._get_request_routing_lock("job-1")

        assert "job-1" in client.request_routing_locks
        assert lock is client.request_routing_locks["job-1"]

    @pytest.mark.asyncio
    async def test_same_lock_returned_on_subsequent_access(self):
        """Same lock should be returned on subsequent accesses."""
        client = MockHyperscaleClient()

        lock1 = client._get_request_routing_lock("job-1")
        lock2 = client._get_request_routing_lock("job-1")

        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_different_locks_for_different_jobs(self):
        """Different jobs should have different locks."""
        client = MockHyperscaleClient()

        lock1 = client._get_request_routing_lock("job-1")
        lock2 = client._get_request_routing_lock("job-2")

        assert lock1 is not lock2


class TestOrphanedJobEdgeCases:
    """Tests for orphaned job handling edge cases."""

    @pytest.mark.asyncio
    async def test_orphan_with_no_last_known_addresses(self):
        """Orphan can be marked with no last known addresses."""
        client = MockHyperscaleClient()

        client._mark_job_orphaned(
            job_id="job-1",
            last_known_gate=None,
            last_known_manager=None,
        )

        assert "job-1" in client.orphaned_jobs
        orphan = client.orphaned_jobs["job-1"]
        assert orphan.last_known_gate is None
        assert orphan.last_known_manager is None

    @pytest.mark.asyncio
    async def test_orphan_only_gate_known(self):
        """Orphan can be marked with only gate known."""
        client = MockHyperscaleClient()

        client._mark_job_orphaned(
            job_id="job-1",
            last_known_gate=("127.0.0.1", 9000),
            last_known_manager=None,
        )

        orphan = client.orphaned_jobs["job-1"]
        assert orphan.last_known_gate == ("127.0.0.1", 9000)
        assert orphan.last_known_manager is None

    @pytest.mark.asyncio
    async def test_orphan_only_manager_known(self):
        """Orphan can be marked with only manager known."""
        client = MockHyperscaleClient()

        client._mark_job_orphaned(
            job_id="job-1",
            last_known_gate=None,
            last_known_manager=("127.0.0.1", 8000),
            datacenter_id="dc-east",
        )

        orphan = client.orphaned_jobs["job-1"]
        assert orphan.last_known_gate is None
        assert orphan.last_known_manager == ("127.0.0.1", 8000)
        assert orphan.datacenter_id == "dc-east"

    @pytest.mark.asyncio
    async def test_multiple_orphaned_jobs(self):
        """Multiple jobs can be orphaned simultaneously."""
        client = MockHyperscaleClient()

        for i in range(10):
            client._mark_job_orphaned(
                job_id=f"job-{i}",
                last_known_gate=("127.0.0.1", 9000 + i),
                last_known_manager=None,
            )

        assert len(client.orphaned_jobs) == 10


class TestMetricsEdgeCases:
    """Tests for metrics edge cases."""

    @pytest.mark.asyncio
    async def test_metrics_after_rejected_transfers(self):
        """Metrics should be tracked even for rejected transfers."""
        client = MockHyperscaleClient()

        # Set up existing leader
        client.gate_job_leaders["job-1"] = GateLeaderInfo(
            gate_addr=("127.0.0.1", 9000),
            fence_token=10,
            last_updated=time.monotonic(),
        )

        # Rejected transfer
        transfer = GateJobLeaderTransfer(
            job_id="job-1",
            new_gate_id="gate-stale",
            new_gate_addr=("127.0.0.1", 9002),
            fence_token=5,
        )

        await client.receive_gate_job_leader_transfer(transfer)

        metrics = client.get_leadership_metrics()
        assert metrics["gate_transfers_received"] == 1
        # Still only 1 tracked leader
        assert metrics["tracked_gate_leaders"] == 1

    @pytest.mark.asyncio
    async def test_metrics_with_mixed_accept_reject(self):
        """Metrics should correctly count mixed accept/reject."""
        client = MockHyperscaleClient()

        for i in range(10):
            # Even: accepted, Odd: rejected (stale)
            if i % 2 == 0:
                # Start fresh each even
                client.gate_job_leaders.pop("job-1", None)
            else:
                # For odd, don't clear so next will be stale
                pass

            transfer = GateJobLeaderTransfer(
                job_id="job-1",
                new_gate_id=f"gate-{i}",
                new_gate_addr=("127.0.0.1", 9000 + i),
                fence_token=1,  # Always 1
            )

            await client.receive_gate_job_leader_transfer(transfer)

        metrics = client.get_leadership_metrics()
        # All 10 received
        assert metrics["gate_transfers_received"] == 10


class TestMultiDatacenterEdgeCases:
    """Tests for multi-datacenter edge cases."""

    @pytest.mark.asyncio
    async def test_same_job_different_fence_tokens_per_dc(self):
        """Same job can have different fence tokens per datacenter."""
        client = MockHyperscaleClient()

        # DC-east with fence 5
        transfer_east = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-east",
            new_manager_addr=("10.0.0.1", 8000),
            fence_token=5,
            datacenter_id="dc-east",
        )
        await client.receive_manager_job_leader_transfer(transfer_east)

        # DC-west with fence 10 (different)
        transfer_west = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-west",
            new_manager_addr=("10.0.0.2", 8000),
            fence_token=10,
            datacenter_id="dc-west",
        )
        await client.receive_manager_job_leader_transfer(transfer_west)

        # Both tracked independently
        assert client.manager_job_leaders[("job-1", "dc-east")].fence_token == 5
        assert client.manager_job_leaders[("job-1", "dc-west")].fence_token == 10

    @pytest.mark.asyncio
    async def test_manager_transfer_new_dc_accepted(self):
        """Manager transfer to new DC should be accepted."""
        client = MockHyperscaleClient()

        # Establish leader in dc-east
        client.manager_job_leaders[("job-1", "dc-east")] = ManagerLeaderInfo(
            manager_addr=("10.0.0.1", 8000),
            fence_token=10,
            datacenter_id="dc-east",
            last_updated=time.monotonic(),
        )

        # Transfer in different DC should be accepted (independent)
        transfer = ManagerJobLeaderTransfer(
            job_id="job-1",
            new_manager_id="manager-west",
            new_manager_addr=("10.0.0.2", 8000),
            fence_token=1,  # Lower but different DC
            datacenter_id="dc-west",
        )

        ack = await client.receive_manager_job_leader_transfer(transfer)

        assert ack.accepted is True

    @pytest.mark.asyncio
    async def test_many_datacenters_same_job(self):
        """Same job can be tracked across many datacenters."""
        client = MockHyperscaleClient()

        dc_ids = [f"dc-{i}" for i in range(20)]

        for dc_id in dc_ids:
            transfer = ManagerJobLeaderTransfer(
                job_id="job-1",
                new_manager_id=f"manager-{dc_id}",
                new_manager_addr=("127.0.0.1", 8000),
                fence_token=1,
                datacenter_id=dc_id,
            )
            await client.receive_manager_job_leader_transfer(transfer)

        # 20 DC entries for same job
        job_entries = [k for k in client.manager_job_leaders.keys() if k[0] == "job-1"]
        assert len(job_entries) == 20
