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

from hyperscale.distributed_rewrite.models import (
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

        async def slow_validate(job_id: str, token: int):
            execution_order.append(token)
            await asyncio.sleep(0.05)  # Simulate slow validation
            return original_validate(job_id, token)

        client._validate_gate_fence_token = slow_validate

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
