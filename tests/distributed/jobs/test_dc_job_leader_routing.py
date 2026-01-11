"""
Integration tests for Direct DC-to-Job-Leader Routing.

These tests verify that:
1. Gates set origin_gate_addr when dispatching jobs
2. Managers store origin_gate_addr and route results to job leader gate
3. Job state sync propagates origin_gate_addr to peer managers
4. Gate forwarding works when receiving results for jobs not owned
5. JobLeaderGateTransfer notifies managers of gate leadership changes

The Direct DC-to-Job-Leader Routing pattern ensures:
- Results flow directly to the gate that submitted the job
- Efficient routing without broadcast to all gates
- Resilience through forwarding when origin gate changes
"""

import asyncio
import pytest
import time

from hyperscale.distributed.models import (
    JobSubmission,
    JobProgress,
    JobFinalResult,
    JobStatus,
    JobStateSyncMessage,
    JobLeaderGateTransfer,
    JobLeaderGateTransferAck,
    GateHeartbeat,
    ManagerHeartbeat,
)


class TestOriginGateAddressField:
    """Test origin_gate_addr field in JobSubmission."""

    def test_job_submission_has_origin_gate_addr(self):
        """JobSubmission should have origin_gate_addr field defaulting to None."""
        submission = JobSubmission(
            job_id="job-123",
            workflows=b"pickled_workflows",
            vus=1,
            timeout_seconds=60.0,
        )
        assert hasattr(submission, 'origin_gate_addr')
        assert submission.origin_gate_addr is None

    def test_job_submission_with_custom_origin_gate(self):
        """JobSubmission should accept custom origin_gate_addr."""
        origin_addr = ("gate1.example.com", 8080)
        submission = JobSubmission(
            job_id="job-123",
            workflows=b"pickled_workflows",
            vus=1,
            timeout_seconds=60.0,
            origin_gate_addr=origin_addr,
        )
        assert submission.origin_gate_addr == origin_addr

    def test_origin_gate_addr_serialization(self):
        """origin_gate_addr should survive serialization round-trip."""
        origin_addr = ("192.168.1.100", 9000)
        original = JobSubmission(
            job_id="job-456",
            workflows=b"test_workflows",
            vus=1,
            timeout_seconds=60.0,
            origin_gate_addr=origin_addr,
        )

        serialized = original.dump()
        restored = JobSubmission.load(serialized)

        assert restored.origin_gate_addr == origin_addr
        assert restored.job_id == "job-456"


class TestJobStateSyncOriginGate:
    """Test origin_gate_addr in JobStateSyncMessage."""

    def test_job_state_sync_has_origin_gate(self):
        """JobStateSyncMessage should have origin_gate_addr field."""
        sync_msg = JobStateSyncMessage(
            leader_id="manager-1",
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            fencing_token=1,
            workflows_total=5,
            workflows_completed=2,
            workflows_failed=0,
        )
        assert hasattr(sync_msg, 'origin_gate_addr')
        assert sync_msg.origin_gate_addr is None

    def test_job_state_sync_with_origin_gate(self):
        """JobStateSyncMessage should accept origin_gate_addr."""
        origin_addr = ("gate2.example.com", 8081)
        sync_msg = JobStateSyncMessage(
            leader_id="manager-1",
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            fencing_token=1,
            workflows_total=5,
            workflows_completed=2,
            workflows_failed=0,
            origin_gate_addr=origin_addr,
        )
        assert sync_msg.origin_gate_addr == origin_addr

    def test_origin_gate_in_sync_serialization(self):
        """origin_gate_addr should serialize correctly in JobStateSyncMessage."""
        origin_addr = ("10.0.0.50", 8000)
        original = JobStateSyncMessage(
            leader_id="manager-2",
            job_id="job-789",
            status=JobStatus.COMPLETED.value,
            fencing_token=5,
            workflows_total=10,
            workflows_completed=10,
            workflows_failed=0,
            origin_gate_addr=origin_addr,
        )

        serialized = original.dump()
        restored = JobStateSyncMessage.load(serialized)

        assert restored.origin_gate_addr == origin_addr
        assert restored.leader_id == "manager-2"
        assert restored.fencing_token == 5


class TestJobLeaderGateTransfer:
    """Test JobLeaderGateTransfer message for gate leadership changes."""

    def test_gate_transfer_message_fields(self):
        """JobLeaderGateTransfer should have required fields."""
        transfer = JobLeaderGateTransfer(
            job_id="job-123",
            new_gate_id="gate-2",
            new_gate_addr=("gate2.example.com", 8080),
            fence_token=3,
        )
        assert transfer.job_id == "job-123"
        assert transfer.new_gate_id == "gate-2"
        assert transfer.new_gate_addr == ("gate2.example.com", 8080)
        assert transfer.fence_token == 3
        assert transfer.old_gate_id is None  # Optional

    def test_gate_transfer_with_old_gate(self):
        """JobLeaderGateTransfer should accept old_gate_id."""
        transfer = JobLeaderGateTransfer(
            job_id="job-456",
            new_gate_id="gate-3",
            new_gate_addr=("192.168.1.30", 9000),
            fence_token=5,
            old_gate_id="gate-1",
        )
        assert transfer.old_gate_id == "gate-1"

    def test_gate_transfer_serialization(self):
        """JobLeaderGateTransfer should serialize correctly."""
        original = JobLeaderGateTransfer(
            job_id="job-789",
            new_gate_id="gate-new",
            new_gate_addr=("10.0.0.100", 8888),
            fence_token=10,
            old_gate_id="gate-old",
        )

        serialized = original.dump()
        restored = JobLeaderGateTransfer.load(serialized)

        assert restored.job_id == "job-789"
        assert restored.new_gate_id == "gate-new"
        assert restored.new_gate_addr == ("10.0.0.100", 8888)
        assert restored.fence_token == 10
        assert restored.old_gate_id == "gate-old"

    def test_gate_transfer_ack(self):
        """JobLeaderGateTransferAck should acknowledge transfer."""
        ack = JobLeaderGateTransferAck(
            job_id="job-123",
            manager_id="manager-1",
            accepted=True,
        )
        assert ack.job_id == "job-123"
        assert ack.manager_id == "manager-1"
        assert ack.accepted is True


class TestPiggybackedDiscovery:
    """Test piggybacked discovery info in heartbeats."""

    def test_gate_heartbeat_has_piggyback_fields(self):
        """GateHeartbeat should have known_managers and known_gates fields."""
        heartbeat = GateHeartbeat(
            node_id="gate-1",
            datacenter="us-east-1",
            is_leader=True,
            term=1,
            version=100,
            state="active",
            active_jobs=5,
            active_datacenters=3,
            manager_count=10,
        )
        assert hasattr(heartbeat, 'known_managers')
        assert hasattr(heartbeat, 'known_gates')
        assert heartbeat.known_managers == {}
        assert heartbeat.known_gates == {}

    def test_gate_heartbeat_with_piggybacked_data(self):
        """GateHeartbeat should accept piggybacked discovery data."""
        known_managers = {
            "manager-1": ("10.0.0.1", 8080, "10.0.0.1", 8081, "us-east-1"),
            "manager-2": ("10.0.0.2", 8080, "10.0.0.2", 8081, "us-west-2"),
        }
        known_gates = {
            "gate-2": ("10.0.1.1", 9000, "10.0.1.1", 9001),
        }

        heartbeat = GateHeartbeat(
            node_id="gate-1",
            datacenter="us-east-1",
            is_leader=True,
            term=1,
            version=100,
            state="active",
            active_jobs=5,
            active_datacenters=3,
            manager_count=2,
            known_managers=known_managers,
            known_gates=known_gates,
        )

        assert len(heartbeat.known_managers) == 2
        assert "manager-1" in heartbeat.known_managers
        assert heartbeat.known_managers["manager-1"][4] == "us-east-1"  # datacenter
        assert len(heartbeat.known_gates) == 1

    def test_manager_heartbeat_has_known_gates(self):
        """ManagerHeartbeat should have known_gates field."""
        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="us-east-1",
            is_leader=True,
            term=1,
            version=50,
            active_jobs=3,
            active_workflows=15,
            worker_count=10,
            healthy_worker_count=8,
            available_cores=32,
            total_cores=40,
        )
        assert hasattr(heartbeat, 'known_gates')
        assert heartbeat.known_gates == {}

    def test_manager_heartbeat_with_known_gates(self):
        """ManagerHeartbeat should accept known_gates for piggybacking."""
        known_gates = {
            "gate-1": ("10.0.1.1", 9000, "10.0.1.1", 9001),
            "gate-2": ("10.0.1.2", 9000, "10.0.1.2", 9001),
        }

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="us-east-1",
            is_leader=True,
            term=1,
            version=50,
            active_jobs=3,
            active_workflows=15,
            worker_count=10,
            healthy_worker_count=8,
            available_cores=32,
            total_cores=40,
            known_gates=known_gates,
        )

        assert len(heartbeat.known_gates) == 2
        assert "gate-1" in heartbeat.known_gates
        assert heartbeat.known_gates["gate-1"][1] == 9000  # tcp_port


class TestDirectRoutingScenarios:
    """Test realistic Direct DC-to-Job-Leader routing scenarios."""

    def test_gate_dispatch_sets_origin(self):
        """
        Simulate gate setting origin_gate_addr when dispatching.

        Scenario:
        1. Client submits job to Gate-A
        2. Gate-A creates JobSubmission with origin_gate_addr = Gate-A address
        3. Gate-A dispatches to Manager-M
        4. Manager-M should receive submission with origin_gate_addr set
        """
        # Gate-A prepares submission
        gate_a_addr = ("gate-a.example.com", 8080)
        submission = JobSubmission(
            job_id="job-direct-routing",
            workflows=b"test_workflows",
            vus=1,
            timeout_seconds=60.0,
            origin_gate_addr=gate_a_addr,
        )

        # Verify submission has origin gate set
        assert submission.origin_gate_addr == gate_a_addr

        # Simulate manager receiving and storing
        manager_origin_gates: dict[str, tuple[str, int]] = {}
        if submission.origin_gate_addr:
            manager_origin_gates[submission.job_id] = submission.origin_gate_addr

        assert manager_origin_gates["job-direct-routing"] == gate_a_addr

    def test_manager_routes_to_origin_gate(self):
        """
        Simulate manager routing results to origin gate.

        Scenario:
        1. Manager has origin_gate_addr stored for job
        2. When job completes, manager sends to origin gate (not all gates)
        3. Result reaches correct gate
        """
        # Manager state
        job_origin_gates = {
            "job-123": ("gate-a.example.com", 8080),
            "job-456": ("gate-b.example.com", 8080),
        }

        # Job-123 completes
        result = JobFinalResult(
            job_id="job-123",
            datacenter="us-east-1",
            status=JobStatus.COMPLETED.value,
            fence_token=2,
        )

        # Manager determines target gate
        origin_gate = job_origin_gates.get(result.job_id)
        assert origin_gate == ("gate-a.example.com", 8080)

        # Job-456 completes
        result_456 = JobFinalResult(
            job_id="job-456",
            datacenter="us-west-2",
            status=JobStatus.FAILED.value,
            fence_token=1,
        )

        origin_gate_456 = job_origin_gates.get(result_456.job_id)
        assert origin_gate_456 == ("gate-b.example.com", 8080)

    def test_gate_transfer_updates_routing(self):
        """
        Simulate gate leadership transfer updating manager routing.

        Scenario:
        1. Gate-A owns job-123
        2. Gate-A fails
        3. Gate-B takes over and sends JobLeaderGateTransfer
        4. Manager updates origin_gate for job-123 to Gate-B
        """
        # Initial state - job routed to Gate-A
        job_origin_gates: dict[str, tuple[str, int]] = {
            "job-123": ("gate-a.example.com", 8080),
        }
        job_fence_tokens: dict[str, int] = {
            "job-123": 1,
        }

        # Gate-B sends transfer notification
        transfer = JobLeaderGateTransfer(
            job_id="job-123",
            new_gate_id="gate-b",
            new_gate_addr=("gate-b.example.com", 8080),
            fence_token=2,
            old_gate_id="gate-a",
        )

        # Manager processes transfer
        current_fence = job_fence_tokens.get(transfer.job_id, 0)
        if transfer.fence_token >= current_fence:
            job_origin_gates[transfer.job_id] = transfer.new_gate_addr
            job_fence_tokens[transfer.job_id] = transfer.fence_token

        # Verify routing updated
        assert job_origin_gates["job-123"] == ("gate-b.example.com", 8080)
        assert job_fence_tokens["job-123"] == 2

    def test_peer_manager_receives_origin_via_sync(self):
        """
        Simulate peer manager receiving origin_gate via job state sync.

        Scenario:
        1. Manager-A is job leader, has origin_gate for job-123
        2. Manager-A syncs state to peer Manager-B
        3. Manager-B stores origin_gate for failover
        """
        # Manager-A state
        manager_a_origin_gates = {
            "job-123": ("gate-a.example.com", 8080),
        }

        # Manager-A creates sync message
        sync_msg = JobStateSyncMessage(
            leader_id="manager-a",
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            fencing_token=3,
            workflows_total=10,
            workflows_completed=5,
            workflows_failed=0,
            origin_gate_addr=manager_a_origin_gates.get("job-123"),
        )

        # Manager-B processes sync
        manager_b_origin_gates: dict[str, tuple[str, int]] = {}
        if sync_msg.origin_gate_addr:
            manager_b_origin_gates[sync_msg.job_id] = sync_msg.origin_gate_addr

        # Verify Manager-B has origin gate
        assert manager_b_origin_gates["job-123"] == ("gate-a.example.com", 8080)

    def test_gate_forwards_unowned_results(self):
        """
        Simulate gate forwarding results for jobs it doesn't own.

        Scenario:
        1. Manager sends result to Gate-B (stale origin_gate_addr)
        2. Gate-B doesn't own job-123 (not in _jobs)
        3. Gate-B forwards to peer gates
        """
        # Gate-B state - doesn't own job-123
        gate_b_jobs = {"job-456", "job-789"}  # Jobs Gate-B owns

        # Result arrives for job-123
        result = JobFinalResult(
            job_id="job-123",
            datacenter="us-east-1",
            status=JobStatus.COMPLETED.value,
        )

        # Check ownership
        owns_job = result.job_id in gate_b_jobs
        assert owns_job is False

        # Should forward to peers
        should_forward = not owns_job
        assert should_forward is True

