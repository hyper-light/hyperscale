"""
Integration tests for Raft-backed leadership failover.

Tests the complete failure chain: SWIM detects failure ->
Raft membership update -> leader election -> job leadership
takeover via dual paths (SWIM leader and per-job Raft leader).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
from hyperscale.distributed.nodes.gate.raft_integration import GateRaftIntegration
from hyperscale.distributed.nodes.manager.raft_integration import ManagerRaftIntegration


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


@pytest.fixture
def mock_task_runner():
    runner = MagicMock()
    runner.run = MagicMock()
    return runner


@pytest.fixture
def mock_job_manager():
    return MagicMock()


@pytest.fixture
def mock_gate_job_manager():
    return MagicMock()


@pytest.fixture
def mock_gate_state():
    return MagicMock()


@pytest.fixture
def mock_send_tcp():
    return AsyncMock(return_value=b"ok")


@pytest.fixture
def leadership_tracker():
    return JobLeadershipTracker[int](
        node_id="node-1",
        node_addr=("127.0.0.1", 8000),
    )


@pytest.fixture
def peer_leadership_tracker():
    return JobLeadershipTracker[int](
        node_id="node-2",
        node_addr=("127.0.0.2", 8000),
    )


# =========================================================================
# Manager Raft Leader Callback Tests
# =========================================================================


class TestManagerRaftLeaderTakeover:
    """Tests for manager per-job Raft leader takeover path."""

    def test_leader_callback_receives_job_id(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """When a Raft node wins election, callback receives correct job_id."""
        received_job_ids: list[str] = []

        def on_leader(job_id: str) -> None:
            received_job_ids.append(job_id)

        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_leader=on_leader,
        )
        integration.consensus._on_become_leader("job-abc")
        assert received_job_ids == ["job-abc"]

    @pytest.mark.asyncio
    async def test_membership_update_removes_dead_node(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """on_node_leave removes the dead node from all Raft groups."""
        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        integration.on_node_join("peer-1", ("10.0.0.2", 8000))
        integration.on_node_join("peer-2", ("10.0.0.3", 8000))

        await integration.consensus.create_job_raft("job-1")
        node = integration.consensus.get_node("job-1")
        assert "peer-1" in node._members
        assert "peer-2" in node._members

        integration.on_node_leave("peer-1")
        assert "peer-1" not in node._members
        assert "peer-2" in node._members

    @pytest.mark.asyncio
    async def test_quorum_size_decreases_after_node_leave(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """After a node leaves, quorum size recalculates for smaller cluster."""
        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        integration.on_node_join("peer-1", ("10.0.0.2", 8000))
        integration.on_node_join("peer-2", ("10.0.0.3", 8000))

        await integration.consensus.create_job_raft("job-1")
        node = integration.consensus.get_node("job-1")

        # 3 members (self + 2 peers), quorum = 2
        quorum_before = node._quorum_size()
        assert quorum_before == 2

        integration.on_node_leave("peer-2")

        # 2 members (self + 1 peer), quorum = 2
        quorum_after = node._quorum_size()
        assert quorum_after == 2

        # But if we also lose peer-1, only 1 member, quorum = 1
        integration.on_node_leave("peer-1")
        assert node._quorum_size() == 1

    @pytest.mark.asyncio
    async def test_lose_leader_callback_receives_job_id(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """When a node loses Raft leadership, lose callback receives job_id."""
        lost_job_ids: list[str] = []

        def on_lose(job_id: str) -> None:
            lost_job_ids.append(job_id)

        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_lose_leader=on_lose,
        )

        await integration.consensus.create_job_raft("job-1")
        node = integration.consensus.get_node("job-1")

        # Simulate losing leadership via step_down
        node._on_lose_leadership()
        assert lost_job_ids == ["job-1"]

    @pytest.mark.asyncio
    async def test_proposal_requires_raft_leadership(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Raft proposals fail when this node is not the per-job Raft leader."""
        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        await integration.consensus.create_job_raft("job-1")
        node = integration.consensus.get_node("job-1")
        assert node.role == "follower"

        # Proposal should fail because we're not the leader
        accepted = await integration.raft_job_manager.takeover_job_leadership("job-1")
        assert accepted is False


# =========================================================================
# Gate Raft Leader Callback Tests
# =========================================================================


class TestGateRaftLeaderTakeover:
    """Tests for gate per-job Raft leader takeover path."""

    def test_leader_callback_receives_job_id(
        self,
        mock_gate_job_manager,
        leadership_tracker,
        mock_gate_state,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Gate Raft leader callback receives correct job_id."""
        received_job_ids: list[str] = []

        def on_leader(job_id: str) -> None:
            received_job_ids.append(job_id)

        integration = GateRaftIntegration(
            node_id="gate-1",
            job_manager=mock_gate_job_manager,
            leadership_tracker=leadership_tracker,
            gate_state=mock_gate_state,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_leader=on_leader,
        )
        integration.consensus._on_become_leader("gate-job-1")
        assert received_job_ids == ["gate-job-1"]

    @pytest.mark.asyncio
    async def test_membership_update_removes_dead_gate(
        self,
        mock_gate_job_manager,
        leadership_tracker,
        mock_gate_state,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """on_node_leave removes the dead gate from all gate Raft groups."""
        integration = GateRaftIntegration(
            node_id="gate-1",
            job_manager=mock_gate_job_manager,
            leadership_tracker=leadership_tracker,
            gate_state=mock_gate_state,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        integration.on_node_join("gate-2", ("10.0.0.2", 9000))

        await integration.consensus.create_job_raft("gate-job-1")
        node = integration.consensus.get_node("gate-job-1")
        assert "gate-2" in node._members

        integration.on_node_leave("gate-2")
        assert "gate-2" not in node._members

    @pytest.mark.asyncio
    async def test_proposal_requires_gate_raft_leadership(
        self,
        mock_gate_job_manager,
        leadership_tracker,
        mock_gate_state,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Gate Raft proposals fail when not the per-job Raft leader."""
        integration = GateRaftIntegration(
            node_id="gate-1",
            job_manager=mock_gate_job_manager,
            leadership_tracker=leadership_tracker,
            gate_state=mock_gate_state,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        await integration.consensus.create_job_raft("gate-job-1")
        node = integration.consensus.get_node("gate-job-1")
        assert node.role == "follower"

        accepted = await integration.raft_job_manager.takeover_gate_leadership("gate-job-1")
        assert accepted is False


# =========================================================================
# Leadership Tracker Fencing Token Tests
# =========================================================================


class TestLeadershipTrackerFencing:
    """Tests that fencing tokens prevent stale leadership claims."""

    def test_takeover_increments_fencing_token(
        self, leadership_tracker: JobLeadershipTracker
    ) -> None:
        """Takeover increments the fencing token monotonically."""
        leadership_tracker.assume_leadership("job-1", initial_token=1)
        assert leadership_tracker.get_fencing_token("job-1") == 1

        leadership_tracker.takeover_leadership("job-1")
        assert leadership_tracker.get_fencing_token("job-1") == 2

        leadership_tracker.takeover_leadership("job-1")
        assert leadership_tracker.get_fencing_token("job-1") == 3

    def test_stale_leadership_claim_rejected(
        self, leadership_tracker: JobLeadershipTracker
    ) -> None:
        """Claims with lower fencing tokens are rejected."""
        leadership_tracker.assume_leadership("job-1", initial_token=5)

        # Lower token should be rejected
        accepted = leadership_tracker.process_leadership_claim(
            job_id="job-1",
            claimer_id="stale-node",
            claimer_addr=("10.0.0.99", 8000),
            fencing_token=3,
        )
        assert accepted is False
        assert leadership_tracker.get_leader("job-1") == "node-1"

    def test_higher_fencing_token_accepted(
        self, leadership_tracker: JobLeadershipTracker
    ) -> None:
        """Claims with higher fencing tokens are accepted."""
        leadership_tracker.assume_leadership("job-1", initial_token=1)

        accepted = leadership_tracker.process_leadership_claim(
            job_id="job-1",
            claimer_id="new-leader",
            claimer_addr=("10.0.0.99", 8000),
            fencing_token=10,
        )
        assert accepted is True
        assert leadership_tracker.get_leader("job-1") == "new-leader"
        assert leadership_tracker.get_fencing_token("job-1") == 10

    def test_release_clears_leadership(
        self, leadership_tracker: JobLeadershipTracker
    ) -> None:
        """Releasing leadership removes the job from tracking."""
        leadership_tracker.assume_leadership("job-1", initial_token=1)
        assert leadership_tracker.is_leader("job-1") is True

        leadership_tracker.release_leadership("job-1")
        assert leadership_tracker.is_leader("job-1") is False
        assert leadership_tracker.get_leader("job-1") is None

    def test_get_all_leaderships_for_orphan_scan(
        self, leadership_tracker: JobLeadershipTracker
    ) -> None:
        """get_all_leaderships provides data for orphan scanning."""
        leadership_tracker.assume_leadership("job-1", initial_token=1)
        leadership_tracker.assume_leadership("job-2", initial_token=1)
        leadership_tracker.process_leadership_claim(
            job_id="job-3",
            claimer_id="peer-node",
            claimer_addr=("10.0.0.2", 8000),
            fencing_token=5,
        )

        all_leaderships = leadership_tracker.get_all_leaderships()
        assert len(all_leaderships) == 3

        job_ids = {entry[0] for entry in all_leaderships}
        assert job_ids == {"job-1", "job-2", "job-3"}


# =========================================================================
# Dual-Path Failover Coverage Tests
# =========================================================================


class TestDualPathFailover:
    """Tests that both SWIM leader path and Raft leader callback path cover all cases."""

    @pytest.mark.asyncio
    async def test_create_job_raft_is_idempotent(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Creating a Raft instance for an existing job returns True (idempotent)."""
        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        result_first = await integration.consensus.create_job_raft("job-1")
        assert result_first is True

        result_second = await integration.consensus.create_job_raft("job-1")
        assert result_second is True

        # Only one instance should exist
        assert integration.consensus.active_instance_count == 1

    @pytest.mark.asyncio
    async def test_backpressure_at_max_instances(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Raft instance creation is rejected when at capacity."""
        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        # Set a low limit for testing
        integration.consensus._max_instances = 2

        result_1 = await integration.consensus.create_job_raft("job-1")
        result_2 = await integration.consensus.create_job_raft("job-2")
        result_3 = await integration.consensus.create_job_raft("job-3")

        assert result_1 is True
        assert result_2 is True
        assert result_3 is False
        assert integration.consensus.active_instance_count == 2

    @pytest.mark.asyncio
    async def test_destroy_job_raft_cleans_up(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Destroying a job Raft instance releases all memory."""
        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        await integration.consensus.create_job_raft("job-1")
        assert integration.consensus.active_instance_count == 1

        await integration.consensus.destroy_job_raft("job-1")
        assert integration.consensus.active_instance_count == 0
        assert integration.consensus.get_node("job-1") is None

    @pytest.mark.asyncio
    async def test_proposal_to_nonexistent_job_fails_gracefully(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Proposing to a job without a Raft instance fails gracefully."""
        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        # No Raft instance created for job-1
        accepted = await integration.raft_job_manager.takeover_job_leadership("job-1")
        assert accepted is False

    @pytest.mark.asyncio
    async def test_multiple_jobs_independent_raft_groups(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Each job has independent Raft state."""
        integration = ManagerRaftIntegration(
            node_id="node-1",
            job_manager=mock_job_manager,
            leadership_tracker=leadership_tracker,
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
        )

        await integration.consensus.create_job_raft("job-1")
        await integration.consensus.create_job_raft("job-2")

        node_1 = integration.consensus.get_node("job-1")
        node_2 = integration.consensus.get_node("job-2")

        assert node_1 is not None
        assert node_2 is not None
        assert node_1 is not node_2
        assert node_1._job_id == "job-1"
        assert node_2._job_id == "job-2"
