"""
Integration tests for Raft-backed leadership failover.

Tests the complete failure chain: SWIM detects failure -> the group's
Raft leader moves the group's membership through its log (AD-52 slice B)
-> leader election -> job leadership takeover via dual paths (SWIM leader
and per-job Raft leader).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.env import Env
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.raft.models.ledger_append_command import LedgerAppendCommand
from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.raft.models import AppendEntriesResponse, RequestVoteResponse
from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
from hyperscale.distributed.nodes.gate.raft_integration import GateRaftIntegration
from hyperscale.distributed.nodes.manager.raft_integration import ManagerRaftIntegration
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock


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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_leader=on_leader, request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )
        integration.consensus._on_become_leader("job-abc")
        assert received_job_ids == ["job-abc"]

    @pytest.mark.asyncio
    async def test_a_member_that_leaves_is_removed_through_the_groups_log(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """A member that leaves the cluster's committed membership stops
        being reached at once, but stays a voter until the group's Raft
        leader takes it out through the group's log -- a joint
        configuration first -- so no member counts a quorum the others
        would not (AD-52 slices B/C)."""
        # The cluster's committed membership (AD-52 slice C): a new mapping
        # each time the configuration changes, as ClusterMembership gives.
        cluster_members = {
            "node-1": ("10.0.0.1", 8000),
            "peer-1": ("10.0.0.2", 8000),
            "peer-2": ("10.0.0.3", 8000),
        }
        integration = ManagerRaftIntegration(
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD,
            cluster_members=lambda: cluster_members,
            storage=VolatileRaftStorage(),
        )

        await integration.consensus.create_job_raft(
            "job-1", integration.consensus.current_members()
        )
        node = integration.consensus.get_node("job-1")
        assert node.configuration.voters == {"node-1", "peer-1", "peer-2"}

        cluster_members = {
            member: address for member, address in cluster_members.items() if member != "peer-1"
        }
        assert node.configuration.voters == {"node-1", "peer-1", "peer-2"}
        assert integration.consensus.member_address("peer-1") is None

        await node.start_election()
        await node.handle_request_vote_response(
            RequestVoteResponse(
                job_id="job-1", term=node.current_term, vote_granted=True, voter_id="peer-2",
            )
        )
        assert node.is_leader()
        # The term-start entry commits before membership may change.
        await node.handle_append_entries_response(
            AppendEntriesResponse(
                job_id="job-1",
                term=node.current_term,
                success=True,
                follower_id="peer-2",
                match_index=node.last_log_index,
            )
        )

        changed = await node.reconcile_membership(integration.consensus.current_members())

        assert changed
        assert node.configuration.voters == {"node-1", "peer-2"}
        assert node.configuration.outgoing_voters == {"node-1", "peer-1", "peer-2"}

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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_lose_leader=on_lose, request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )

        await integration.consensus.create_job_raft("job-1", frozenset({"node-1"}))
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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp, request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )

        await integration.consensus.create_job_raft(
            "job-1", frozenset({"node-1", "node-2", "node-3"})
        )
        node = integration.consensus.get_node("job-1")
        assert node.role == "follower"

        # Proposal should fail because we're not the leader
        accepted = (await integration.consensus.propose_command("job-1", LedgerAppendCommand(job_id="job-1", ledger_event_type=JobEventType.JOB_LEADERSHIP_ACQUIRED, ledger_payload=b"")))[0]
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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            cluster_size=lambda: 3,
            proposal_timeout_seconds=5.0,
            node_id="gate-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_leader=on_leader, request_timeout_seconds=Env().GATE_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )
        integration.consensus._on_become_leader("gate-job-1")
        assert received_job_ids == ["gate-job-1"]

    @pytest.mark.asyncio
    async def test_a_gate_group_keeps_voters_it_could_not_commit_without(
        self,
        mock_gate_job_manager,
        leadership_tracker,
        mock_gate_state,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """A gate that leaves the cluster's committed membership stays a
        voter of the job's group while too few gates would remain to reach
        the group's quorum floor (a majority of the configured cohort):
        that change could never commit, and the group would be stuck in it
        (AD-52 slice B)."""
        # The cluster's committed membership (AD-52 slice C): a new mapping
        # each time the configuration changes, as ClusterMembership gives.
        cluster_members = {
            "gate-1": ("10.0.0.1", 9000),
            "gate-2": ("10.0.0.2", 9000),
        }
        integration = GateRaftIntegration(
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            cluster_size=lambda: 3,
            proposal_timeout_seconds=5.0,
            node_id="gate-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            request_timeout_seconds=Env().GATE_TCP_TIMEOUT_STANDARD,
            cluster_members=lambda: cluster_members,
            storage=VolatileRaftStorage(),
        )

        await integration.consensus.create_job_raft(
            "gate-job-1", integration.consensus.current_members()
        )
        node = integration.consensus.get_node("gate-job-1")
        await node.start_election()
        await node.handle_request_vote_response(
            RequestVoteResponse(
                job_id="gate-job-1", term=node.current_term, vote_granted=True, voter_id="gate-2",
            )
        )
        await node.handle_append_entries_response(
            AppendEntriesResponse(
                job_id="gate-job-1",
                term=node.current_term,
                success=True,
                follower_id="gate-2",
                match_index=node.last_log_index,
            )
        )
        assert node.is_leader()

        cluster_members = {"gate-1": ("10.0.0.1", 9000)}
        changed = await node.reconcile_membership(integration.consensus.current_members())

        assert not changed
        assert node.configuration.voters == {"gate-1", "gate-2"}

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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            cluster_size=lambda: 3,
            proposal_timeout_seconds=5.0,
            node_id="gate-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp, request_timeout_seconds=Env().GATE_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )

        await integration.consensus.create_job_raft(
            "gate-job-1", frozenset({"gate-1", "gate-2", "gate-3"})
        )
        node = integration.consensus.get_node("gate-job-1")
        assert node.role == "follower"

        accepted = (await integration.consensus.propose_command("gate-job-1", LedgerAppendCommand(job_id="gate-job-1", ledger_event_type=JobEventType.JOB_LEADERSHIP_ACQUIRED, ledger_payload=b"")))[0]
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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp, request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )

        result_first = await integration.consensus.create_job_raft("job-1", frozenset({"node-1"}))
        assert result_first is True

        result_second = await integration.consensus.create_job_raft("job-1", frozenset({"node-1"}))
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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp, request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )

        # Set a low limit for testing
        integration.consensus._max_instances = 2

        result_1 = await integration.consensus.create_job_raft("job-1", frozenset({"node-1"}))
        result_2 = await integration.consensus.create_job_raft("job-2", frozenset({"node-1"}))
        result_3 = await integration.consensus.create_job_raft("job-3", frozenset({"node-1"}))

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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp, request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )

        await integration.consensus.create_job_raft("job-1", frozenset({"node-1"}))
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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp, request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )

        # No Raft instance created for job-1
        accepted = (await integration.consensus.propose_command("job-1", LedgerAppendCommand(job_id="job-1", ledger_event_type=JobEventType.JOB_LEADERSHIP_ACQUIRED, ledger_payload=b"")))[0]
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
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp, request_timeout_seconds=Env().MANAGER_TCP_TIMEOUT_STANDARD, cluster_members=lambda: {}, storage=VolatileRaftStorage(),
        )

        await integration.consensus.create_job_raft("job-1", frozenset({"node-1"}))
        await integration.consensus.create_job_raft("job-2", frozenset({"node-1"}))

        node_1 = integration.consensus.get_node("job-1")
        node_2 = integration.consensus.get_node("job-2")

        assert node_1 is not None
        assert node_2 is not None
        assert node_1 is not node_2
        assert node_1._job_id == "job-1"
        assert node_2._job_id == "job-2"
