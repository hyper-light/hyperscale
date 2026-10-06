"""
Integration tests for Raft node integration (Phase 5).

Tests that ManagerRaftIntegration and GateRaftIntegration correctly
wire Raft consensus into the server lifecycle, TCP handlers, and the
cluster membership group (AD-52 slice C) that job groups take their
members from.
"""

import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
from hyperscale.distributed.nodes.gate.raft_integration import GateRaftIntegration
from hyperscale.distributed.nodes.manager.raft_integration import ManagerRaftIntegration
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
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
def leadership_tracker():
    return JobLeadershipTracker[int](
        node_id="node-1",
        node_addr=("127.0.0.1", 8000),
    )


@pytest.fixture
def mock_send_tcp():
    return AsyncMock(return_value=b"ok")


@pytest.fixture
def cluster_members_view():
    """The cluster's committed membership as the integration reads it --
    replaced whole when it changes, as ``ClusterMembership.node_addresses``
    is. Only this node until the cluster forms."""
    return [{"node-1": ("127.0.0.1", 8000)}]


@pytest.fixture
def manager_integration(
    mock_job_manager,
    leadership_tracker,
    mock_logger,
    mock_task_runner,
    mock_send_tcp,
    cluster_members_view,
):
    return ManagerRaftIntegration(
        cluster_members=lambda: cluster_members_view[0],
        clock=new_hybrid_logical_clock(),
        may_lead=lambda: True,
        ledger_replica=JobLedgerReplica(),
        request_timeout_seconds=5.0,
        node_id="node-1",
        logger=mock_logger,
        task_runner=mock_task_runner,
        send_tcp=mock_send_tcp, storage=VolatileRaftStorage()
    )


@pytest.fixture
def gate_integration(
    mock_gate_job_manager,
    leadership_tracker,
    mock_gate_state,
    mock_logger,
    mock_task_runner,
    mock_send_tcp,
    cluster_members_view,
):
    return GateRaftIntegration(
        cluster_members=lambda: cluster_members_view[0],
        clock=new_hybrid_logical_clock(),
        may_lead=lambda: True,
        ledger_replica=JobLedgerReplica(),
        request_timeout_seconds=5.0,
        cluster_size=lambda: 3,
        proposal_timeout_seconds=5.0,
        node_id="node-1",
        logger=mock_logger,
        task_runner=mock_task_runner,
        send_tcp=mock_send_tcp, storage=VolatileRaftStorage()
    )


# =========================================================================
# Manager Integration Tests
# =========================================================================


class TestManagerRaftIntegration:
    """Tests for ManagerRaftIntegration wiring."""

    def test_consensus_property(self, manager_integration: ManagerRaftIntegration) -> None:
        """Consensus property returns the underlying RaftConsensus."""
        consensus = manager_integration.consensus
        assert consensus is not None
        assert consensus._node_id == "node-1"

    async def test_start_begins_tick_loop(self, manager_integration: ManagerRaftIntegration) -> None:
        """start() initiates the Raft tick loop."""
        await manager_integration.start()
        assert manager_integration.consensus._tick_running is True

    @pytest.mark.asyncio
    async def test_stop_destroys_all(self, manager_integration: ManagerRaftIntegration) -> None:
        """stop() destroys all Raft instances and stops ticking."""
        await manager_integration.start()
        await manager_integration.stop()
        assert manager_integration.consensus._tick_running is False
        assert len(manager_integration.consensus._nodes) == 0

    def test_job_groups_take_their_members_from_the_cluster_membership(
        self,
        manager_integration: ManagerRaftIntegration,
        cluster_members_view: list[dict[str, tuple[str, int]]],
    ) -> None:
        """Before the cluster forms only this node is known; once it has,
        a job group's voters and every peer's address come from the
        committed membership."""
        consensus = manager_integration.consensus
        assert consensus.current_members() == frozenset({"node-1"})
        assert consensus.member_addresses() == {}

        cluster_members_view[0] = {
            "node-1": ("127.0.0.1", 8000),
            "peer-1": ("10.0.0.2", 8000),
            "peer-2": ("10.0.0.3", 8000),
        }

        assert consensus.current_members() == frozenset({"node-1", "peer-1", "peer-2"})
        assert consensus.member_address("peer-1") == ("10.0.0.2", 8000)
        assert consensus.member_addresses() == {
            "peer-1": ("10.0.0.2", 8000),
            "peer-2": ("10.0.0.3", 8000),
        }

    @pytest.mark.asyncio
    async def test_handle_request_vote_roundtrip(
        self, manager_integration: ManagerRaftIntegration
    ) -> None:
        """RequestVote handler deserializes, routes, and serializes response."""
        # Create a Raft instance for a job first
        await manager_integration.consensus.create_job_raft(
            "job-1", manager_integration.consensus.current_members()
        )

        request = RequestVote(
            job_id="job-1",
            term=1,
            candidate_id="peer-1",
            last_log_index=0,
            last_log_term=0,
        )
        response_bytes = await manager_integration.handle_request_vote(request.dump())
        assert response_bytes is not None
        response = RequestVoteResponse.load(response_bytes)
        assert response.job_id == "job-1"

    @pytest.mark.asyncio
    async def test_handle_append_entries_roundtrip(
        self, manager_integration: ManagerRaftIntegration
    ) -> None:
        """AppendEntries handler deserializes, routes, and serializes response."""
        await manager_integration.consensus.create_job_raft(
            "job-1", manager_integration.consensus.current_members()
        )

        request = AppendEntries(
            job_id="job-1",
            term=1,
            leader_id="peer-1",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        response_bytes = await manager_integration.handle_append_entries(request.dump())
        assert response_bytes is not None
        response = AppendEntriesResponse.load(response_bytes)
        assert response.job_id == "job-1"

    @pytest.mark.asyncio
    async def test_send_raft_message_dispatches_tcp(
        self, manager_integration: ManagerRaftIntegration, mock_send_tcp: AsyncMock
    ) -> None:
        """_send_raft_message only enqueues; _exchange uses the correct TCP method name."""
        vote = RequestVote(
            job_id="job-1",
            term=1,
            candidate_id="node-1",
            last_log_index=0,
            last_log_term=0,
        )
        await manager_integration._send_raft_message(("10.0.0.2", 8000), vote)
        mock_send_tcp.assert_not_called()
        assert manager_integration._outbox.pending_count == 1

        mock_send_tcp.return_value = b""
        await manager_integration._exchange(("10.0.0.2", 8000), vote)
        mock_send_tcp.assert_called_once()
        call_args = mock_send_tcp.call_args
        assert call_args[0][1] == "raft_request_vote"


# =========================================================================
# Gate Integration Tests
# =========================================================================


class TestGateRaftIntegration:
    """Tests for GateRaftIntegration wiring."""

    def test_consensus_property(self, gate_integration: GateRaftIntegration) -> None:
        """Consensus property returns the underlying GateRaftConsensus."""
        consensus = gate_integration.consensus
        assert consensus is not None
        assert consensus._node_id == "node-1"

    async def test_start_begins_tick_loop(self, gate_integration: GateRaftIntegration) -> None:
        """start() initiates the gate Raft tick loop."""
        await gate_integration.start()
        assert gate_integration.consensus._tick_running is True

    @pytest.mark.asyncio
    async def test_stop_destroys_all(self, gate_integration: GateRaftIntegration) -> None:
        """stop() destroys all gate Raft instances."""
        await gate_integration.start()
        await gate_integration.stop()
        assert gate_integration.consensus._tick_running is False

    def test_job_groups_take_their_members_from_the_cluster_membership(
        self,
        gate_integration: GateRaftIntegration,
        cluster_members_view: list[dict[str, tuple[str, int]]],
    ) -> None:
        """A gate job group's voters and peers' addresses come from the
        gate cluster's committed membership."""
        consensus = gate_integration.consensus
        assert consensus.current_members() == frozenset({"node-1"})

        cluster_members_view[0] = {
            "node-1": ("127.0.0.1", 8000),
            "gate-2": ("10.0.0.2", 9000),
        }

        assert consensus.current_members() == frozenset({"node-1", "gate-2"})
        assert consensus.member_addresses() == {"gate-2": ("10.0.0.2", 9000)}

    @pytest.mark.asyncio
    async def test_handle_request_vote_roundtrip(
        self, gate_integration: GateRaftIntegration
    ) -> None:
        """RequestVote handler roundtrips through gate Raft."""
        await gate_integration.consensus.create_job_raft(
            "gate-job-1", gate_integration.consensus.current_members()
        )

        request = RequestVote(
            job_id="gate-job-1",
            term=1,
            candidate_id="gate-2",
            last_log_index=0,
            last_log_term=0,
        )
        response_bytes = await gate_integration.handle_request_vote(request.dump())
        assert response_bytes is not None
        response = RequestVoteResponse.load(response_bytes)
        assert response.job_id == "gate-job-1"

    @pytest.mark.asyncio
    async def test_handle_append_entries_roundtrip(
        self, gate_integration: GateRaftIntegration
    ) -> None:
        """AppendEntries handler roundtrips through gate Raft."""
        await gate_integration.consensus.create_job_raft(
            "gate-job-1", gate_integration.consensus.current_members()
        )

        request = AppendEntries(
            job_id="gate-job-1",
            term=1,
            leader_id="gate-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        response_bytes = await gate_integration.handle_append_entries(request.dump())
        assert response_bytes is not None
        response = AppendEntriesResponse.load(response_bytes)
        assert response.job_id == "gate-job-1"

    @pytest.mark.asyncio
    async def test_send_raft_message_uses_gate_prefix(
        self, gate_integration: GateRaftIntegration, mock_send_tcp: AsyncMock
    ) -> None:
        """Gate _exchange uses gate_raft_ prefixed method names."""
        vote = RequestVote(
            job_id="gate-job-1",
            term=1,
            candidate_id="node-1",
            last_log_index=0,
            last_log_term=0,
        )
        mock_send_tcp.return_value = (b"", 0)
        await gate_integration._exchange(("10.0.0.2", 9000), vote)
        mock_send_tcp.assert_called_once()
        call_args = mock_send_tcp.call_args
        assert call_args[0][1] == "gate_raft_request_vote"

    @pytest.mark.asyncio
    async def test_send_append_entries_uses_gate_prefix(
        self, gate_integration: GateRaftIntegration, mock_send_tcp: AsyncMock
    ) -> None:
        """Gate _exchange uses the gate_raft_append_entries method name."""
        append = AppendEntries(
            job_id="gate-job-1",
            term=1,
            leader_id="node-1",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        mock_send_tcp.return_value = (b"", 0)
        await gate_integration._exchange(("10.0.0.2", 9000), append)
        mock_send_tcp.assert_called_once()
        call_args = mock_send_tcp.call_args
        assert call_args[0][1] == "gate_raft_append_entries"


# =========================================================================
# Cross-Integration Tests
# =========================================================================


class TestCrossIntegration:
    """Tests that manager and gate integrations can exchange Raft messages."""

    @pytest.mark.asyncio
    async def test_manager_vote_processed_by_gate(
        self,
        manager_integration: ManagerRaftIntegration,
        gate_integration: GateRaftIntegration,
    ) -> None:
        """A RequestVote serialized by manager can be deserialized by gate."""
        await gate_integration.consensus.create_job_raft(
            "shared-job", gate_integration.consensus.current_members()
        )

        vote = RequestVote(
            job_id="shared-job",
            term=1,
            candidate_id="node-1",
            last_log_index=0,
            last_log_term=0,
        )
        serialized = vote.dump()
        response_bytes = await gate_integration.handle_request_vote(serialized)
        assert response_bytes is not None

    @pytest.mark.asyncio
    async def test_gate_append_entries_processed_by_manager(
        self,
        manager_integration: ManagerRaftIntegration,
        gate_integration: GateRaftIntegration,
    ) -> None:
        """AppendEntries serialized by gate can be deserialized by manager."""
        await manager_integration.consensus.create_job_raft(
            "shared-job", manager_integration.consensus.current_members()
        )

        append = AppendEntries(
            job_id="shared-job",
            term=1,
            leader_id="node-1",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        serialized = append.dump()
        response_bytes = await manager_integration.handle_append_entries(serialized)
        assert response_bytes is not None

    @pytest.mark.asyncio
    async def test_cleanup_on_stop(
        self,
        manager_integration: ManagerRaftIntegration,
        gate_integration: GateRaftIntegration,
    ) -> None:
        """Both integrations clean up all resources on stop."""
        await manager_integration.start()
        await gate_integration.start()

        await manager_integration.consensus.create_job_raft(
            "job-1", manager_integration.consensus.current_members()
        )
        await gate_integration.consensus.create_job_raft(
            "job-1", gate_integration.consensus.current_members()
        )

        assert manager_integration.consensus.active_instance_count == 1
        assert gate_integration.consensus.active_instance_count == 1

        await manager_integration.stop()
        await gate_integration.stop()

        assert manager_integration.consensus.active_instance_count == 0
        assert gate_integration.consensus.active_instance_count == 0


# =========================================================================
# Raft Leader Callback Tests
# =========================================================================


class TestManagerRaftLeaderCallbacks:
    """Tests for per-job Raft leader callbacks wiring."""

    def test_leader_callback_passed_to_consensus(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """on_job_raft_leader callback is forwarded to RaftConsensus."""
        callback = MagicMock()
        integration = ManagerRaftIntegration(
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            request_timeout_seconds=5.0,
            cluster_members=lambda: {},
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_leader=callback, storage=VolatileRaftStorage()
        )
        assert integration.consensus._on_become_leader is callback

    def test_lose_leader_callback_passed_to_consensus(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """on_job_raft_lose_leader callback is forwarded to RaftConsensus."""
        callback = MagicMock()
        integration = ManagerRaftIntegration(
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            request_timeout_seconds=5.0,
            cluster_members=lambda: {},
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_lose_leader=callback, storage=VolatileRaftStorage()
        )
        assert integration.consensus._on_lose_leadership is callback

    @pytest.mark.asyncio
    async def test_leader_callback_invoked_on_job_raft_creation(
        self,
        mock_job_manager,
        leadership_tracker,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Per-job RaftNode receives wrapped callback that passes job_id."""
        invoked_jobs: list[str] = []

        def on_leader(job_id: str) -> None:
            invoked_jobs.append(job_id)

        integration = ManagerRaftIntegration(
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            request_timeout_seconds=5.0,
            cluster_members=lambda: {},
            node_id="node-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_leader=on_leader, storage=VolatileRaftStorage()
        )

        await integration.consensus.create_job_raft(
            "job-1", integration.consensus.current_members()
        )
        node = integration.consensus.get_node("job-1")
        assert node is not None

        # Simulate the callback being triggered (as RaftNode would call it)
        node._on_become_leader()
        assert invoked_jobs == ["job-1"]

    def test_no_callback_when_not_provided(
        self, manager_integration: ManagerRaftIntegration
    ) -> None:
        """No callbacks passed means RaftConsensus has None for callbacks."""
        assert manager_integration.consensus._on_become_leader is None
        assert manager_integration.consensus._on_lose_leadership is None


class TestGateRaftLeaderCallbacks:
    """Tests for gate per-job Raft leader callbacks wiring."""

    def test_leader_callback_passed_to_consensus(
        self,
        mock_gate_job_manager,
        leadership_tracker,
        mock_gate_state,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """on_job_raft_leader callback is forwarded to GateRaftConsensus."""
        callback = MagicMock()
        integration = GateRaftIntegration(
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            request_timeout_seconds=5.0,
            cluster_members=lambda: {},
            cluster_size=lambda: 3,
            proposal_timeout_seconds=5.0,
            node_id="gate-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_leader=callback, storage=VolatileRaftStorage()
        )
        assert integration.consensus._on_become_leader is callback

    def test_lose_leader_callback_passed_to_consensus(
        self,
        mock_gate_job_manager,
        leadership_tracker,
        mock_gate_state,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """on_job_raft_lose_leader callback is forwarded to GateRaftConsensus."""
        callback = MagicMock()
        integration = GateRaftIntegration(
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            request_timeout_seconds=5.0,
            cluster_members=lambda: {},
            cluster_size=lambda: 3,
            proposal_timeout_seconds=5.0,
            node_id="gate-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_lose_leader=callback, storage=VolatileRaftStorage()
        )
        assert integration.consensus._on_lose_leadership is callback

    @pytest.mark.asyncio
    async def test_leader_callback_invoked_on_job_raft_creation(
        self,
        mock_gate_job_manager,
        leadership_tracker,
        mock_gate_state,
        mock_logger,
        mock_task_runner,
        mock_send_tcp,
    ) -> None:
        """Per-job gate RaftNode receives wrapped callback that passes job_id."""
        invoked_jobs: list[str] = []

        def on_leader(job_id: str) -> None:
            invoked_jobs.append(job_id)

        integration = GateRaftIntegration(
            clock=new_hybrid_logical_clock(),
            may_lead=lambda: True,
            ledger_replica=JobLedgerReplica(),
            request_timeout_seconds=5.0,
            cluster_members=lambda: {},
            cluster_size=lambda: 3,
            proposal_timeout_seconds=5.0,
            node_id="gate-1",
            logger=mock_logger,
            task_runner=mock_task_runner,
            send_tcp=mock_send_tcp,
            on_job_raft_leader=on_leader, storage=VolatileRaftStorage()
        )

        await integration.consensus.create_job_raft(
            "gate-job-1", integration.consensus.current_members()
        )
        node = integration.consensus.get_node("gate-job-1")
        assert node is not None

        # Simulate the callback being triggered
        node._on_become_leader()
        assert invoked_jobs == ["gate-job-1"]

    def test_no_callback_when_not_provided(
        self, gate_integration: GateRaftIntegration
    ) -> None:
        """No callbacks passed means GateRaftConsensus has None for callbacks."""
        assert gate_integration.consensus._on_become_leader is None
        assert gate_integration.consensus._on_lose_leadership is None
