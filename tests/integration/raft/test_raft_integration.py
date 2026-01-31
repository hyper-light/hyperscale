"""
Integration tests for Raft node integration (Phase 5).

Tests that ManagerRaftIntegration and GateRaftIntegration correctly
wire Raft consensus into the server lifecycle, TCP handlers, and
SWIM membership callbacks.
"""

import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
from hyperscale.distributed.nodes.gate.raft_integration import GateRaftIntegration
from hyperscale.distributed.nodes.manager.raft_integration import ManagerRaftIntegration
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)


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
def manager_integration(
    mock_job_manager,
    leadership_tracker,
    mock_logger,
    mock_task_runner,
    mock_send_tcp,
):
    return ManagerRaftIntegration(
        node_id="node-1",
        job_manager=mock_job_manager,
        leadership_tracker=leadership_tracker,
        logger=mock_logger,
        task_runner=mock_task_runner,
        send_tcp=mock_send_tcp,
    )


@pytest.fixture
def gate_integration(
    mock_gate_job_manager,
    leadership_tracker,
    mock_gate_state,
    mock_logger,
    mock_task_runner,
    mock_send_tcp,
):
    return GateRaftIntegration(
        node_id="node-1",
        job_manager=mock_gate_job_manager,
        leadership_tracker=leadership_tracker,
        gate_state=mock_gate_state,
        logger=mock_logger,
        task_runner=mock_task_runner,
        send_tcp=mock_send_tcp,
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

    def test_raft_job_manager_property(self, manager_integration: ManagerRaftIntegration) -> None:
        """Raft job manager property returns the wrapper."""
        raft_jm = manager_integration.raft_job_manager
        assert raft_jm is not None
        assert raft_jm._node_id == "node-1"

    def test_start_begins_tick_loop(self, manager_integration: ManagerRaftIntegration) -> None:
        """start() initiates the Raft tick loop."""
        manager_integration.start()
        assert manager_integration.consensus._tick_running is True

    @pytest.mark.asyncio
    async def test_stop_destroys_all(self, manager_integration: ManagerRaftIntegration) -> None:
        """stop() destroys all Raft instances and stops ticking."""
        manager_integration.start()
        await manager_integration.stop()
        assert manager_integration.consensus._tick_running is False
        assert len(manager_integration.consensus._nodes) == 0

    def test_membership_on_node_join(self, manager_integration: ManagerRaftIntegration) -> None:
        """on_node_join updates Raft cluster membership."""
        manager_integration.on_node_join("peer-1", ("10.0.0.2", 8000))
        assert "peer-1" in manager_integration.consensus._members
        assert manager_integration.consensus._member_addrs["peer-1"] == ("10.0.0.2", 8000)

    def test_membership_on_node_leave(self, manager_integration: ManagerRaftIntegration) -> None:
        """on_node_leave removes peer from Raft cluster membership."""
        manager_integration.on_node_join("peer-1", ("10.0.0.2", 8000))
        manager_integration.on_node_leave("peer-1")
        assert "peer-1" not in manager_integration.consensus._members
        assert "peer-1" not in manager_integration.consensus._member_addrs

    def test_set_initial_membership(self, manager_integration: ManagerRaftIntegration) -> None:
        """set_initial_membership seeds the Raft cluster from SWIM state."""
        members = {"peer-1", "peer-2"}
        addrs = {
            "peer-1": ("10.0.0.2", 8000),
            "peer-2": ("10.0.0.3", 8000),
        }
        manager_integration.set_initial_membership(members, addrs)
        assert manager_integration.consensus._members == members
        assert manager_integration.consensus._member_addrs == addrs

    @pytest.mark.asyncio
    async def test_handle_request_vote_roundtrip(
        self, manager_integration: ManagerRaftIntegration
    ) -> None:
        """RequestVote handler deserializes, routes, and serializes response."""
        # Create a Raft instance for a job first
        await manager_integration.consensus.create_job_raft("job-1")

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
        await manager_integration.consensus.create_job_raft("job-1")

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
        """_send_raft_message routes message types to correct TCP method names."""
        vote = RequestVote(
            job_id="job-1",
            term=1,
            candidate_id="node-1",
            last_log_index=0,
            last_log_term=0,
        )
        await manager_integration._send_raft_message(("10.0.0.2", 8000), vote)
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

    def test_raft_job_manager_property(self, gate_integration: GateRaftIntegration) -> None:
        """Raft job manager property returns the gate wrapper."""
        raft_jm = gate_integration.raft_job_manager
        assert raft_jm is not None
        assert raft_jm._node_id == "node-1"

    def test_start_begins_tick_loop(self, gate_integration: GateRaftIntegration) -> None:
        """start() initiates the gate Raft tick loop."""
        gate_integration.start()
        assert gate_integration.consensus._tick_running is True

    @pytest.mark.asyncio
    async def test_stop_destroys_all(self, gate_integration: GateRaftIntegration) -> None:
        """stop() destroys all gate Raft instances."""
        gate_integration.start()
        await gate_integration.stop()
        assert gate_integration.consensus._tick_running is False

    def test_membership_on_node_join(self, gate_integration: GateRaftIntegration) -> None:
        """on_node_join updates gate Raft cluster membership."""
        gate_integration.on_node_join("gate-2", ("10.0.0.2", 9000))
        assert "gate-2" in gate_integration.consensus._members
        assert gate_integration.consensus._member_addrs["gate-2"] == ("10.0.0.2", 9000)

    def test_membership_on_node_leave(self, gate_integration: GateRaftIntegration) -> None:
        """on_node_leave removes gate peer from Raft cluster membership."""
        gate_integration.on_node_join("gate-2", ("10.0.0.2", 9000))
        gate_integration.on_node_leave("gate-2")
        assert "gate-2" not in gate_integration.consensus._members

    @pytest.mark.asyncio
    async def test_handle_request_vote_roundtrip(
        self, gate_integration: GateRaftIntegration
    ) -> None:
        """RequestVote handler roundtrips through gate Raft."""
        await gate_integration.consensus.create_job_raft("gate-job-1")

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
        await gate_integration.consensus.create_job_raft("gate-job-1")

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
        """Gate _send_raft_message uses gate_raft_ prefixed method names."""
        vote = RequestVote(
            job_id="gate-job-1",
            term=1,
            candidate_id="node-1",
            last_log_index=0,
            last_log_term=0,
        )
        await gate_integration._send_raft_message(("10.0.0.2", 9000), vote)
        mock_send_tcp.assert_called_once()
        call_args = mock_send_tcp.call_args
        assert call_args[0][1] == "gate_raft_request_vote"

    @pytest.mark.asyncio
    async def test_send_append_entries_uses_gate_prefix(
        self, gate_integration: GateRaftIntegration, mock_send_tcp: AsyncMock
    ) -> None:
        """Gate _send_raft_message uses gate_raft_append_entries method name."""
        append = AppendEntries(
            job_id="gate-job-1",
            term=1,
            leader_id="node-1",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        await gate_integration._send_raft_message(("10.0.0.2", 9000), append)
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
        await gate_integration.consensus.create_job_raft("shared-job")

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
        await manager_integration.consensus.create_job_raft("shared-job")

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
        manager_integration.start()
        gate_integration.start()

        await manager_integration.consensus.create_job_raft("job-1")
        await gate_integration.consensus.create_job_raft("job-1")

        assert manager_integration.consensus.active_instance_count == 1
        assert gate_integration.consensus.active_instance_count == 1

        await manager_integration.stop()
        await gate_integration.stop()

        assert manager_integration.consensus.active_instance_count == 0
        assert gate_integration.consensus.active_instance_count == 0
