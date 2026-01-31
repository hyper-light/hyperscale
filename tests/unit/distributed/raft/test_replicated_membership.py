"""
Unit tests for ReplicatedMembershipLog.

Tests event recording, pending tracking, committed history,
active member derivation, and cleanup.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.raft.replicated_membership_log import (
    MembershipEvent,
    ReplicatedMembershipLog,
)


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


@pytest.fixture
def membership_log(mock_logger):
    return ReplicatedMembershipLog(
        logger=mock_logger,
        node_id="node-1",
        max_history_per_job=50,
    )


class TestMembershipEvent:
    """Tests for MembershipEvent dataclass."""

    def test_fields(self) -> None:
        event = MembershipEvent(
            event_type="join",
            node_id="worker-1",
            node_addr=("10.0.0.1", 8000),
            timestamp=100.0,
        )
        assert event.event_type == "join"
        assert event.node_id == "worker-1"
        assert event.node_addr == ("10.0.0.1", 8000)

    def test_leave_event_no_addr(self) -> None:
        event = MembershipEvent(
            event_type="leave",
            node_id="worker-1",
            node_addr=None,
            timestamp=200.0,
        )
        assert event.node_addr is None


class TestReplicatedMembershipLog:
    """Tests for ReplicatedMembershipLog."""

    def test_record_event(self, membership_log: ReplicatedMembershipLog) -> None:
        membership_log.record_event("job-1", "join", "worker-1", ("10.0.0.1", 8000))
        assert membership_log.pending_count("job-1") == 1

    def test_record_multiple_events(self, membership_log: ReplicatedMembershipLog) -> None:
        membership_log.record_event("job-1", "join", "worker-1", ("10.0.0.1", 8000))
        membership_log.record_event("job-1", "join", "worker-2", ("10.0.0.2", 8000))
        membership_log.record_event("job-1", "leave", "worker-1")
        assert membership_log.pending_count("job-1") == 3

    def test_pending_count_unknown_job(self, membership_log: ReplicatedMembershipLog) -> None:
        assert membership_log.pending_count("nonexistent") == 0

    def test_total_pending(self, membership_log: ReplicatedMembershipLog) -> None:
        membership_log.record_event("job-1", "join", "w-1")
        membership_log.record_event("job-2", "join", "w-2")
        assert membership_log.total_pending == 2

    @pytest.mark.asyncio
    async def test_propose_pending_manager(
        self, membership_log: ReplicatedMembershipLog
    ) -> None:
        membership_log.record_event("job-1", "join", "w-1", ("10.0.0.1", 8000))
        membership_log.record_event("job-1", "leave", "w-2")

        mock_raft_jm = MagicMock()
        mock_raft_jm.node_membership_event = AsyncMock(return_value=True)

        proposed = await membership_log.propose_pending_manager("job-1", mock_raft_jm)
        assert proposed == 2
        assert membership_log.pending_count("job-1") == 0

    @pytest.mark.asyncio
    async def test_propose_pending_manager_rejection(
        self, membership_log: ReplicatedMembershipLog
    ) -> None:
        membership_log.record_event("job-1", "join", "w-1")
        membership_log.record_event("job-1", "join", "w-2")

        mock_raft_jm = MagicMock()
        mock_raft_jm.node_membership_event = AsyncMock(return_value=False)

        proposed = await membership_log.propose_pending_manager("job-1", mock_raft_jm)
        assert proposed == 0
        # Events should still be pending
        assert membership_log.pending_count("job-1") == 2

    @pytest.mark.asyncio
    async def test_propose_pending_gate(
        self, membership_log: ReplicatedMembershipLog
    ) -> None:
        membership_log.record_event("job-1", "join", "gate-2", ("10.0.0.2", 9000))

        mock_gate_jm = MagicMock()
        mock_gate_jm.gate_membership_event = AsyncMock(return_value=True)

        proposed = await membership_log.propose_pending_gate("job-1", mock_gate_jm)
        assert proposed == 1

    @pytest.mark.asyncio
    async def test_propose_empty_pending(
        self, membership_log: ReplicatedMembershipLog
    ) -> None:
        mock_raft_jm = MagicMock()
        mock_raft_jm.node_membership_event = AsyncMock(return_value=True)
        proposed = await membership_log.propose_pending_manager("job-1", mock_raft_jm)
        assert proposed == 0

    def test_on_committed(self, membership_log: ReplicatedMembershipLog) -> None:
        membership_log.on_committed("job-1", "join", "w-1", ("10.0.0.1", 8000))
        events = membership_log.get_committed_events("job-1")
        assert len(events) == 1
        assert events[0].event_type == "join"
        assert events[0].node_id == "w-1"

    def test_committed_history_bounded(self) -> None:
        logger = MagicMock()
        logger.log = AsyncMock()
        log = ReplicatedMembershipLog(
            logger=logger, node_id="n-1", max_history_per_job=3
        )
        for i in range(5):
            log.on_committed("job-1", "join", f"w-{i}")
        events = log.get_committed_events("job-1")
        assert len(events) == 3
        # Should keep most recent
        assert events[0].node_id == "w-2"
        assert events[2].node_id == "w-4"

    def test_get_active_members(self, membership_log: ReplicatedMembershipLog) -> None:
        membership_log.on_committed("job-1", "join", "w-1")
        membership_log.on_committed("job-1", "join", "w-2")
        membership_log.on_committed("job-1", "join", "w-3")
        membership_log.on_committed("job-1", "leave", "w-2")

        members = membership_log.get_active_members("job-1")
        assert members == {"w-1", "w-3"}

    def test_get_active_members_empty(self, membership_log: ReplicatedMembershipLog) -> None:
        members = membership_log.get_active_members("nonexistent")
        assert members == set()

    def test_suspect_does_not_change_membership(
        self, membership_log: ReplicatedMembershipLog
    ) -> None:
        membership_log.on_committed("job-1", "join", "w-1")
        membership_log.on_committed("job-1", "suspect", "w-1")
        members = membership_log.get_active_members("job-1")
        assert "w-1" in members

    def test_cleanup_job(self, membership_log: ReplicatedMembershipLog) -> None:
        membership_log.record_event("job-1", "join", "w-1")
        membership_log.on_committed("job-1", "join", "w-1")
        membership_log.cleanup_job("job-1")
        assert membership_log.pending_count("job-1") == 0
        assert membership_log.get_committed_events("job-1") == []

    def test_clear(self, membership_log: ReplicatedMembershipLog) -> None:
        membership_log.record_event("job-1", "join", "w-1")
        membership_log.record_event("job-2", "join", "w-2")
        membership_log.on_committed("job-1", "join", "w-1")
        membership_log.clear()
        assert membership_log.total_pending == 0
        assert membership_log.get_committed_events("job-1") == []
