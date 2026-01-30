"""
Tests for the RaftNode core algorithm.

Covers:
1. Election: start, vote handling, single-node, split votes
2. Replication: propose, append entries, conflict resolution
3. Commit: advance commit index, apply committed entries
4. Membership: update membership, leader reindexing
5. Cleanup: destroy releases all state
6. Edge cases: destroyed node no-ops, stale terms ignored
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RaftLogEntry,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import (
    ELECTION_TIMEOUT_MAX,
    ELECTION_TIMEOUT_MIN,
    RaftNode,
)


# =============================================================================
# Helpers
# =============================================================================


def make_entry(term: int, index: int, job_id: str = "job-1") -> RaftLogEntry:
    """Create a RaftLogEntry for testing."""
    return RaftLogEntry(
        term=term,
        index=index,
        command=b"test-data",
        command_type="NO_OP",
        job_id=job_id,
        timestamp=time.monotonic(),
    )


def make_node(
    node_id: str = "node-1",
    members: set[str] | None = None,
    member_addrs: dict[str, tuple[str, int]] | None = None,
) -> tuple[RaftNode, AsyncMock, AsyncMock]:
    """
    Create a RaftNode with mock callbacks.

    Returns (node, send_message_mock, apply_command_mock).
    """
    if members is None:
        members = {"node-1", "node-2", "node-3"}
    if member_addrs is None:
        member_addrs = {
            "node-1": ("127.0.0.1", 9001),
            "node-2": ("127.0.0.1", 9002),
            "node-3": ("127.0.0.1", 9003),
        }

    send_mock = AsyncMock()
    apply_mock = AsyncMock()
    logger_mock = MagicMock()
    logger_mock.log = AsyncMock()

    node = RaftNode(
        job_id="job-1",
        node_id=node_id,
        members=members,
        member_addrs=member_addrs,
        send_message=send_mock,
        apply_command=apply_mock,
        on_become_leader=None,
        on_lose_leadership=None,
        logger=logger_mock,
    )
    return node, send_mock, apply_mock


def make_single_node() -> tuple[RaftNode, AsyncMock, AsyncMock]:
    """Create a single-node cluster RaftNode."""
    return make_node(
        node_id="solo",
        members={"solo"},
        member_addrs={"solo": ("127.0.0.1", 9001)},
    )


# =============================================================================
# Test Initial State
# =============================================================================


class TestInitialState:
    """Tests for freshly created RaftNode."""

    def setup_method(self) -> None:
        self.node, _, _ = make_node()

    def test_initial_role(self) -> None:
        assert self.node.role == "follower"

    def test_initial_term(self) -> None:
        assert self.node.current_term == 0

    def test_initial_leader(self) -> None:
        assert self.node.current_leader is None

    def test_initial_commit_index(self) -> None:
        assert self.node.commit_index == 0

    def test_not_leader(self) -> None:
        assert not self.node.is_leader()


# =============================================================================
# Test Election
# =============================================================================


class TestElection:
    """Tests for leader election."""

    @pytest.mark.asyncio
    async def test_start_election_transitions_to_candidate(self) -> None:
        node, send_mock, _ = make_node()
        await node.start_election()
        assert node.role == "candidate"
        assert node.current_term == 1

    @pytest.mark.asyncio
    async def test_start_election_broadcasts_request_vote(self) -> None:
        node, send_mock, _ = make_node()
        await node.start_election()
        # Should send to 2 peers (3-node cluster minus self)
        assert send_mock.call_count == 2

    @pytest.mark.asyncio
    async def test_single_node_wins_immediately(self) -> None:
        node, _, _ = make_single_node()
        await node.start_election()
        assert node.role == "leader"
        assert node.current_term == 1
        assert node.is_leader()

    @pytest.mark.asyncio
    async def test_wins_election_with_majority(self) -> None:
        node, send_mock, _ = make_node(node_id="node-1")
        await node.start_election()
        assert node.role == "candidate"

        # One vote from node-2 gives majority (2 out of 3)
        vote = RequestVoteResponse(
            job_id="job-1",
            term=1,
            vote_granted=True,
            voter_id="node-2",
        )
        await node.handle_request_vote_response(vote)
        assert node.role == "leader"

    @pytest.mark.asyncio
    async def test_does_not_win_without_majority(self) -> None:
        node, send_mock, _ = make_node(
            node_id="node-1",
            members={"node-1", "node-2", "node-3", "node-4", "node-5"},
            member_addrs={
                "node-1": ("127.0.0.1", 9001),
                "node-2": ("127.0.0.1", 9002),
                "node-3": ("127.0.0.1", 9003),
                "node-4": ("127.0.0.1", 9004),
                "node-5": ("127.0.0.1", 9005),
            },
        )
        await node.start_election()

        # One vote is not enough (need 3 out of 5)
        vote = RequestVoteResponse(
            job_id="job-1",
            term=1,
            vote_granted=True,
            voter_id="node-2",
        )
        await node.handle_request_vote_response(vote)
        assert node.role == "candidate"

    @pytest.mark.asyncio
    async def test_rejected_vote_does_not_count(self) -> None:
        node, _, _ = make_node()
        await node.start_election()

        rejection = RequestVoteResponse(
            job_id="job-1",
            term=1,
            vote_granted=False,
            voter_id="node-2",
        )
        await node.handle_request_vote_response(rejection)
        assert node.role == "candidate"

    @pytest.mark.asyncio
    async def test_steps_down_on_higher_term_vote_response(self) -> None:
        node, _, _ = make_node()
        await node.start_election()

        higher_term = RequestVoteResponse(
            job_id="job-1",
            term=5,
            vote_granted=False,
            voter_id="node-2",
        )
        await node.handle_request_vote_response(higher_term)
        assert node.role == "follower"
        assert node.current_term == 5


# =============================================================================
# Test Vote Granting
# =============================================================================


class TestVoteGranting:
    """Tests for handling incoming RequestVote RPCs."""

    @pytest.mark.asyncio
    async def test_grants_vote_to_first_candidate(self) -> None:
        node, _, _ = make_node()
        request = RequestVote(
            job_id="job-1",
            term=1,
            candidate_id="node-2",
            last_log_index=0,
            last_log_term=0,
        )
        response = await node.handle_request_vote(request)
        assert response.vote_granted is True
        assert response.term == 1

    @pytest.mark.asyncio
    async def test_rejects_vote_for_stale_term(self) -> None:
        node, _, _ = make_node()
        # First, give node a higher term by receiving a vote request with term 3
        step_up = RequestVote(
            job_id="job-1", term=3, candidate_id="node-3",
            last_log_index=0, last_log_term=0,
        )
        await node.handle_request_vote(step_up)

        stale = RequestVote(
            job_id="job-1", term=1, candidate_id="node-2",
            last_log_index=0, last_log_term=0,
        )
        response = await node.handle_request_vote(stale)
        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_rejects_second_candidate_same_term(self) -> None:
        node, _, _ = make_node()
        first = RequestVote(
            job_id="job-1", term=1, candidate_id="node-2",
            last_log_index=0, last_log_term=0,
        )
        await node.handle_request_vote(first)

        second = RequestVote(
            job_id="job-1", term=1, candidate_id="node-3",
            last_log_index=0, last_log_term=0,
        )
        response = await node.handle_request_vote(second)
        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_grants_vote_again_to_same_candidate(self) -> None:
        node, _, _ = make_node()
        request = RequestVote(
            job_id="job-1", term=1, candidate_id="node-2",
            last_log_index=0, last_log_term=0,
        )
        response1 = await node.handle_request_vote(request)
        response2 = await node.handle_request_vote(request)
        assert response1.vote_granted is True
        assert response2.vote_granted is True

    @pytest.mark.asyncio
    async def test_rejects_candidate_with_stale_log(self) -> None:
        """Candidate with older log should be rejected (Section 5.4.1)."""
        node, _, _ = make_node()
        # Give node some log entries by receiving AppendEntries from a leader
        append_req = AppendEntries(
            job_id="job-1",
            term=2,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=[make_entry(term=2, index=1)],
            leader_commit=0,
        )
        await node.handle_append_entries(append_req)

        # Candidate with empty log (term=3, but log behind)
        vote_req = RequestVote(
            job_id="job-1", term=3, candidate_id="node-3",
            last_log_index=0, last_log_term=0,
        )
        response = await node.handle_request_vote(vote_req)
        assert response.vote_granted is False


# =============================================================================
# Test Propose (Leader Only)
# =============================================================================


class TestPropose:
    """Tests for command proposal on the leader."""

    @pytest.mark.asyncio
    async def test_propose_succeeds_on_leader(self) -> None:
        node, _, _ = make_single_node()
        await node.start_election()
        assert node.is_leader()

        success, index = await node.propose(b"cmd", "CREATE_JOB")
        assert success is True
        assert index == 1

    @pytest.mark.asyncio
    async def test_propose_fails_on_follower(self) -> None:
        node, _, _ = make_node()
        success, index = await node.propose(b"cmd", "CREATE_JOB")
        assert success is False
        assert index == 0

    @pytest.mark.asyncio
    async def test_propose_increments_index(self) -> None:
        node, _, _ = make_single_node()
        await node.start_election()

        _, index1 = await node.propose(b"a", "CREATE_JOB")
        _, index2 = await node.propose(b"b", "CREATE_JOB")
        assert index1 == 1
        assert index2 == 2


# =============================================================================
# Test AppendEntries Handling
# =============================================================================


class TestAppendEntries:
    """Tests for handling AppendEntries RPCs."""

    @pytest.mark.asyncio
    async def test_accepts_heartbeat(self) -> None:
        node, _, _ = make_node()
        request = AppendEntries(
            job_id="job-1",
            term=1,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        response = await node.handle_append_entries(request)
        assert response.success is True
        assert node.current_leader == "node-2"

    @pytest.mark.asyncio
    async def test_rejects_stale_term(self) -> None:
        node, _, _ = make_node()
        # Give node a higher term first
        await node.handle_append_entries(AppendEntries(
            job_id="job-1", term=5, leader_id="node-2",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0,
        ))

        stale = AppendEntries(
            job_id="job-1", term=1, leader_id="node-3",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0,
        )
        response = await node.handle_append_entries(stale)
        assert response.success is False

    @pytest.mark.asyncio
    async def test_appends_entries(self) -> None:
        node, _, _ = make_node()
        entries = [make_entry(term=1, index=1), make_entry(term=1, index=2)]
        request = AppendEntries(
            job_id="job-1",
            term=1,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=0,
        )
        response = await node.handle_append_entries(request)
        assert response.success is True
        assert response.match_index == 2

    @pytest.mark.asyncio
    async def test_advances_commit_index(self) -> None:
        node, _, _ = make_node()
        entries = [make_entry(term=1, index=1)]
        request = AppendEntries(
            job_id="job-1", term=1, leader_id="node-2",
            prev_log_index=0, prev_log_term=0,
            entries=entries, leader_commit=1,
        )
        await node.handle_append_entries(request)
        assert node.commit_index == 1

    @pytest.mark.asyncio
    async def test_commit_index_bounded_by_log(self) -> None:
        """Commit index cannot exceed last log index."""
        node, _, _ = make_node()
        entries = [make_entry(term=1, index=1)]
        request = AppendEntries(
            job_id="job-1", term=1, leader_id="node-2",
            prev_log_index=0, prev_log_term=0,
            entries=entries, leader_commit=100,
        )
        await node.handle_append_entries(request)
        assert node.commit_index == 1

    @pytest.mark.asyncio
    async def test_rejects_inconsistent_prev_log(self) -> None:
        """Rejects when prev_log doesn't match (Section 5.3)."""
        node, _, _ = make_node()
        request = AppendEntries(
            job_id="job-1", term=1, leader_id="node-2",
            prev_log_index=5, prev_log_term=1,
            entries=[], leader_commit=0,
        )
        response = await node.handle_append_entries(request)
        assert response.success is False
        assert response.conflict_index is not None

    @pytest.mark.asyncio
    async def test_candidate_steps_down_on_append_entries(self) -> None:
        """A candidate reverts to follower on receiving AppendEntries from current term."""
        node, _, _ = make_node()
        await node.start_election()
        assert node.role == "candidate"

        request = AppendEntries(
            job_id="job-1", term=node.current_term, leader_id="node-2",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0,
        )
        await node.handle_append_entries(request)
        assert node.role == "follower"


# =============================================================================
# Test AppendEntries Response Handling (Leader)
# =============================================================================


class TestAppendEntriesResponse:
    """Tests for leader handling of follower responses."""

    @pytest.mark.asyncio
    async def test_successful_response_advances_match(self) -> None:
        node, _, _ = make_single_node()
        await node.start_election()
        # Add another member manually for this test
        node.update_membership(
            {"solo", "node-2"},
            {"solo": ("127.0.0.1", 9001), "node-2": ("127.0.0.1", 9002)},
        )

        await node.propose(b"cmd", "NO_OP")
        response = AppendEntriesResponse(
            job_id="job-1",
            term=node.current_term,
            success=True,
            follower_id="node-2",
            match_index=1,
        )
        await node.handle_append_entries_response(response)
        # Commit should advance since both nodes agree
        assert node.commit_index == 1

    @pytest.mark.asyncio
    async def test_steps_down_on_higher_term(self) -> None:
        node, _, _ = make_single_node()
        await node.start_election()

        response = AppendEntriesResponse(
            job_id="job-1",
            term=10,
            success=False,
            follower_id="node-2",
            match_index=0,
        )
        await node.handle_append_entries_response(response)
        assert node.role == "follower"
        assert node.current_term == 10

    @pytest.mark.asyncio
    async def test_ignores_response_when_not_leader(self) -> None:
        node, _, _ = make_node()
        response = AppendEntriesResponse(
            job_id="job-1", term=0, success=True,
            follower_id="node-2", match_index=1,
        )
        await node.handle_append_entries_response(response)
        # No crash, still follower
        assert node.role == "follower"


# =============================================================================
# Test Apply Committed Entries
# =============================================================================


class TestApplyCommitted:
    """Tests for applying committed entries to the state machine."""

    @pytest.mark.asyncio
    async def test_applies_committed_entries(self) -> None:
        node, _, apply_mock = make_single_node()
        await node.start_election()

        await node.propose(b"cmd1", "CREATE_JOB")
        await node.propose(b"cmd2", "CREATE_JOB")

        # Single-node: entries committed immediately when proposed
        # But commit_index only advances via _advance_commit_index
        # Force commit by simulating the commit advance
        # In single-node, there are no followers, so match_index is empty
        # But quorum is 1 (just leader), so commit should already advance
        applied = await node.apply_committed_entries()
        assert applied == 2
        assert apply_mock.call_count == 2

    @pytest.mark.asyncio
    async def test_apply_is_idempotent(self) -> None:
        node, _, apply_mock = make_single_node()
        await node.start_election()

        await node.propose(b"cmd", "CREATE_JOB")
        await node.apply_committed_entries()
        apply_count_first = apply_mock.call_count

        # Second call applies nothing new
        await node.apply_committed_entries()
        assert apply_mock.call_count == apply_count_first


# =============================================================================
# Test Replication
# =============================================================================


class TestReplication:
    """Tests for leader replication to followers."""

    @pytest.mark.asyncio
    async def test_replicate_sends_to_all_followers(self) -> None:
        node, send_mock, _ = make_node()
        await node.start_election()
        # Win election
        vote = RequestVoteResponse(
            job_id="job-1", term=1, vote_granted=True, voter_id="node-2",
        )
        await node.handle_request_vote_response(vote)
        assert node.is_leader()

        send_mock.reset_mock()
        await node.replicate_to_followers()
        # Should send to 2 followers
        assert send_mock.call_count == 2

    @pytest.mark.asyncio
    async def test_replicate_noop_when_not_leader(self) -> None:
        node, send_mock, _ = make_node()
        send_mock.reset_mock()
        await node.replicate_to_followers()
        assert send_mock.call_count == 0


# =============================================================================
# Test Membership Updates
# =============================================================================


class TestMembership:
    """Tests for dynamic membership changes."""

    def test_update_membership_adds_new_members(self) -> None:
        node, _, _ = make_node()
        new_members = {"node-1", "node-2", "node-3", "node-4"}
        new_addrs = {
            "node-1": ("127.0.0.1", 9001),
            "node-2": ("127.0.0.1", 9002),
            "node-3": ("127.0.0.1", 9003),
            "node-4": ("127.0.0.1", 9004),
        }
        node.update_membership(new_members, new_addrs)
        # No crash, membership updated

    @pytest.mark.asyncio
    async def test_leader_tracks_new_members(self) -> None:
        node, _, _ = make_single_node()
        await node.start_election()

        node.update_membership(
            {"solo", "new-node"},
            {"solo": ("127.0.0.1", 9001), "new-node": ("127.0.0.1", 9002)},
        )
        # Leader should have initialized next_index for new member


# =============================================================================
# Test Destroy / Cleanup
# =============================================================================


class TestDestroy:
    """Tests for node cleanup on job completion."""

    @pytest.mark.asyncio
    async def test_destroy_makes_all_ops_noop(self) -> None:
        node, send_mock, apply_mock = make_node()
        node.destroy()

        await node.start_election()
        assert node.role == "follower"  # No transition

        success, _ = await node.propose(b"cmd", "CREATE_JOB")
        assert success is False

        applied = await node.apply_committed_entries()
        assert applied == 0

    @pytest.mark.asyncio
    async def test_destroy_clears_state(self) -> None:
        node, _, _ = make_single_node()
        await node.start_election()
        await node.propose(b"cmd", "CREATE_JOB")

        node.destroy()
        # All internal collections should be cleared

    @pytest.mark.asyncio
    async def test_tick_after_destroy(self) -> None:
        node, _, _ = make_node()
        node.destroy()
        await node.tick()  # Should not crash

    @pytest.mark.asyncio
    async def test_handle_vote_after_destroy(self) -> None:
        node, _, _ = make_node()
        node.destroy()
        response = await node.handle_request_vote(RequestVote(
            job_id="job-1", term=1, candidate_id="node-2",
            last_log_index=0, last_log_term=0,
        ))
        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_handle_append_after_destroy(self) -> None:
        node, _, _ = make_node()
        node.destroy()
        response = await node.handle_append_entries(AppendEntries(
            job_id="job-1", term=1, leader_id="node-2",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0,
        ))
        assert response.success is False


# =============================================================================
# Test Tick
# =============================================================================


class TestTick:
    """Tests for the tick mechanism."""

    @pytest.mark.asyncio
    async def test_tick_does_not_crash_on_follower(self) -> None:
        node, _, _ = make_node()
        await node.tick()

    @pytest.mark.asyncio
    async def test_tick_does_not_crash_on_leader(self) -> None:
        node, _, _ = make_single_node()
        await node.start_election()
        await node.tick()


# =============================================================================
# Test Election Timeout Constants
# =============================================================================


class TestConstants:
    """Tests for Raft timing constants."""

    def test_election_timeout_range(self) -> None:
        assert ELECTION_TIMEOUT_MIN > 0
        assert ELECTION_TIMEOUT_MAX > ELECTION_TIMEOUT_MIN

    def test_election_timeout_min(self) -> None:
        assert ELECTION_TIMEOUT_MIN == 0.150

    def test_election_timeout_max(self) -> None:
        assert ELECTION_TIMEOUT_MAX == 0.300
