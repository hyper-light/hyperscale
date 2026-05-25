"""LeaderState pre-vote cleanup invariants."""

from hyperscale.distributed.swim.leadership.leader_state import LeaderState


LEADER_ADDR = ("127.0.0.1", 9000)
VOTER_ADDR = ("127.0.0.1", 9001)


def _start_pre_vote(state: LeaderState, term: int) -> None:
    state.start_pre_vote(term)
    state.record_pre_vote(VOTER_ADDR)


def _assert_pre_vote_cleared(state: LeaderState) -> None:
    assert state.pre_voting_in_progress is False
    assert state.pre_votes_received == set()
    assert state.pre_vote_term == 0


def test_update_heartbeat_aborts_active_pre_vote() -> None:
    """Accepting a leader heartbeat makes local pre-vote state invalid."""
    state = LeaderState(current_term=1)
    _start_pre_vote(state, term=2)

    state.update_heartbeat(LEADER_ADDR, term=1)

    assert state.current_leader == LEADER_ADDR
    assert state.is_lease_valid()
    _assert_pre_vote_cleared(state)


def test_become_follower_with_known_leader_aborts_active_pre_vote() -> None:
    """Following a known leader clears any local pre-vote campaign."""
    state = LeaderState(current_term=1)
    _start_pre_vote(state, term=2)

    state.become_follower(term=2, leader=LEADER_ADDR)

    assert state.current_leader == LEADER_ADDR
    assert state.is_lease_valid()
    _assert_pre_vote_cleared(state)


def test_become_leader_aborts_active_pre_vote() -> None:
    """Winning leadership clears pre-vote bookkeeping."""
    state = LeaderState(current_term=1)
    _start_pre_vote(state, term=2)

    assert state.become_leader(term=2) is True

    assert state.is_leader()
    _assert_pre_vote_cleared(state)
