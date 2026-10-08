"""
The leader's quorum lease (Raft thesis 6.2 CheckQuorum, 6.4.1 leases).

A leader holds leadership only while a majority of its cohort -- itself
included -- acknowledged beats sent within one lease duration; a voter is
bound for a lease duration after granting its vote, so the claim anchors
the lease until the first beats are acknowledged. Together they make two
leaders at one instant impossible: the old leader's lease ends no later
than the leases its acknowledging followers hold from the same beats.
"""

import pytest

from hyperscale.distributed.swim.leadership import leader_state as leader_state_module
from hyperscale.distributed.swim.leadership.leader_quorum_lease import LeaderQuorumLease
from hyperscale.distributed.swim.leadership.leader_state import LeaderState
from hyperscale.distributed.swim.leadership.local_leader_election import LocalLeaderElection

LEASE_DURATION = 5.0
LEADER = ("127.0.0.1", 9001)
FOLLOWER = ("127.0.0.1", 9011)
OTHER_FOLLOWER = ("127.0.0.1", 9021)
OUTSIDER = ("127.0.0.1", 9031)
COHORT = frozenset({LEADER, FOLLOWER, OTHER_FOLLOWER})


class SteppedClock:
    """A monotonic clock the test advances by hand."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    stepped_clock = SteppedClock()
    monkeypatch.setattr(leader_state_module, "_DEFAULT_CLOCK", stepped_clock)
    return stepped_clock


def _leading_election(clock: SteppedClock) -> LocalLeaderElection:
    """A three-member cohort's leader of term 1, its claim sent at the clock's now."""
    election = LocalLeaderElection(
        state=LeaderState(lease_duration=LEASE_DURATION),
        self_addr=LEADER,
        _get_member_count=lambda: len(COHORT),
        _is_cohort_voter=COHORT.__contains__,
        _clock=clock,
    )
    election.state.start_election(1)
    election.quorum_lease.open_term(1, clock.now)
    election.state.become_leader(1)
    return election


def _send_beat(election: LocalLeaderElection, clock: SteppedClock) -> int:
    """Record a beat of the leader's term sent now; its sequence."""
    election.state.heartbeat_seq += 1
    election.quorum_lease.record_beat(
        election.state.current_term, election.state.heartbeat_seq, clock.now, LEASE_DURATION
    )
    return election.state.heartbeat_seq


def test_an_unacknowledged_leader_holds_only_the_lease_its_claim_anchors(clock: SteppedClock):
    election = _leading_election(clock)
    _send_beat(election, clock)

    clock.now = LEASE_DURATION - 0.001
    assert election.holds_leadership()
    clock.now = LEASE_DURATION
    assert not election.holds_leadership()
    assert election.should_step_down()


def test_an_acknowledged_beat_extends_the_lease_from_its_send_instant(clock: SteppedClock):
    election = _leading_election(clock)
    clock.now = 3.0
    acknowledged_sequence = _send_beat(election, clock)
    clock.now = 4.0
    election.handle_heartbeat_ack(FOLLOWER, 1, acknowledged_sequence)

    clock.now = 3.0 + LEASE_DURATION - 0.001
    assert election.holds_leadership()
    clock.now = 3.0 + LEASE_DURATION
    assert not election.holds_leadership()


def test_acknowledgements_that_prove_nothing_are_not_credited(clock: SteppedClock):
    election = _leading_election(clock)
    clock.now = 3.0
    sequence = _send_beat(election, clock)

    election.handle_heartbeat_ack(OUTSIDER, 1, sequence)  # not a cohort voter
    election.handle_heartbeat_ack(FOLLOWER, 2, sequence)  # another term's beat
    election.handle_heartbeat_ack(FOLLOWER, 1, sequence + 1)  # a beat never sent

    clock.now = LEASE_DURATION
    assert not election.holds_leadership()


def test_a_sole_member_holds_its_lease_indefinitely():
    lease = LeaderQuorumLease()
    lease.open_term(1, 0.0)
    assert lease.expires_at(1, 0, LEASE_DURATION) == float("inf")
    assert lease.expires_at(2, 1, LEASE_DURATION) == float("-inf")


def test_the_majority_th_newest_acknowledgement_bounds_a_five_member_lease():
    lease = LeaderQuorumLease()
    lease.open_term(1, 0.0)
    for sequence, sent_at in enumerate((1.0, 2.0, 3.0), start=1):
        lease.record_beat(1, sequence, sent_at, LEASE_DURATION)
    lease.record_acknowledgement(FOLLOWER, 1, 3)
    lease.record_acknowledgement(OTHER_FOLLOWER, 1, 1)

    # Five members: a majority is three, the leader plus two peers.
    assert lease.expires_at(1, 2, LEASE_DURATION) == 1.0 + LEASE_DURATION


def test_beats_older_than_a_lease_are_forgotten():
    lease = LeaderQuorumLease()
    lease.open_term(1, 0.0)
    lease.record_beat(1, 1, 1.0, LEASE_DURATION)
    lease.record_beat(1, 2, 1.0 + LEASE_DURATION + 1.0, LEASE_DURATION)

    lease.record_acknowledgement(FOLLOWER, 1, 1)
    assert lease.expires_at(1, 1, LEASE_DURATION) == 0.0 + LEASE_DURATION


def test_a_lapsed_lease_sends_no_more_beats_and_steps_down(clock: SteppedClock):
    election = _leading_election(clock)
    clock.now = LEASE_DURATION + 1.0

    assert election.state.is_leader()
    assert not election.holds_leadership()
    assert election.should_step_down()


def test_a_granted_vote_binds_the_voter_for_a_lease(clock: SteppedClock):
    state = LeaderState(lease_duration=LEASE_DURATION)
    state.grant_vote(LEADER, 1)

    clock.now = LEASE_DURATION - 0.001
    assert not state.should_start_election()
    assert not state.can_grant_pre_vote(FOLLOWER, 2, 0, 100)
    assert state.time_until_unbound() > 0

    clock.now = LEASE_DURATION
    assert state.should_start_election()
    assert state.can_grant_pre_vote(FOLLOWER, 2, 0, 100)


def test_a_follower_acknowledges_only_beats_that_renewed_its_lease(clock: SteppedClock):
    follower = LocalLeaderElection(
        state=LeaderState(lease_duration=LEASE_DURATION),
        self_addr=FOLLOWER,
        _clock=clock,
    )
    follower.state.update_heartbeat(LEADER, 2, 4)

    assert follower.heartbeat_acknowledgement(LEADER, 2) == b"leader-heartbeat-ack:2:4>127.0.0.1:9001"
    # A deposed leader's beat of an older term renewed nothing.
    assert follower.heartbeat_acknowledgement(LEADER, 1) is None
    # Another claimant of the followed term is not the followed leader.
    assert follower.heartbeat_acknowledgement(OTHER_FOLLOWER, 2) is None
