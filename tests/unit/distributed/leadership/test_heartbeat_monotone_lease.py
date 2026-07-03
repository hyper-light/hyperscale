"""
Monotonic heartbeat sequence + authoritative lease grant (fixes 2/4, 4/4).

A leader stamps each beat with a strictly increasing ``heartbeat_seq``
and carries the lease length it grants. A follower applies beats
MONOTONICALLY — a newer ``(term, seq)`` renews; an older-or-equal beat
(reorder / replay within a term, or a stale-term beat) is ignored — and
adopts the leader's granted lease duration. This makes lease renewal
idempotent and reorder/replay-safe by construction, independent of the
transport-level duplicate suppressor.
"""

from hyperscale.distributed.swim.leadership.leader_state import LeaderState


LEADER_A = ("127.0.0.1", 9001)
LEADER_B = ("127.0.0.1", 9002)
LEADER_C = ("127.0.0.1", 9003)


def _fresh_state() -> LeaderState:
    return LeaderState()


def test_newer_sequence_within_term_renews():
    state = _fresh_state()
    state.update_heartbeat(LEADER_A, term=1, heartbeat_seq=1)
    assert state.current_leader == LEADER_A
    assert state.applied_heartbeat_seq == 1

    state.update_heartbeat(LEADER_A, term=1, heartbeat_seq=2)
    assert state.applied_heartbeat_seq == 2


def test_replayed_or_reordered_beat_within_term_is_ignored():
    state = _fresh_state()
    state.update_heartbeat(LEADER_A, term=1, heartbeat_seq=5)
    assert state.applied_heartbeat_seq == 5

    # A replayed lower/equal sequence — even from a DIFFERENT claimant in
    # the same term (split-brain attempt) — must not be applied.
    state.update_heartbeat(LEADER_B, term=1, heartbeat_seq=5)
    assert state.current_leader == LEADER_A
    state.update_heartbeat(LEADER_B, term=1, heartbeat_seq=3)
    assert state.current_leader == LEADER_A
    assert state.applied_heartbeat_seq == 5


def test_new_term_resets_the_sequence_watermark():
    state = _fresh_state()
    state.update_heartbeat(LEADER_A, term=1, heartbeat_seq=9)
    assert state.applied_heartbeat_seq == 9

    # A newer term is a new leadership epoch: seq restarts from the
    # leader's fresh counter, so a low seq in the higher term is accepted.
    state.update_heartbeat(LEADER_B, term=2, heartbeat_seq=1)
    assert state.current_leader == LEADER_B
    assert state.leader_term == 2
    assert state.applied_heartbeat_seq == 1


def test_stale_term_beat_is_ignored():
    state = _fresh_state()
    state.update_heartbeat(LEADER_B, term=3, heartbeat_seq=1)
    assert state.current_leader == LEADER_B

    # A deposed leader's beat (older term) must never win, regardless of
    # its sequence.
    state.update_heartbeat(LEADER_A, term=2, heartbeat_seq=99)
    assert state.current_leader == LEADER_B
    assert state.leader_term == 3


def test_lease_duration_is_authoritative_from_the_grant():
    state = _fresh_state()
    default_duration = state.lease_duration
    state.update_heartbeat(
        LEADER_A, term=1, heartbeat_seq=1, lease_duration=default_duration + 3.0
    )
    assert state.lease_duration == default_duration + 3.0


def test_sequenceless_beat_always_renews():
    state = _fresh_state()
    state.update_heartbeat(LEADER_A, term=1, heartbeat_seq=7)
    assert state.applied_heartbeat_seq == 7

    # A beat without a sequence (heartbeat_seq < 0) bypasses the monotone
    # gate and still renews — the sequence watermark is left untouched.
    state.update_heartbeat(LEADER_A, term=1)
    assert state.current_leader == LEADER_A
    assert state.applied_heartbeat_seq == 7


def test_leader_resets_outgoing_sequence_on_election():
    state = _fresh_state()
    state.heartbeat_seq = 42
    state.applied_heartbeat_seq = 40
    assert state.become_leader(5) is True
    # A fresh leadership epoch starts its beat counter clean so the first
    # beat it sends is seq 1.
    assert state.heartbeat_seq == 0
    assert state.applied_heartbeat_seq == -1
