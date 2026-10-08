---
ad_number: 5
name: Pre-Voting for Split-Brain Prevention
description: Leader election uses a pre-vote phase before the actual election
---

# AD-5: Pre-Voting for Split-Brain Prevention

**Decision**: Leader election uses a pre-vote phase before the actual election.

**Rationale**:
- Pre-vote doesn't increment term (prevents term explosion)
- Candidate checks if it would win before disrupting cluster
- Nodes only grant pre-vote if no healthy leader exists

**Implementation**:
- `_run_pre_vote()` gathers pre-votes without changing state
- Only proceeds to real election if pre-vote majority achieved
- If pre-vote fails, election is aborted

## Addendum — Quorum lease: leadership exclusive at every instant (2026-10-07)

Pre-voting kept a healthy leader from being disrupted, but nothing made a
leader that could no longer reach its cohort give leadership up: it renewed
its own lease on every beat it *sent*, and only stepped down on hearing a
higher term or on the gate's slow SWIM quorum check. A gate frozen (or cut
off) while leading kept the flag while the followers, their leases lapsed,
elected another — two gates leading at once (chaos seed 5: gate-a and gate-b
over [62, 68) s).

The SWIM leader now holds a **quorum lease** (Raft thesis 6.2 CheckQuorum and
6.4.1 leases; `swim/leadership/leader_quorum_lease.py`):

* A follower answers every beat it applies with
  `leader-heartbeat-ack:{term}:{seq}` naming the newest beat its lease runs
  from (`LeaderHeartbeatHandler`, `LeaderHeartbeatAckHandler`).
* The leader credits each cohort voter with that beat's *send* instant. Its
  lease ends one lease duration after the newest instant a majority (itself
  included) acknowledged; it beats no longer than that and steps down when
  it ends (`LocalLeaderElection._lead_tick`, `should_step_down`).
* A follower's own lease runs from receipt, never earlier than the send, for
  the same granted duration, and it grants no pre-vote while it holds it —
  so no majority can elect anyone before the old leader's lease ends.
* Until the first acknowledgements arrive the claim anchors the lease: a
  voter is bound for a lease duration after granting its vote (Raft section
  5.2's timer reset on a granted vote; `LeaderState.grant_vote`).
* Code acting as leader reads `HealthAwareServer.is_leader()`, which now also
  requires the lease: a process thawed from a freeze, whose lease ran out,
  does not act before its next lead tick steps it down.

Found alongside: a follower adopting a new leader's term through
`leader-elected` kept the previous leader's beat watermark, so it rejected
the new leader's first beats as replays and let its lease lapse
(`LeaderState._adopt_leader_term`).

Proof: `test_frozen_leader_gate_steps_down_before_a_successor_is_elected`
(+ replay twin) in `tests/unit/simulation/sim/test_multiprocess_gate_faults.py`,
chaos seed 5 in the `vopr_chaos` corpus, and
`tests/unit/distributed/leadership/test_leader_quorum_lease.py`.
