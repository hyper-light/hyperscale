"""
Pinned EXTREME gate-cluster fault scenarios under multi-process SIM —
the schedules too adversarial for the generated vopr_gates space,
each probed first and pinned with observed-timeline constants.

Canonical topology in every scenario: three peered ``GateServer``
processes (leader-watch entries: dc-health / gate-peers / gate-leader
milestones), one ``ManagerServer`` registered with ALL three gates,
one 2-core ``WorkerServer``, and client(s) running the multi-gate
sustained-load entry (6 virtual seconds of chained ACTION execution).

Probed baseline (seed 211, no faults): gates discover both peers by
t=0.5; gate-c wins the initial election at ~1.5 and holds leadership
all run; DC healthy ~5.5; submission accepted t=5.53 at gate index 1
(gate-b — the SUBMISSION gate every kill/restart scenario targets or
deliberately spares); dispatch t=8.75; client-visible completion
t=14.769 (dispatch + the full 6s duration + push); worker drain 20.25.

Scenario families (mission points 1-5 + scope extension), each pinned
to PROBED current behavior — loud truths, with aspirational invariants
skip-marked where the probes exposed gaps:

* gate_kill — a follower, the LEADER, and the SUBMISSION gate killed
  mid-execution: the job completes via surviving gates (a follower or
  leader kill is invisible to the job path; killing the ACCEPTING
  gate costs exactly one 5s dead-origin failover before a peer gate
  delivers), survivors detect the death inside the witness-less
  [25, 85]s bound, and re-election (leader kill) lands within ~11s.
* gate_restart — power-loss + amnesiac reboot of the submission gate
  (gates have NO durable tier — Phase 8): the client observes the
  SAME loud completion as the kill case via peer-gate failover; the
  rebooted generation rejoins without split-brain. The durable-resume
  invariant is skip-marked.
* gate_partition — heal-bounded peer isolation causes ZERO membership
  churn (the no-false-death property); TOTAL isolation (peers +
  manager) elects a majority leader in ~10.5s, the islanded ex-leader
  steps down at heal, the split-window is harmless — and the
  peer-readmission watch restores FULL membership within one check
  tick of the heal (every gate back to 2 active peers by ~108).
* client connectivity — submission-window cuts converge acceptance
  through the retry cycle; delivery-window cuts are beaten by push
  failover; a full blackout ends in LOUD abandonment; delay+jitter
  never regress the observed status order.
* long-horizon — 420 virtual seconds: kill inside live execution,
  partition + noise waves, then quiesce; the primary job completes;
  the late client is ACCEPTED (gate BUSY!=UNHEALTHY overload config)
  and its job COMPLETES at 316.24 (the manager-tier worker-heartbeat
  starvation fell with the SWIM probe-cycle repairs — heartbeats
  never stop, allocation never starves); leadership HOLDS through
  the kill+partition composite (post-heal re-admission restores
  quorum before step-down matures — exactly one leader end to end).
* client restart — power-loss of the client (a leaf): the first job
  survives server-side, the successor generation is loud end to end
  and its fresh job COMPLETES (the second-job dispatch failure was
  the workflow-duration overload poisoning, now fixed), with the
  ceiling held below the manager's client-orphan virtual-time spin
  (documented liveness gap).
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.gate_fault_client_demo import (
    multi_gate_load_client_entry,
)
from tests.simulation.harness.sim.multiprocess.gate_cluster_demo import (
    multi_gate_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.gate_leader_watch_demo import (
    leader_watch_gate_tier_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)
from tests.simulation.oracle import JobStatusOracle

import pytest

_GATE_PIDS = ("gate-a", "gate-b", "gate-c")
_GATE_HOSTS = {
    "gate-a": "sim-gate-a",
    "gate-b": "sim-gate-b",
    "gate-c": "sim-gate-c",
}

# Probed baseline instants (seed 211): submission accepted ~5.53 at
# gate index 1 (gate-b), dispatch ~8.75, client-visible completion
# 14.769. A fault at t=12.0 provably lands INSIDE live execution.
_SEED = 211
_SUBMISSION_GATE_INDEX = 1
_SUBMISSION_GATE = "gate-b"
_INITIAL_LEADER_GATE = "gate-c"
_FOLLOWER_GATE = "gate-a"
_MID_EXECUTION_AT = 12.0
_WORKFLOW_DURATION = 6.0
_JOB_TIMEOUT = 30.0

_TERMINAL_STATUSES = frozenset({"completed", "failed", "timeout", "cancelled"})


def _build_cluster(
    seed: int,
    ceiling: float,
    wait_timeout: float,
    late_client_at: float = 0.0,
    max_submit_attempts: int = 0,
) -> SimulationCoordinator:
    """The canonical gate-cluster topology every pinned scenario runs."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=ceiling, seed=seed
    )
    datacenter_managers = {"dc-1": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"dc-1": [("sim-mgr", 9001)]}

    for gate_pid in _GATE_PIDS:
        peer_hosts = [
            _GATE_HOSTS[peer_pid]
            for peer_pid in _GATE_PIDS
            if peer_pid != gate_pid
        ]
        coordinator.add_process(
            gate_pid,
            leader_watch_gate_tier_entry,
            _GATE_HOSTS[gate_pid],
            9000,
            9001,
            datacenter_managers,
            datacenter_manager_udp,
            [(peer_host, 9000) for peer_host in peer_hosts],
            [(peer_host, 9001) for peer_host in peer_hosts],
        )

    coordinator.add_process(
        "manager",
        multi_gate_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "dc-1",
        [(_GATE_HOSTS[gate_pid], 9000) for gate_pid in _GATE_PIDS],
        [(_GATE_HOSTS[gate_pid], 9001) for gate_pid in _GATE_PIDS],
    )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "dc-1",
        ("sim-mgr", 9000),
        2,
    )
    gate_addresses = [
        (_GATE_HOSTS[gate_pid], 9000) for gate_pid in _GATE_PIDS
    ]
    coordinator.add_process(
        "client",
        multi_gate_load_client_entry,
        "sim-cli",
        9500,
        gate_addresses,
        _WORKFLOW_DURATION,
        2,
        _JOB_TIMEOUT,
        wait_timeout,
        0.0,
        max_submit_attempts,
    )
    if late_client_at > 0.0:
        coordinator.add_process(
            "late-client",
            multi_gate_load_client_entry,
            "sim-cli-late",
            9500,
            gate_addresses,
            _WORKFLOW_DURATION,
            2,
            _JOB_TIMEOUT,
            wait_timeout,
            late_client_at,
        )
    return coordinator


def _assert_no_unswapped_seams(results: dict) -> None:
    audit_rows = [
        (process_id, entry)
        for process_id, entries in results.items()
        if isinstance(entries, list)
        for entry in entries
        if isinstance(entry, tuple)
        and entry
        and entry[0] == "determinism-audit-unswapped"
    ]
    assert not audit_rows, audit_rows


def _assert_clean_completed_client(client_log: list, label: str) -> float:
    """Oracle-clean, loudly-completed client log; returns finish time."""
    assert not JobStatusOracle().check_client_log(client_log), (
        label,
        client_log,
    )
    assert not [
        entry for entry in client_log if entry[0] in ("client-error", "wait-timeout")
    ], (label, client_log)
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, (label, client_log)
    assert finished[0][1] == "completed", (label, client_log)
    return finished[0][2]


def _final_leader_flags(results: dict, gate_pids: tuple) -> dict[str, int]:
    flags: dict[str, int] = {}
    for gate_pid in gate_pids:
        gate_log = results.get(gate_pid)
        assert isinstance(gate_log, list), (gate_pid, gate_log)
        leader_entries = [
            entry for entry in gate_log if entry[0] == "gate-leader"
        ]
        assert leader_entries, (gate_pid, gate_log)
        flags[gate_pid] = leader_entries[-1][1]
    return flags


# =========================================================================
# 1. gate_kill — follower, leader, submission gate
# =========================================================================


def _run_kill_follower() -> dict:
    """Kill the pure-follower gate (gate-a: neither leader nor the
    accepting gate for seed 211) at t=12 — inside live execution."""
    coordinator = _build_cluster(_SEED, 150.0, wait_timeout=100.0)
    coordinator.schedule_kill(_FOLLOWER_GATE, _MID_EXECUTION_AT)
    return coordinator.run()


def test_kill_follower_gate_job_completes_and_leader_holds():
    results = _run_kill_follower()
    _assert_no_unswapped_seams(results)

    # Kill semantics: the victim executes nothing after the kill and
    # produces no result.
    assert _FOLLOWER_GATE not in results, sorted(results)

    client_log = results["client"]
    submit_targets = [
        entry for entry in client_log if entry[0] == "submit-target"
    ]
    assert submit_targets and submit_targets[0][1] == _SUBMISSION_GATE_INDEX, (
        client_log
    )
    finish_time = _assert_clean_completed_client(client_log, "kill-follower")
    # A follower death is INVISIBLE to the job path: probed completion
    # lands at the fault-free baseline instant (~14.77 — dispatch 8.75
    # + 6s execution + push), so any drift past 16 means the kill
    # perturbed a path it must not touch.
    assert finish_time < 16.0, client_log

    # The surviving gates hold leadership without churn: the initial
    # leader keeps its flag and nobody else ever claims it.
    flags = _final_leader_flags(results, (_SUBMISSION_GATE, _INITIAL_LEADER_GATE))
    assert flags == {_SUBMISSION_GATE: 0, _INITIAL_LEADER_GATE: 1}, flags

    # Membership truth: both survivors observed the death through
    # production SWIM inside the EVIDENCE-ACCELERATED design bound
    # [5, 30]s after the kill (probed: 11.5s on both survivors). The
    # original pin sat on the witness-less max leg (~70s, bound
    # [25, 85]) — but that slow leg was itself largely a BUG: direct
    # probes and indirect-witness confirmations to one target clobbered
    # each other's shared ack futures (pop-and-cancel in
    # _probe_with_timeout), so witness evidence kept aborting and
    # detection degraded to the no-witness maximum. With the shared/
    # shielded ack futures, the two surviving gates witness for each
    # other and the AD-30 accelerated leg applies.
    for surviving_gate in (_SUBMISSION_GATE, _INITIAL_LEADER_GATE):
        drop_times = [
            entry[2]
            for entry in results[surviving_gate]
            if entry[0] == "gate-peers" and entry[1] == 1
        ]
        assert drop_times, results[surviving_gate]
        detection_latency = drop_times[0] - _MID_EXECUTION_AT
        assert 5.0 <= detection_latency <= 30.0, (
            surviving_gate,
            detection_latency,
        )


def test_kill_follower_is_replay_deterministic():
    assert _run_kill_follower() == _run_kill_follower()


def _run_kill_leader() -> dict:
    """Kill the initial gate LEADER (gate-c) at t=12: mid-execution
    leader loss — the survivors must re-elect exactly one leader and
    the in-flight job (owned by gate-b) must complete undisturbed."""
    coordinator = _build_cluster(_SEED, 150.0, wait_timeout=100.0)
    coordinator.schedule_kill(_INITIAL_LEADER_GATE, _MID_EXECUTION_AT)
    return coordinator.run()


def test_kill_leader_gate_reelects_exactly_one_and_job_completes():
    results = _run_kill_leader()
    _assert_no_unswapped_seams(results)
    assert _INITIAL_LEADER_GATE not in results, sorted(results)

    finish_time = _assert_clean_completed_client(results["client"], "kill-leader")
    # Leader death must not perturb an in-flight job owned by another
    # gate: probed completion at the baseline instant (~14.77).
    assert finish_time < 16.0, results["client"]

    # Eventual convergence: EXACTLY one surviving gate ends as leader.
    flags = _final_leader_flags(results, (_FOLLOWER_GATE, _SUBMISSION_GATE))
    assert sum(flags.values()) == 1, flags

    # Re-election liveness is decoupled from the (slow) SWIM death
    # reap: the winner claims leadership on missed leadership
    # heartbeats within (kill, kill+30] — probed 10.5s after the kill
    # (t=22.5), vs the membership drop at ~74.5s. A claim later than
    # +30 means election liveness regressed to riding full death
    # detection.
    winner = [
        gate_pid
        for gate_pid, flag in flags.items()
        if flag == 1
    ][0]
    claim_times = [
        entry[2]
        for entry in results[winner]
        if entry[0] == "gate-leader" and entry[1] == 1
    ]
    assert claim_times, results[winner]
    claim_latency = claim_times[0] - _MID_EXECUTION_AT
    assert 0.0 < claim_latency <= 30.0, (winner, claim_latency)


def test_kill_leader_is_replay_deterministic():
    assert _run_kill_leader() == _run_kill_leader()


def _run_kill_submission_gate() -> dict:
    """Kill the gate that ACCEPTED the job (gate-b, probed submit-target
    index 1) at t=12, while the workflow is mid-run: the manager's
    completion push hits a dead origin and must fail over to a
    surviving peer gate, which delivers the client-ready result."""
    coordinator = _build_cluster(_SEED, 150.0, wait_timeout=100.0)
    coordinator.schedule_kill(_SUBMISSION_GATE, _MID_EXECUTION_AT)
    return coordinator.run()


def test_kill_submission_gate_result_arrives_via_surviving_gates():
    results = _run_kill_submission_gate()
    _assert_no_unswapped_seams(results)
    assert _SUBMISSION_GATE not in results, sorted(results)

    client_log = results["client"]
    submit_targets = [
        entry for entry in client_log if entry[0] == "submit-target"
    ]
    # The victim IS the accepting gate — that is the whole scenario.
    assert submit_targets and submit_targets[0][1] == _SUBMISSION_GATE_INDEX, (
        client_log
    )
    finish_time = _assert_clean_completed_client(
        client_log, "kill-submission-gate"
    )
    # The completion push hits the DEAD origin gate, burns exactly one
    # failover timeout, and a surviving peer gate delivers: probed
    # finish at 19.789 = baseline 14.769 + the 5s dead-origin push
    # timeout. Bound: after the baseline (the detour is real) but
    # within one failover cycle (a second cycle means the first
    # surviving peer failed too).
    assert 16.0 < finish_time < 26.0, client_log

    flags = _final_leader_flags(results, (_FOLLOWER_GATE, _INITIAL_LEADER_GATE))
    assert sum(flags.values()) == 1, flags


def test_kill_submission_gate_is_replay_deterministic():
    assert _run_kill_submission_gate() == _run_kill_submission_gate()


# =========================================================================
# 2. gate_restart — power loss + amnesiac reboot (Phase 8 gap)
# =========================================================================


def _run_restart_submission_gate() -> dict:
    """Power-lose the accepting gate at t=12 for 20 virtual seconds.

    Gates have NO durable tier (Phase 8): generation 1 reboots with
    total amnesia about the job it accepted. The LOUD current behavior
    (probed) is that the manager's push failover re-homes the job to a
    surviving peer gate during the down window, so the client still
    observes a terminal outcome — never silence.
    """
    coordinator = _build_cluster(_SEED, 150.0, wait_timeout=100.0)
    coordinator.schedule_restart(
        _SUBMISSION_GATE, _MID_EXECUTION_AT, down_seconds=20.0
    )
    return coordinator.run()


def test_restart_submission_gate_client_still_observes_terminal():
    results = _run_restart_submission_gate()
    _assert_no_unswapped_seams(results)

    # The pre-restart generation's log is preserved under ``.gen1``
    # (the coordinator keys PRIOR generations as ``{pid}.gen{n}``; the
    # LIVE generation owns the bare pid — probed key shape). The
    # rebooted generation restarts its life from scratch (fresh
    # watcher state — probed: its leader watcher reports flag 0 at
    # exactly t=32.0, the reboot instant).
    prior_generation = results[f"{_SUBMISSION_GATE}.gen1"]
    assert any(
        entry[0] == "gate-started" for entry in prior_generation
    ), prior_generation
    rebooted_log = results[_SUBMISSION_GATE]
    rebooted_times = [
        entry[-1] for entry in rebooted_log if isinstance(entry[-1], float)
    ]
    assert rebooted_times and min(rebooted_times) >= 32.0, rebooted_log

    finish_time = _assert_clean_completed_client(
        results["client"], "restart-submission-gate"
    )
    # Probed: identical loud outcome to the KILL of the same gate —
    # completion at 19.789 via the peer-gate failover during the down
    # window. The client never notices the difference between a dead
    # and an amnesiac-rebooting origin gate; what it must never see is
    # silence.
    assert 16.0 < finish_time < 26.0, results["client"]

    # No split-brain across the reboot: at most one leader among ALL
    # three gates at the end, and exactly one somewhere.
    flags = _final_leader_flags(results, _GATE_PIDS)
    assert sum(flags.values()) == 1, flags


def test_restart_submission_gate_is_replay_deterministic():
    assert _run_restart_submission_gate() == _run_restart_submission_gate()


@pytest.mark.skip(reason="Phase 8: gate durable tier")
def test_restarted_gate_resumes_its_own_jobs_from_durable_state():
    """ASPIRATIONAL: a restarted gate should recover its accepted jobs
    from a durable ledger (as managers already do) and re-serve status
    for them itself, instead of relying on peer-gate failover. Today a
    rebooted gate has amnesia — no wal_data_dir / resume path exists
    for the gate tier."""
    raise AssertionError("requires the Phase 8 gate durable tier")


# =========================================================================
# 3. gate_partition — total leader isolation must not split-brain
# =========================================================================


def _run_leader_peer_isolation() -> dict:
    """Cut BOTH of the leader's peer links (gate-c <-> a and c <-> b)
    for 35 virtual seconds spanning live execution, leaving the
    leader's MANAGER link intact.

    Probed invariant (the no-false-death property, gate tier): a 35s
    peer cut is well inside the witness-less death bound (~[25,85]s of
    SUSTAINED silence measured from suspicion, and leadership liveness
    still disseminates via the manager plane), so the tier rides it
    out with ZERO membership churn — no peer-count drop, no election,
    no second leader — and the job (owned by gate-b, unpartitioned)
    completes at the baseline instant."""
    coordinator = _build_cluster(_SEED, 180.0, wait_timeout=120.0)
    coordinator.schedule_partition(
        _INITIAL_LEADER_GATE, _FOLLOWER_GATE, 10.0, heal_time=45.0
    )
    coordinator.schedule_partition(
        _INITIAL_LEADER_GATE, _SUBMISSION_GATE, 10.0, heal_time=45.0
    )
    return coordinator.run()


def test_leader_peer_isolation_causes_no_false_deaths_or_churn():
    results = _run_leader_peer_isolation()
    _assert_no_unswapped_seams(results)

    finish_time = _assert_clean_completed_client(
        results["client"], "leader-peer-isolation"
    )
    assert finish_time < 16.0, results["client"]

    # No false deaths: NO gate's active-peer count ever left 2 — the
    # heal-bounded cut must not evict anybody (probed: peer counts sit
    # at [0 -> 2 at t=0.5] and never move again).
    for gate_pid in _GATE_PIDS:
        peer_counts = [
            entry[1]
            for entry in results[gate_pid]
            if entry[0] == "gate-peers"
        ]
        assert peer_counts[-1] == 2, (gate_pid, results[gate_pid])
        assert 1 not in peer_counts and 0 not in peer_counts[1:], (
            gate_pid,
            peer_counts,
        )

    # Leadership never moved: the isolated leader retains its single
    # flag (leadership visibility rides the intact manager plane), and
    # no second leader ever arose anywhere.
    flags = _final_leader_flags(results, _GATE_PIDS)
    assert flags == {
        _FOLLOWER_GATE: 0,
        _SUBMISSION_GATE: 0,
        _INITIAL_LEADER_GATE: 1,
    }, flags
    for gate_pid in (_FOLLOWER_GATE, _SUBMISSION_GATE):
        claims = [
            entry
            for entry in results[gate_pid]
            if entry[0] == "gate-leader" and entry[1] == 1
        ]
        assert not claims, (gate_pid, claims)


def test_leader_peer_isolation_is_replay_deterministic():
    assert _run_leader_peer_isolation() == _run_leader_peer_isolation()


def _run_leader_total_isolation() -> dict:
    """ISLAND the leader completely: cut gate-c from BOTH peers AND
    the manager over [10, 100) — a 90s window that exceeds every
    detection bound, so this time the tier MUST act.

    Probed timeline: the majority (a, b) marks the unreachable leader
    dead and gate-b claims leadership at t=20.5 (leader-unreachability
    is evidence-accelerated — 10.5s, NOT the ~70s witness-less reap);
    the islanded gate-c keeps believing it is leader until heal
    (harmless: it can reach no client, no manager, no peer) and steps
    down at t=100.5 — 0.5s after heal — when it learns of the higher
    term. The job (owned by gate-b) completes at the baseline instant.

    Post-heal, the peer-readmission watch re-admits every falsely
    evicted membership (probed: all three gates back to active-peer
    count 2 at 107.5-108.0 — heal + one 10s dead-peer check tick +
    TCP liveness verification + recovery jitter): each gate
    TCP-ping-verifies the configured peers it holds DEAD and, on proof
    of life, drives the rejoin composite (death-record reset at the
    rejoin incarnation + probe re-enrolment + peer recovery + a fresh
    JOIN toward the peer so one-sided evictions heal symmetrically).
    """
    coordinator = _build_cluster(_SEED, 210.0, wait_timeout=120.0)
    coordinator.schedule_partition(
        _INITIAL_LEADER_GATE, _FOLLOWER_GATE, 10.0, heal_time=100.0
    )
    coordinator.schedule_partition(
        _INITIAL_LEADER_GATE, _SUBMISSION_GATE, 10.0, heal_time=100.0
    )
    coordinator.schedule_partition(
        _INITIAL_LEADER_GATE, "manager", 10.0, heal_time=100.0
    )
    return coordinator.run()


def test_leader_total_isolation_majority_elects_and_islander_steps_down():
    results = _run_leader_total_isolation()
    _assert_no_unswapped_seams(results)

    finish_time = _assert_clean_completed_client(
        results["client"], "leader-total-isolation"
    )
    assert finish_time < 16.0, results["client"]

    # Majority side: SOME majority gate claims leadership DURING the
    # isolation within the accelerated bound (probed: gate-b at 20.5 —
    # 10.5s after the cut; anything past +30 means unreachable-leader
    # acceleration regressed to the slow membership reap). The FINAL
    # holder may legitimately differ: when the healed islander returns,
    # the reforming tier runs one reconciliation handoff (probed:
    # gate-b steps down 102.5, gate-a claims 103.0 — strictly ordered,
    # never overlapping), so the during-isolation claim and the final
    # flag are asserted separately.
    majority_claims_during_isolation = [
        entry[2]
        for gate_pid in (_FOLLOWER_GATE, _SUBMISSION_GATE)
        for entry in results[gate_pid]
        if entry[0] == "gate-leader" and entry[1] == 1 and entry[2] < 100.0
    ]
    assert majority_claims_during_isolation, results
    assert 10.0 < min(majority_claims_during_isolation) <= 40.0, (
        majority_claims_during_isolation
    )

    majority_flags = _final_leader_flags(
        results, (_FOLLOWER_GATE, _SUBMISSION_GATE)
    )
    assert sum(majority_flags.values()) == 1, majority_flags

    # Post-heal stability: whatever reconciliation the reforming tier
    # ran, leadership must be QUIET once re-admission settles (probed:
    # last transition at 103.0; re-admission completes 107.5-108.0).
    for gate_pid in _GATE_PIDS:
        late_leader_moves = [
            entry
            for entry in results[gate_pid]
            if entry[0] == "gate-leader" and entry[2] > 110.0
        ]
        assert not late_leader_moves, (gate_pid, late_leader_moves)

    # The islanded ex-leader must NOT hold a leadership claim past
    # heal: it steps down as soon as connectivity returns (probed
    # t=100.5). Its claim while islanded is harmless split-window —
    # it can affect nobody — but persisting past heal would be real
    # split-brain.
    islander_log = results[_INITIAL_LEADER_GATE]
    islander_flags = [
        entry for entry in islander_log if entry[0] == "gate-leader"
    ]
    assert islander_flags[-1][1] == 0, islander_flags
    step_down_times = [
        entry[2] for entry in islander_flags if entry[1] == 0 and entry[2] > 10.0
    ]
    assert step_down_times and step_down_times[0] <= 105.0, islander_flags

    # Post-heal RE-ADMISSION (the peer-readmission watch): the false
    # deaths the isolation manufactured are undone once connectivity
    # returns — every gate ends with active-peer count 2 (probed
    # recovery instants 107.5-108.0; the mechanism bound is asserted
    # in test_gate_peers_readmit_after_total_isolation_heals).
    for gate_pid in _GATE_PIDS:
        peer_counts = [
            entry[1]
            for entry in results[gate_pid]
            if entry[0] == "gate-peers"
        ]
        assert peer_counts[-1] == 2, (gate_pid, peer_counts)


def test_leader_total_isolation_is_replay_deterministic():
    assert _run_leader_total_isolation() == _run_leader_total_isolation()


def test_gate_peers_readmit_after_total_isolation_heals():
    """The re-admission watch AND the originator-corrected suspicion
    math, pinned together — the two sides of the isolation behave
    asymmetrically ON PURPOSE:

    * The MAJORITY gates (two independent observers) evict the
      unreachable leader fast (leader-unreachability acceleration —
      probed 26.0) and the re-admission watch restores it after the
      heal via TCP liveness proof + the rejoin-incarnation reset.
      Design bound: after heal (t=100) and within heal + one 10s
      dead-peer check tick + the 2s TCP verification timeout +
      recovery jitter + the 0.5s sampler (probed: 105.5 and 107.0).
    * The ISLANDER never evicts anyone: its suspicions of the two
      unreachable peers carry ZERO independent confirmations
      (originator semantics — its own accusation is the suspicion,
      not corroboration), so the bracket holds at the honest
      no-corroboration maximum (probed 144.55s > the 90s window) and
      the heal REFUTES the suspicions before expiry. Its peer count
      holds at 2 for the entire run — no churn, no re-admission
      needed. (Pre-fix, the islander's self-vote cut the bracket to
      ~72s, it evicted both peers on zero external evidence at ~87,
      and re-admission had to repair it.)
    """
    results = _run_leader_total_isolation()
    _assert_no_unswapped_seams(results)

    heal_time = 100.0
    readmission_deadline = heal_time + 15.0
    for gate_pid in (_FOLLOWER_GATE, _SUBMISSION_GATE):
        peer_counts = [
            entry
            for entry in results[gate_pid]
            if entry[0] == "gate-peers"
        ]
        assert peer_counts[-1][1] == 2, (gate_pid, peer_counts)
        eviction_dips = [
            entry for entry in peer_counts if entry[1] < 2 and entry[2] > 5.0
        ]
        assert eviction_dips, (
            "majority gates must evict the unreachable leader",
            gate_pid,
            peer_counts,
        )
        recovery_times = [
            entry[2]
            for entry in peer_counts
            if entry[1] == 2 and entry[2] > heal_time
        ]
        assert recovery_times, (gate_pid, peer_counts)
        assert recovery_times[0] <= readmission_deadline, (
            gate_pid,
            recovery_times,
        )

    islander_counts = [
        entry
        for entry in results[_INITIAL_LEADER_GATE]
        if entry[0] == "gate-peers"
    ]
    assert islander_counts[-1][1] == 2, islander_counts
    islander_dips = [
        entry for entry in islander_counts if entry[1] < 2 and entry[2] > 5.0
    ]
    assert not islander_dips, (
        "the islander evicted on uncorroborated self-accusation — the "
        f"originator-excluded suspicion math regressed: {islander_counts}"
    )


def _run_submission_blackout() -> dict:
    """Cut the client from ALL THREE gates for the entire retry budget
    (a 150s blackout against a 2-attempt client): submission must be
    abandoned LOUDLY — rejection milestones then an explicit
    submit-abandoned — never a silent hang.

    Probed timeline: submit_job cycle 1 exhausts its 6 target attempts
    against silent cuts and raises at t=60.0; cycle 2 raises at
    t=121.0; the bounded client logs submit-abandoned at t=121.0."""
    coordinator = _build_cluster(
        _SEED, 160.0, wait_timeout=60.0, max_submit_attempts=2
    )
    for gate_pid in _GATE_PIDS:
        coordinator.schedule_partition(
            "client", gate_pid, 0.0, heal_time=150.0
        )
    return coordinator.run()


def test_submission_blackout_abandons_loudly_never_silently():
    results = _run_submission_blackout()
    _assert_no_unswapped_seams(results)

    client_log = results["client"]
    rejections = [
        entry for entry in client_log if entry[0] == "submit-rejected"
    ]
    abandoned = [
        entry for entry in client_log if entry[0] == "submit-abandoned"
    ]
    assert len(rejections) == 2, client_log
    assert len(abandoned) == 1, client_log
    # Each blackout submit cycle costs its full target-attempt budget
    # (probed: 60.0s and 121.0s); the loud give-up follows immediately.
    assert 55.0 <= rejections[0][-1] <= 65.0, client_log
    assert 115.0 <= abandoned[0][1] <= 130.0, client_log

    # Nothing was ever accepted and nothing finished — and that is the
    # LOUD contract: no job-submitted, no job-finished, no silence.
    assert not [
        entry
        for entry in client_log
        if entry[0] in ("job-submitted", "job-finished", "client-error")
    ], client_log

    # The gate tier itself is untouched: single stable leader.
    flags = _final_leader_flags(results, _GATE_PIDS)
    assert sum(flags.values()) == 1, flags


def test_submission_blackout_is_replay_deterministic():
    assert _run_submission_blackout() == _run_submission_blackout()


def _run_dispatch_window_manager_partition() -> dict:
    """Cut the ACCEPTING gate (gate-b) from the manager over [6, 26) —
    the job is accepted at t=5.53, so the ENTIRE dispatch window lands
    inside the cut.

    Probed invariant: an accepted job's dispatch is not a one-shot —
    the gate retries against the cut for its whole span and lands the
    dispatch immediately after heal ('running' at t=27.03, worker
    active [27.0, 38.5], completion at t=32.888 = post-heal dispatch +
    the full 6s duration + push). No failure, no silence, no
    leadership disturbance. (Contrast, documented in the report: a
    SECOND job's dispatch dies in ~5.4s — the retry robustness exists
    only on this first-job path today.)"""
    coordinator = _build_cluster(_SEED, 150.0, wait_timeout=100.0)
    coordinator.schedule_partition(
        _SUBMISSION_GATE, "manager", 6.0, heal_time=26.0
    )
    return coordinator.run()


def test_dispatch_window_manager_partition_retries_across_the_cut():
    results = _run_dispatch_window_manager_partition()
    _assert_no_unswapped_seams(results)

    client_log = results["client"]
    finish_time = _assert_clean_completed_client(
        client_log, "dispatch-window-partition"
    )

    # Accepted BEFORE the cut, on the gate the cut targets.
    submit_targets = [
        entry for entry in client_log if entry[0] == "submit-target"
    ]
    assert submit_targets and submit_targets[0][1] == _SUBMISSION_GATE_INDEX, (
        client_log
    )
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted and submitted[0][1] < 6.0, client_log

    # Execution began only AFTER the heal (probed 'running' at 27.03):
    # the dispatch rode the entire 20s cut on retries instead of
    # failing the accepted job.
    running_seen = [
        entry
        for entry in client_log
        if entry[0] == "status-seen" and entry[1] == "running"
    ]
    assert running_seen and 26.0 < running_seen[0][2] <= 29.0, client_log

    # Completion = post-heal dispatch + the full duration + push
    # (probed 32.888); past 36 would mean extra dispatch cycles.
    assert 32.0 < finish_time < 36.0, client_log

    # The membership plane never flinched: single stable leader.
    flags = _final_leader_flags(results, _GATE_PIDS)
    assert flags == {
        _FOLLOWER_GATE: 0,
        _SUBMISSION_GATE: 0,
        _INITIAL_LEADER_GATE: 1,
    }, flags


def test_dispatch_window_manager_partition_is_replay_deterministic():
    assert (
        _run_dispatch_window_manager_partition()
        == _run_dispatch_window_manager_partition()
    )


# =========================================================================
# 4. client connectivity — partitions + delay on the client links
# =========================================================================


def _run_client_link_faults() -> dict:
    """Client-tier connectivity faults, all three classes at once:

    * client <-> gate-a cut over [0, 12): the FIRST submission target
      is unreachable, so acceptance must converge through the retry
      cycle onto a reachable gate;
    * client <-> gate-b cut over [13, 26): the delivery window of the
      accepting gate — status pushes are lost mid-flight and the poll
      fallback must recover the terminal outcome after heal;
    * 50ms (+20ms seeded jitter) delay on every client link over
      [5, 45): late pushes race polls; the client's order guard (the
      oracle checks the observed history) must hold.
    """
    coordinator = _build_cluster(_SEED, 180.0, wait_timeout=120.0)
    coordinator.schedule_partition("client", "gate-a", 0.0, heal_time=12.0)
    coordinator.schedule_partition("client", "gate-b", 13.0, heal_time=26.0)
    for gate_pid in _GATE_PIDS:
        coordinator.schedule_delay(
            "client",
            gate_pid,
            0.05,
            at_time=5.0,
            until_time=45.0,
            jitter_seconds=0.02,
        )
        coordinator.schedule_delay(
            gate_pid,
            "client",
            0.05,
            at_time=5.0,
            until_time=45.0,
            jitter_seconds=0.02,
        )
    return coordinator.run()


def test_client_link_faults_converge_to_clean_completion():
    results = _run_client_link_faults()
    _assert_no_unswapped_seams(results)

    client_log = results["client"]
    finish_time = _assert_clean_completed_client(client_log, "client-link-faults")

    # Acceptance converged through the retry cycle: the cut first
    # target costs the silent 10s TCP timeout (no fast rejection —
    # probed: ZERO submit-rejected milestones, unlike the warmup
    # rejections of the fault-free baseline), then the next target
    # accepts. Probed acceptance t=10.617 at gate index 1.
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted and 10.0 <= submitted[0][1] < 12.0, client_log

    # Delivery beat the delivery-window cut's HEAL (probed finish
    # 22.078 < heal 26.0): the accepting gate's push failed into the
    # cut and a peer gate delivered the client-ready result — the
    # push-failover path, not the poll fallback, is what carried it.
    assert finish_time < 26.0, client_log

    flags = _final_leader_flags(results, _GATE_PIDS)
    assert sum(flags.values()) == 1, flags


def test_client_link_faults_are_replay_deterministic():
    assert _run_client_link_faults() == _run_client_link_faults()


# =========================================================================
# 5. long-horizon — chaos waves, quiesce, convergence probe
# =========================================================================


def _run_long_horizon_chaos_waves() -> dict:
    """420 virtual seconds, three separated fault waves, then quiet:

    * wave 1 (t=12): the follower gate dies for good — INSIDE live
      execution (probed window [8.75, 14.77] client-visible), so the
      kill provably intersects the running workflow;
    * wave 2 (t=30-60): the two surviving gates partition from each
      other — a heal-bounded cut the membership must ride out with
      zero churn (the no-false-death property);
    * wave 3 (t=80-115): membership-plane noise (20% loss + 50%
      duplication) between the survivors and the manager;
    * quiesce: nothing after t=115; a SECOND client starts at t=300 —
      185 quiet seconds later.

    The late client pins the POST-KILL SUBMISSION path: acceptance is
    RESTORED (the capacity axis of the gate's overload classifier no
    longer maps a zero-available-cores heartbeat to UNHEALTHY — full
    capacity is BUSY/DEGRADED per the AD-16 contract, so the tier
    accepts instead of fast-rejecting forever). Probed: the late
    submission burns the silent 10s TCP timeout on the dead first
    target, is accepted at gate index 1 at t=310.12, and ends in a
    LOUD AD-34 global ``timeout`` at t=350.037 — the RESIDUAL gap is
    manager-tier (traced: the worker's SWIM-embedded heartbeats starve
    while its tracker holds an unresolvable SUSPECT for the killed
    gate; WorkerPool liveness stales at +30s, the routing decision
    flips EVICT, and core allocation starves against an idle worker),
    so the accepted job cannot COMPLETE until that tier is fixed —
    the aspirational completion test stays skip-marked with that
    mechanism.

    The leadership tail pins the composite CONVERGING: the mutual
    partition eviction of the survivors is undone by the post-heal
    peer-readmission watch (probed: both survivors re-admit each other
    at t=62.0 — heal 60 + one check tick), so the initial leader's
    quorum recovers before consecutive-failure step-down matures and
    leadership simply HOLDS (no step-down at 73.5, no churn, exactly
    one leader end to end). The killed gate's membership is reaped on
    the witness-less bound (gate-b peer count 2 -> 1 at 88.5) and
    never re-admitted — its readmission ping fails forever, which is
    the correct truth for a genuinely dead peer.
    """
    coordinator = _build_cluster(
        _SEED, 420.0, wait_timeout=100.0, late_client_at=300.0
    )
    coordinator.schedule_kill(_FOLLOWER_GATE, _MID_EXECUTION_AT)
    coordinator.schedule_partition(
        _SUBMISSION_GATE, _INITIAL_LEADER_GATE, 30.0, heal_time=60.0
    )
    coordinator.schedule_drop_rate(
        _INITIAL_LEADER_GATE, "manager", 0.20, at_time=80.0, until_time=110.0
    )
    coordinator.schedule_drop_rate(
        "manager", _INITIAL_LEADER_GATE, 0.20, at_time=80.0, until_time=110.0
    )
    coordinator.schedule_duplicate(
        "manager", _SUBMISSION_GATE, 0.5, at_time=80.0, until_time=115.0
    )
    coordinator.schedule_duplicate(
        _SUBMISSION_GATE, "manager", 0.5, at_time=80.0, until_time=115.0
    )
    return coordinator.run()


def test_long_horizon_chaos_then_quiesce_holds_loud_invariants():
    results = _run_long_horizon_chaos_waves()
    _assert_no_unswapped_seams(results)
    assert _FOLLOWER_GATE not in results, sorted(results)

    # The primary job — mid-execution when wave 1 landed — completed
    # at the baseline instant (probed: a follower kill is invisible to
    # the job path) and survived waves 2-3 untouched.
    primary_finish = _assert_clean_completed_client(
        results["client"], "long-horizon primary"
    )
    assert primary_finish < 16.0, results["client"]

    # The post-quiesce client COMPLETES — the full convergence
    # invariant, live end to end: the first target (the killed gate)
    # costs the silent 10s TCP timeout, the next target accepts
    # (probed t=310.12 at gate index 1), dispatch reaches the worker,
    # and the client observes ``completed`` at 316.24 (= acceptance +
    # ~1s dispatch + workflow + push detour). Both halves of the old
    # post-kill blinding are gone: the gate half (capacity->UNHEALTHY
    # fast-reject) fell to the BUSY!=UNHEALTHY overload config, and
    # the manager half (worker-heartbeat starvation -> allocation
    # starves) fell to the SWIM probe-cycle repairs — the worker
    # provably ACTIVATES for the late job.
    late_log = results["late-client"]
    assert not JobStatusOracle().check_client_log(late_log), late_log
    assert not [
        entry
        for entry in late_log
        if entry[0]
        in ("submit-rejected", "client-error", "wait-timeout")
    ], late_log
    late_submit_targets = [
        entry for entry in late_log if entry[0] == "submit-target"
    ]
    assert late_submit_targets, late_log
    assert late_submit_targets[0][1] == _SUBMISSION_GATE_INDEX, late_log
    late_submitted = [
        entry for entry in late_log if entry[0] == "job-submitted"
    ]
    assert late_submitted, late_log
    late_submitted_time = late_submitted[0][1]
    assert 310.0 <= late_submitted_time <= 312.0, late_log
    late_finished = [
        entry for entry in late_log if entry[0] == "job-finished"
    ]
    assert len(late_finished) == 1, late_log
    assert late_finished[0][1] == "completed", (
        "the late job must COMPLETE (both blinding halves are fixed); "
        f"a timeout here is a regression: {late_log}"
    )
    assert (
        late_submitted_time
        < late_finished[0][2]
        <= late_submitted_time + 10.0
    ), late_log
    late_worker_activations = [
        entry
        for entry in results["worker"]
        if entry[0] == "workflows-active"
        and entry[1] > 0
        and entry[2] > 300.0
    ]
    assert late_worker_activations, (
        "the late job completed so the worker must show its execution",
        results["worker"],
    )

    # Leadership tail — the composite CONVERGES: post-heal peer
    # re-admission (probed: both survivors back to each other at
    # t=62.0) restores the initial leader's quorum before step-down
    # matures, so leadership HOLDS end to end — the initial leader
    # keeps its single flag, the other survivor never claims, and no
    # leader transition of any kind lands after t=10.
    flags = _final_leader_flags(
        results, (_SUBMISSION_GATE, _INITIAL_LEADER_GATE)
    )
    assert flags == {_SUBMISSION_GATE: 0, _INITIAL_LEADER_GATE: 1}, flags
    for gate_pid in (_SUBMISSION_GATE, _INITIAL_LEADER_GATE):
        late_leader_moves = [
            entry
            for entry in results[gate_pid]
            if entry[0] == "gate-leader" and entry[2] > 10.0
        ]
        assert not late_leader_moves, (gate_pid, late_leader_moves)

    # Membership tail. The KILLED gate is detected inside the
    # evidence-accelerated bound and stays out (its readmission ping
    # fails forever). The survivor pair rides its own partition wave
    # [30, 60) with NO false deaths AT ALL: each survivor's direct
    # probes to the other fail, but the indirect path through the
    # MANAGER (a live common witness) confirms the peer alive —
    # working witness confirmation is exactly what the shared/shielded
    # ack-future repair restored (pre-repair, witness confirmations
    # aborted on future clobbering, the pair mutually declared death
    # during the cut, and re-admission had to heal it at ~62). Any
    # transient false death under the NOISE wave must be undone by the
    # re-admission watch within its design bound (10s check tick +
    # TCP verify — observed: one blip at 113.0 healed at 115.0), and
    # both survivors end with exactly ONE active peer — each other.
    for gate_pid in (_SUBMISSION_GATE, _INITIAL_LEADER_GATE):
        peer_entries = [
            entry
            for entry in results[gate_pid]
            if entry[0] == "gate-peers"
        ]
        assert peer_entries[-1][1] == 1, (gate_pid, peer_entries)

        kill_detections = [
            entry
            for previous_entry, entry in zip(peer_entries, peer_entries[1:])
            if entry[1] < previous_entry[1]
            and _MID_EXECUTION_AT + 5.0 <= entry[2] <= _MID_EXECUTION_AT + 30.0
        ]
        assert kill_detections, (gate_pid, peer_entries)

        # Every false death (partition-wave or noise-wave) heals inside
        # the re-admission bound: a drop to zero peers is followed by a
        # rise within max(heal, drop) + 12s (10s check tick + verify —
        # probed: gate-c's wave-2 eviction at 37.5 re-admits at 62.5 =
        # heal 60 + 2.5; the noise-wave blip at 113 re-admits at 115).
        for index, entry in enumerate(peer_entries):
            if entry[1] == 0 and entry[2] > 30.0:
                recovery_deadline = max(60.0, entry[2]) + 12.0
                recoveries = [
                    later
                    for later in peer_entries[index + 1 :]
                    if later[1] >= 1 and later[2] <= recovery_deadline
                ]
                assert recoveries, (gate_pid, entry, peer_entries)

    # AT MOST ONE survivor evicts the other during the partition wave
    # — never both (mutual eviction was the old leaderless precursor).
    # Mechanism asymmetry, pinned deliberately: a survivor holding TWO
    # silent targets (the killed follower + the cut peer) crosses the
    # AD-53 burst threshold, and the burst's candidate confirmation
    # runs DIRECT-ONLY (no indirect-witness leg) — so gate-c evicts
    # gate-b at 37.5 despite the live manager witness, while gate-b's
    # ordinary probe path (one silent target, witness consulted via
    # indirect probe) correctly holds gate-c alive. The re-admission
    # watch repairs the one-sided eviction at heal+2.5. (Adding the
    # indirect leg to AD-53 burst confirmation is a queued follow-up;
    # this pin flips when it lands.)
    survivors_with_wave_eviction = [
        gate_pid
        for gate_pid in (_SUBMISSION_GATE, _INITIAL_LEADER_GATE)
        if any(
            entry[1] == 0 and 30.0 <= entry[2] <= 60.0
            for entry in results[gate_pid]
            if entry[0] == "gate-peers"
        )
    ]
    assert len(survivors_with_wave_eviction) <= 1, (
        survivors_with_wave_eviction
    )


def test_long_horizon_chaos_is_replay_deterministic():
    assert _run_long_horizon_chaos_waves() == _run_long_horizon_chaos_waves()


def test_long_horizon_late_job_completes_after_quiesce():
    """FIXED-BUG PIN (both halves of the post-kill blinding): a job
    submitted long after the chaos quiesces COMPLETES through the
    surviving tier. Half one — acceptance — fell to the gate-side
    BUSY!=UNHEALTHY overload config (accepted t=310.12). Half two —
    execution — was the manager-tier worker-heartbeat starvation: the
    heartbeat carrier died with the node's SWIM probe cycle (a stray
    ack-future cancellation read as shutdown), WorkerPool liveness
    staled to EVICT, and allocation starved an idle healthy worker to
    the loud AD-34 timeout at 350.037. With the probe cycle immortal
    (shared+shielded ack futures; genuine-cancel discrimination) the
    heartbeats never stop and the late job runs to ``completed`` at
    316.24 — asserted with its execution evidence in the primary
    long-horizon test; this test pins the end-to-end claim."""
    results = _run_long_horizon_chaos_waves()
    late_log = results["late-client"]
    late_finished = [
        entry for entry in late_log if entry[0] == "job-finished"
    ]
    assert len(late_finished) == 1, late_log
    assert late_finished[0][1] == "completed", late_log


def test_long_horizon_survivors_reelect_exactly_one_leader():
    """After the chaos waves quiesce, the surviving two-gate tier ends
    with EXACTLY ONE leader. Post-heal peer re-admission (probed:
    survivors re-admit each other at t=62.0) restores quorum before
    the initial leader's consecutive-quorum-failure step-down matures,
    so the composite converges by the leader simply HOLDING — probed:
    the initial leader keeps flag 1 from t=1.5 through the t=420
    ceiling with zero transitions, and the other survivor never
    claims. (Pre-fix truth: mutual partition eviction decayed quorum,
    the leader stepped down at t=73.5, and the remainder was
    permanently leaderless.)"""
    results = _run_long_horizon_chaos_waves()
    _assert_no_unswapped_seams(results)

    flags = _final_leader_flags(
        results, (_SUBMISSION_GATE, _INITIAL_LEADER_GATE)
    )
    assert sum(flags.values()) == 1, flags


# =========================================================================
# 6. client restart — power loss of a leaf client
# =========================================================================


def _run_client_restart() -> dict:
    """Power-lose the CLIENT at t=12 (mid-execution) for 10 seconds.

    The client is a leaf (no spawned children) with no durable state:
    the reboot re-runs the entry from its spec, so the successor
    generation is a brand-new client that submits a FRESH job.

    Ceiling 100 is deliberate — probed: the manager's client-orphan
    machinery busy-spins the virtual clock at t=142.14 (a
    ``Timeout._on_timeout`` re-arming at the same instant once the
    120s CLIENT_ORPHAN_GRACE_PERIOD after the vanished first client
    expires; wall clocks advance through the spin, the virtual clock
    does not — the same liveness-gap class as the VU-generator spin).
    The scenario ends before that instant and the gap is documented
    rather than crashed into."""
    coordinator = _build_cluster(_SEED, 100.0, wait_timeout=60.0)
    coordinator.schedule_restart("client", _MID_EXECUTION_AT, down_seconds=10.0)
    return coordinator.run()


def test_client_restart_first_job_survives_and_successor_is_loud():
    results = _run_client_restart()
    _assert_no_unswapped_seams(results)

    # The pre-restart generation (preserved under ``client.gen1``)
    # submitted and observed 'running', then power-lost mid-wait: its
    # log ends WITHOUT a terminal milestone — exactly what it saw.
    prior_generation = results["client.gen1"]
    assert any(
        entry[0] == "job-submitted" for entry in prior_generation
    ), prior_generation
    assert any(
        entry[0] == "status-seen" and entry[1] == "running"
        for entry in prior_generation
    ), prior_generation
    assert not [
        entry for entry in prior_generation if entry[0] == "job-finished"
    ], prior_generation

    # Server side never wedged on the vanished client: the worker ran
    # the first job to full drain (probed window [8.75, 20.25]).
    worker_log = results["worker"]
    drain_times = [
        entry[2]
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] == 0 and entry[2] > 0.0
    ]
    assert drain_times and drain_times[0] > 15.0, worker_log

    # The successor generation is LOUD end to end: its fresh job is
    # accepted (probed t=22.12 at gate index 0) and reaches an explicit
    # terminal the client SEES — since the overload-detector fix
    # (workflow duration is no longer a latency sample, so the first
    # job's 6s execution no longer flips the worker overloaded at
    # drain) that terminal is 'completed' at t=28.26 (= second
    # dispatch 22.25 + the 6s duration + push; the worker's second
    # activation window is asserted below via the drain check). The
    # completion invariant itself lives in
    # test_client_restart_successor_job_completes.
    successor_log = results["client"]
    assert not JobStatusOracle().check_client_log(successor_log), successor_log
    assert not [
        entry
        for entry in successor_log
        if entry[0] in ("client-error", "wait-timeout")
    ], successor_log
    submitted = [
        entry for entry in successor_log if entry[0] == "job-submitted"
    ]
    assert submitted and submitted[0][1] > _MID_EXECUTION_AT, successor_log
    finished = [
        entry for entry in successor_log if entry[0] == "job-finished"
    ]
    assert len(finished) == 1, successor_log
    assert finished[0][1] in _TERMINAL_STATUSES, successor_log

    flags = _final_leader_flags(results, _GATE_PIDS)
    assert sum(flags.values()) == 1, flags


def test_client_restart_is_replay_deterministic():
    assert _run_client_restart() == _run_client_restart()


def test_client_restart_successor_job_completes():
    """The successor client's fresh job COMPLETES. The second-job
    dispatch failure this pinned (accepted then 'failed' in ~5.4s,
    worker never activating) was the workflow-duration-as-latency
    overload poisoning: the first job's 6s execution was recorded as
    a >2000ms latency sample at drain, flipping the worker OVERLOADED
    permanently, so the manager's allocator starved every later
    dispatch. With the duration sample removed the successor dispatch
    allocates immediately: probed acceptance t=22.12, worker second
    activation [22.25, 33.75], client-visible completion t=28.26
    (= dispatch + the 6s duration + push). Bound: after the successor
    submission, within dispatch + duration + one push cycle — drift
    past 32 means the second dispatch is riding retries again."""
    results = _run_client_restart()
    _assert_no_unswapped_seams(results)

    successor_log = results["client"]
    finished = [
        entry for entry in successor_log if entry[0] == "job-finished"
    ]
    assert len(finished) == 1, successor_log
    assert finished[0][1] == "completed", successor_log
    assert _MID_EXECUTION_AT < finished[0][2] < 32.0, successor_log

    # The worker really ran it: a second activation window opens after
    # the successor submission and drains back to zero.
    worker_log = results["worker"]
    second_activations = [
        entry
        for entry in worker_log
        if entry[0] == "workflows-active"
        and entry[1] > 0
        and entry[2] > _MID_EXECUTION_AT
    ]
    assert second_activations, worker_log
    assert worker_log[-1][:2] == ("workflows-active", 0), worker_log
