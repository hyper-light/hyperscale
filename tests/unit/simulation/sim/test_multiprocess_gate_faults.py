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
  steps down at heal, and the split-window is harmless — with the
  post-heal peer re-admission gap pinned and skip-marked.
* client connectivity — submission-window cuts converge acceptance
  through the retry cycle; delivery-window cuts are beaten by push
  failover; a full blackout ends in LOUD abandonment; delay+jitter
  never regress the observed status order.
* long-horizon — 420 virtual seconds: kill inside live execution,
  partition + noise waves, then quiesce; the primary job completes,
  and the tail pins TWO probed gaps as loud current truth: the
  post-kill submission blinding (late client rejected on a ~11s
  cadence) and the kill+partition composite leaving the surviving
  pair permanently leaderless (step-down at 73.5, no re-claim) —
  aspirational completion and re-election invariants skip-marked.
* client restart — power-loss of the client (a leaf): the first job
  survives server-side, the successor generation is loud end to end —
  with the second-job dispatch-failure gap pinned (aspirational
  completion skip-marked) and the ceiling held below the manager's
  client-orphan virtual-time spin (documented liveness gap).
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
    # production SWIM inside the witness-less design bound [25, 85]s
    # after the kill (probed: ~70.0s and ~71.5s — the AD-30 max leg;
    # see test_multiprocess_network_faults for the traced
    # decomposition).
    for surviving_gate in (_SUBMISSION_GATE, _INITIAL_LEADER_GATE):
        drop_times = [
            entry[2]
            for entry in results[surviving_gate]
            if entry[0] == "gate-peers" and entry[1] == 1
        ]
        assert drop_times, results[surviving_gate]
        detection_latency = drop_times[0] - _MID_EXECUTION_AT
        assert 25.0 <= detection_latency <= 85.0, (
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

    # Majority side: the new leader claims within the accelerated
    # bound (probed t=20.5 — 10.5s after the cut; anything past +30
    # means unreachable-leader acceleration regressed to the slow
    # membership reap).
    majority_flags = _final_leader_flags(
        results, (_FOLLOWER_GATE, _SUBMISSION_GATE)
    )
    assert sum(majority_flags.values()) == 1, majority_flags
    majority_winner = [
        gate_pid for gate_pid, flag in majority_flags.items() if flag == 1
    ][0]
    claim_times = [
        entry[2]
        for entry in results[majority_winner]
        if entry[0] == "gate-leader" and entry[1] == 1
    ]
    assert claim_times and 10.0 < claim_times[0] <= 40.0, (
        majority_winner,
        claim_times,
    )

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

    # KNOWN GAP (pinned as the loud current truth): after the heal the
    # evicted memberships are never re-admitted — every gate ends with
    # active-peer count 1, not 2 (leadership/term information flows
    # again, but the peer registries stay degraded). The aspirational
    # re-admission invariant is the skip-marked test below.
    for gate_pid in _GATE_PIDS:
        peer_counts = [
            entry[1]
            for entry in results[gate_pid]
            if entry[0] == "gate-peers"
        ]
        assert peer_counts[-1] == 1, (gate_pid, peer_counts)


def test_leader_total_isolation_is_replay_deterministic():
    assert _run_leader_total_isolation() == _run_leader_total_isolation()


@pytest.mark.skip(reason="gate peer re-admission after partition-driven eviction")
def test_gate_peers_readmit_after_total_isolation_heals():
    """ASPIRATIONAL: after a partition-driven eviction heals, the gate
    tier's peer registries should re-admit the evicted gates (as the
    worker tier's rejoin machinery does after manager<->worker cuts).
    Probed today: every gate ends the total-isolation scenario with
    active-peer count 1 — the tier never regains full membership."""
    raise AssertionError("requires gate peer rejoin-after-eviction")


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

    The late client pins a DOCUMENTED GAP as loud current truth: from
    ~2.5s after any gate kill, the surviving gates classify the DC
    unhealthy permanently (the manager-side heartbeat/health path
    never recovers while the dead gate stays dead — probed: recovery
    resumes only when a RESTARTED gate returns), so post-kill
    submissions are explicitly REJECTED on a ~11s cadence (one 10s
    dead-gate timeout + a fast permanent rejection per cycle), forever.
    Loud — never silence.

    The leadership tail pins a SECOND probed gap: the kill composed
    with the survivor partition leaves the two-gate remainder
    PERMANENTLY LEADERLESS — the initial leader steps down at t=73.5
    (its view of the tier decays: the killed gate plus the partition
    cost it quorum) and NO gate ever re-claims through t=420, even
    though each fault alone converges cleanly (kill-only: leader
    holds; partition-only: zero churn). The aspirational invariants
    (late submission completes; tier re-elects) are the skip-marked
    tests below.
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

    # The post-quiesce client is LOUD, never silent: every submission
    # cycle ends in an explicit rejection (documented post-kill
    # submission-blinding gap), no acceptance, no phantom outcome, no
    # client error, and the rejection stream keeps flowing until the
    # ceiling (>= 3 cycles proves it is periodic, not a one-shot).
    late_log = results["late-client"]
    assert not JobStatusOracle().check_client_log(late_log), late_log
    late_rejections = [
        entry for entry in late_log if entry[0] == "submit-rejected"
    ]
    assert len(late_rejections) >= 3, late_log
    assert late_rejections[0][-1] >= 300.0, late_log
    assert not [
        entry
        for entry in late_log
        if entry[0]
        in ("job-submitted", "job-finished", "client-error", "wait-timeout")
    ], late_log

    # Leadership tail — pinned CURRENT truth (documented gap): the
    # kill + survivor-partition composite leaves the tier LEADERLESS.
    # The initial leader steps down inside (60, 85] (probed 73.5 —
    # after the partition heals but as its decayed membership view
    # matures) and NOBODY re-claims for the remaining ~346 virtual
    # seconds: the final flags are all zero and no gate-leader
    # transition of any kind lands after t=85. Loud and stable — but
    # the wrong stable state; the re-election requirement is the
    # skip-marked aspirational test below.
    flags = _final_leader_flags(
        results, (_SUBMISSION_GATE, _INITIAL_LEADER_GATE)
    )
    assert flags == {_SUBMISSION_GATE: 0, _INITIAL_LEADER_GATE: 0}, flags
    step_downs = [
        entry
        for entry in results[_INITIAL_LEADER_GATE]
        if entry[0] == "gate-leader" and entry[1] == 0 and entry[2] > 10.0
    ]
    assert step_downs and 60.0 < step_downs[0][2] <= 85.0, step_downs
    for gate_pid in (_SUBMISSION_GATE, _INITIAL_LEADER_GATE):
        late_leader_moves = [
            entry
            for entry in results[gate_pid]
            if entry[0] == "gate-leader" and entry[2] > 85.0
        ]
        assert not late_leader_moves, (gate_pid, late_leader_moves)


def test_long_horizon_chaos_is_replay_deterministic():
    assert _run_long_horizon_chaos_waves() == _run_long_horizon_chaos_waves()


@pytest.mark.skip(reason="post-kill submission blinding + second-job dispatch "
                  "failure (see report): late submissions cannot succeed today")
def test_long_horizon_late_job_completes_after_quiesce():
    """ASPIRATIONAL: a job submitted long after the chaos quiesces
    should COMPLETE through the surviving tier. Probed today it cannot:
    (1) after any gate kill the surviving gates classify the DC
    unhealthy permanently and reject all new submissions; (2) even
    fault-free, a second/late job is accepted and then fails in ~5.4s
    because gate->DC dispatch never reaches the (healthy) manager."""
    raise AssertionError("requires the post-kill health recovery and "
                         "second-job dispatch fixes")


@pytest.mark.skip(reason="leaderless remainder after kill+partition composite "
                  "(see report): no re-election happens today")
def test_long_horizon_survivors_reelect_exactly_one_leader():
    """ASPIRATIONAL: after the chaos waves quiesce, the surviving
    two-gate tier should converge back to EXACTLY ONE leader. Probed
    today: the kill + survivor-partition composite makes the initial
    leader step down at t=73.5 and no gate ever claims again through
    t=420 — a permanently leaderless (but loud) remainder, even though
    each fault alone converges cleanly."""
    raise AssertionError("requires re-election liveness after composite "
                         "kill+partition membership decay")


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
    # terminal the client SEES. KNOWN GAP pinned here as current truth:
    # that terminal is 'failed' (probed t=27.49) because a second job
    # accepted by a gate that has not previously dispatched dies in
    # gate->DC dispatch (~5.4s, worker never activates) — the same
    # second-job dispatch failure the long-horizon scenario documents.
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


@pytest.mark.skip(reason="second-job gate dispatch failure (see report): a job "
                  "accepted by a not-previously-dispatching gate dies in "
                  "gate->DC dispatch")
def test_client_restart_successor_job_completes():
    """ASPIRATIONAL: the successor client's fresh job should COMPLETE.
    Probed today it is accepted and then fails in ~5.4s because the
    accepting gate's dispatch to the (healthy) manager times out —
    reproduced fault-free by any second/late job whose accepting gate
    never dispatched before (probe evidence in the program report)."""
    raise AssertionError("requires the second-job dispatch fix")
