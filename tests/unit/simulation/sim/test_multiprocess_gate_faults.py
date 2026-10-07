"""
Pinned EXTREME gate-cluster fault scenarios under multi-process SIM —
the schedules too adversarial for the generated vopr_gates space,
each probed first and pinned with observed-timeline constants.

Canonical topology in every scenario: three peered ``GateServer``
processes (leader-watch entries: dc-health / gate-peers / gate-leader
milestones), one ``ManagerServer`` registered with ALL three gates,
one 2-core ``WorkerServer``, and client(s) running the multi-gate
sustained-load entry (6 virtual seconds of chained ACTION execution).

Roles and instants are DERIVED, never pinned: which gate wins the
initial election, which gate accepts the submission, and when the job
runs are functions of the seed and of every change that shifts the
deterministic schedule (the per-frame Snowflake id moved them all on
2026-10-06: seed 210's leader went gate-c -> gate-a and its job began
finishing at 11.64, before the old t=12 "mid-execution" faults). Each
scenario reads its roles and instants from its FAULT-FREE TWIN (same
seed, same topology, no faults — ``GateClusterBaseline``): the SIM is
deterministic and a fault changes nothing before its own instant, so
the twin IS the faulted run up to the fault (asserted per scenario by
``_assert_identical_before``), and its completion is the
counterfactual every "unperturbed by the fault" bound compares to.
Faults land at the twin's mid-execution instant (the midpoint of the
worker's live run), so they provably intersect live execution under
any schedule. Each scenario sweeps seeds; scenarios whose premise
needs the leader and the submission gate to be DIFFERENT gates sweep
only seeds whose twin has that layout (asserted, never assumed).

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
* gate_partition — heal-bounded peer isolation of the leader causes
  NO false deaths (the no-false-death property) while the majority
  re-elects and the cut ex-leader steps down at heal; TOTAL
  isolation (peers + manager) elects a majority leader in ~10.5s,
  the islanded ex-leader steps down at heal, the split-window is
  harmless — and the
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

import functools
import itertools

from hyperscale.distributed.env import Env
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.gate_cluster_baseline import (
    GateClusterBaseline,
)
from tests.simulation.harness.sim.multiprocess.gate_fault_client_demo import (
    multi_gate_load_client_entry,
)
from tests.simulation.harness.sim.multiprocess.gate_cluster_demo import (
    multi_gate_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.gate_leader_watch_demo import (
    leader_watch_gate_tier_entry,
)
from tests.simulation.harness.sim.multiprocess.soak_job_demo import (
    soak_gate_dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)
from tests.simulation.oracle import ClusterTraceOracle, JobStatusOracle

import pytest

_GATE_PIDS = ("gate-a", "gate-b", "gate-c")
_GATE_HOSTS = {
    "gate-a": "sim-gate-a",
    "gate-b": "sim-gate-b",
    "gate-c": "sim-gate-c",
}

# The original pinned seed. Under the current schedule its fault-free
# twin elects gate-a and submits through gate-b; it sweeps every scenario
# whose premise holds for any layout, and the scenarios that need the
# leader and the submission gate to differ sweep only distinct-layout
# seeds (each asserted from the twin).
_SEED = 210
# Seeds whose fault-free twins have DISTINCT leader / submission gates,
# across different role assignments (re-probed 2026-10-06 over seeds
# 200-223 after the client's hinted refusals were waited out on the final
# attempt too): 200 leader gate-c / submission gate-b, 217 leader gate-b /
# submission gate-a, 220 leader gate-a / submission gate-b.
# 217 replaces 203 for the premise "leader and submission gate differ"
# (and keeps 203's old leader gate-b / submission gate-a layout): 203's
# twin now collapses both roles onto gate-b.
_DISTINCT_ROLE_SEEDS = (200, 217, 220)
# Any layout (re-probed 2026-10-06): 210 (leader gate-a / submission
# gate-b) and 207 (leader gate-b / submission gate-c) and 220 (leader
# gate-a / submission gate-b) -- the premise holds for every layout, so
# the seeds stay as pinned.
_ANY_LAYOUT_SEEDS = (_SEED, 207, 220)
_LATENCY = 0.01
_WORKFLOW_DURATION = 6.0
_JOB_TIMEOUT = 30.0
_LATE_CLIENT_AT = 300.0
# A fault that perturbed the job path costs at least one failure
# detection/retry cycle -- no less than a SWIM probe interval -- so a
# completion within one interval of the fault-free twin's is unperturbed.
_UNPERTURBED_TOLERANCE_SECONDS = float(Env().SWIM_UDP_POLL_INTERVAL)
# The manager's completion push to a gate (and its failover to the next
# gate) is bounded by its standard TCP send timeout.
_COMPLETION_PUSH_TIMEOUT_SECONDS = Env().MANAGER_TCP_TIMEOUT_STANDARD

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
        latency=_LATENCY, max_virtual_time=ceiling, seed=seed
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


@functools.cache
def _fault_free_twin(
    seed: int,
    ceiling: float,
    wait_timeout: float,
    late_client_at: float = 0.0,
) -> dict:
    """The scenario topology run WITHOUT faults: the faulted run's exact
    timeline up to its first fault (cached — one twin per topology)."""
    return _build_cluster(
        seed, ceiling, wait_timeout=wait_timeout, late_client_at=late_client_at
    ).run()


def _baseline(
    seed: int,
    ceiling: float,
    wait_timeout: float,
    late_client_at: float = 0.0,
) -> GateClusterBaseline:
    """Roles and job instants of the scenario's fault-free twin."""
    return GateClusterBaseline.from_results(
        _fault_free_twin(seed, ceiling, wait_timeout, late_client_at),
        _GATE_PIDS,
    )


def _distinct_role_baseline(
    seed: int,
    ceiling: float,
    wait_timeout: float,
    late_client_at: float = 0.0,
) -> GateClusterBaseline:
    """The twin's baseline for a scenario whose premise needs the leader
    and the submission gate to be different gates — refused loudly when
    the seed's schedule collapses the two roles onto one gate."""
    baseline = _baseline(seed, ceiling, wait_timeout, late_client_at)
    assert baseline.roles_are_distinct, (
        f"seed {seed}: the fault-free twin elects the submission gate "
        f"{baseline.submission_gate} -- the scenario needs distinct roles",
        baseline,
    )
    return baseline


def _rows_before(rows: list, instant: float) -> list:
    """The leading rows of a milestone log stamped strictly before ``instant``."""
    return list(
        itertools.takewhile(
            lambda row: isinstance(row[-1], float) and row[-1] < instant, rows
        )
    )


def _assert_identical_before(
    results: dict,
    seed: int,
    ceiling: float,
    wait_timeout: float,
    first_fault_at: float,
    late_client_at: float = 0.0,
) -> None:
    """The derivation premise, checked: up to its first fault the faulted
    run is its fault-free twin, row for row, in every process (a
    restarted process's pre-fault generation lives under ``.gen1``)."""
    twin = _fault_free_twin(seed, ceiling, wait_timeout, late_client_at)
    for process_id, twin_rows in twin.items():
        faulted_rows = results.get(f"{process_id}.gen1", results.get(process_id))
        if not isinstance(faulted_rows, list) or not isinstance(twin_rows, list):
            continue
        assert _rows_before(faulted_rows, first_fault_at) == _rows_before(
            twin_rows, first_fault_at
        ), (process_id, first_fault_at)


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


_KILL_CEILING = 150.0
_KILL_WAIT_TIMEOUT = 100.0


def _run_kill_follower(seed: int = _SEED) -> dict:
    """Kill a pure-follower gate (neither the twin's leader nor its
    accepting gate) at the twin's mid-execution instant."""
    baseline = _baseline(seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT)
    coordinator = _build_cluster(seed, _KILL_CEILING, wait_timeout=_KILL_WAIT_TIMEOUT)
    coordinator.schedule_kill(baseline.follower_gate, baseline.mid_execution_at)
    return coordinator.run()


@pytest.mark.parametrize("seed", _ANY_LAYOUT_SEEDS)
def test_kill_follower_gate_job_completes_and_leader_holds(seed: int):
    baseline = _baseline(seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT)
    kill_at = baseline.mid_execution_at
    results = _run_kill_follower(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(results, seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT, kill_at)

    # Kill semantics: the victim executes nothing after the kill and
    # produces no result.
    assert baseline.follower_gate not in results, sorted(results)

    client_log = results["client"]
    submit_targets = [
        entry for entry in client_log if entry[0] == "submit-target"
    ]
    assert submit_targets and submit_targets[0][1] == baseline.submission_gate_index, (
        client_log
    )
    finish_time = _assert_clean_completed_client(client_log, "kill-follower")
    # A follower death is INVISIBLE to the job path: completion lands at
    # the fault-free twin's instant (dispatch + 6s execution + push), so
    # completion past the unperturbed bound means the kill perturbed a
    # path it must not touch.
    assert finish_time < baseline.completion_at + _UNPERTURBED_TOLERANCE_SECONDS, client_log

    # The surviving gates hold leadership without churn: the initial
    # leader keeps its flag and nobody else ever claims it.
    survivors = tuple(gate_pid for gate_pid in _GATE_PIDS if gate_pid != baseline.follower_gate)
    flags = _final_leader_flags(results, survivors)
    assert flags == {
        gate_pid: int(gate_pid == baseline.leader_gate) for gate_pid in survivors
    }, flags

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
    for surviving_gate in survivors:
        drop_times = [
            entry[2]
            for entry in results[surviving_gate]
            if entry[0] == "gate-peers" and entry[1] == 1
        ]
        assert drop_times, results[surviving_gate]
        detection_latency = drop_times[0] - kill_at
        assert 5.0 <= detection_latency <= 30.0, (
            surviving_gate,
            detection_latency,
        )


def test_kill_follower_is_replay_deterministic():
    assert _run_kill_follower() == _run_kill_follower()


def _run_kill_leader(seed: int = _DISTINCT_ROLE_SEEDS[0]) -> dict:
    """Kill the twin's initial gate LEADER at its mid-execution instant:
    mid-execution leader loss — the survivors must re-elect exactly one
    leader and the in-flight job (owned by the submission gate, a
    different gate) must complete undisturbed."""
    baseline = _distinct_role_baseline(seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT)
    coordinator = _build_cluster(seed, _KILL_CEILING, wait_timeout=_KILL_WAIT_TIMEOUT)
    coordinator.schedule_kill(baseline.leader_gate, baseline.mid_execution_at)
    return coordinator.run()


@pytest.mark.parametrize("seed", _DISTINCT_ROLE_SEEDS)
def test_kill_leader_gate_reelects_exactly_one_and_job_completes(seed: int):
    baseline = _distinct_role_baseline(seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT)
    kill_at = baseline.mid_execution_at
    results = _run_kill_leader(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(results, seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT, kill_at)
    assert baseline.leader_gate not in results, sorted(results)

    finish_time = _assert_clean_completed_client(results["client"], "kill-leader")
    # Leader death must not perturb an in-flight job owned by another
    # gate: completion at the fault-free twin's instant.
    assert finish_time < baseline.completion_at + _UNPERTURBED_TOLERANCE_SECONDS, results["client"]

    # Eventual convergence: EXACTLY one surviving gate ends as leader.
    survivors = tuple(gate_pid for gate_pid in _GATE_PIDS if gate_pid != baseline.leader_gate)
    flags = _final_leader_flags(results, survivors)
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
    claim_latency = claim_times[0] - kill_at
    assert 0.0 < claim_latency <= 30.0, (winner, claim_latency)


def test_kill_leader_is_replay_deterministic():
    assert _run_kill_leader() == _run_kill_leader()


def _run_kill_submission_gate(seed: int = _SEED) -> dict:
    """Kill the gate that ACCEPTED the job (the twin's submit-target) at
    the twin's mid-execution instant, while the workflow is mid-run: the
    manager's completion push hits a dead origin and must fail over to a
    surviving peer gate, which delivers the client-ready result."""
    baseline = _baseline(seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT)
    coordinator = _build_cluster(seed, _KILL_CEILING, wait_timeout=_KILL_WAIT_TIMEOUT)
    coordinator.schedule_kill(baseline.submission_gate, baseline.mid_execution_at)
    return coordinator.run()


def _assert_one_failover_cycle_completion(
    finish_time: float, baseline: GateClusterBaseline, client_log: list
) -> None:
    """The completion push hits the DEAD origin gate, burns exactly one
    failover timeout, and a surviving peer gate delivers: the twin's
    completion plus the dead-origin push timeout. Bound: after the twin
    (the detour is real) but before a SECOND push timeout could elapse
    (a second cycle means the first surviving peer failed too)."""
    assert (
        baseline.completion_at
        < finish_time
        < baseline.completion_at + 2 * _COMPLETION_PUSH_TIMEOUT_SECONDS
    ), client_log


@pytest.mark.parametrize("seed", _ANY_LAYOUT_SEEDS)
def test_kill_submission_gate_result_arrives_via_surviving_gates(seed: int):
    baseline = _baseline(seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT)
    results = _run_kill_submission_gate(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(
        results, seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT, baseline.mid_execution_at
    )
    assert baseline.submission_gate not in results, sorted(results)

    client_log = results["client"]
    submit_targets = [
        entry for entry in client_log if entry[0] == "submit-target"
    ]
    # The victim IS the accepting gate — that is the whole scenario.
    assert submit_targets and submit_targets[0][1] == baseline.submission_gate_index, (
        client_log
    )
    finish_time = _assert_clean_completed_client(
        client_log, "kill-submission-gate"
    )
    _assert_one_failover_cycle_completion(finish_time, baseline, client_log)

    survivors = tuple(gate_pid for gate_pid in _GATE_PIDS if gate_pid != baseline.submission_gate)
    flags = _final_leader_flags(results, survivors)
    assert sum(flags.values()) == 1, flags


def test_kill_submission_gate_is_replay_deterministic():
    assert _run_kill_submission_gate() == _run_kill_submission_gate()


# =========================================================================
# 2. gate_restart — power loss + amnesiac reboot (Phase 8 gap)
# =========================================================================


_RESTART_DOWN_SECONDS = 20.0


def _run_restart_submission_gate(seed: int = _SEED) -> dict:
    """Power-lose the accepting gate at the twin's mid-execution instant
    for 20 virtual seconds.

    Gates have NO durable tier (Phase 8): generation 1 reboots with
    total amnesia about the job it accepted. The LOUD current behavior
    (probed) is that the manager's push failover re-homes the job to a
    surviving peer gate during the down window, so the client still
    observes a terminal outcome — never silence.
    """
    baseline = _baseline(seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT)
    coordinator = _build_cluster(seed, _KILL_CEILING, wait_timeout=_KILL_WAIT_TIMEOUT)
    coordinator.schedule_restart(
        baseline.submission_gate, baseline.mid_execution_at, down_seconds=_RESTART_DOWN_SECONDS
    )
    return coordinator.run()


@pytest.mark.parametrize("seed", _ANY_LAYOUT_SEEDS)
def test_restart_submission_gate_client_still_observes_terminal(seed: int):
    baseline = _baseline(seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT)
    restart_at = baseline.mid_execution_at
    results = _run_restart_submission_gate(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(results, seed, _KILL_CEILING, _KILL_WAIT_TIMEOUT, restart_at)

    # The pre-restart generation's log is preserved under ``.gen1``
    # (the coordinator keys PRIOR generations as ``{pid}.gen{n}``; the
    # LIVE generation owns the bare pid — probed key shape). The
    # rebooted generation restarts its life from scratch (fresh
    # watcher state — probed: its leader watcher reports flag 0 at
    # exactly the reboot instant).
    prior_generation = results[f"{baseline.submission_gate}.gen1"]
    assert any(
        entry[0] == "gate-started" for entry in prior_generation
    ), prior_generation
    rebooted_log = results[baseline.submission_gate]
    rebooted_times = [
        entry[-1] for entry in rebooted_log if isinstance(entry[-1], float)
    ]
    assert rebooted_times and min(rebooted_times) >= restart_at + _RESTART_DOWN_SECONDS, rebooted_log

    finish_time = _assert_clean_completed_client(
        results["client"], "restart-submission-gate"
    )
    # Probed: identical loud outcome to the KILL of the same gate —
    # completion after the twin's via the peer-gate failover during
    # the down window. The client never notices the difference between
    # a dead and an amnesiac-rebooting origin gate; what it must never
    # see is silence.
    _assert_one_failover_cycle_completion(finish_time, baseline, results["client"])

    # No split-brain across the reboot: at most one leader among ALL
    # three gates at the end, and exactly one somewhere.
    flags = _final_leader_flags(results, _GATE_PIDS)
    assert sum(flags.values()) == 1, flags


def test_restart_submission_gate_is_replay_deterministic():
    assert _run_restart_submission_gate() == _run_restart_submission_gate()


# Midpoint of the probed window between the dispatch landing on the
# worker (8.25) and the job's completion send (~9.2; the client sees it
# at 9.26), seed 211: gen-1 dies after recording the acceptance (8.08)
# and before the first completion send. Re-probed 2026-10-04: the gate
# now accepts at 8.08, as soon as the client submits, once a lone
# manager leads the moment its own vote is the majority (Raft section
# 5.2) -- 10.05 while it waited out a full pre-vote and vote wait.
_DURABLE_RESTART_AT = 8.7
_DURABLE_DOWN_SECONDS = 8.0
_DURABLE_GEN2_BOOT = _DURABLE_RESTART_AT + _DURABLE_DOWN_SECONDS
_DURABLE_WORKFLOW_SECONDS = 20.0
_DURABLE_CEILING = 120.0
# The solo-gate topology has no leader/submission/follower roles, so it
# keeps the seed its timeline (below) was probed under.
_DURABLE_SEED = 211
# The solo gate the client is pinned to (it is the whole gate tier).
_DURABLE_GATE = "gate-a"


def _run_durable_gate_restart() -> dict:
    """Power-cycle a SOLO gate mid-flight with the Phase 8 durable tier
    armed (wal_data_dir on the gate), the client pinned to it.

    SOLO is the load-bearing topology choice: with peer gates present,
    gate-to-gate replication + the manager's completion-notice
    failover ("forwarded" discharge) already mask a gate reboot — the
    probe showed a peer resolving the job mid-downtime. A solo gate
    has no peer to hide behind: pre-Phase-8 its reboot orphaned the
    job FOREVER (the manager's completion hit "unknown job" on the
    amnesiac gen-2 until the notice aged out, and the client
    stranded); with the ledger, gen-2 replays the WAL, re-learns the
    job, resumes AD-34 with the remaining budget, and ACCEPTS the owed
    completion the manager's obligation machinery is still resending —
    the two halves of the durable story meeting.

    Probed timeline (seed 211, 2026-10-04): gate started 2.04, submit_at
    8.0 accepted 8.08 (durable acceptance recorded), dispatch on the
    worker 8.25, restart at 8.7 (down 8 — covers the job's ~9.2
    completion instant, so the first completion send dies with gen-1),
    gen-2 boots 16.7 and recovers the job from the WAL, the obligation
    resend delivers the final result to gen-2, and the client observes
    ``completed`` at 19.22."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_DURABLE_CEILING, seed=_DURABLE_SEED
    )
    datacenter_managers = {"dc-1": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"dc-1": [("sim-mgr", 9001)]}

    coordinator.add_process(
        _DURABLE_GATE,
        leader_watch_gate_tier_entry,
        _GATE_HOSTS[_DURABLE_GATE],
        9000,
        9001,
        datacenter_managers,
        datacenter_manager_udp,
        [],
        [],
        f"/sim/{_GATE_HOSTS[_DURABLE_GATE]}-9000/gate-ledger",
    )
    coordinator.add_process(
        "manager",
        multi_gate_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "dc-1",
        [(_GATE_HOSTS[_DURABLE_GATE], 9000)],
        [(_GATE_HOSTS[_DURABLE_GATE], 9001)],
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
    coordinator.add_process(
        "client",
        soak_gate_dispatch_client_entry,
        "sim-cli",
        9500,
        (_GATE_HOSTS[_DURABLE_GATE], 9000),
        _DURABLE_WORKFLOW_SECONDS,
        60.0,
        90.0,
        8.0,
        ["dc-1"],
    )
    coordinator.schedule_restart(
        _DURABLE_GATE, _DURABLE_RESTART_AT, down_seconds=_DURABLE_DOWN_SECONDS
    )
    return coordinator.run()


def test_restarted_gate_resumes_its_own_jobs_from_durable_state():
    """FIXED-GAP PIN (Phase 8 gate durable tier): a restarted SOLO gate
    recovers its accepted jobs from its own JobLedger — the same WAL +
    checkpoint + archive composite the manager runs — and re-serves
    them itself, with no peer to hide behind.

    Measured chain (seed 211): submit 8.08 accepted on gen-1, which
    durably records the acceptance at 8.891; power loss at 9.0 kills
    gen-1 mid-flight (covering the job's natural completion instant,
    so the manager's first completion send dies); gen-2 boots 19.04
    and WAL replay re-learns the job (1 recovered) plus the client's
    callback contact; the manager's completion-notice obligation
    redelivers, gen-2 APPLIES it (handler ``ok`` at 24.931) and pushes
    the terminal to the client, observed ``completed`` at 24.961; the
    obligation's later resend gets ``already_completed`` and
    discharges.

    Pre-Phase-8 the same schedule stranded the job forever: the
    amnesiac gen-2 answered "unknown job", the manager kept resending
    until the 1800s age ceiling, and the client waited out its full
    budget."""
    results = _run_durable_gate_restart()
    client_log = results["client"]

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert len(submitted) == 1, client_log
    assert submitted[0][1] < _DURABLE_RESTART_AT, client_log

    gen1_log = results[f"{_DURABLE_GATE}.gen1"]
    assert any(entry[0] == "gate-started" for entry in gen1_log), gen1_log
    gen2_log = results[_DURABLE_GATE]
    gen2_starts = [entry for entry in gen2_log if entry[0] == "gate-started"]
    assert len(gen2_starts) == 1, gen2_log
    assert gen2_starts[0][1] >= _DURABLE_GEN2_BOOT, gen2_log

    # The workflow ran ONCE and was undisturbed by the gate's power
    # cycle (a gate is a coordinator, not an executor).
    worker_log = results["worker"]
    active_rises = [
        entry
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] > 0
    ]
    assert len(active_rises) == 1, worker_log

    # THE claim: the client observes the loud terminal from the
    # RECOVERED generation — after gen-2's boot, well inside the job's
    # 60s AD-34 budget (a timeout here would mean recovery failed and
    # the tracker resolved it instead).
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", (
        "the recovered gate must resolve the job it re-learned from "
        f"its ledger: {client_log}"
    )
    assert _DURABLE_GEN2_BOOT < finished[0][2] < submitted[0][1] + 60.0, client_log
    # 19.22 (2026-10-04; gen-2 boots at 16.7): 19.941 with the restart
    # at 10.6, before a lone manager led the moment its own vote was the
    # majority; 24.961 before that, when the completion push sent wire
    # action "global_job_result", which NO endpoint implements, so every
    # push burned its full 5s send timeout before the chain could
    # continue.
    assert abs(finished[0][2] - 19.22) <= 2.0, client_log

    assert not JobStatusOracle().check_client_log(client_log), client_log
    _assert_no_unswapped_seams(results)


def test_durable_gate_restart_is_replay_deterministic():
    assert _run_durable_gate_restart() == _run_durable_gate_restart()


# =========================================================================
# 3. gate_partition — total leader isolation must not split-brain
# =========================================================================


_PEER_CUT_SECONDS = 35.0
_PEER_CUT_CEILING = 180.0
_PEER_CUT_WAIT_TIMEOUT = 120.0


def _run_leader_peer_isolation(seed: int = _DISTINCT_ROLE_SEEDS[0]) -> dict:
    """Cut BOTH of the twin leader's peer links for 35 virtual seconds
    from the twin's mid-execution instant, leaving the leader's MANAGER
    link intact.

    Probed invariants (gate tier): a 35s peer cut is well inside the
    witness-less death bound (~[25,85]s of SUSTAINED silence measured
    from suspicion; indirect probes through the manager keep reaching
    gate-c), so membership rides it out with NO false deaths. Leader
    heartbeats travel only over the peer links, so the peers' leases
    on gate-c lapse and the two of them -- a majority of three --
    elect gate-a in their first round (probed: t=14.5); the cut leader
    keeps its flag until the heal shows it the higher term and steps
    down (probed: t=46.5). The job (owned by gate-a, unpartitioned)
    completes at the baseline instant.

    The cut gate's suspicions of gate-a reach it many times over, re-
    gossiped through the manager. Each copy below gate-a's refuted
    incarnation is superseded and costs no local health: before that
    rule each copy raised gate-a's LHM (probed: 0 -> 6 by t=22.4),
    driving the elected leader past its eligibility bound into a step-
    down and a second election mid-cut.

    Pinned before 2026-10-03 as "no election": each gate also recorded
    ITSELF in its peer roles from gossip about it, so a three-gate tier
    counted four and needed all three gates to elect."""
    baseline = _distinct_role_baseline(seed, _PEER_CUT_CEILING, _PEER_CUT_WAIT_TIMEOUT)
    cut_at = baseline.mid_execution_at
    coordinator = _build_cluster(seed, _PEER_CUT_CEILING, wait_timeout=_PEER_CUT_WAIT_TIMEOUT)
    for peer_gate in (baseline.follower_gate, baseline.submission_gate):
        coordinator.schedule_partition(
            baseline.leader_gate,
            peer_gate,
            cut_at,
            heal_time=cut_at + _PEER_CUT_SECONDS,
        )
    return coordinator.run()


@pytest.mark.parametrize("seed", _DISTINCT_ROLE_SEEDS)
def test_leader_peer_isolation_reelects_without_false_deaths(seed: int):
    baseline = _distinct_role_baseline(seed, _PEER_CUT_CEILING, _PEER_CUT_WAIT_TIMEOUT)
    cut_at = baseline.mid_execution_at
    heals_at = cut_at + _PEER_CUT_SECONDS
    results = _run_leader_peer_isolation(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(results, seed, _PEER_CUT_CEILING, _PEER_CUT_WAIT_TIMEOUT, cut_at)

    finish_time = _assert_clean_completed_client(
        results["client"], "leader-peer-isolation"
    )
    assert finish_time < baseline.completion_at + _UNPERTURBED_TOLERANCE_SECONDS, results["client"]

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

    # The majority elects exactly one leader while the cut holds, and
    # never before a peer's lease on the cut leader could have lapsed:
    # the last heartbeat to reach them left at most one heartbeat
    # interval before the cut.
    leader_env = Env()
    earliest_lease_lapse = (
        cut_at
        + leader_env.LEADER_LEASE_DURATION
        - leader_env.LEADER_HEARTBEAT_INTERVAL
    )
    majority_claims = [
        (gate_pid, entry[2])
        for gate_pid in (baseline.follower_gate, baseline.submission_gate)
        for entry in results[gate_pid]
        if entry[0] == "gate-leader" and entry[1] == 1
    ]
    assert len(majority_claims) == 1, majority_claims
    ((elected_gate, elected_at),) = majority_claims
    assert earliest_lease_lapse < elected_at < heals_at, (
        majority_claims
    )

    # The cut leader steps down once the heal shows it the higher term
    # (within one lease of the heal) and never claims again; the
    # majority's leader keeps leadership through the heal.
    cut_leader_moves = [
        entry
        for entry in results[baseline.leader_gate]
        if entry[0] == "gate-leader" and entry[2] > cut_at
    ]
    assert len(cut_leader_moves) == 1 and cut_leader_moves[0][1] == 0, (
        cut_leader_moves
    )
    assert (
        cut_leader_moves[0][2]
        <= heals_at + leader_env.LEADER_LEASE_DURATION
    ), cut_leader_moves

    flags = _final_leader_flags(results, _GATE_PIDS)
    assert flags == {
        gate_pid: int(gate_pid == elected_gate) for gate_pid in _GATE_PIDS
    }, flags


def test_leader_peer_isolation_is_replay_deterministic():
    assert _run_leader_peer_isolation() == _run_leader_peer_isolation()


_ISLAND_SECONDS = 90.0
_ISLAND_CEILING = 210.0
_ISLAND_WAIT_TIMEOUT = 120.0


def _island_window(seed: int) -> tuple[GateClusterBaseline, float, float]:
    """The twin's baseline and the island window: from its mid-execution
    instant, for 90 virtual seconds."""
    baseline = _distinct_role_baseline(seed, _ISLAND_CEILING, _ISLAND_WAIT_TIMEOUT)
    return baseline, baseline.mid_execution_at, baseline.mid_execution_at + _ISLAND_SECONDS


def _run_leader_total_isolation(seed: int = _DISTINCT_ROLE_SEEDS[0]) -> dict:
    """ISLAND the leader completely: cut the twin's leader from BOTH
    peers AND the manager for 90s from the twin's mid-execution instant
    (the original pin: gate-c over [10, 100)) — a window that exceeds
    every detection bound, so this time the tier MUST act.

    Probed timeline: the majority (a, b) marks the unreachable leader
    dead and gate-b claims leadership at t=20.5 (leader-unreachability
    is evidence-accelerated — 10.5s, NOT the ~70s witness-less reap);
    the islanded gate-c steps down once its quorum lease lapses — one
    lease after the last beat a peer acknowledged, before the majority
    can elect — so the two never lead at one instant (AD-5 addendum).
    The job (owned by gate-b) completes at the baseline instant.

    Post-heal, the peer-readmission watch re-admits every falsely
    evicted membership (probed: all three gates back to active-peer
    count 2 at 107.5-108.0 — heal + one 10s dead-peer check tick +
    TCP liveness verification + recovery jitter): each gate
    TCP-ping-verifies the configured peers it holds DEAD and, on proof
    of life, drives the rejoin composite (death-record reset at the
    rejoin incarnation + probe re-enrolment + peer recovery + a fresh
    JOIN toward the peer so one-sided evictions heal symmetrically).
    """
    baseline, cut_at, heal_at = _island_window(seed)
    coordinator = _build_cluster(seed, _ISLAND_CEILING, wait_timeout=_ISLAND_WAIT_TIMEOUT)
    for cut_process in (baseline.follower_gate, baseline.submission_gate, "manager"):
        coordinator.schedule_partition(
            baseline.leader_gate, cut_process, cut_at, heal_time=heal_at
        )
    return coordinator.run()


@pytest.mark.parametrize("seed", _DISTINCT_ROLE_SEEDS)
def test_leader_total_isolation_majority_elects_and_islander_steps_down(seed: int):
    baseline, cut_at, heal_at = _island_window(seed)
    results = _run_leader_total_isolation(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(results, seed, _ISLAND_CEILING, _ISLAND_WAIT_TIMEOUT, cut_at)

    finish_time = _assert_clean_completed_client(
        results["client"], "leader-total-isolation"
    )
    assert finish_time < baseline.completion_at + _UNPERTURBED_TOLERANCE_SECONDS, results["client"]

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
        for gate_pid in (baseline.follower_gate, baseline.submission_gate)
        for entry in results[gate_pid]
        if entry[0] == "gate-leader" and entry[1] == 1 and entry[2] < heal_at
    ]
    assert majority_claims_during_isolation, results
    assert cut_at < min(majority_claims_during_isolation) <= cut_at + 30.0, (
        majority_claims_during_isolation
    )

    majority_flags = _final_leader_flags(
        results, (baseline.follower_gate, baseline.submission_gate)
    )
    assert sum(majority_flags.values()) == 1, majority_flags

    # Post-heal stability: whatever reconciliation the reforming tier
    # ran, leadership must be QUIET once re-admission settles (probed:
    # last transition 3.0s after heal; re-admission completes 7.5-8.0s
    # after it) — nothing more than 10s past the heal.
    for gate_pid in _GATE_PIDS:
        late_leader_moves = [
            entry
            for entry in results[gate_pid]
            if entry[0] == "gate-leader" and entry[2] > heal_at + 10.0
        ]
        assert not late_leader_moves, (gate_pid, late_leader_moves)

    # The islanded ex-leader gives the flag up when its quorum lease
    # lapses, before the majority elects: leadership is exclusive at
    # every instant, and it holds no claim past heal.
    oracle = ClusterTraceOracle(gate_process_ids=_GATE_PIDS)
    assert oracle.check_gate_leader_exclusivity(results) == [], results
    islander_log = results[baseline.leader_gate]
    islander_flags = [
        entry for entry in islander_log if entry[0] == "gate-leader"
    ]
    assert islander_flags[-1][1] == 0, islander_flags
    step_down_times = [
        entry[2] for entry in islander_flags if entry[1] == 0 and entry[2] > cut_at
    ]
    assert step_down_times and step_down_times[0] <= heal_at + 5.0, islander_flags

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


_FREEZE_SECONDS = 30.0
_FREEZE_CEILING = 150.0
_FREEZE_WAIT_TIMEOUT = 100.0
# The gate-leader watcher samples the flag every half second
# (gate_leader_watch_demo.watch_gate_leadership), so a transition is
# recorded at most one sample after it happens.
_LEADER_WATCH_SAMPLE_SECONDS = 0.5


def _freeze_window(seed: int) -> tuple[GateClusterBaseline, float, float]:
    """The twin's baseline and a SIGSTOP window on its leader: from its
    mid-execution instant, longer than a lease plus a re-election."""
    baseline = _distinct_role_baseline(seed, _FREEZE_CEILING, _FREEZE_WAIT_TIMEOUT)
    return baseline, baseline.mid_execution_at, baseline.mid_execution_at + _FREEZE_SECONDS


def _run_leader_freeze(seed: int = _DISTINCT_ROLE_SEEDS[0]) -> dict:
    """FREEZE the twin's leader gate (SIGSTOP for 30s): the two followers
    stop hearing it, their leases lapse and they elect one of themselves,
    while the frozen leader's own clock still runs its lead ticks (the
    coordinator's thaw sweep, ``schedule_pause``)."""
    baseline, freeze_at, thaw_at = _freeze_window(seed)
    coordinator = _build_cluster(seed, _FREEZE_CEILING, wait_timeout=_FREEZE_WAIT_TIMEOUT)
    coordinator.schedule_pause(baseline.leader_gate, freeze_at, thaw_at)
    return coordinator.run()


@pytest.mark.parametrize("seed", _DISTINCT_ROLE_SEEDS)
def test_frozen_leader_gate_steps_down_before_a_successor_is_elected(seed: int):
    """A frozen leader holds the flag only while its quorum lease lasts
    (Raft thesis 6.2 CheckQuorum, 6.4.1 leases): no beat it sends during
    the freeze is acknowledged, so it steps down one lease after its last
    acknowledged beat -- before the followers, whose leases ran from the
    same beats, can elect a successor. Leadership is exclusive at every
    instant (chaos seed 5 caught two gates holding the flag at once)."""
    baseline, freeze_at, thaw_at = _freeze_window(seed)
    results = _run_leader_freeze(seed)
    _assert_no_unswapped_seams(results)

    oracle = ClusterTraceOracle(gate_process_ids=_GATE_PIDS)
    assert oracle.check_gate_leader_exclusivity(results) == [], results

    frozen_leader_flags = [
        entry for entry in results[baseline.leader_gate] if entry[0] == "gate-leader"
    ]
    step_downs_after_freeze = [
        entry[2] for entry in frozen_leader_flags if entry[1] == 0 and entry[2] > freeze_at
    ]
    lease_ends_by = freeze_at + Env().LEADER_LEASE_DURATION + _LEADER_WATCH_SAMPLE_SECONDS
    assert step_downs_after_freeze and step_downs_after_freeze[0] <= lease_ends_by, (
        frozen_leader_flags
    )

    successor_claims = [
        entry[2]
        for gate_pid in (baseline.follower_gate, baseline.submission_gate)
        for entry in results[gate_pid]
        if entry[0] == "gate-leader" and entry[1] == 1 and freeze_at < entry[2] < thaw_at
    ]
    # One successor, holding steady for the rest of the freeze: its own
    # quorum lease is renewed by the other follower's acknowledgements.
    assert len(successor_claims) == 1, results
    # Same watcher sample at the earliest: the flag dropped first.
    assert min(successor_claims) >= step_downs_after_freeze[0], (
        successor_claims,
        frozen_leader_flags,
    )


def test_leader_freeze_is_replay_deterministic():
    assert _run_leader_freeze() == _run_leader_freeze()


_DATACENTER_CUT_HEALS_AT = 60.0
_DATACENTER_CUT_CEILING = 120.0
_DATACENTER_CUT_WAIT_TIMEOUT = 90.0
# The original seed, and two seeds whose cut gate wins the race to claim
# BEFORE any peer has a datacenter (it then has to relinquish).
_DATACENTER_CUT_SEEDS = (_SEED, 204, 213)
# The leader-watch entries sample milestones every half virtual second.
_WATCHER_SAMPLE_SECONDS = 0.5
# How long a peer's readiness takes to reach a gate and act there: its
# gate heartbeat rides the SWIM probe round (one probe interval per
# member -- the two peer gates and the manager), the gate acts on its
# next lead tick (one leader heartbeat interval), and both instants are
# seen through the watcher's sampling.
_READINESS_REACTION_SECONDS = (
    len(_GATE_PIDS) * float(Env().SWIM_UDP_POLL_INTERVAL)
    + Env().LEADER_HEARTBEAT_INTERVAL
    + _WATCHER_SAMPLE_SECONDS
)


def _first_fault_free_claimer(seed: int) -> str:
    """The gate that wins the fault-free twin's race to claim leadership."""
    twin = _fault_free_twin(seed, _DATACENTER_CUT_CEILING, _DATACENTER_CUT_WAIT_TIMEOUT)
    return min(
        (row[2], gate_pid)
        for gate_pid in _GATE_PIDS
        for row in twin[gate_pid]
        if row[0] == "gate-leader" and row[1] == 1
    )[1]


def _run_leader_without_datacenters(seed: int = _SEED) -> dict:
    """Cut the gate that wins the fault-free twin's election race from
    the manager from boot until t=60, so it reaches no datacenter.

    AD-19: a gate that cannot do a leader's work leaves leadership to a
    live peer whose heartbeat says it can. Probed (original schedule):
    gate-c never claims (its datacenter stays INITIALIZING -- no manager
    heartbeat ever arrives -- until the heal); gate-b claims at 3.0, one
    round after the baseline's 2.0; the healed gate-c rejoins as a
    follower and leadership stays put. Before the rule, gate-c won at 2.0
    and led the tier with no datacenter to dispatch to.

    When the cut gate's claim races AHEAD of every peer's readiness
    (probed 2026-10-06, seed 204: claim at 1.0, before any gate has a
    datacenter -- no peer can lead, so it stands, by design), it must
    RELINQUISH once a ready peer exists: it used to keep leadership with
    no datacenter until the heal (refusal was checked only at candidacy);
    a leader now steps down on its next lead tick when its role refuses
    leadership (relinquished at 5.0; gate-a claims at 6.0)."""
    coordinator = _build_cluster(
        seed, _DATACENTER_CUT_CEILING, wait_timeout=_DATACENTER_CUT_WAIT_TIMEOUT
    )
    coordinator.schedule_partition(
        _first_fault_free_claimer(seed),
        "manager",
        0.0,
        heal_time=_DATACENTER_CUT_HEALS_AT,
    )
    return coordinator.run()


def _first_peer_datacenter_seen_at(results: dict, cut_gate: str) -> float:
    """The first instant a peer of ``cut_gate`` reported a datacenter
    past INITIALIZING (it heard a manager — it can do a leader's work)."""
    return min(
        row[3]
        for gate_pid in _GATE_PIDS
        if gate_pid != cut_gate
        for row in results[gate_pid]
        if row[0] == "dc-health" and row[2] != "initializing"
    )


@pytest.mark.parametrize("seed", _DATACENTER_CUT_SEEDS)
def test_a_gate_without_datacenters_leaves_leadership_to_a_ready_peer(seed: int):
    cut_gate = _first_fault_free_claimer(seed)
    results = _run_leader_without_datacenters(seed)
    _assert_no_unswapped_seams(results)
    _assert_clean_completed_client(results["client"], "leader-without-datacenters")

    # The cut gate never leads once a ready peer could: any claim it makes
    # races AHEAD of the peers' readiness reaching it (no gate could lead
    # then, so it stands as usual) and is relinquished within one
    # readiness reaction of a peer becoming ready -- strictly before any
    # ready gate claims. Under a schedule where readiness precedes the
    # election, it never claims at all (the original pin).
    peer_ready_at = _first_peer_datacenter_seen_at(results, cut_gate)
    cut_gate_moves = [
        (entry[1], entry[2])
        for entry in results[cut_gate]
        if entry[0] == "gate-leader" and entry[2] > 0.0
    ]
    assert all(
        claimed_at < peer_ready_at + _READINESS_REACTION_SECONDS
        for flag, claimed_at in cut_gate_moves
        if flag == 1
    ), (peer_ready_at, cut_gate_moves)
    assert [flag for flag, _moved_at in cut_gate_moves] in ([], [1, 0]), cut_gate_moves
    cut_gate_tenure_ends = [moved_at for flag, moved_at in cut_gate_moves if flag == 0]
    assert all(
        ended_at <= peer_ready_at + _READINESS_REACTION_SECONDS
        for ended_at in cut_gate_tenure_ends
    ), (peer_ready_at, cut_gate_moves)

    ready_gate_claims = [
        (gate_pid, entry[2])
        for gate_pid in _GATE_PIDS
        if gate_pid != cut_gate
        for entry in results[gate_pid]
        if entry[0] == "gate-leader" and entry[1] == 1
    ]
    assert len(ready_gate_claims) == 1, ready_gate_claims
    assert ready_gate_claims[0][1] < _DATACENTER_CUT_HEALS_AT, ready_gate_claims
    assert all(
        ended_at < ready_gate_claims[0][1] for ended_at in cut_gate_tenure_ends
    ), (cut_gate_moves, ready_gate_claims)

    flags = _final_leader_flags(results, _GATE_PIDS)
    assert flags == {
        gate_pid: int(gate_pid == ready_gate_claims[0][0]) for gate_pid in _GATE_PIDS
    }, flags


def test_leader_without_datacenters_is_replay_deterministic():
    assert _run_leader_without_datacenters() == _run_leader_without_datacenters()


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
    baseline, cut_at, heal_time = _island_window(_DISTINCT_ROLE_SEEDS[0])
    results = _run_leader_total_isolation()
    _assert_no_unswapped_seams(results)
    _assert_identical_before(
        results, _DISTINCT_ROLE_SEEDS[0], _ISLAND_CEILING, _ISLAND_WAIT_TIMEOUT, cut_at
    )

    readmission_deadline = heal_time + 15.0
    for gate_pid in (baseline.follower_gate, baseline.submission_gate):
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
        for entry in results[baseline.leader_gate]
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


# One blackout submit cycle: every attempt (the first and
# CLIENT_SUBMISSION_MAX_RETRIES retries) runs out its silent
# CLIENT_SUBMISSION_TIMEOUT, and each retry first backs off an un-hinted,
# equal-jittered base * 2**retry * [0.5, 1.5) (base: one
# OVERLOAD_SAMPLE_INTERVAL_SECONDS) -- so the back-offs sum to between
# half and one and a half times base * (2**retries - 1).
_BLACKOUT_ENV = Env()
_BLACKOUT_ATTEMPTS_SECONDS = (
    _BLACKOUT_ENV.CLIENT_SUBMISSION_MAX_RETRIES + 1
) * _BLACKOUT_ENV.CLIENT_SUBMISSION_TIMEOUT
_BLACKOUT_BACKOFF_SUM_SECONDS = _BLACKOUT_ENV.OVERLOAD_SAMPLE_INTERVAL_SECONDS * (
    2**_BLACKOUT_ENV.CLIENT_SUBMISSION_MAX_RETRIES - 1
)
_BLACKOUT_CYCLE_MIN_SECONDS = _BLACKOUT_ATTEMPTS_SECONDS + 0.5 * _BLACKOUT_BACKOFF_SUM_SECONDS
_BLACKOUT_CYCLE_MAX_SECONDS = _BLACKOUT_ATTEMPTS_SECONDS + 1.5 * _BLACKOUT_BACKOFF_SUM_SECONDS
# The multi-gate client entry pauses this long between submit cycles.
_BLACKOUT_CYCLE_PAUSE_SECONDS = 1.0
# The cut outlasts both cycles at their slowest, and the run outlasts the
# cut by a few link latencies' worth of settling (as before: 10s).
_BLACKOUT_HEAL_AT = 2 * _BLACKOUT_CYCLE_MAX_SECONDS + _BLACKOUT_CYCLE_PAUSE_SECONDS
_BLACKOUT_CEILING = _BLACKOUT_HEAL_AT + 10.0


def _run_submission_blackout() -> dict:
    """Cut the client from ALL THREE gates for the entire retry budget
    (a blackout outlasting both cycles of a 2-attempt client): submission
    must be abandoned LOUDLY — rejection milestones then an explicit
    submit-abandoned — never a silent hang.

    Each submit_job cycle exhausts its 6 target attempts against silent
    cuts, backing off between them, and raises; the bounded client logs
    submit-abandoned at its second raise."""
    coordinator = _build_cluster(
        _SEED, _BLACKOUT_CEILING, wait_timeout=60.0, max_submit_attempts=2
    )
    for gate_pid in _GATE_PIDS:
        coordinator.schedule_partition(
            "client", gate_pid, 0.0, heal_time=_BLACKOUT_HEAL_AT
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
    # plus its back-offs; the loud give-up follows the second at once.
    first_raise_at = rejections[0][-1]
    assert _BLACKOUT_CYCLE_MIN_SECONDS <= first_raise_at <= _BLACKOUT_CYCLE_MAX_SECONDS, client_log
    second_cycle_start = first_raise_at + _BLACKOUT_CYCLE_PAUSE_SECONDS
    assert (
        second_cycle_start + _BLACKOUT_CYCLE_MIN_SECONDS
        <= abandoned[0][1]
        <= second_cycle_start + _BLACKOUT_CYCLE_MAX_SECONDS
    ), client_log
    assert abandoned[0][1] == rejections[1][-1], client_log

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


# The cut opens between the client-observed acceptance and the gate's
# dispatch to the manager, so the whole dispatch window lands inside it.
# With dc-1 healthy at submission the dispatch follows the client's
# observation of the acceptance by under one delivery latency (probed
# 2026-10-04: a cut 2ms after the observed acceptance catches it, one
# 13.4ms after does not) -- so the cut opens half a latency after the
# twin's observed acceptance, and the 'running only after the heal'
# assertion below proves it caught the dispatch.
_DISPATCH_WINDOW_CUT_AFTER_ACCEPTANCE_SECONDS = _LATENCY / 2.0
_DISPATCH_WINDOW_CUT_SECONDS = 20.0
_DISPATCH_WINDOW_CEILING = 150.0
_DISPATCH_WINDOW_WAIT_TIMEOUT = 100.0
# The gate's dispatch retry (dispatch_coordinator._try_dispatch_to_manager):
# an attempt sent into the cut ends at its send timeout, and the next waits
# a full-jitter backoff of at most one leader heartbeat -- for as long as
# the datacenter's leader failover lasts.
_DISPATCH_RETRY_BACKOFF_CAP_SECONDS = Env().LEADER_HEARTBEAT_INTERVAL


def _dispatch_window(seed: int) -> tuple[GateClusterBaseline, float, float]:
    """The twin's baseline and the dispatch-window cut: opening between
    the twin's observed acceptance and its dispatch, for 20 seconds."""
    baseline = _distinct_role_baseline(
        seed, _DISPATCH_WINDOW_CEILING, _DISPATCH_WINDOW_WAIT_TIMEOUT
    )
    cut_at = baseline.submitted_at + _DISPATCH_WINDOW_CUT_AFTER_ACCEPTANCE_SECONDS
    return baseline, cut_at, cut_at + _DISPATCH_WINDOW_CUT_SECONDS


def _run_dispatch_window_manager_partition(seed: int = _DISTINCT_ROLE_SEEDS[0]) -> dict:
    """Cut the ACCEPTING gate from the manager from between the twin's
    acceptance and its dispatch, for 20 virtual seconds — so the ENTIRE
    dispatch window lands inside the cut.

    Probed invariant: an accepted job's dispatch is not a one-shot —
    the gate retries against the cut for its whole span and lands the
    dispatch on its first retry after heal ('running' at t=30.28 --
    the attempt in flight at the heal ends at its send timeout, the next
    waits its jittered backoff; completion at t=36.30 = post-heal
    dispatch + the full 6s duration + push). No failure, no silence, no
    leadership disturbance. (Contrast, documented in the report: a
    SECOND job's dispatch dies in ~5.4s — the retry robustness exists
    only on this first-job path today.)"""
    baseline, cut_at, heal_at = _dispatch_window(seed)
    coordinator = _build_cluster(
        seed, _DISPATCH_WINDOW_CEILING, wait_timeout=_DISPATCH_WINDOW_WAIT_TIMEOUT
    )
    coordinator.schedule_partition(
        baseline.submission_gate,
        "manager",
        cut_at,
        heal_time=heal_at,
    )
    return coordinator.run()


@pytest.mark.parametrize("seed", _DISTINCT_ROLE_SEEDS)
def test_dispatch_window_manager_partition_retries_across_the_cut(seed: int):
    baseline, cut_at, heal_at = _dispatch_window(seed)
    results = _run_dispatch_window_manager_partition(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(
        results, seed, _DISPATCH_WINDOW_CEILING, _DISPATCH_WINDOW_WAIT_TIMEOUT, cut_at
    )

    client_log = results["client"]
    finish_time = _assert_clean_completed_client(
        client_log, "dispatch-window-partition"
    )

    # Accepted BEFORE the cut, on the gate the cut targets.
    submit_targets = [
        entry for entry in client_log if entry[0] == "submit-target"
    ]
    assert submit_targets and submit_targets[0][1] == baseline.submission_gate_index, (
        client_log
    )
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted and submitted[0][1] < cut_at, client_log

    # Execution began only AFTER the heal (probed 'running' at 30.28),
    # on the first retry after it: the attempt in flight at the heal ends
    # at its send timeout, and the next waits at most one capped backoff.
    # The dispatch rode the entire 20s cut on retries instead of failing
    # the accepted job.
    first_retry_after_heal = (
        heal_at + Env().GATE_TCP_TIMEOUT_STANDARD + _DISPATCH_RETRY_BACKOFF_CAP_SECONDS
    )
    running_seen = [
        entry
        for entry in client_log
        if entry[0] == "status-seen" and entry[1] == "running"
    ]
    assert running_seen and heal_at < running_seen[0][2] <= first_retry_after_heal, client_log

    # Completion = post-heal dispatch + the full duration + push
    # (probed 36.30, 6.02 after 'running'); a later one would mean extra
    # dispatch cycles.
    assert abs((finish_time - running_seen[0][2]) - 6.02) <= 1.0, client_log

    # The membership plane never flinched: single stable leader.
    flags = _final_leader_flags(results, _GATE_PIDS)
    assert flags == {
        gate_pid: int(gate_pid == baseline.leader_gate) for gate_pid in _GATE_PIDS
    }, flags


def test_dispatch_window_manager_partition_is_replay_deterministic():
    assert (
        _run_dispatch_window_manager_partition()
        == _run_dispatch_window_manager_partition()
    )


# =========================================================================
# 4. client connectivity — partitions + delay on the client links
# =========================================================================


# Probed with the first-target cut and the client-link delay schedule in
# place (no delivery cut): gate-b refuses the first attempt, the attempt
# against the cut gate-a starts inside the cut and costs one 10s timeout,
# and after its un-hinted back-off gate-c (index 2) accepts at 13.238;
# 'running' at 13.738. Re-probed 2026-10-06 once a bare timeout backs off
# like any transient failure (was: gate-c at 11.336 when a timeout retried
# at once).
_CLIENT_LINK_FIRST_CUT_HEAL_AT = 10.0
_CLIENT_LINK_ACCEPTING_GATE = "gate-c"
_CLIENT_LINK_ACCEPTED_AT = 13.238149
_CLIENT_LINK_RUNNING_AT = 13.738149
_CLIENT_LINK_DELIVERY_CUT_AT = (_CLIENT_LINK_ACCEPTED_AT + _CLIENT_LINK_RUNNING_AT) / 2
_CLIENT_LINK_DELIVERY_HEAL_AT = 30.0


def _run_client_link_faults() -> dict:
    """Client-tier connectivity faults, all three classes at once:

    * client <-> gate-a cut over [0, 10): a submission target
      is unreachable, so acceptance must converge through the retry
      cycle onto a reachable gate (gate-c);
    * client <-> gate-c cut from between acceptance and dispatch until
      t=30: the whole delivery window of the accepting gate — status
      pushes are lost mid-flight and a peer gate must deliver the
      terminal outcome before the heal;
    * 50ms (+20ms seeded jitter) delay on every client link over
      [5, 45): late pushes race polls; the client's order guard (the
      oracle checks the observed history) must hold.
    """
    coordinator = _build_cluster(_SEED, 180.0, wait_timeout=120.0)
    coordinator.schedule_partition(
        "client", "gate-a", 0.0, heal_time=_CLIENT_LINK_FIRST_CUT_HEAL_AT
    )
    coordinator.schedule_partition(
        "client",
        _CLIENT_LINK_ACCEPTING_GATE,
        _CLIENT_LINK_DELIVERY_CUT_AT,
        heal_time=_CLIENT_LINK_DELIVERY_HEAL_AT,
    )
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

    # Acceptance converged through the retry cycle: the cut target
    # costs the silent 10s TCP timeout, then gate-c accepts --
    # after the first cut and before the delivery-window cut.
    submit_targets = [entry for entry in client_log if entry[0] == "submit-target"]
    assert submit_targets and submit_targets[0][1] == _GATE_PIDS.index(_CLIENT_LINK_ACCEPTING_GATE), client_log
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted and (
        _CLIENT_LINK_FIRST_CUT_HEAL_AT <= submitted[0][1] < _CLIENT_LINK_DELIVERY_CUT_AT
    ), client_log

    # Delivery beat the delivery-window cut's HEAL: the accepting gate's
    # push failed into the cut and a peer gate delivered the
    # client-ready result — the push-failover path, not the poll
    # fallback after heal, is what carried it.
    assert finish_time < _CLIENT_LINK_DELIVERY_HEAL_AT, client_log

    flags = _final_leader_flags(results, _GATE_PIDS)
    assert sum(flags.values()) == 1, flags


def test_client_link_faults_are_replay_deterministic():
    assert _run_client_link_faults() == _run_client_link_faults()


# =========================================================================
# 5. long-horizon — chaos waves, quiesce, convergence probe
# =========================================================================


_LONG_HORIZON_CEILING = 420.0
_LONG_HORIZON_WAIT_TIMEOUT = 100.0


def _long_horizon_baseline(seed: int) -> GateClusterBaseline:
    """The long-horizon topology's twin (the late client is part of it)."""
    return _distinct_role_baseline(
        seed, _LONG_HORIZON_CEILING, _LONG_HORIZON_WAIT_TIMEOUT, _LATE_CLIENT_AT
    )


def _run_long_horizon_chaos_waves(seed: int = _DISTINCT_ROLE_SEEDS[0]) -> dict:
    """420 virtual seconds, three separated fault waves, then quiet:

    * wave 1 (the twin's mid-execution instant): the follower gate dies
      for good — INSIDE live execution, so the kill provably intersects
      the running workflow;
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

    The leadership tail pins the composite CONVERGING: with the
    follower dead, the wave-2 cut leaves the initial leader without a
    majority, so its quorum lease lapses and it steps down (AD-5
    addendum); nobody leads until the heal, then the pair elects one
    leader that holds to the end -- exclusive at every instant. The killed gate's membership is reaped on
    the witness-less bound (gate-b peer count 2 -> 1 at 88.5) and
    never re-admitted — its readmission ping fails forever, which is
    the correct truth for a genuinely dead peer.
    """
    baseline = _long_horizon_baseline(seed)
    leader_gate = baseline.leader_gate
    submission_gate = baseline.submission_gate
    coordinator = _build_cluster(
        seed,
        _LONG_HORIZON_CEILING,
        wait_timeout=_LONG_HORIZON_WAIT_TIMEOUT,
        late_client_at=_LATE_CLIENT_AT,
    )
    coordinator.schedule_kill(baseline.follower_gate, baseline.mid_execution_at)
    coordinator.schedule_partition(
        submission_gate, leader_gate, 30.0, heal_time=60.0
    )
    coordinator.schedule_drop_rate(
        leader_gate, "manager", 0.20, at_time=80.0, until_time=110.0
    )
    coordinator.schedule_drop_rate(
        "manager", leader_gate, 0.20, at_time=80.0, until_time=110.0
    )
    coordinator.schedule_duplicate(
        "manager", submission_gate, 0.5, at_time=80.0, until_time=115.0
    )
    coordinator.schedule_duplicate(
        submission_gate, "manager", 0.5, at_time=80.0, until_time=115.0
    )
    return coordinator.run()


@pytest.mark.parametrize("seed", _DISTINCT_ROLE_SEEDS)
def test_long_horizon_chaos_then_quiesce_holds_loud_invariants(seed: int):
    baseline = _long_horizon_baseline(seed)
    kill_at = baseline.mid_execution_at
    survivors = (baseline.submission_gate, baseline.leader_gate)
    results = _run_long_horizon_chaos_waves(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(
        results,
        seed,
        _LONG_HORIZON_CEILING,
        _LONG_HORIZON_WAIT_TIMEOUT,
        kill_at,
        _LATE_CLIENT_AT,
    )
    assert baseline.follower_gate not in results, sorted(results)

    # The primary job — mid-execution when wave 1 landed — completed
    # at the twin's instant (a follower kill is invisible to the job
    # path) and survived waves 2-3 untouched.
    primary_finish = _assert_clean_completed_client(
        results["client"], "long-horizon primary"
    )
    assert primary_finish < baseline.completion_at + _UNPERTURBED_TOLERANCE_SECONDS, results["client"]

    # The post-quiesce client COMPLETES — the full convergence
    # invariant, live end to end: a SURVIVING gate accepts (the client
    # ranks its gates per job (AD-28), so the killed gate is tried first
    # only for some job ids, at the cost of one silent 10s TCP timeout;
    # probed: this job's first target is gate-c, accepted at 300.12),
    # dispatch reaches the worker, and the client observes
    # ``completed`` soon after (acceptance + dispatch + workflow + push). Both halves of the old
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
    killed_gate_index = _GATE_PIDS.index(baseline.follower_gate)
    assert late_submit_targets[0][1] != killed_gate_index, late_log
    late_submitted = [
        entry for entry in late_log if entry[0] == "job-submitted"
    ]
    assert late_submitted, late_log
    late_submitted_time = late_submitted[0][1]
    # No later than one dead-target timeout (the killed gate ranked first)
    # plus the acceptance slack after the late client starts.
    assert _LATE_CLIENT_AT <= late_submitted_time <= _LATE_CLIENT_AT + 12.0, late_log
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
        and entry[2] > _LATE_CLIENT_AT
    ]
    assert late_worker_activations, (
        "the late job completed so the worker must show its execution",
        results["worker"],
    )

    # Leadership tail — exclusive throughout, and CONVERGED after heal.
    # With the follower dead, the wave-2 cut leaves the initial leader
    # without a majority: its quorum lease lapses one lease after the
    # last beat the other survivor acknowledged, and it steps down (Raft
    # thesis 6.2/6.4.1, AD-5 addendum) -- no gate leads while no
    # majority can be reached. After the heal the pair elects one leader
    # within a lease (a vote binds its voter that long) plus the
    # randomized election wait, and it holds to the end.
    settings = Env()
    partition_at, heal_at = 30.0, 60.0
    assert baseline.leader_claimed_at < 10.0, baseline
    oracle = ClusterTraceOracle(gate_process_ids=_GATE_PIDS)
    assert oracle.check_gate_leader_exclusivity(
        results, killed_process_ids=(baseline.follower_gate,)
    ) == [], results
    initial_leader_step_downs = [
        entry[2]
        for entry in results[baseline.leader_gate]
        if entry[0] == "gate-leader" and entry[1] == 0 and entry[2] > 10.0
    ]
    assert initial_leader_step_downs, results[baseline.leader_gate]
    assert (
        partition_at
        < initial_leader_step_downs[0]
        <= partition_at + settings.LEADER_LEASE_DURATION + _LEADER_WATCH_SAMPLE_SECONDS
    ), results[baseline.leader_gate]
    leaderless_claims = [
        entry
        for gate_pid in survivors
        for entry in results[gate_pid]
        if entry[0] == "gate-leader" and entry[1] == 1 and 10.0 < entry[2] < heal_at
    ]
    assert not leaderless_claims, leaderless_claims
    flags = _final_leader_flags(results, survivors)
    assert sum(flags.values()) == 1, flags
    settled_by = (
        heal_at
        + settings.LEADER_LEASE_DURATION
        + settings.LEADER_ELECTION_TIMEOUT_JITTER
        + _LEADER_WATCH_SAMPLE_SECONDS
    )
    for gate_pid in survivors:
        late_leader_moves = [
            entry
            for entry in results[gate_pid]
            if entry[0] == "gate-leader" and entry[2] > settled_by
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
    for gate_pid in survivors:
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
            and kill_at + 5.0 <= entry[2] <= kill_at + 30.0
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
        for gate_pid in survivors
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
    with EXACTLY ONE leader. The initial leader steps down when the
    wave-2 cut takes its majority (its quorum lease lapses, AD-5
    addendum); after the heal the pair re-elects one leader. (Pre-fix
    truth, before re-admission: mutual partition eviction decayed
    quorum and the remainder was permanently leaderless.)"""
    baseline = _long_horizon_baseline(_DISTINCT_ROLE_SEEDS[0])
    results = _run_long_horizon_chaos_waves()
    _assert_no_unswapped_seams(results)

    flags = _final_leader_flags(
        results, (baseline.submission_gate, baseline.leader_gate)
    )
    assert sum(flags.values()) == 1, flags


# =========================================================================
# 6. client restart — power loss of a leaf client
# =========================================================================


_CLIENT_RESTART_DOWN_SECONDS = 10.0
_CLIENT_RESTART_CEILING = 100.0
_CLIENT_RESTART_WAIT_TIMEOUT = 60.0
# The successor job is accepted right after the reboot and completes at
# dispatch + the 6s duration + push (probed: reboot 22.0, acceptance
# 22.12, completion 28.26); the original bound sat 10s after the reboot
# (t=32) -- past it, the second dispatch is riding retries again.
_SUCCESSOR_COMPLETION_WINDOW_SECONDS = 10.0


def _client_restart_baseline(seed: int) -> GateClusterBaseline:
    """The client-restart topology's twin."""
    return _baseline(seed, _CLIENT_RESTART_CEILING, _CLIENT_RESTART_WAIT_TIMEOUT)


def _run_client_restart(seed: int = _SEED) -> dict:
    """Power-lose the CLIENT at the twin's mid-execution instant for 10
    seconds.

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
    baseline = _client_restart_baseline(seed)
    coordinator = _build_cluster(
        seed, _CLIENT_RESTART_CEILING, wait_timeout=_CLIENT_RESTART_WAIT_TIMEOUT
    )
    coordinator.schedule_restart(
        "client", baseline.mid_execution_at, down_seconds=_CLIENT_RESTART_DOWN_SECONDS
    )
    return coordinator.run()


@pytest.mark.parametrize("seed", _ANY_LAYOUT_SEEDS)
def test_client_restart_first_job_survives_and_successor_is_loud(seed: int):
    baseline = _client_restart_baseline(seed)
    restart_at = baseline.mid_execution_at
    results = _run_client_restart(seed)
    _assert_no_unswapped_seams(results)
    _assert_identical_before(
        results, seed, _CLIENT_RESTART_CEILING, _CLIENT_RESTART_WAIT_TIMEOUT, restart_at
    )

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
    # the first job to full drain -- no earlier than the fault-free
    # twin's drain (the original pin: drain past 15.0, twin drain 15.75).
    worker_log = results["worker"]
    drain_times = [
        entry[2]
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] == 0 and entry[2] > 0.0
    ]
    assert drain_times and drain_times[0] >= baseline.worker_drained_at, worker_log

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
    assert submitted and submitted[0][1] > restart_at, successor_log
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
    past the reboot + 10s (t=32 in the probe) means the second dispatch
    is riding retries again."""
    restart_at = _client_restart_baseline(_SEED).mid_execution_at
    successor_boot_at = restart_at + _CLIENT_RESTART_DOWN_SECONDS
    results = _run_client_restart()
    _assert_no_unswapped_seams(results)

    successor_log = results["client"]
    finished = [
        entry for entry in successor_log if entry[0] == "job-finished"
    ]
    assert len(finished) == 1, successor_log
    assert finished[0][1] == "completed", successor_log
    assert (
        restart_at
        < finished[0][2]
        < successor_boot_at + _SUCCESSOR_COMPLETION_WINDOW_SECONDS
    ), successor_log

    # The worker really ran it: a second activation window opens after
    # the successor submission and drains back to zero.
    worker_log = results["worker"]
    second_activations = [
        entry
        for entry in worker_log
        if entry[0] == "workflows-active"
        and entry[1] > 0
        and entry[2] > restart_at
    ]
    assert second_activations, worker_log
    assert worker_log[-1][:2] == ("workflows-active", 0), worker_log
