"""
Gate-tier VOPR execution + invariants: run a generated ``FaultPlan``
against the real production stack under the deterministic coordinator,
and judge the outcome.

The topology is the canonical GATE-CLUSTER — three real peered
``GateServer`` processes (SWIM peer discovery, gate leader election,
per-job gate leadership), one real ``ManagerServer`` registered
upstream with ALL three gates, one real ``WorkerServer`` with its
2-core executor pool, and a real ``HyperscaleClient`` configured with
the WHOLE gate tier submitting a sustained-load TEST workflow — so
every generated schedule exercises the production L3 path end to end,
not a model of it.
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

from .fault_plan import GATE_PROCESS_IDS, FaultPlan

# Client-observed job states that count as a TERMINAL outcome. A
# generated schedule may legitimately push a job into failure/timeout
# (a gate kill racing a partition), but it must never leave the client
# in silence — every job reaches a terminal state the client SEES,
# before the ceiling.
_TERMINAL_STATUSES = frozenset({"completed", "failed", "timeout", "cancelled"})

_GATE_HOSTS = {
    "gate-a": "sim-gate-a",
    "gate-b": "sim-gate-b",
    "gate-c": "sim-gate-c",
}

# Sustained-load workflow sizing (probed baseline, seed 211): 6 virtual
# seconds of TEST-hook VU load holds the job in-flight over
# ~[dispatch 8.8, drain ~15], so generated fault windows genuinely
# intersect live execution instead of always landing on an idle tier.
_WORKFLOW_DURATION_SECONDS = 6.0
_WORKFLOW_VUS = 2
_JOB_TIMEOUT_SECONDS = 30.0

# Client wait budget: submission lands ~5.5s (probed); a killed
# submission gate costs one 5s dead-origin push failover (probed) and
# even a full witness-less death reap (~70-74.5s) plus takeover fits
# many times over. 120s from submission still lands inside the 145s
# ceiling, and a wait expiry is a LOUD milestone the invariants
# reject — silence is impossible by construction.
_WAIT_TIMEOUT_SECONDS = 120.0

# Leadership must be quiet for the tail of every run: the last fault
# window in the generated space ends by ~80s and probed detection +
# re-election after a leader kill completes within tens of seconds, so
# a leader transition inside the final window means flapping, not
# recovery.
_LEADER_STABILITY_WINDOW_SECONDS = 30.0


def run_fault_plan(plan: FaultPlan) -> dict:
    """Execute one generated schedule; returns the per-process logs."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=plan.ceiling, seed=plan.seed
    )

    datacenter_managers = {"dc-1": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"dc-1": [("sim-mgr", 9001)]}

    for gate_process_id in GATE_PROCESS_IDS:
        gate_host = _GATE_HOSTS[gate_process_id]
        peer_hosts = [
            _GATE_HOSTS[peer_id]
            for peer_id in GATE_PROCESS_IDS
            if peer_id != gate_process_id
        ]
        coordinator.add_process(
            gate_process_id,
            leader_watch_gate_tier_entry,
            gate_host,
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
        [(_GATE_HOSTS[gate_id], 9000) for gate_id in GATE_PROCESS_IDS],
        [(_GATE_HOSTS[gate_id], 9001) for gate_id in GATE_PROCESS_IDS],
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
        multi_gate_load_client_entry,
        "sim-cli",
        9500,
        [(_GATE_HOSTS[gate_id], 9000) for gate_id in GATE_PROCESS_IDS],
        _WORKFLOW_DURATION_SECONDS,
        _WORKFLOW_VUS,
        _JOB_TIMEOUT_SECONDS,
        _WAIT_TIMEOUT_SECONDS,
    )

    for event in plan.events:
        if event[0] == "gate_kill":
            _tag, victim_gate, at_time = event
            coordinator.schedule_kill(victim_gate, at_time)
        elif event[0] == "gate_gate_partition":
            _tag, gate_x, gate_y, at_time, heal_time = event
            coordinator.schedule_partition(
                gate_x, gate_y, at_time, heal_time=heal_time
            )
        elif event[0] == "gate_manager_partition":
            _tag, gate_x, at_time, heal_time = event
            coordinator.schedule_partition(
                gate_x, "manager", at_time, heal_time=heal_time
            )
        elif event[0] == "client_gate_partition":
            _tag, gate_x, at_time, heal_time = event
            coordinator.schedule_partition(
                "client", gate_x, at_time, heal_time=heal_time
            )
        elif event[0] == "drop":
            _tag, src, dst, probability, at_time, until_time = event
            coordinator.schedule_drop_rate(
                src, dst, probability, at_time=at_time, until_time=until_time
            )
        elif event[0] == "delay":
            _tag, src, dst, extra, jitter, at_time, until_time = event
            coordinator.schedule_delay(
                src,
                dst,
                extra,
                at_time=at_time,
                until_time=until_time,
                jitter_seconds=jitter,
            )
        elif event[0] == "duplicate":
            _tag, src, dst, probability, at_time, until_time = event
            coordinator.schedule_duplicate(
                src, dst, probability, at_time=at_time, until_time=until_time
            )
        else:
            raise ValueError(f"unknown fault-plan event kind: {event[0]!r}")

    return coordinator.run()


def _check_determinism_audit(results: dict) -> list[str]:
    """Any ``determinism-audit-unswapped`` row in ANY child's result is
    a live nondeterminism source — an unconditional failure."""
    return [
        f"determinism audit: {process_id} carries unswapped seams {entry[1]}"
        for process_id, entries in results.items()
        if isinstance(entries, list)
        for entry in entries
        if isinstance(entry, tuple)
        and entry
        and entry[0] == "determinism-audit-unswapped"
    ]


def _check_gate_leader_convergence(
    plan: FaultPlan, results: dict
) -> list[str]:
    """EVENTUAL leader convergence + terminal stability window.

    Judged from each surviving gate's ``("gate-leader", flag, t)``
    transitions (killed victims are absent from results by kill
    semantics and are skipped):

    * exactly ONE surviving gate ends the run believing it is leader —
      zero means the tier wedged leaderless, two+ means split-brain;
    * no surviving gate's leader flag changes inside the final
      stability window — every generated fault ends early enough that
      a late transition is churn, not recovery.
    """
    violations: list[str] = []
    killed_gates = {
        event[1] for event in plan.events if event[0] == "gate_kill"
    }
    stability_deadline = plan.ceiling - _LEADER_STABILITY_WINDOW_SECONDS

    final_leader_flags: dict[str, int] = {}
    for gate_process_id in GATE_PROCESS_IDS:
        if gate_process_id in killed_gates:
            continue
        gate_log = results.get(gate_process_id)
        if not isinstance(gate_log, list):
            violations.append(
                f"gate {gate_process_id} produced no milestone log: "
                f"{gate_log!r}"
            )
            continue
        leader_transitions = [
            entry for entry in gate_log if entry[0] == "gate-leader"
        ]
        if not leader_transitions:
            violations.append(
                f"gate {gate_process_id} never reported a leader flag: "
                f"{gate_log}"
            )
            continue
        final_leader_flags[gate_process_id] = leader_transitions[-1][1]
        late_transitions = [
            entry
            for entry in leader_transitions
            if entry[2] > stability_deadline
        ]
        if late_transitions:
            violations.append(
                f"gate {gate_process_id} leadership still moving inside "
                f"the final {_LEADER_STABILITY_WINDOW_SECONDS}s stability "
                f"window: {late_transitions}"
            )

    leader_count = sum(final_leader_flags.values())
    if final_leader_flags and leader_count != 1:
        violations.append(
            "surviving gate tier must converge to exactly one leader, "
            f"got {leader_count}: {final_leader_flags}"
        )
    return violations


def check_invariants(plan: FaultPlan, results: dict) -> list[str]:
    """Judge one run; returns human-readable violations (empty = pass).

    Invariants every generated schedule must satisfy:

    1. NO child result carries a determinism-audit-unswapped row.
    2. The client SUBMITTED the job: every generated schedule leaves
       the tier able to accept (at most one dead gate, heal-bounded
       partitions, unbounded client retries) — logged rejections are
       legal, never-accepted is not.
    3. The client observed a TERMINAL job state before the ceiling —
       completion normally; explicit failure/timeout is acceptable
       only under faults that can strand in-flight work (a gate kill
       or a partition window). A client-side wait expiry
       (``wait-timeout``) or an unexpected client error is always a
       violation: the wait budget is sized past every recovery path.
    4. A loss/delay/duplication-only schedule (and the fault-free
       baseline) must COMPLETE the job.
    5. The observed history LINEARIZES (the JobStatusOracle): ranks
       never regress, terminals absorb, the finished result agrees
       with the observed terminal, results deliver exactly once.
    6. The surviving gate tier converges to EXACTLY ONE leader and
       holds it stable through the final window.
    """
    violations: list[str] = []
    client_log = results.get("client") or []

    violations.extend(_check_determinism_audit(results))

    violations.extend(
        f"oracle: {violation}"
        for violation in JobStatusOracle().check_client_log(client_log)
    )

    violations.extend(_check_gate_leader_convergence(plan, results))

    client_errors = [
        entry for entry in client_log if entry[0] == "client-error"
    ]
    if client_errors:
        violations.append(
            f"client flow raised unexpectedly: {client_errors}"
        )

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    if not submitted:
        violations.append(f"job was never accepted: client log {client_log}")
        return violations

    wait_expiries = [
        entry for entry in client_log if entry[0] == "wait-timeout"
    ]
    if wait_expiries:
        violations.append(
            "client wait expired without a terminal outcome (the wait "
            f"budget outlives every recovery path): {client_log}"
        )

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    terminal_statuses_seen = {
        entry[1]
        for entry in client_log
        if entry[0] in ("status-seen", "job-finished")
        and entry[1] in _TERMINAL_STATUSES
    }
    if not terminal_statuses_seen:
        violations.append(
            "client never observed a terminal job state (silent strand): "
            f"{client_log}"
        )

    schedule_can_strand = any(
        event[0]
        in (
            "gate_kill",
            "gate_gate_partition",
            "gate_manager_partition",
            "client_gate_partition",
        )
        for event in plan.events
    )
    completed = bool(finished) and finished[0][1] == "completed"
    if not completed and not schedule_can_strand:
        violations.append(
            "loss/delay/duplication-only schedule must complete the job: "
            f"events={plan.events} client={client_log}"
        )

    return violations
