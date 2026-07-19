"""
Multi-DC VOPR execution + invariants: run a generated ``MdcFaultPlan``
against the real production stack under the deterministic coordinator,
and judge the outcome.

The topology is the canonical L3 pair — a real ``GateServer`` fronting
dc-east and dc-west (each a real ``ManagerServer`` with WAL enabled plus
a 2-core ``WorkerServer`` whose executors spawn as coordinator
children), and a real ``HyperscaleClient`` submitting an 8-virtual-
second soak workflow through the gate — so every generated schedule
exercises production cross-DC code end to end, not a model of it.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.gate_cluster_demo import (
    gate_tier_entry,
)
from tests.simulation.harness.sim.multiprocess.multi_dc_fault_demo import (
    faulted_multi_gate_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.soak_job_demo import (
    soak_gate_dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)

from tests.simulation.oracle import JobStatusOracle

from .fault_plan import (
    DC_PROCESS_IDS,
    DATACENTER_IDS,
    JOB_TIMEOUT_SECONDS,
    MdcFaultPlan,
    WORKFLOW_DURATION_SECONDS,
)

_GATE_PROCESS_ID = "sim-gate-a"
_CLIENT_PROCESS_ID = "client-a"

# Client-side wait_for_job deadline: expiry logs ("wait-timed-out", t)
# and the entry then waits unbounded, so a late terminal still lands in
# the log before the ceiling.
_WAIT_TIMEOUT_SECONDS = 150.0

_DC_TOPOLOGY = (
    ("dc-east", "sim-mgr-east", "sim-wkr-east"),
    ("dc-west", "sim-mgr-west", "sim-wkr-west"),
)

# Client-observed job states that count as a TERMINAL outcome. Both live
# timeout spellings are terminal in production's own rank table
# (managers write ``timeout``, the gate tracker records ``timed_out``).
_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "timeout", "timed_out", "cancelled"}
)


def run_mdc_fault_plan(plan: MdcFaultPlan) -> dict:
    """Execute one generated multi-DC schedule; returns per-process logs."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=plan.ceiling, seed=plan.seed
    )

    # Storage events ride the target manager's entry args (the knobs
    # live on the CHILD's in-memory filesystem); everything else goes
    # through coordinator scheduling below.
    storage_schedule_by_dc: dict[str, tuple[tuple, ...]] = {
        datacenter_id: tuple(
            (event[0], *event[2:])
            for event in plan.events
            if event[0] in ("slow_disk", "disk_full")
            and event[1] == datacenter_id
        )
        for datacenter_id in DATACENTER_IDS
    }

    coordinator.add_process(
        _GATE_PROCESS_ID,
        gate_tier_entry,
        "sim-gate-a",
        9000,
        9001,
        {
            "dc-east": [("sim-mgr-east", 9000)],
            "dc-west": [("sim-mgr-west", 9000)],
        },
        {
            "dc-east": [("sim-mgr-east", 9001)],
            "dc-west": [("sim-mgr-west", 9001)],
        },
    )
    for datacenter_id, manager_host, worker_host in _DC_TOPOLOGY:
        coordinator.add_process(
            f"manager-{datacenter_id}",
            faulted_multi_gate_manager_entry,
            manager_host,
            9000,
            9001,
            datacenter_id,
            [("sim-gate-a", 9000)],
            [("sim-gate-a", 9001)],
            storage_schedule_by_dc[datacenter_id],
        )
        coordinator.add_process(
            f"worker-{datacenter_id}",
            worker_entry,
            worker_host,
            9000,
            9001,
            datacenter_id,
            (manager_host, 9000),
            2,
        )
    coordinator.add_process(
        _CLIENT_PROCESS_ID,
        soak_gate_dispatch_client_entry,
        "sim-cli-a",
        9500,
        ("sim-gate-a", 9000),
        WORKFLOW_DURATION_SECONDS,
        JOB_TIMEOUT_SECONDS,
        _WAIT_TIMEOUT_SECONDS,
        0.0,
        None,
    )

    for event in plan.events:
        if event[0] == "dc_loss":
            _tag, datacenter_id, at_time = event
            for victim_id in DC_PROCESS_IDS[datacenter_id]:
                coordinator.schedule_kill(victim_id, at_time)
        elif event[0] == "dc_partition":
            _tag, datacenter_id, at_time, heal_time = event
            coordinator.schedule_partition(
                _GATE_PROCESS_ID,
                f"manager-{datacenter_id}",
                at_time,
                heal_time=heal_time,
            )
        elif event[0] == "manager_restart":
            _tag, datacenter_id, at_time, down_seconds, fsync_seed = event
            coordinator.schedule_restart(
                f"manager-{datacenter_id}",
                at_time,
                down_seconds=down_seconds,
                fsync_reorder_seed=fsync_seed,
            )
        elif event[0] == "gate_link_drop":
            _tag, datacenter_id, probability, at_time, until_time = event
            for link_src, link_dst in (
                (_GATE_PROCESS_ID, f"manager-{datacenter_id}"),
                (f"manager-{datacenter_id}", _GATE_PROCESS_ID),
            ):
                coordinator.schedule_drop_rate(
                    link_src,
                    link_dst,
                    probability,
                    at_time=at_time,
                    until_time=until_time,
                )
        elif event[0] == "client_partition":
            _tag, at_time, heal_time = event
            coordinator.schedule_partition(
                _CLIENT_PROCESS_ID,
                _GATE_PROCESS_ID,
                at_time,
                heal_time=heal_time,
            )
        elif event[0] == "client_delay":
            _tag, extra_seconds, jitter_seconds, at_time, until_time = event
            for link_src, link_dst in (
                (_CLIENT_PROCESS_ID, _GATE_PROCESS_ID),
                (_GATE_PROCESS_ID, _CLIENT_PROCESS_ID),
            ):
                coordinator.schedule_delay(
                    link_src,
                    link_dst,
                    extra_seconds,
                    at_time=at_time,
                    until_time=until_time,
                    jitter_seconds=jitter_seconds,
                )
        elif event[0] == "client_duplicate":
            _tag, probability, at_time, until_time = event
            for link_src, link_dst in (
                (_CLIENT_PROCESS_ID, _GATE_PROCESS_ID),
                (_GATE_PROCESS_ID, _CLIENT_PROCESS_ID),
            ):
                coordinator.schedule_duplicate(
                    link_src,
                    link_dst,
                    probability,
                    at_time=at_time,
                    until_time=until_time,
                )
        elif event[0] in ("slow_disk", "disk_full"):
            continue  # armed inside the target manager child via entry args
        else:
            raise ValueError(f"unknown fault-plan event kind: {event[0]!r}")

    return coordinator.run()


def _datacenter_ran_workflow(results: dict, datacenter_id: str) -> bool:
    worker_log = results.get(f"worker-{datacenter_id}") or []
    return any(
        entry[0] == "workflows-active" and entry[1] > 0 for entry in worker_log
    )


def check_mdc_invariants(plan: MdcFaultPlan, results: dict) -> list[str]:
    """Judge one run; returns human-readable violations (empty = pass).

    Invariants every generated multi-DC schedule must satisfy:

    1. NO SILENT SWAP ESCAPES: no child result (any generation) carries
       a ``("determinism-audit-unswapped", ...)`` entry.
    2. The client SUBMITTED the job. Nothing in this fault space may
       block acceptance through the ceiling: both DCs exist, at most
       one manager is storage-faulted, and client cuts heal by t<=60.
    3. The client observed a TERMINAL state before the ceiling —
       silence is always a violation.
    4. The observed history LINEARIZES (JobStatusOracle): ranks never
       regress, terminals absorb, ``job-finished`` agrees with the
       observed terminal and delivers exactly once.
    5. Schedules with no stranding-capable event must COMPLETE the job
       (loss/delay/duplication/slow-disk-only schedules, and the
       fault-free baseline).
    6. EXACTLY-ONCE PLACEMENT, as observable from worker execution: the
       workflow ran in AT MOST one datacenter under every schedule
       (there is deliberately no mid-flight cross-DC re-dispatch), and
       in EXACTLY one when the client observed completion — unless a
       dc_loss could have SIGKILLed the executing worker's milestone
       log away after the fact (killed children never report).
    7. CLASSIFICATION CONVERGENCE: the gate's final health for a
       dc_loss'd datacenter is ``unhealthy`` (the 30s heartbeat-
       staleness detection held); every other datacenter must END
       ``healthy`` (partitions healed, restarts resumed, drop windows
       ended — transient flaps are legitimate, divergence is not).
    """
    violations: list[str] = []

    for process_id, process_log in results.items():
        audit_entries = [
            entry
            for entry in (process_log or [])
            if isinstance(entry, tuple)
            and entry[:1] == ("determinism-audit-unswapped",)
        ]
        if audit_entries:
            violations.append(
                f"determinism audit found unswapped imports in "
                f"{process_id}: {audit_entries}"
            )

    client_log = results.get(_CLIENT_PROCESS_ID) or []

    violations.extend(
        f"oracle: {violation}"
        for violation in JobStatusOracle().check_client_log(client_log)
    )

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    if not submitted:
        violations.append(f"job was never accepted: client log {client_log}")
        return violations

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

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    completed = bool(finished) and finished[0][1] == "completed"
    if not completed and not plan.can_strand():
        violations.append(
            "schedule without stranding-capable events must complete the "
            f"job: events={plan.events} client={client_log}"
        )

    ran_datacenters = [
        datacenter_id
        for datacenter_id in DATACENTER_IDS
        if _datacenter_ran_workflow(results, datacenter_id)
    ]
    if len(ran_datacenters) > 1:
        violations.append(
            "workflow executed in BOTH datacenters — placement must be "
            f"exactly-once: events={plan.events}"
        )
    # A SIGKILLed worker never reports its milestone log, so a dc_loss
    # can destroy the execution evidence of a job that completed there
    # first (measured: seed 302 completes in dc-west at 11.6, the kill
    # at 17.95 erases the worker's log). Execution visibility is only
    # REQUIRED when no total loss could have swallowed it.
    if completed and not ran_datacenters and not plan.lost_datacenters():
        violations.append(
            "client observed completion but no datacenter shows workflow "
            f"execution: events={plan.events}"
        )

    gate_log = results.get(_GATE_PROCESS_ID) or []
    final_health = {
        entry[1]: entry[2] for entry in gate_log if entry[0] == "dc-health"
    }
    lost_datacenters = plan.lost_datacenters()
    for datacenter_id in DATACENTER_IDS:
        expected_health = (
            "unhealthy" if datacenter_id in lost_datacenters else "healthy"
        )
        if final_health.get(datacenter_id) != expected_health:
            violations.append(
                f"gate's final classification of {datacenter_id} is "
                f"{final_health.get(datacenter_id)!r}, expected "
                f"{expected_health!r}: events={plan.events} "
                f"gate={gate_log}"
            )

    return violations
