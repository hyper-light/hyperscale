"""
VOPR execution + invariants: run a generated ``FaultPlan`` against the
real production stack under the deterministic coordinator, and judge
the outcome.

The topology is the canonical L2 job triangle — a real ``ManagerServer``,
a real ``WorkerServer`` with its 2 executor-pool children, and a real
``HyperscaleClient`` submitting a real workflow — so every generated
schedule exercises production code end to end, not a model of it.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

from tests.simulation.oracle import JobStatusOracle

from .fault_plan import FaultPlan

# Client-observed job states that count as a TERMINAL outcome. A
# generated schedule may legitimately push a job into failure/timeout
# (an executor kill racing a partition), but it must never leave the
# client in silence — every job reaches a terminal state the client
# SEES, before the ceiling.
_TERMINAL_STATUSES = frozenset({"completed", "failed", "timeout", "cancelled"})


def run_fault_plan(plan: FaultPlan) -> dict:
    """Execute one generated schedule; returns the per-process logs."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=plan.ceiling, seed=plan.seed
    )
    # Storage events ride the manager's entry args (the knobs live on
    # the CHILD's in-memory filesystem, so only the child can arm them);
    # network/kill events go through coordinator scheduling below.
    storage_fault_schedule = tuple(
        event
        for event in plan.events
        if event[0] in ("slow_disk", "disk_full")
    )
    coordinator.add_process(
        "manager",
        manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        None,
        None,
        storage_fault_schedule,
    )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )

    for event in plan.events:
        if event[0] == "kill":
            _tag, victim_id, at_time = event
            coordinator.schedule_kill(victim_id, at_time)
        elif event[0] == "partition":
            _tag, process_a, process_b, at_time, heal_time = event
            coordinator.schedule_partition(
                process_a, process_b, at_time, heal_time=heal_time
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
        elif event[0] == "restart":
            _tag, at_time, down_seconds, fsync_reorder_seed = event
            coordinator.schedule_restart(
                "manager",
                at_time,
                down_seconds=down_seconds,
                fsync_reorder_seed=fsync_reorder_seed,
            )
        elif event[0] in ("slow_disk", "disk_full"):
            continue  # armed inside the manager child via its entry args
        else:
            raise ValueError(f"unknown fault-plan event kind: {event[0]!r}")

    return coordinator.run()


def check_invariants(plan: FaultPlan, results: dict) -> list[str]:
    """Judge one run; returns human-readable violations (empty = pass).

    Invariants every generated schedule must satisfy:

    1. The client SUBMITTED the job (the cluster became available).
    2. The client observed a TERMINAL job state before the ceiling —
       completion normally; explicit failure/timeout is acceptable only
       under faults that can strand in-flight work (an executor kill).
       Silence — no terminal state ever seen — is always a violation.
    3. A fault-free schedule must COMPLETE the job (the baseline).
    4. The observed history LINEARIZES (the JobStatusOracle): status
       ranks never regress, terminals absorb, the finished result
       agrees with the observed terminal, and results deliver
       exactly once — under EVERY schedule, faulted or not.
    """
    violations: list[str] = []
    client_log = results.get("client") or []

    violations.extend(
        f"oracle: {violation}"
        for violation in JobStatusOracle().check_client_log(client_log)
    )

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    if not submitted:
        # A manager that cannot persist its idempotency reservation
        # (disk_full armed before submission) MUST reject — explicitly.
        # Observed rejection is a legitimate loud outcome; silence is
        # not.
        rejected = [
            entry for entry in client_log if entry[0] == "submit-rejected"
        ]
        disk_full_scheduled = any(
            event[0] == "disk_full" for event in plan.events
        )
        if disk_full_scheduled and rejected:
            return violations
        violations.append(f"job was never accepted: client log {client_log}")
        return violations

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

    # disk_full joins kill/partition: an exhausted manager WAL may
    # legitimately fail the job, but never silently. slow_disk does NOT —
    # bounded delays must be ridden out to completion.
    # restart joins the stranding-capable set: resume usually
    # completes the job across the reboot, but a crash landing between
    # the ledger record and the submission-payload write legitimately
    # degrades to the loud durable-FAILED path.
    schedule_can_strand = any(
        event[0] in ("kill", "partition", "disk_full", "restart")
        for event in plan.events
    )
    completed = bool(finished) and finished[0][1] == "completed"
    if not completed and not schedule_can_strand:
        violations.append(
            "loss/delay/duplication-only schedule must complete the job: "
            f"events={plan.events} client={client_log}"
        )

    return violations
