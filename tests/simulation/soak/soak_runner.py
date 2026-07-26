"""
Soak execution + invariants: run a generated ``SoakPlan`` against the
real production stack under the deterministic coordinator, and judge
the whole horizon.

The topology is the gateless L2 job path — a real ``ManagerServer``
(WAL enabled), a real ``WorkerServer`` with its 2 executor-pool
children, plus (kill plans only) a late-joining replacement worker —
and one real ``HyperscaleClient`` submitting the plan's whole job
schedule sequentially. Every process stays alive to the ceiling except
the scheduled kill victims, so the horizon is judged end to end: every
job, every fault window, every membership transition.

Run as a module for probe/triage tooling (parameterized, no pytest):

    uv run python -m tests.simulation.soak.soak_runner --seed 901
    uv run python -m tests.simulation.soak.soak_runner \\
        --seed 902 --ceiling 300 --twin --print-client-log
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.soak_multi_job_demo import (
    soak_manager_entry,
    soak_multi_job_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)

from tests.simulation.oracle import JobStatusOracle

from .soak_plan import (
    JOB_TIMEOUT_SECONDS,
    WAIT_TIMEOUT_SECONDS,
    SoakPlan,
)

_MANAGER_PROCESS_ID = "manager"
_CLIENT_PROCESS_ID = "client"

# Host-death reap latency design bounds, measured decomposition in the
# committed kill scenarios: evidence-accelerated detection runs
# [20, 70] (test_multiprocess_worker_kill pins it), witness-less legs
# stretch to the AD-30 maximum (~71s; [25, 85] band per the traced
# decomposition in test_multiprocess_network_faults). The soak's
# witness count VARIES by schedule — the replacement worker joins
# 10-20s after the kill and may or may not be registered before the
# reap — so the asserted band is the union of the two design bands
# (plus the 0.5s count-watcher sampling grain), never a wide window:
# a reap before 20s would mean detection outran its own design floor;
# one after 85.5s would mean the AD-30 ceiling was breached.
_REAP_LATENCY_MIN_SECONDS = 20.0
_REAP_LATENCY_MAX_SECONDS = 85.5

# Client-observed job states that count as a TERMINAL outcome (the
# gateless vocabulary: managers write ``timeout``).
_TERMINAL_STATUSES = frozenset({"completed", "failed", "timeout", "cancelled"})

# Client milestone suffixes (after the ``job<k>-`` prefix) mapped onto
# the standard single-job vocabulary the JobStatusOracle judges.
_JOB_TAG_SUFFIXES = {
    "submitted": "job-submitted",
    "status-seen": "status-seen",
    "finished": "job-finished",
    "rejected": "submit-rejected",
    "wait-timed-out": "wait-timed-out",
}


def run_soak_plan(plan: SoakPlan) -> dict:
    """Execute one generated horizon; returns the per-process logs."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=plan.ceiling, seed=plan.seed
    )
    # Storage windows ride the manager's entry args (the knobs live on
    # the CHILD's in-memory filesystem); network/kill events go through
    # coordinator scheduling below.
    storage_fault_schedule = tuple(
        event for event in plan.events if event[0] == "slow_disk"
    )
    coordinator.add_process(
        _MANAGER_PROCESS_ID,
        soak_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        storage_fault_schedule,
    )
    coordinator.add_process(
        "worker-a",
        worker_entry,
        "sim-wkr-a",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )

    kill_event = plan.host_kill()
    if kill_event is not None:
        _tag, kill_at, worker_b_start = kill_event
        coordinator.add_process(
            "worker-b",
            worker_entry,
            "sim-wkr-b",
            9000,
            9001,
            "sim-dc",
            ("sim-mgr", 9000),
            2,
            worker_b_start,
        )
        # Host death: the worker and BOTH executor children die at one
        # instant (the pinned worker-retry recipe — kills at the same
        # instant apply before anything else runs at it).
        coordinator.schedule_kill("worker-a", kill_at)
        coordinator.schedule_kill("executor-sim-wkr-a-9009", kill_at)
        coordinator.schedule_kill("executor-sim-wkr-a-9011", kill_at)

    coordinator.add_process(
        _CLIENT_PROCESS_ID,
        soak_multi_job_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        plan.submit_times,
        plan.durations,
        JOB_TIMEOUT_SECONDS,
        WAIT_TIMEOUT_SECONDS,
    )

    for event in plan.events:
        if event[0] == "drop":
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
        elif event[0] in ("host_kill", "slow_disk"):
            continue  # scheduled above / armed inside the manager child
        else:
            raise ValueError(f"unknown soak-plan event kind: {event[0]!r}")

    return coordinator.run()


def job_log_slice(client_log: list, job_index: int) -> tuple[list, list[str]]:
    """Extract job ``job_index``'s milestones from the multi-job client
    log, remapped onto the standard single-job vocabulary.

    Returns ``(sliced_log, violations)`` — an unknown ``job<k>-``
    suffix is a vocabulary violation (never silently dropped).
    """
    prefix = f"job{job_index}-"
    sliced: list = []
    violations: list[str] = []
    for entry in client_log:
        tag = entry[0]
        if not isinstance(tag, str) or not tag.startswith(prefix):
            continue
        mapped_tag = _JOB_TAG_SUFFIXES.get(tag[len(prefix):])
        if mapped_tag is None:
            violations.append(
                f"job {job_index}: unknown client milestone tag {tag!r}"
            )
            continue
        sliced.append((mapped_tag, *entry[1:]))
    return sliced, violations


def check_soak_invariants(plan: SoakPlan, results: dict) -> list[str]:
    """Judge one horizon; returns human-readable violations (empty =
    pass).

    Per job (every job in the plan):

    1. SUBMITTED exactly once — the horizon was actually occupied on
       the drawn schedule (rejection retries before acceptance are
       legitimate and logged).
    2. LOUD TERMINAL — a client-observed terminal state before the
       ceiling; silence is always a violation.
    3. LINEARIZES — the per-job history satisfies the JobStatusOracle
       (ranks never regress, terminals absorb, ``job-finished`` agrees
       with the observed terminal and delivers exactly once).
    4. COMPLETED — every event class in the soak space is calibrated
       survivable (mild windows are ride-through by design; the host
       kill has a late-joining replacement, so the retry path must
       deliver completion exactly as the pinned worker-retry scenario
       does, mid-horizon included). Any non-completed terminal is a
       violation, not an accepted loud outcome.

    Horizon-level:

    5. NO SILENT SWAP ESCAPES — no process result (any generation)
       carries a ``("determinism-audit-unswapped", ...)`` entry.
    6. MEMBERSHIP CONVERGED — the manager's worker-count transitions
       end at exactly ONE registered worker; kill-free horizons never
       lose a worker (no decrease), kill horizons show EXACTLY one
       reap, inside the SWIM design band relative to the kill instant,
       and the replacement worker both reports a healthy manager and
       (when jobs remain) actually executes workflows.

    Byte-identical replay of the whole horizon is asserted by the
    caller (the twin run), completing the invariant set.
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
    oracle = JobStatusOracle()

    for job_index in range(1, len(plan.submit_times) + 1):
        job_log, slice_violations = job_log_slice(client_log, job_index)
        violations.extend(slice_violations)

        violations.extend(
            f"job {job_index} oracle: {violation}"
            for violation in oracle.check_client_log(job_log)
        )

        submitted = [entry for entry in job_log if entry[0] == "job-submitted"]
        if len(submitted) != 1:
            violations.append(
                f"job {job_index} submitted {len(submitted)} times "
                f"(exactly once required): {job_log}"
            )
            continue

        terminal_seen = {
            entry[1]
            for entry in job_log
            if entry[0] in ("status-seen", "job-finished")
            and entry[1] in _TERMINAL_STATUSES
        }
        if not terminal_seen:
            violations.append(
                f"job {job_index} never reached a client-observed terminal "
                f"state (silent strand): {job_log}"
            )
            continue

        finished = [entry for entry in job_log if entry[0] == "job-finished"]
        if not finished or finished[0][1] != "completed":
            violations.append(
                f"job {job_index} did not complete — every soak event class "
                f"is calibrated survivable: finished={finished} "
                f"events={plan.events}"
            )

    manager_log = results.get(_MANAGER_PROCESS_ID) or []
    worker_counts = [
        (entry[1], entry[2])
        for entry in manager_log
        if entry[0] == "worker-count"
    ]
    if not worker_counts or worker_counts[-1][0] != 1:
        violations.append(
            "manager's final worker registry is not exactly one live "
            f"worker: transitions={worker_counts}"
        )

    decreases = [
        (previous_count, current_count, at_time)
        for (previous_count, _), (current_count, at_time) in zip(
            worker_counts, worker_counts[1:]
        )
        if current_count < previous_count
    ]

    kill_event = plan.host_kill()
    if kill_event is None:
        if decreases:
            violations.append(
                "kill-free horizon lost a registered worker: "
                f"decreases={decreases}"
            )
        return violations

    _tag, kill_at, worker_b_start = kill_event
    if len(decreases) != 1:
        violations.append(
            f"host-kill horizon must show exactly one registry reap: "
            f"decreases={decreases} kill_at={kill_at}"
        )
    else:
        reap_latency = decreases[0][2] - kill_at
        if not (
            _REAP_LATENCY_MIN_SECONDS
            <= reap_latency
            <= _REAP_LATENCY_MAX_SECONDS
        ):
            violations.append(
                f"dead host reaped {reap_latency:.3f}s after the kill — "
                f"outside the SWIM design band "
                f"[{_REAP_LATENCY_MIN_SECONDS}, {_REAP_LATENCY_MAX_SECONDS}]"
            )

    worker_b_log = results.get("worker-b") or []
    healthy = [
        entry for entry in worker_b_log if entry[0] == "manager-healthy"
    ]
    if not healthy or healthy[0][1] <= kill_at:
        violations.append(
            "replacement worker never registered against a healthy manager "
            f"after the kill: worker_b={worker_b_log}"
        )
    if any(
        submit_time > worker_b_start for submit_time in plan.submit_times
    ):
        activations = [
            entry
            for entry in worker_b_log
            if entry[0] == "workflows-active" and entry[1] > 0
        ]
        if not activations:
            violations.append(
                "jobs were scheduled after the replacement worker joined "
                "but it never executed a workflow: "
                f"worker_b={worker_b_log}"
            )

    return violations


def main() -> int:
    """Probe/triage CLI: expand a seed, run it (optionally twice for
    the replay twin), judge it, and print measured wall/virtual rates —
    the numbers the suite's default ceiling is sized from."""
    import argparse
    import time as wall_time

    from .soak_plan import generate_soak_plan

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument(
        "--ceiling",
        type=float,
        default=None,
        help=(
            "REGENERATE the plan at this ceiling (virtual seconds) — "
            "a different schedule than the default-ceiling plan"
        ),
    )
    parser.add_argument(
        "--truncate-ceiling",
        type=float,
        default=None,
        help=(
            "Keep the DEFAULT-ceiling plan (identical events and "
            "submit schedule) but stop the run at this virtual "
            "instant — the cheap way to replay just the head of a "
            "failing long horizon. Invariants are reported for triage "
            "but a truncated run legitimately violates occupancy/"
            "terminal checks for jobs past the cut."
        ),
    )
    parser.add_argument(
        "--twin",
        action="store_true",
        help="Run twice and assert byte-identical replay",
    )
    parser.add_argument("--print-client-log", action="store_true")
    parser.add_argument("--print-manager-log", action="store_true")
    parser.add_argument(
        "--print-worker-logs",
        action="store_true",
        help=(
            "Print worker milestone logs (workflows-active transitions "
            "= the worker-side live-execution windows)"
        ),
    )
    arguments = parser.parse_args()
    if arguments.ceiling is not None and arguments.truncate_ceiling is not None:
        parser.error("--ceiling and --truncate-ceiling are mutually exclusive")

    plan = (
        generate_soak_plan(arguments.seed, ceiling=arguments.ceiling)
        if arguments.ceiling is not None
        else generate_soak_plan(arguments.seed)
    )
    if arguments.truncate_ceiling is not None:
        plan.ceiling = arguments.truncate_ceiling
    print(
        f"soak plan seed={plan.seed} ceiling={plan.ceiling} "
        f"jobs={len(plan.submit_times)} events={len(plan.events)}"
    )
    for event in plan.events or [("no-faults",)]:
        print(f"  {event}")

    run_started = wall_time.monotonic()
    first_results = run_soak_plan(plan)
    first_elapsed = wall_time.monotonic() - run_started
    print(
        f"run 1: {first_elapsed:.1f}s wall for {plan.ceiling:g}s virtual "
        f"({plan.ceiling / first_elapsed:.1f}x real time, "
        f"{first_elapsed / plan.ceiling:.4f} wall-s per virtual-s)"
    )

    violations = check_soak_invariants(plan, first_results)

    client_log = first_results.get(_CLIENT_PROCESS_ID) or []
    for job_index in range(1, len(plan.submit_times) + 1):
        job_log, _ = job_log_slice(client_log, job_index)
        submitted = [e for e in job_log if e[0] == "job-submitted"]
        finished = [e for e in job_log if e[0] == "job-finished"]
        rejections = len([e for e in job_log if e[0] == "submit-rejected"])
        print(
            f"  job {job_index}: submitted={submitted[0][1] if submitted else None} "
            f"finished={finished[0][1:] if finished else None} "
            f"rejections={rejections}"
        )
    if arguments.print_client_log:
        for entry in client_log:
            print(f"  client {entry}")
    if arguments.print_manager_log:
        for entry in first_results.get(_MANAGER_PROCESS_ID) or []:
            print(f"  manager {entry}")
    if arguments.print_worker_logs:
        for process_id in ("worker-a", "worker-b"):
            for entry in first_results.get(process_id) or []:
                print(f"  {process_id} {entry}")

    if arguments.twin:
        twin_started = wall_time.monotonic()
        second_results = run_soak_plan(plan)
        twin_elapsed = wall_time.monotonic() - twin_started
        print(f"run 2 (twin): {twin_elapsed:.1f}s wall")
        if first_results != second_results:
            print("REPLAY DIVERGED")
            return 2

    if violations:
        print("VIOLATIONS:")
        for violation in violations:
            print(f"  {violation}")
        return 1
    print("invariants hold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
