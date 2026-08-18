"""
Chaos execution + invariants: run a generated ``ChaosPlan`` against the
real production stack under the deterministic coordinator, and judge
the run with the E2 SAFETY/LIVENESS split.

Topologies (all existing entries — F3):

* ``l2`` — gateless: ``chaos_manager_entry`` (WAL on, extended
  storage/clock knobs, G3 job milestones), 1-2 ``dag_worker_entry``
  workers (+ late-join replacements per host kill), the sequential
  multi-job ``chaos_multi_job_client_entry``, and a
  ``recovery_dispatch_client_entry`` PROBE submitting after quiesce.
* ``l3`` — three peered ``leader_watch_gate_tier_entry`` gates,
  ``chaos_multi_gate_manager_entry``, one worker, a
  ``multi_gate_load_client_entry`` job through the tier, and a second
  one as the post-quiesce probe.
* ``mdc`` — one ``gate_tier_entry`` fronting two datacenters (each a
  chaos manager + worker), ``soak_gate_dispatch_client_entry`` main +
  probe clients.

WHY POST-RUN CHECKING EQUALS CONTINUOUS CHECKING (G5): every child of
one simulation shares ONE coherent virtual timeline and the run is
byte-reproducible from its seed, so the merged milestone logs ARE the
run's observable history — an invariant violated at any instant is in
the trace at that instant, and judging the complete trace after the
run reaches the verdict an online checker would have reached without
perturbing the schedule under test (an in-run checker would itself
consume virtual time). ``ClusterTraceOracle``'s module docstring
carries the full argument; erased evidence (SIGKILLed children never
report) is handled by passing ``killed_process_ids``.

Run as a module for probe/triage tooling (parameterized, no pytest):

    uv run python -m tests.simulation.vopr_chaos.chaos_runner --seed 7
    uv run python -m tests.simulation.vopr_chaos.chaos_runner \\
        --seed 7 --twin --print-client-log --print-manager-log
    uv run python -m tests.simulation.vopr_chaos.chaos_runner --scan 40
"""

import re

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.chaos_cluster_demo import (
    chaos_manager_entry,
    chaos_multi_gate_manager_entry,
    chaos_multi_job_client_entry,
)
from tests.simulation.harness.sim.multiprocess.gate_cluster_demo import (
    gate_tier_entry,
)
from tests.simulation.harness.sim.multiprocess.gate_fault_client_demo import (
    multi_gate_load_client_entry,
)
from tests.simulation.harness.sim.multiprocess.gate_leader_watch_demo import (
    leader_watch_gate_tier_entry,
)
from tests.simulation.harness.sim.multiprocess.l2_workload_demo import (
    dag_worker_entry,
)
from tests.simulation.harness.sim.multiprocess.recovery_faults_demo import (
    recovery_dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.soak_job_demo import (
    soak_gate_dispatch_client_entry,
)

from hyperscale.distributed.jobs.job_status_order import JobStatusOrder

from tests.simulation.oracle import (
    ClusterTraceOracle,
    JobLogSplitter,
    JobStatusOracle,
)

from .chaos_plan import (
    GATE_PROCESS_IDS,
    L2_WORKER_HOSTS,
    MDC_DATACENTER_IDS,
    TOPOLOGY_L2,
    TOPOLOGY_L3,
    TOPOLOGY_MDC,
    ChaosPlan,
    generate_chaos_plan,
)

_CLIENT_PROCESS_ID = "client"
_PROBE_PROCESS_ID = "probe-client"

_GATE_HOSTS = {
    "gate-a": "sim-gate-a",
    "gate-b": "sim-gate-b",
    "gate-c": "sim-gate-c",
}

# Probe-client wait budget: the probe runs on a FAULT-FREE cluster
# (every chaos window has closed), so its worst path is residual
# recovery — post-reboot dispatch backoff pacing (probed 34.6s) plus
# acceptance retry cycles — far inside this. Expiry is loud and the
# liveness check rejects it for viable plans.
_PROBE_WAIT_TIMEOUT_SECONDS = 100.0

# L3 main-client wait: acceptance lands ~5.5-8s (probed; client-link
# cuts push it to <=~40), the gate AD-34 tracker declares a stranded
# job at submit + 45 + <=15s tick, and a cut completion push converges
# via the poll fallback by heal + ~10 — every leg lands well inside
# 120s from acceptance.
_L3_WAIT_TIMEOUT_SECONDS = 120.0

# MDC main-client wait: the vopr_mdc calibration (dc_loss strand
# resolves as the gate's loud ``timed_out`` at submit + 60 + <=15).
_MDC_WAIT_TIMEOUT_SECONDS = 150.0

# L3 gate-leader stability: the committed vopr_gates tail window.
_LEADER_STABILITY_WINDOW_SECONDS = 30.0

_JOB_PREFIX_PATTERN = re.compile(r"^job(\d+)-(.+)$")

_STATUS_ORDER = JobStatusOrder()

# Allowed milestone vocabulary per process family, prefix-stripped
# (the E2 "no unknown vocabulary" safety invariant — an unrecognized
# tag anywhere is a violation, never silently ignored). The audit tag
# is allowed here because the dedicated audit check owns its failure.
_AUDIT_TAG = "determinism-audit-unswapped"
_MANAGER_TAGS = frozenset(
    {
        "manager-started",
        "manager-start-failed",
        "worker-count",
        "accepted",
        "terminal",
        _AUDIT_TAG,
    }
)
_WORKER_TAGS = frozenset(
    {
        "worker-started",
        "manager-healthy",
        "workflows-active",
        "workflow-started",
        "workflow-executed",
        _AUDIT_TAG,
    }
)
_GATE_TAGS = frozenset(
    {"gate-started", "dc-health", "gate-peers", "gate-leader", _AUDIT_TAG}
)
_MULTI_JOB_CLIENT_TAGS = frozenset(
    {
        "submit-rejected",
        "job-submitted",
        "status-seen",
        "wait-timed-out",
        "job-finished",
        "client-error",
        _AUDIT_TAG,
    }
)
_GATE_CLIENT_TAGS = frozenset(
    {
        "submit-rejected",
        "submit-abandoned",
        "submit-target",
        "job-submitted",
        "status-seen",
        "wait-timeout",
        "wait-timed-out",
        "job-finished",
        "client-error",
        _AUDIT_TAG,
    }
)


def run_chaos_plan(plan: ChaosPlan) -> dict:
    """Execute one generated chaos scenario; returns per-process logs."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=plan.ceiling, seed=plan.seed
    )
    if plan.topology == TOPOLOGY_L2:
        _add_l2_processes(coordinator, plan)
    elif plan.topology == TOPOLOGY_L3:
        _add_l3_processes(coordinator, plan)
    else:
        _add_mdc_processes(coordinator, plan)
    _apply_coordinator_events(coordinator, plan)
    return coordinator.run()


def _storage_fault_schedule_for(plan: ChaosPlan, target: str) -> tuple:
    """The target manager's entry-arg storage schedule (chaos entry
    vocabulary — the plan tuples minus their target element)."""
    return tuple(
        (event[0], *event[2:])
        for event in plan.events
        if event[0]
        in ("slow_disk", "disk_full_window", "read_corruption", "io_error", "misdirect")
        and event[1] == target
    )


def _clock_skew_schedule_for(plan: ChaosPlan, target: str) -> tuple:
    return tuple(
        (event[0], *event[2:])
        for event in plan.events
        if event[0] == "wall_skew" and event[1] == target
    )


def _add_l2_processes(coordinator: SimulationCoordinator, plan: ChaosPlan) -> None:
    coordinator.add_process(
        "manager",
        chaos_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        _storage_fault_schedule_for(plan, "manager"),
        _clock_skew_schedule_for(plan, "manager"),
    )
    for worker_id in plan.initial_worker_ids():
        coordinator.add_process(
            worker_id,
            dag_worker_entry,
            L2_WORKER_HOSTS[worker_id],
            9000,
            9001,
            "sim-dc",
            ("sim-mgr", 9000),
            2,
        )
    for worker_id, start_at in plan.replacement_workers():
        coordinator.add_process(
            worker_id,
            dag_worker_entry,
            L2_WORKER_HOSTS[worker_id],
            9000,
            9001,
            "sim-dc",
            ("sim-mgr", 9000),
            2,
            start_at,
        )
    coordinator.add_process(
        _CLIENT_PROCESS_ID,
        chaos_multi_job_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        plan.submit_times,
        plan.durations,
        plan.job_timeout_seconds,
        plan.wait_timeout_seconds,
    )
    coordinator.add_process(
        _PROBE_PROCESS_ID,
        recovery_dispatch_client_entry,
        "sim-cli-probe",
        9500,
        ("sim-mgr", 9000),
        _PROBE_WAIT_TIMEOUT_SECONDS,
        plan.probe_submit_at,
    )


def _add_l3_processes(coordinator: SimulationCoordinator, plan: ChaosPlan) -> None:
    datacenter_managers = {"dc-1": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"dc-1": [("sim-mgr", 9001)]}
    gate_tcp_addresses = [(host, 9000) for host in _GATE_HOSTS.values()]
    gate_udp_addresses = [(host, 9001) for host in _GATE_HOSTS.values()]

    for gate_process_id, gate_host in _GATE_HOSTS.items():
        peer_hosts = [
            host
            for peer_id, host in _GATE_HOSTS.items()
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
        chaos_multi_gate_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "dc-1",
        gate_tcp_addresses,
        gate_udp_addresses,
        _storage_fault_schedule_for(plan, "manager"),
        _clock_skew_schedule_for(plan, "manager"),
    )
    coordinator.add_process(
        "worker",
        dag_worker_entry,
        "sim-wkr",
        9000,
        9001,
        "dc-1",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        _CLIENT_PROCESS_ID,
        multi_gate_load_client_entry,
        "sim-cli",
        9500,
        gate_tcp_addresses,
        plan.durations[0],
        2,
        plan.job_timeout_seconds,
        _L3_WAIT_TIMEOUT_SECONDS,
    )
    coordinator.add_process(
        _PROBE_PROCESS_ID,
        multi_gate_load_client_entry,
        "sim-cli-probe",
        9500,
        gate_tcp_addresses,
        6.0,
        2,
        plan.job_timeout_seconds,
        _PROBE_WAIT_TIMEOUT_SECONDS,
        plan.probe_submit_at,
        0,
    )


def _add_mdc_processes(coordinator: SimulationCoordinator, plan: ChaosPlan) -> None:
    manager_hosts = {"dc-east": "sim-mgr-east", "dc-west": "sim-mgr-west"}
    worker_hosts = {"dc-east": "sim-wkr-east", "dc-west": "sim-wkr-west"}
    coordinator.add_process(
        "gate",
        gate_tier_entry,
        "sim-gate-a",
        9000,
        9001,
        {
            datacenter_id: [(manager_hosts[datacenter_id], 9000)]
            for datacenter_id in MDC_DATACENTER_IDS
        },
        {
            datacenter_id: [(manager_hosts[datacenter_id], 9001)]
            for datacenter_id in MDC_DATACENTER_IDS
        },
    )
    for datacenter_id in MDC_DATACENTER_IDS:
        coordinator.add_process(
            f"manager-{datacenter_id}",
            chaos_multi_gate_manager_entry,
            manager_hosts[datacenter_id],
            9000,
            9001,
            datacenter_id,
            [("sim-gate-a", 9000)],
            [("sim-gate-a", 9001)],
            _storage_fault_schedule_for(plan, f"manager-{datacenter_id}"),
            _clock_skew_schedule_for(plan, f"manager-{datacenter_id}"),
        )
        coordinator.add_process(
            f"worker-{datacenter_id}",
            dag_worker_entry,
            worker_hosts[datacenter_id],
            9000,
            9001,
            datacenter_id,
            (manager_hosts[datacenter_id], 9000),
            2,
        )
    coordinator.add_process(
        _CLIENT_PROCESS_ID,
        soak_gate_dispatch_client_entry,
        "sim-cli-a",
        9500,
        ("sim-gate-a", 9000),
        plan.durations[0],
        plan.job_timeout_seconds,
        _MDC_WAIT_TIMEOUT_SECONDS,
        0.0,
        None,
    )
    coordinator.add_process(
        _PROBE_PROCESS_ID,
        soak_gate_dispatch_client_entry,
        "sim-cli-probe",
        9500,
        ("sim-gate-a", 9000),
        6.0,
        plan.job_timeout_seconds,
        _PROBE_WAIT_TIMEOUT_SECONDS,
        plan.probe_submit_at,
        None,
    )


def _apply_coordinator_events(
    coordinator: SimulationCoordinator, plan: ChaosPlan
) -> None:
    """Map plan events onto coordinator scheduling. Storage/skew events
    ride the target manager's entry args (armed inside the child)."""
    for event in plan.events:
        kind = event[0]
        if kind == "partition":
            _tag, src, dst, at_time, heal_time, bidirectional = event
            coordinator.schedule_partition(
                src,
                dst,
                at_time,
                heal_time=heal_time,
                bidirectional=bool(bidirectional),
            )
        elif kind == "drop":
            _tag, src, dst, probability, at_time, until_time = event
            coordinator.schedule_drop_rate(
                src, dst, probability, at_time=at_time, until_time=until_time
            )
        elif kind == "delay":
            _tag, src, dst, extra, jitter, at_time, until_time = event
            coordinator.schedule_delay(
                src,
                dst,
                extra,
                at_time=at_time,
                until_time=until_time,
                jitter_seconds=jitter,
            )
        elif kind == "duplicate":
            _tag, src, dst, probability, at_time, until_time = event
            coordinator.schedule_duplicate(
                src, dst, probability, at_time=at_time, until_time=until_time
            )
        elif kind == "corrupt":
            _tag, src, dst, probability, at_time, until_time = event
            coordinator.schedule_corrupt(
                src, dst, probability, at_time=at_time, until_time=until_time
            )
        elif kind == "kill":
            coordinator.schedule_kill(event[1], event[2])
        elif kind == "host_kill":
            _tag, worker_id, at_time, _replacement, _start = event
            coordinator.schedule_kill(worker_id, at_time)
            for executor_id in _executor_ids(plan, worker_id):
                coordinator.schedule_kill(executor_id, at_time)
        elif kind == "worker_restart":
            _tag, worker_id, at_time, down_seconds = event
            for executor_id in _executor_ids(plan, worker_id):
                coordinator.schedule_kill(executor_id, at_time)
            coordinator.schedule_restart(
                worker_id, at_time, down_seconds=down_seconds
            )
        elif kind == "restart":
            _tag, process_id, at_time, down_seconds, fsync_seed = event
            coordinator.schedule_restart(
                process_id,
                at_time,
                down_seconds=down_seconds,
                fsync_reorder_seed=fsync_seed,
            )
        elif kind == "pause":
            _tag, process_id, at_time, resume_time = event
            coordinator.schedule_pause(process_id, at_time, resume_time)
        elif kind == "dc_loss":
            from .chaos_plan import MDC_DC_PROCESS_IDS

            for victim_id in MDC_DC_PROCESS_IDS[event[1]]:
                coordinator.schedule_kill(victim_id, event[2])
        elif kind in (
            "slow_disk",
            "disk_full_window",
            "read_corruption",
            "io_error",
            "misdirect",
            "wall_skew",
        ):
            continue  # armed inside the target child via entry args
        else:
            raise ValueError(f"unknown chaos-plan event kind: {kind!r}")


def _executor_ids(plan: ChaosPlan, worker_id: str) -> tuple[str, str]:
    from .chaos_plan import worker_executor_ids

    return worker_executor_ids(plan.topology, worker_id)


# ----------------------------------------------------------------------
# H1 — the reusable convergence-after-quiesce invariant
# ----------------------------------------------------------------------


def check_convergence_after_quiesce(
    job_streams: dict[str, list[tuple]],
    probe_stream: list[tuple] | None,
    *,
    convergence_deadline: float,
    expect_probe_completed: bool = True,
) -> list[str]:
    """The E2/H1 LIVENESS invariant as a named, reusable check —
    designed for adoption by any suite (values in, violations out; no
    plan coupling).

    ``job_streams`` maps a label to one job's client-observed stream
    in the STANDARD single-job vocabulary (``job-submitted`` /
    ``status-seen`` / ``job-finished`` / ``wait-timed-out`` rows,
    timestamps last) — multi-job logs get here through
    ``JobLogSplitter.split``. For every stream:

    * SUBMITTED exactly once — the pre-quiesce workload actually ran
      (rejection retries before acceptance are legitimate and logged);
    * a client-observed TERMINAL (any of the production terminal
      vocabulary — chaos may legitimately fail/timeout a job; SILENCE
      past the deadline is the violation) observed at
      ``t <= convergence_deadline``.

    ``probe_stream`` is the cluster-is-alive canary submitted after
    quiesce: it must deliver ``job-finished`` by the deadline —
    ``completed`` when ``expect_probe_completed`` (the fault-free-tail
    guarantee), else any terminal (the flagged workerless flavor: a
    worker-less cluster must still declare LOUD timeouts on the AD-34
    grid, and a ``completed`` with no worker alive would be phantom
    execution, caught by the caller's placement checks).

    Budgets must be derived from traced detection bounds (the plan
    docstring's calibration table), never widened to make a seed pass.
    """
    violations: list[str] = []
    for label, stream in sorted(job_streams.items()):
        submitted_rows = [row for row in stream if row[0] == "job-submitted"]
        if len(submitted_rows) != 1:
            violations.append(
                f"{label}: submitted {len(submitted_rows)} times (exactly "
                f"once required): {stream}"
            )
            continue
        terminal_instants = [
            row[-1]
            for row in stream
            if row[0] in ("status-seen", "job-finished")
            and isinstance(row[1], str)
            and _STATUS_ORDER.is_terminal(row[1])
        ]
        if not terminal_instants:
            violations.append(
                f"{label}: no client-observed terminal (silent strand): "
                f"{stream}"
            )
        elif min(terminal_instants) > convergence_deadline:
            violations.append(
                f"{label}: first terminal at {min(terminal_instants):.3f} "
                f"exceeds the convergence deadline {convergence_deadline:.3f}"
            )

    if probe_stream is None:
        return violations
    finished_rows = [row for row in probe_stream if row[0] == "job-finished"]
    if not finished_rows:
        if not expect_probe_completed:
            # WORKERLESS flavor: which loud outcome the probe gets is a
            # race between the last worker's registry reap and the
            # probe's submit instant — accepted-then-loud-AD-34-timeout
            # (reap lost) and a sustained capacity-fence rejection
            # stream to the ceiling (reap won) are BOTH the fence
            # doing its job on a dead cluster. Only SILENCE violates.
            rejection_rows = [
                row for row in probe_stream if row[0] == "submit-rejected"
            ]
            if rejection_rows:
                return violations
        violations.append(
            "probe job submitted after quiesce never delivered a result "
            f"(the cluster-is-alive check): {probe_stream}"
        )
        return violations
    finished_status, finished_at = finished_rows[0][1], finished_rows[0][-1]
    if finished_at > convergence_deadline:
        violations.append(
            f"probe job finished at {finished_at:.3f}, past the convergence "
            f"deadline {convergence_deadline:.3f}"
        )
    if expect_probe_completed and finished_status != "completed":
        violations.append(
            "probe job on a viable post-quiesce cluster must complete, got "
            f"{finished_status!r}: {probe_stream}"
        )
    if not expect_probe_completed and finished_status == "completed":
        violations.append(
            "probe job completed on a worker-less cluster — phantom "
            f"execution: {probe_stream}"
        )
    return violations


# ----------------------------------------------------------------------
# Invariants
# ----------------------------------------------------------------------


def check_chaos_invariants(plan: ChaosPlan, results: dict) -> list[str]:
    """Judge one chaos run; returns human-readable violations.

    SAFETY (whole run, any density — E2/E3): determinism-audit
    absence; allowed milestone vocabulary everywhere; no client-error
    rows; per-job client-history linearization (``JobLogSplitter`` +
    ``JobStatusOracle``); the ``ClusterTraceOracle`` cross-node set
    (leader exclusivity, execution accounting, single-DC placement,
    health convergence where expected); manager-side acceptance
    accounting and index-aligned manager/client terminal agreement.

    LIVENESS (post-quiesce only — E2/E4/H1): every pre-chaos job
    reaches a client-observed terminal and the probe job completes by
    ``chaos_end + convergence_budget`` (``check_convergence_after_
    quiesce``); the manager's final worker registry equals the live
    topology; surviving workers end drained (workflows-active 0 —
    zombie execution shows here); L3 adds gate-leader convergence +
    stability. The flagged flavors invert liveness: ``doomed`` (J2)
    demands loud rejection/wait-timeout evidence instead of terminals,
    ``workerless`` (K6) demands loud AD-34 terminals and forbids
    phantom completion.
    """
    violations = list(ClusterTraceOracle.check_determinism_audit_absence(results))
    violations.extend(_check_vocabulary(plan, results))
    violations.extend(_check_client_errors(results))
    violations.extend(_check_client_histories(plan, results))
    violations.extend(_check_cluster_trace(plan, results))
    violations.extend(_check_manager_job_accounting(plan, results))
    violations.extend(_check_liveness(plan, results))
    return violations


def _base_process_id(results_key: str) -> str:
    return results_key.split(".gen", 1)[0]


def _allowed_tags_for(plan: ChaosPlan, process_id: str) -> frozenset | None:
    if process_id.startswith("executor-"):
        return None  # executor children return no milestone log
    if process_id.startswith("manager"):
        return _MANAGER_TAGS
    if process_id.startswith("worker"):
        return _WORKER_TAGS
    if process_id.startswith("gate"):
        return _GATE_TAGS
    if process_id == _CLIENT_PROCESS_ID and plan.topology == TOPOLOGY_L2:
        return _MULTI_JOB_CLIENT_TAGS
    if plan.topology == TOPOLOGY_L2 and process_id == _PROBE_PROCESS_ID:
        return _GATE_CLIENT_TAGS  # recovery client vocabulary is a subset
    return _GATE_CLIENT_TAGS


def _check_vocabulary(plan: ChaosPlan, results: dict) -> list[str]:
    """No unknown milestone vocabulary anywhere (prefix-stripped)."""
    violations: list[str] = []
    for results_key, process_log in results.items():
        if not isinstance(process_log, list):
            continue
        allowed_tags = _allowed_tags_for(plan, _base_process_id(results_key))
        if allowed_tags is None:
            continue
        for row in process_log:
            if not isinstance(row, tuple) or not row or not isinstance(row[0], str):
                violations.append(
                    f"{results_key}: unjudgeable milestone row {row!r}"
                )
                continue
            base_tag = row[0]
            if prefixed := _JOB_PREFIX_PATTERN.match(base_tag):
                base_tag = prefixed.group(2)
            if base_tag not in allowed_tags:
                violations.append(
                    f"{results_key}: unknown milestone tag {row[0]!r} — "
                    "vocabulary is closed, never silently extended"
                )
    return violations


def _check_client_errors(results: dict) -> list[str]:
    return [
        f"{results_key}: client flow raised unexpectedly: {row}"
        for results_key, process_log in results.items()
        if isinstance(process_log, list)
        for row in process_log
        if row and row[0] == "client-error"
    ]


def _check_client_histories(plan: ChaosPlan, results: dict) -> list[str]:
    """Per-job linearization for every client stream (G1 under chaos)."""
    violations: list[str] = []
    status_oracle = JobStatusOracle()
    client_log = results.get(_CLIENT_PROCESS_ID) or []
    if plan.topology == TOPOLOGY_L2:
        violations.extend(
            f"client {violation}"
            for violation in JobLogSplitter(status_oracle).split_and_check(
                client_log
            )
        )
    else:
        violations.extend(
            f"client oracle: {violation}"
            for violation in status_oracle.check_client_log(client_log)
        )
    probe_log = results.get(_PROBE_PROCESS_ID) or []
    violations.extend(
        f"probe oracle: {violation}"
        for violation in status_oracle.check_client_log(probe_log)
    )
    return violations


def _trace_oracle_for(plan: ChaosPlan) -> ClusterTraceOracle:
    if plan.topology == TOPOLOGY_L2:
        return ClusterTraceOracle(
            client_process_id=_PROBE_PROCESS_ID,
            manager_process_ids=("manager",),
            worker_process_ids_by_datacenter={"sim-dc": plan.worker_ids()},
        )
    if plan.topology == TOPOLOGY_L3:
        return ClusterTraceOracle(
            client_process_id=_CLIENT_PROCESS_ID,
            gate_process_ids=GATE_PROCESS_IDS,
            manager_process_ids=("manager",),
            worker_process_ids_by_datacenter={"dc-1": ("worker",)},
            leader_stability_window_seconds=_LEADER_STABILITY_WINDOW_SECONDS,
        )
    return ClusterTraceOracle(
        client_process_id=_CLIENT_PROCESS_ID,
        gate_process_ids=("gate",),
        manager_process_ids=tuple(
            f"manager-{datacenter_id}" for datacenter_id in MDC_DATACENTER_IDS
        ),
        worker_process_ids_by_datacenter={
            datacenter_id: (f"worker-{datacenter_id}",)
            for datacenter_id in MDC_DATACENTER_IDS
        },
    )


def _check_cluster_trace(plan: ChaosPlan, results: dict) -> list[str]:
    """The G3 cross-node composite, configured per topology. MDC
    single-DC placement is judged PER JOB by splitting worker evidence
    at the probe-submit instant (main-job execution provably drains
    before it: dispatch, restart-resume and pause-thaw legs all end by
    chaos_end, and the probe starts 12s later)."""
    oracle = _trace_oracle_for(plan)
    killed = plan.killed_process_ids()
    violations = list(
        oracle.check_gate_leader_exclusivity(results, killed, plan.ceiling)
    )
    violations.extend(oracle.check_terminal_agreement(results, killed))
    violations.extend(oracle.check_workflow_execution(results, killed))
    if plan.topology == TOPOLOGY_L3:
        if not plan.is_doomed():
            violations.extend(
                oracle.check_datacenter_health_convergence(
                    results, plan.expected_health_by_datacenter(), killed
                )
            )
        violations.extend(
            oracle.check_gate_leader_convergence(results, plan.ceiling, killed)
        )
    if plan.topology == TOPOLOGY_MDC:
        violations.extend(
            oracle.check_datacenter_health_convergence(
                results, plan.expected_health_by_datacenter(), killed
            )
        )
        for window_label, in_window in (
            ("main job", lambda instant: instant <= plan.probe_submit_at),
            ("probe job", lambda instant: instant > plan.probe_submit_at),
        ):
            windowed_results = {
                results_key: [
                    row
                    for row in process_log
                    if isinstance(row[-1], (int, float)) and in_window(row[-1])
                ]
                for results_key, process_log in results.items()
                if isinstance(process_log, list)
                and _base_process_id(results_key).startswith("worker")
            }
            violations.extend(
                f"{window_label}: {violation}"
                for violation in oracle.check_single_datacenter_placement(
                    windowed_results
                )
            )
    return violations


def _manager_process_ids(plan: ChaosPlan) -> tuple[str, ...]:
    if plan.topology == TOPOLOGY_MDC:
        return tuple(
            f"manager-{datacenter_id}" for datacenter_id in MDC_DATACENTER_IDS
        )
    return ("manager",)


def _client_acceptance_instants(results: dict) -> list[float]:
    """Every client-observed acceptance instant (both clients, both
    tag conventions), ascending — the alignment axis for the
    index-keyed manager evidence."""
    acceptance_instants: list[float] = []
    for client_id in (_CLIENT_PROCESS_ID, _PROBE_PROCESS_ID):
        for row in results.get(client_id) or []:
            tag = row[0]
            if prefixed := _JOB_PREFIX_PATTERN.match(tag):
                tag = prefixed.group(2)
            if tag == "job-submitted":
                acceptance_instants.append(row[-1])
    return sorted(acceptance_instants)


def _client_terminal_by_acceptance_order(results: dict) -> list[str | None]:
    """Client-side terminal statuses ordered by acceptance instant."""
    jobs: list[tuple[float, str | None]] = []
    splitter = JobLogSplitter()
    for client_id in (_CLIENT_PROCESS_ID, _PROBE_PROCESS_ID):
        for stream in splitter.split(results.get(client_id) or []).values():
            submitted = [row for row in stream if row[0] == "job-submitted"]
            if not submitted:
                continue
            finished = [row for row in stream if row[0] == "job-finished"]
            terminal_status = finished[0][1] if finished else None
            jobs.append((submitted[0][-1], terminal_status))
    return [status for _instant, status in sorted(jobs)]


def _check_manager_job_accounting(plan: ChaosPlan, results: dict) -> list[str]:
    """The manager-side G3 slice (values only, index-keyed):

    * the LIVE manager generation must know AT LEAST as many accepted
      jobs as the clients observed accepted — fewer means the durable
      job table LOST an acceptance (a WAL/recovery integrity hole);
      more is legitimate under client-link faults (a lost ACCEPT
      reply orphans a manager-side job — the documented L4 delta);
    * when the counts agree exactly, manager job k IS the k-th
      client acceptance (one sequential client + a later probe), so
      their terminal records must AGREE (timeout spellings
      normalized) wherever both sides produced terminal evidence.

    Skipped for doomed plans (the killed manager's log is erased) and
    judged per-manager in MDC (each DC's manager sees only its own
    placements, so only the >= direction is meaningful there).
    """
    if plan.is_doomed():
        return []
    violations: list[str] = []
    acceptance_instants = _client_acceptance_instants(results)
    client_terminals = _client_terminal_by_acceptance_order(results)

    if plan.topology == TOPOLOGY_MDC:
        any_manager_killed = any(
            manager_id in plan.killed_process_ids()
            for manager_id in _manager_process_ids(plan)
        )
        total_manager_accepted = 0
        for manager_id in _manager_process_ids(plan):
            if manager_id in plan.killed_process_ids():
                continue
            manager_log = results.get(manager_id) or []
            total_manager_accepted += len(
                [row for row in manager_log if row[0].endswith("-accepted")]
            )
        if any_manager_killed:
            # A SIGKILLed child returns NO result rows, so a job placed
            # on the killed DC is accounting-invisible even though it
            # was durably accepted and may have COMPLETED before the
            # kill (measured, seed 2: the client's job completed at
            # 11.52 on dc-east, dc_loss at 76.8 erased dc-east's log,
            # and the equality form false-positived "acceptance LOST"
            # while both jobs in the run finished successfully). With a
            # dead manager the client-observed terminal guarantee — the
            # convergence checker's job — is the surviving invariant;
            # here only the PHANTOM direction stays assertable:
            # survivors must never know MORE acceptances than clients
            # observed.
            if total_manager_accepted > len(acceptance_instants):
                violations.append(
                    f"surviving managers know {total_manager_accepted} "
                    f"accepted jobs but clients observed only "
                    f"{len(acceptance_instants)} acceptances — phantom "
                    "manager-side acceptance"
                )
            return violations
        if total_manager_accepted < len(acceptance_instants):
            violations.append(
                f"managers know {total_manager_accepted} accepted jobs but "
                f"clients observed {len(acceptance_instants)} acceptances — "
                "a durable acceptance was LOST"
            )
        return violations

    manager_log = results.get("manager") or []
    accepted_rows = [
        row for row in manager_log if _JOB_PREFIX_PATTERN.match(row[0])
        and row[0].endswith("-accepted")
    ]
    if len(accepted_rows) < len(acceptance_instants):
        violations.append(
            f"manager's live generation knows {len(accepted_rows)} accepted "
            f"jobs but clients observed {len(acceptance_instants)} "
            "acceptances — a durable acceptance was LOST"
        )
        return violations
    if len(accepted_rows) != len(acceptance_instants):
        return violations  # orphan acceptance: alignment erased (L4 delta)

    manager_terminals: dict[int, str] = {}
    for row in manager_log:
        if (matched := _JOB_PREFIX_PATTERN.match(row[0])) and matched.group(
            2
        ) == "terminal":
            manager_terminals.setdefault(int(matched.group(1)), row[1])
    normalize = {"timed_out": "timeout"}
    for job_position, client_terminal in enumerate(client_terminals, 1):
        manager_terminal = manager_terminals.get(job_position)
        if manager_terminal is None or client_terminal is None:
            continue  # absent evidence is tolerated, contradiction is not
        if normalize.get(manager_terminal, manager_terminal) != normalize.get(
            client_terminal, client_terminal
        ):
            violations.append(
                f"manager recorded job {job_position} terminal "
                f"{manager_terminal!r} while the client observed "
                f"{client_terminal!r} — cross-node terminal disagreement"
            )
    return violations


def _check_liveness(plan: ChaosPlan, results: dict) -> list[str]:
    """The post-quiesce liveness set, branched per plan flavor."""
    if plan.is_doomed():
        return _check_doomed_loudness(plan, results)

    violations: list[str] = []
    client_log = results.get(_CLIENT_PROCESS_ID) or []
    probe_log = results.get(_PROBE_PROCESS_ID) or []
    if plan.topology == TOPOLOGY_L2:
        job_streams = {
            f"job {job_index}": stream
            for job_index, stream in JobLogSplitter().split(client_log).items()
            if job_index > 0
        }
    else:
        job_streams = {"job 1": client_log}
    violations.extend(
        check_convergence_after_quiesce(
            job_streams,
            probe_log,
            convergence_deadline=plan.convergence_deadline(),
            expect_probe_completed=not plan.is_workerless(),
        )
    )
    violations.extend(_check_membership_convergence(plan, results))
    violations.extend(_check_worker_drain(plan, results))
    return violations


def _check_membership_convergence(plan: ChaosPlan, results: dict) -> list[str]:
    """E4: every surviving manager's FINAL worker registry equals the
    live worker topology (evicted-then-healed workers re-admitted,
    dead ones reaped, late joiners registered)."""
    violations: list[str] = []
    if plan.topology == TOPOLOGY_L2:
        expected_by_manager = {"manager": len(plan.live_worker_ids_at_end())}
    elif plan.topology == TOPOLOGY_L3:
        expected_by_manager = {"manager": 1}
    else:
        lost = plan.lost_datacenters()
        expected_by_manager = {
            f"manager-{datacenter_id}": 1
            for datacenter_id in MDC_DATACENTER_IDS
            if datacenter_id not in lost
        }
    for manager_id, expected_count in expected_by_manager.items():
        manager_log = results.get(manager_id) or []
        count_rows = [row for row in manager_log if row[0] == "worker-count"]
        if not count_rows or count_rows[-1][1] != expected_count:
            violations.append(
                f"{manager_id}: final worker registry is "
                f"{count_rows[-1][1] if count_rows else None}, expected "
                f"{expected_count} (live topology): {count_rows}"
            )
    return violations


def _check_worker_drain(plan: ChaosPlan, results: dict) -> list[str]:
    """Every surviving worker ends DRAINED (last workflows-active 0):
    execution lingering past the last terminal is zombie work."""
    violations: list[str] = []
    killed = set(plan.killed_process_ids())
    for worker_id in plan.worker_ids():
        if worker_id in killed:
            continue
        worker_log = results.get(worker_id) or []
        active_rows = [row for row in worker_log if row[0] == "workflows-active"]
        if active_rows and active_rows[-1][1] != 0:
            violations.append(
                f"{worker_id}: still executing at the ceiling "
                f"(workflows-active {active_rows[-1][1]}) — zombie work"
            )
    return violations


def _check_doomed_loudness(plan: ChaosPlan, results: dict) -> list[str]:
    """J2 inverted liveness: with the manager permanently dead, every
    outcome must be LOUD — jobs end in a terminal or a logged
    wait-timeout; a job that never submitted must be explained by a
    blocked queue (its predecessor stuck loudly) or logged rejections;
    the probe must be rejected, never silently absent."""
    violations: list[str] = []
    client_log = results.get(_CLIENT_PROCESS_ID) or []
    if plan.topology == TOPOLOGY_L2:
        streams = JobLogSplitter().split(client_log)
        previous_stream: list[tuple] = []
        for job_index in range(1, len(plan.submit_times) + 1):
            stream = streams.get(job_index, [])
            if any(row[0] == "job-submitted" for row in stream):
                loud = any(
                    row[0] in ("job-finished", "wait-timed-out")
                    or (
                        row[0] == "status-seen"
                        and isinstance(row[1], str)
                        and _STATUS_ORDER.is_terminal(row[1])
                    )
                    for row in stream
                )
                if not loud:
                    violations.append(
                        f"doomed plan: job {job_index} was accepted but left "
                        f"NO loud outcome (terminal or wait-timeout): {stream}"
                    )
            else:
                predecessor_blocked = any(
                    row[0] == "wait-timed-out" for row in previous_stream
                ) and not any(
                    row[0] == "job-finished" for row in previous_stream
                )
                rejected = any(row[0] == "submit-rejected" for row in stream)
                if not predecessor_blocked and not rejected:
                    violations.append(
                        f"doomed plan: job {job_index} silently never "
                        f"submitted (no blocked predecessor, no rejections)"
                    )
            previous_stream = stream
    else:
        violations.extend(
            check_convergence_after_quiesce(
                {"job 1": client_log},
                None,
                convergence_deadline=plan.convergence_deadline(),
            )
        )

    probe_log = results.get(_PROBE_PROCESS_ID) or []
    if any(row[0] == "job-submitted" for row in probe_log):
        finished = [row for row in probe_log if row[0] == "job-finished"]
        if finished and finished[0][1] == "completed":
            violations.append(
                "doomed plan: probe job COMPLETED against a permanently "
                f"dead manager: {probe_log}"
            )
    elif not any(
        row[0] in ("submit-rejected", "submit-abandoned") for row in probe_log
    ):
        violations.append(
            "doomed plan: probe left no loud evidence (no acceptance, no "
            f"rejections): {probe_log}"
        )
    return violations


# ----------------------------------------------------------------------
# Probe / triage CLI
# ----------------------------------------------------------------------


def main() -> int:
    """Probe/triage CLI (soak_runner's shape): expand a seed, run it
    (optionally twice for the replay twin), judge it, and print wall
    economics; ``--scan`` prints plan summaries for a seed range
    (pure expansion, no cluster)."""
    import argparse
    import time as wall_time

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--scan",
        type=int,
        default=None,
        help="Print plan summaries for seeds [--scan-base, +N) and exit",
    )
    parser.add_argument("--scan-base", type=int, default=1)
    parser.add_argument(
        "--truncate-ceiling",
        type=float,
        default=None,
        help=(
            "Keep the generated plan but stop the run at this virtual "
            "instant (cheap head-replay of a failing seed; truncated "
            "runs legitimately violate liveness checks past the cut)"
        ),
    )
    parser.add_argument("--twin", action="store_true")
    parser.add_argument("--print-client-log", action="store_true")
    parser.add_argument("--print-probe-log", action="store_true")
    parser.add_argument("--print-manager-log", action="store_true")
    parser.add_argument("--print-worker-logs", action="store_true")
    parser.add_argument("--print-gate-logs", action="store_true")
    arguments = parser.parse_args()

    if arguments.scan is not None:
        for seed in range(arguments.scan_base, arguments.scan_base + arguments.scan):
            plan = generate_chaos_plan(seed)
            flavor = (
                "doomed"
                if plan.is_doomed()
                else "workerless"
                if plan.is_workerless()
                else "viable"
            )
            print(
                f"seed {seed}: {plan.topology} {flavor} "
                f"jobs={len(plan.submit_times)} events={len(plan.events)} "
                f"kinds={sorted({event[0] for event in plan.events})}"
            )
        return 0

    if arguments.seed is None:
        parser.error("--seed or --scan is required")
    plan = generate_chaos_plan(arguments.seed)
    if arguments.truncate_ceiling is not None:
        plan.ceiling = arguments.truncate_ceiling
    print(
        f"chaos plan seed={plan.seed} topology={plan.topology} "
        f"ceiling={plan.ceiling:g} chaos_end={plan.chaos_end:g} "
        f"deadline={plan.convergence_deadline():g} "
        f"jobs={len(plan.submit_times)} doomed={plan.is_doomed()} "
        f"workerless={plan.is_workerless()}"
    )
    for event in plan.events or [("no-faults",)]:
        print(f"  {event}")

    run_started = wall_time.monotonic()
    first_results = run_chaos_plan(plan)
    first_elapsed = wall_time.monotonic() - run_started
    print(
        f"run 1: {first_elapsed:.1f}s wall for {plan.ceiling:g}s virtual "
        f"({plan.ceiling / first_elapsed:.1f}x, "
        f"{first_elapsed / plan.ceiling:.4f} wall-s per virtual-s)"
    )
    violations = check_chaos_invariants(plan, first_results)

    if arguments.print_client_log:
        for row in first_results.get(_CLIENT_PROCESS_ID) or []:
            print(f"  client {row}")
    if arguments.print_probe_log:
        for row in first_results.get(_PROBE_PROCESS_ID) or []:
            print(f"  probe {row}")
    if arguments.print_manager_log:
        for results_key in sorted(first_results):
            if _base_process_id(results_key).startswith("manager"):
                for row in first_results.get(results_key) or []:
                    print(f"  {results_key} {row}")
    if arguments.print_worker_logs:
        for results_key in sorted(first_results):
            if _base_process_id(results_key).startswith("worker"):
                for row in first_results.get(results_key) or []:
                    print(f"  {results_key} {row}")
    if arguments.print_gate_logs:
        for results_key in sorted(first_results):
            if _base_process_id(results_key).startswith("gate"):
                for row in first_results.get(results_key) or []:
                    print(f"  {results_key} {row}")

    if arguments.twin:
        twin_started = wall_time.monotonic()
        second_results = run_chaos_plan(plan)
        print(f"run 2 (twin): {wall_time.monotonic() - twin_started:.1f}s wall")
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
