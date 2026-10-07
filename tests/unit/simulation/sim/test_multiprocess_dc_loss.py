"""
Pinned EXTREME multi-DC scenarios: total-datacenter loss, gate<->DC
partitions, manager power-loss restart, and per-DC storage faults over
the L3 two-datacenter topology — every schedule probe-measured first,
then pinned with DESIGN-BOUND assertions, and every scenario replayed
byte-identically.

Topology (10-11 real OS processes): one ``GateServer`` fronting dc-east
and dc-west, each datacenter a real ``ManagerServer`` (WAL on) plus a
2-core ``WorkerServer`` (2 executor children), and 1-2 clients
submitting a parameterizable-duration ``SimSoakWorkflow`` through the
gate so faults land INSIDE live execution.

Probed mechanism constants these scenarios assert around (seed 61
anchors; see the per-scenario docstrings for the measured timelines):

* The gate classifies a datacenter from MANAGER-HEARTBEAT STALENESS
  (30s timeout, 10s heartbeat period), not SWIM death: a killed DC
  flips ``unhealthy`` at kill + [20, 31] (staleness minus up to one
  heartbeat period, plus the 0.5s health sampler).
* A job stranded on a dead DC ends via the gate's AD-34 global-timeout
  tracker: a LOUD client-observed ``timeout`` at
  submit + job_timeout(60) + up to one 15s tracker tick.
* A job confined to the datacenter it loses (its placement constraint
  lists that datacenter alone) has nowhere to fail over to (AD-36 Part
  13 moves a lost datacenter's share only within the job's placement):
  the surviving DC never executes the stranded job.
* The manager's completion notification to the gate
  (``_notify_gate_of_completion``) is a SINGLE 5s-timeout send and the
  manager cleans the job up even when it fails — so a gate<->manager
  partition covering that send loses the completion and the job
  resolves as gate ``timeout`` despite having run. The gate->CLIENT
  direction is redelivery-protected (terminal re-lands < 1s after a
  client-link heal) — the asymmetry is deliberate to pin.
"""


import functools
from collections.abc import Callable

from hyperscale.distributed.env import Env
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

_SEED = 61
# Coordinator link latency, one way: the earliest instant any reaction to
# an observed row can take effect.
_LATENCY = 0.01
_DURATION_SECONDS = 8.0
_JOB_TIMEOUT_SECONDS = 60.0

# The gate's DC-death classification: a phi-accrual detector over each
# manager's heartbeats (AD-52 section 8). Design bound: never before the
# heartbeat pause it tolerates, always inside the 30s fixed staleness
# window it replaced (measured 12.0-16.25s after a loss, seed 61).
_DEATH_CLASSIFY_MIN = Env().PHI_ACCRUAL_ACCEPTABLE_HEARTBEAT_PAUSE_SECONDS
_DEATH_CLASSIFY_MAX = 30.0

# AD-34 global-timeout delivery: job_timeout after submission plus up
# to one 15s tracker check tick plus push/poll latency.
_TRACKER_TICK_SECONDS = 15.0
_TERMINAL_SLACK_SECONDS = 1.5

_DC_VICTIMS = {
    "dc-east": (
        "manager-dc-east",
        "worker-dc-east",
        "executor-sim-wkr-east-9009",
        "executor-sim-wkr-east-9011",
    ),
    "dc-west": (
        "manager-dc-west",
        "worker-dc-west",
        "executor-sim-wkr-west-9009",
        "executor-sim-wkr-west-9011",
    ),
}


# Every scenario faults dc-west (dc-east is the healthy standby), so the
# job under fault is pinned there (the post-quiesce job keeps free
# selection -- routing around the dead DC is what it tests): unpinned, the router scores the two DCs on the
# health each gate has ingested by submission time, and which one wins
# is an accident of warm-up timing, not part of any scenario.
_JOB_PLACEMENT = ["dc-west"]


def _add_multi_dc_topology(
    coordinator: SimulationCoordinator,
    storage_schedule_by_dc: dict[str, tuple] | None = None,
) -> None:
    """The canonical L3 pair: one gate, two single-manager DCs with
    2-core workers. Storage fault schedules ride the manager entries."""
    storage_schedules = storage_schedule_by_dc or {}
    coordinator.add_process(
        "sim-gate-a",
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
    for datacenter_id, manager_host, worker_host in (
        ("dc-east", "sim-mgr-east", "sim-wkr-east"),
        ("dc-west", "sim-mgr-west", "sim-wkr-west"),
    ):
        coordinator.add_process(
            f"manager-{datacenter_id}",
            faulted_multi_gate_manager_entry,
            manager_host,
            9000,
            9001,
            datacenter_id,
            [("sim-gate-a", 9000)],
            [("sim-gate-a", 9001)],
            storage_schedules.get(datacenter_id, ()),
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


def _add_soak_client(
    coordinator: SimulationCoordinator,
    process_id: str,
    host: str,
    wait_timeout_seconds: float,
    submit_at: float = 0.0,
    pinned_datacenters: list[str] | None = None,
) -> None:
    coordinator.add_process(
        process_id,
        soak_gate_dispatch_client_entry,
        host,
        9500,
        ("sim-gate-a", 9000),
        _DURATION_SECONDS,
        _JOB_TIMEOUT_SECONDS,
        wait_timeout_seconds,
        submit_at,
        pinned_datacenters,
    )


def _assert_no_unswapped_imports(results: dict) -> None:
    """The determinism audit must be clean in EVERY child (all
    generations): an unswapped deferred import silently forks the
    schedule."""
    for process_id, process_log in results.items():
        audit_entries = [
            entry
            for entry in (process_log or [])
            if isinstance(entry, tuple)
            and entry[:1] == ("determinism-audit-unswapped",)
        ]
        assert not audit_entries, (process_id, audit_entries)


def _assert_oracle_clean(client_log: list) -> None:
    violations = JobStatusOracle().check_client_log(client_log)
    assert not violations, (violations, client_log)


def _submitted_at(client_log: list) -> float:
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted, client_log
    return submitted[0][1]


def _finished(client_log: list) -> tuple:
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    return finished[0]


def _active_rise_times(worker_log: list) -> list[float]:
    return [
        entry[2]
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] > 0
    ]


def _health_times(gate_log: list, datacenter_id: str, health: str) -> list[float]:
    return [
        entry[3]
        for entry in gate_log
        if entry[0] == "dc-health"
        and entry[1] == datacenter_id
        and entry[2] == health
    ]


def _final_health(gate_log: list) -> dict[str, str]:
    return {entry[1]: entry[2] for entry in gate_log if entry[0] == "dc-health"}


def _is_running_status(row: tuple) -> bool:
    """The client's first observation of its job ``running``."""
    return row[:2] == ("status-seen", "running")


def _schedule_on_client_running(
    coordinator: SimulationCoordinator,
    fault_instants: dict[str, float],
    schedule_fault: Callable[[float], None],
) -> None:
    """Fault the run one coordinator latency after the client first
    observes its job ``running`` -- inside live execution on dc-west,
    whatever instant the seed's schedule puts dispatch at (a pinned
    instant stops landing inside execution the moment the schedule
    moves). ``schedule_fault(fault_at)`` arms the scenario's faults;
    the instant is recorded as ``fault_instants["fault_at"]``."""

    def fault_once_running(running_row: tuple) -> None:
        fault_at = running_row[2] + _LATENCY
        fault_instants["fault_at"] = fault_at
        schedule_fault(fault_at)

    coordinator.schedule_on_event("client-a", _is_running_status, fault_once_running)


def _schedule_west_loss_once_running(
    coordinator: SimulationCoordinator, fault_instants: dict[str, float]
) -> None:
    """Kill all of dc-west (manager, worker, both executors) once the
    client has observed its job running there."""

    def kill_west(loss_at: float) -> None:
        for victim_id in _DC_VICTIMS["dc-west"]:
            coordinator.schedule_kill(victim_id, loss_at)

    _schedule_on_client_running(coordinator, fault_instants, kill_west)


# ---------------------------------------------------------------------------
# Scenario 1: total loss of the job's OWN datacenter mid-execution
# ---------------------------------------------------------------------------

_LOSS_CEILING = 140.0


def _run_stranded_dc_loss() -> tuple[dict, dict[str, float]]:
    """Kill dc-west (manager + worker + both executors) one latency after
    the client first observes its job running there -- INSIDE live
    execution: the job is placed in dc-west and the workflow (an action
    hook: about a second whatever its duration) is still executing.
    Returns the results and the kill instant (``fault_at``).

    Original probe (when the kill was a pinned instant): submit
    2.12, dispatch 9.5, kill 9.8, gate flips dc-west unhealthy 38.5,
    client observes terminal ``timeout`` 77.01 (= submit + 60s job
    timeout + one 15s AD-34 tick - just-missed check at 62.0)."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0, pinned_datacenters=_JOB_PLACEMENT)
    fault_instants: dict[str, float] = {}
    _schedule_west_loss_once_running(coordinator, fault_instants)
    return coordinator.run(), fault_instants


def test_dc_loss_strands_job_to_loud_gate_timeout():
    """A job confined to a datacenter that dies mid-execution must end
    LOUDLY at the client — the gate's AD-34 global timeout — and the
    surviving datacenter must NOT execute it: the job's placement
    constraint lists dc-west alone, so AD-36's mid-flight failover finds
    no datacenter to move its share to and asks again every check until
    the timeout ends the job."""
    results, fault_instants = _run_stranded_dc_loss()
    loss_at = fault_instants["fault_at"]
    client_log = results["client-a"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < loss_at, client_log

    # The kill landed inside live execution: the client had already
    # observed ``running`` (the kill's trigger). The worker's own log is
    # unavailable — SIGKILLed children never report their milestones.
    running_times = [
        entry[2]
        for entry in client_log
        if entry[0] == "status-seen" and entry[1] == "running"
    ]
    assert running_times and running_times[0] < loss_at, client_log

    # Loud terminal, correctly bounded: never before the job timeout,
    # never later than one full tracker tick past it.
    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "timeout", client_log
    earliest = submitted_time + _JOB_TIMEOUT_SECONDS
    latest = earliest + _TRACKER_TICK_SECONDS + _TERMINAL_SLACK_SECONDS
    assert earliest <= finished_time <= latest, (
        f"stranded-job terminal at {finished_time} outside the AD-34 "
        f"design bound [{earliest}, {latest}]: {client_log}"
    )

    # Outside the job's placement: the surviving datacenter never ran it.
    assert not _active_rise_times(results["worker-dc-east"]), results[
        "worker-dc-east"
    ]

    # Death classification via heartbeat staleness, inside its bound.
    gate_log = results["sim-gate-a"]
    west_unhealthy_times = _health_times(gate_log, "dc-west", "unhealthy")
    assert west_unhealthy_times, gate_log
    detection_latency = west_unhealthy_times[-1] - loss_at
    assert _DEATH_CLASSIFY_MIN <= detection_latency <= _DEATH_CLASSIFY_MAX, (
        f"dead-DC classification latency {detection_latency}s outside "
        "the heartbeat-staleness design bound (30s timeout - up to one "
        f"10s heartbeat period + sampler): {gate_log}"
    )
    assert _final_health(gate_log) == {
        "dc-east": "healthy",
        "dc-west": "unhealthy",
    }, gate_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_dc_loss_strand_is_replay_deterministic():
    assert _run_stranded_dc_loss() == _run_stranded_dc_loss()


# ---------------------------------------------------------------------------
# Scenario 2: manager power-loss restart inside one DC, mid-execution
# ---------------------------------------------------------------------------

_RESTART_DOWN_SECONDS = 30.0
_RESTART_CEILING = 160.0


def _run_manager_restart_mid_execution() -> tuple[dict, dict[str, float]]:
    """Power-lose dc-west's manager one latency after the client first
    observes its job running (the workflow is running on its worker) and
    reboot it from the surviving durable disk 30s later. Returns the
    results and the power-loss instant (``fault_at``). The timeline
    below is the original (pinned-instant) probe's.

    Measured timeline: dispatch 9.5, restart 9.8, first worker
    execution cycle ends 34.0 (its result had no live manager), gen-2
    boots 39.82, worker re-registered 40.32, resume RE-dispatches
    (second execution cycle 40.25-42.25), client observes completion
    41.27 — exactly-once at the client across at-least-once execution.
    dc-west dips unhealthy at 38.5 (staleness bound) and returns
    healthy at 42.5 on gen-2 heartbeats."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_RESTART_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 140.0, pinned_datacenters=_JOB_PLACEMENT)
    fault_instants: dict[str, float] = {}
    _schedule_on_client_running(
        coordinator,
        fault_instants,
        lambda restart_at: coordinator.schedule_restart(
            "manager-dc-west", restart_at, down_seconds=_RESTART_DOWN_SECONDS
        ),
    )
    return coordinator.run(), fault_instants


def test_job_completes_exactly_once_across_manager_restart_in_l3():
    """Manager durable resume must hold in the L3 topology: the job
    submitted through the GATE completes across the mid-execution
    manager reboot, exactly once at the client, and BEFORE the AD-34
    job timeout would have fired."""
    results, fault_instants = _run_manager_restart_mid_execution()
    restart_at = fault_instants["fault_at"]
    client_log = results["client-a"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < restart_at, client_log

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    generation_two_up = restart_at + _RESTART_DOWN_SECONDS
    assert generation_two_up < finished_time < (
        submitted_time + _JOB_TIMEOUT_SECONDS
    ), (
        "completion must come from the RESUMED generation and beat the "
        f"AD-34 timeout: {client_log}"
    )

    # At-least-once execution across the reboot, all inside dc-west:
    # the pre-restart run (its result died with gen-1) plus the resumed
    # re-dispatch.
    west_rises = _active_rise_times(results["worker-dc-west"])
    assert len(west_rises) >= 2, results["worker-dc-west"]
    assert west_rises[0] < restart_at < west_rises[-1], results[
        "worker-dc-west"
    ]
    assert not _active_rise_times(results["worker-dc-east"]), results[
        "worker-dc-east"
    ]

    # Both generations admitted the worker (gen-1 pre-loss, the final
    # generation after reboot).
    assert any(
        entry[0] == "worker-registered"
        for entry in results["manager-dc-west.gen1"]
    ), results["manager-dc-west.gen1"]
    assert any(
        entry[0] == "worker-registered"
        for entry in results["manager-dc-west"]
    ), results["manager-dc-west"]

    # The down window exceeds the 30s staleness bound, so the gate must
    # dip dc-west unhealthy inside the death-classification bracket —
    # and recover on gen-2 heartbeats: final classification healthy for
    # BOTH datacenters.
    gate_log = results["sim-gate-a"]
    west_unhealthy_times = _health_times(gate_log, "dc-west", "unhealthy")
    assert west_unhealthy_times, gate_log
    dip_latency = west_unhealthy_times[-1] - restart_at
    assert _DEATH_CLASSIFY_MIN <= dip_latency <= _DEATH_CLASSIFY_MAX, gate_log
    assert _final_health(gate_log) == {
        "dc-east": "healthy",
        "dc-west": "healthy",
    }, gate_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_manager_restart_mid_execution_is_replay_deterministic():
    assert (
        _run_manager_restart_mid_execution()
        == _run_manager_restart_mid_execution()
    )


# ---------------------------------------------------------------------------
# Scenario 3: gate<->DC partition covering the completion push
# ---------------------------------------------------------------------------

# The cut opens once the client observes its job running (the workflow is
# still executing, so the completion send lands inside the cut) and heals
# before the gate's detector could suspect dc-west (12.0s after the cut at
# the earliest measured): eight seconds after it opens, as before.
_PUSH_CUT_SECONDS = 8.0
# Measured (2026-10-04): the client observed the completion 13.99 - 11.5
# seconds after the heal.
_POST_HEAL_DELIVERY_SECONDS = 2.49


def _run_partition_over_completion_push() -> tuple[dict, dict[str, float]]:
    """Cable-cut gate <-> manager-dc-west for 8s from one latency after
    the client first observes its job running (``fault_at``) — the
    window covers the manager's completion notification but heals before
    the gate's phi-accrual detector suspects dc-west, so classification
    never flips. (The history below is the original pinned-instant
    probe.)

    Measured (post notice-backoff): the workflow runs to completion on
    dc-west's worker (active 9.5 -> 16.25); the manager's first
    ``job_final_result`` send dies in the cut, the completion becomes
    an OWED OBLIGATION (serialized payload, capped-exponential resend
    on the reap-loop cadence), and the resend after heal delivers it —
    the client observes ``completed`` at 65.06. Before the obligation
    pattern the single 5s send was followed unconditionally by job
    cleanup: the completion was lost FOREVER and the gate resolved the
    job as a false ``timeout`` at 77.01 despite the successful run."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0, pinned_datacenters=_JOB_PLACEMENT)
    fault_instants: dict[str, float] = {}
    _schedule_on_client_running(
        coordinator,
        fault_instants,
        lambda cut_at: coordinator.schedule_partition(
            "sim-gate-a", "manager-dc-west", cut_at, heal_time=cut_at + _PUSH_CUT_SECONDS
        ),
    )
    return coordinator.run(), fault_instants


def test_partition_over_completion_push_delivers_after_heal():
    """The completion-notice OBLIGATION at work: a partition that
    swallows the manager's first completion send must not lose the
    completion — the owed notice (serialized payload, capped-
    exponential resend on the reap-loop cadence) delivers after heal
    and the client observes ``completed``. Design bound: heal (25) +
    up to one reap-loop resend cycle + gate->client push/apply slack —
    measured 65.06. Never the false ``timeout`` at 77.01 the
    single-send behavior produced, and never past the AD-34 bound
    (which remains the backstop if delivery ever regresses)."""
    results, fault_instants = _run_partition_over_completion_push()
    cut_at = fault_instants["fault_at"]
    heal_at = cut_at + _PUSH_CUT_SECONDS
    client_log = results["client-a"]

    # The workflow genuinely ran and drained on dc-west.
    west_rises = _active_rise_times(results["worker-dc-west"])
    assert west_rises and west_rises[0] < cut_at, results[
        "worker-dc-west"
    ]

    submitted_time = _submitted_at(client_log)
    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", (
        "the owed completion notice must deliver after heal — a "
        f"timeout here means the obligation resend regressed: {client_log}"
    )
    ad34_backstop = (
        submitted_time
        + _JOB_TIMEOUT_SECONDS
        + _TRACKER_TICK_SECONDS
        + _TERMINAL_SLACK_SECONDS
    )
    assert heal_at < finished_time < ad34_backstop, client_log
    # Measured 13.99 (2026-10-04) with the heal pinned at 11.5: heal + the
    # owed notice's next resend + gate->client delivery. (21.87 with the
    # heal at 19.5; 60.04 when the cut healed at 25 and the resend backoff
    # had grown longer.) Pinned relative to the heal the run derived.
    expected_delivery_at = heal_at + _POST_HEAL_DELIVERY_SECONDS
    assert abs(finished_time - expected_delivery_at) <= 2.0, (
        f"post-heal delivery at {finished_time} drifted from the "
        f"measured heal + {_POST_HEAL_DELIVERY_SECONDS}s ({expected_delivery_at}): {client_log}"
    )

    # The window stayed under the staleness bound: dc-west must never
    # classify unhealthy, and both DCs end healthy.
    gate_log = results["sim-gate-a"]
    assert not _health_times(gate_log, "dc-west", "unhealthy"), gate_log
    assert _final_health(gate_log) == {
        "dc-east": "healthy",
        "dc-west": "healthy",
    }, gate_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_partition_over_completion_push_is_replay_deterministic():
    assert (
        _run_partition_over_completion_push()
        == _run_partition_over_completion_push()
    )


# ---------------------------------------------------------------------------
# Scenario 4: LONG partition of the standby DC — classification recovery
# ---------------------------------------------------------------------------

_STANDBY_CUT_AT = 9.8
_STANDBY_CUT_HEAL = 50.0
_RECLASSIFY_SLACK = 15.0


def _run_standby_dc_long_partition() -> dict:
    """Cable-cut gate <-> manager-dc-east over [9.8, 50) — well past
    the 30s staleness bound — while the job runs and completes in
    dc-west.

    Measured: job completes 10.544 untouched; dc-east flips unhealthy
    at 39.5 (cut + 29.7, inside the staleness bracket) and returns
    healthy at 58.5 (heal + 8.5: next 10s-period heartbeat +
    reclassification)."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0, pinned_datacenters=_JOB_PLACEMENT)
    coordinator.schedule_partition(
        "sim-gate-a",
        "manager-dc-east",
        _STANDBY_CUT_AT,
        heal_time=_STANDBY_CUT_HEAL,
    )
    return coordinator.run()


def test_job_unaffected_and_cut_dc_classification_recovers_after_heal():
    """Mission invariant for gate<->DC cuts: jobs targeting the healthy
    datacenter complete, and the cut datacenter's classification flips
    inside the staleness bracket and RECOVERS after heal."""
    results = _run_standby_dc_long_partition()
    client_log = results["client-a"]

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    assert finished_time < _STANDBY_CUT_AT + _DURATION_SECONDS, (
        "the healthy-DC job must complete on the baseline timeline, "
        f"untouched by the standby cut: {client_log}"
    )
    assert _active_rise_times(results["worker-dc-west"]), results[
        "worker-dc-west"
    ]
    assert not _active_rise_times(results["worker-dc-east"]), results[
        "worker-dc-east"
    ]

    gate_log = results["sim-gate-a"]
    east_unhealthy_times = _health_times(gate_log, "dc-east", "unhealthy")
    assert east_unhealthy_times, gate_log
    cut_latency = east_unhealthy_times[-1] - _STANDBY_CUT_AT
    assert _DEATH_CLASSIFY_MIN <= cut_latency <= _DEATH_CLASSIFY_MAX, gate_log

    east_healthy_times = _health_times(gate_log, "dc-east", "healthy")
    recovery_times = [
        healthy_time
        for healthy_time in east_healthy_times
        if healthy_time > _STANDBY_CUT_HEAL
    ]
    assert recovery_times, gate_log
    assert recovery_times[0] <= _STANDBY_CUT_HEAL + _RECLASSIFY_SLACK, (
        f"post-heal reclassification took {recovery_times[0] - _STANDBY_CUT_HEAL}s "
        f"(bound: next 10s heartbeat + sampler, <= {_RECLASSIFY_SLACK}s): "
        f"{gate_log}"
    )
    assert _final_health(gate_log) == {
        "dc-east": "healthy",
        "dc-west": "healthy",
    }, gate_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_standby_dc_long_partition_is_replay_deterministic():
    assert (
        _run_standby_dc_long_partition() == _run_standby_dc_long_partition()
    )


# ---------------------------------------------------------------------------
# Scenario 5: slow disk on the job's DC during live execution
# ---------------------------------------------------------------------------

_SLOW_DISK_SCHEDULE = (("slow_disk", 5.0, 0.02, 25.0),)


def _run_slow_disk_through_execution() -> dict:
    """dc-west's manager disk charges 20ms of virtual time per storage
    operation over [5, 25) — covering the job's whole WAL-append
    lifetime (submission ledger ~2.1 through completion records ~10.5)
    and the live execution window.

    Measured: completion 10.664 vs 10.544 baseline — the +0.12s is
    exactly the charged completion-path operations; dispatch arrival
    shifts 9.5 -> 9.75."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(
        coordinator, storage_schedule_by_dc={"dc-west": _SLOW_DISK_SCHEDULE}
    )
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0, pinned_datacenters=_JOB_PLACEMENT)
    return coordinator.run()


def test_job_completes_through_slow_disk_window():
    """Bounded storage delay is never a legitimate strand: the job must
    complete THROUGH the slow-disk window, with only the charged
    per-operation delay as slack (design bound: baseline completion
    ~10.5 + well under 4s of charged operations)."""
    results = _run_slow_disk_through_execution()
    client_log = results["client-a"]

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    assert finished_time < 14.0, (
        "slow-disk completion drifted past the charged-delay design "
        f"bound: {client_log}"
    )
    assert _active_rise_times(results["worker-dc-west"]), results[
        "worker-dc-west"
    ]
    assert not _active_rise_times(results["worker-dc-east"]), results[
        "worker-dc-east"
    ]
    assert _final_health(results["sim-gate-a"]) == {
        "dc-east": "healthy",
        "dc-west": "healthy",
    }, results["sim-gate-a"]

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_slow_disk_execution_is_replay_deterministic():
    assert (
        _run_slow_disk_through_execution() == _run_slow_disk_through_execution()
    )


# ---------------------------------------------------------------------------
# Scenario 6: disk-full manager — loud dispatch-time failure
# ---------------------------------------------------------------------------

# Scenario 6b's arm: before the first job's dispatch-time writes.
_DISK_FULL_ARM_AT = 2.86
_DISK_FULL_BUDGET_BYTES = 1024


@functools.cache
def _fault_free_pinned_job_twin() -> dict:
    """The pinned-job topology run WITHOUT faults (cached): a storage
    fault cannot change anything before its own instant, so this twin IS
    the disk-full run's timeline up to the arm."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0, pinned_datacenters=_JOB_PLACEMENT)
    return coordinator.run()


def _disk_full_arm_at() -> float:
    """One coordinator latency after the twin's client observed the
    gate's durable acceptance: the job is accepted before the disk
    fills, and dc-west's dispatch-time WAL appends (probed 2026-10-04:
    ~37ms after the acceptance) follow the arm."""
    return _submitted_at(_fault_free_pinned_job_twin()["client-a"]) + _LATENCY


def _run_disk_full_dispatch_failure() -> tuple[dict, float]:
    """dc-west's manager disk accepts 1024 further bytes from just after
    the job's acceptance (derived from the fault-free twin), then every
    write raises ENOSPC — armed after submission (so the job is durably
    accepted) but before dispatch, so the dispatch-time WAL appends
    exhaust it mid-job. Returns the results and the arm instant.

    Measured (original pinned probe: accepted 2.8525, armed 2.86): the
    client observes a LOUD ``failed`` at 2.89 — right after the dispatch
    attempt hit ENOSPC. Neither worker ever runs the workflow (no silent
    cross-DC re-route of a dispatch-time storage failure). The refused
    write makes the manager report its storage unwritable, and the gate
    classifies dc-west UNHEALTHY from the next heartbeat for as long as
    the disk stays full."""
    disk_full_arm_at = _disk_full_arm_at()
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(
        coordinator,
        storage_schedule_by_dc={
            "dc-west": (("disk_full", disk_full_arm_at, _DISK_FULL_BUDGET_BYTES),)
        },
    )
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0, pinned_datacenters=_JOB_PLACEMENT)
    return coordinator.run(), disk_full_arm_at


def test_disk_full_manager_fails_job_loudly_without_cross_dc_retry():
    results, disk_full_arm_at = _run_disk_full_dispatch_failure()
    client_log = results["client-a"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < disk_full_arm_at, client_log

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "failed", client_log
    assert disk_full_arm_at <= finished_time <= 20.0, (
        "dispatch-time ENOSPC must fail the job promptly and loudly "
        f"(never waiting out the 60s job timeout): {client_log}"
    )

    # TRUE current behavior, pinned: no execution anywhere — the
    # failure is not silently retried into the clean datacenter.
    assert not _active_rise_times(results["worker-dc-west"]), results[
        "worker-dc-west"
    ]
    assert not _active_rise_times(results["worker-dc-east"]), results[
        "worker-dc-east"
    ]

    # Storage-aware classification: the manager that cannot write
    # durably reports it, and its datacenter stays UNHEALTHY for
    # placement while the disk stays full (dc-east is untouched).
    assert _final_health(results["sim-gate-a"]) == {
        "dc-east": "healthy",
        "dc-west": "unhealthy",
    }, results["sim-gate-a"]
    west_unhealthy_times = _health_times(results["sim-gate-a"], "dc-west", "unhealthy")
    assert west_unhealthy_times and west_unhealthy_times[0] <= finished_time + _HEARTBEAT_INTERVAL_SECONDS, (
        results["sim-gate-a"]
    )

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_disk_full_failure_is_replay_deterministic():
    assert (
        _run_disk_full_dispatch_failure() == _run_disk_full_dispatch_failure()
    )


# ---------------------------------------------------------------------------
# Scenario 6b: storage-aware placement -- route around a full disk, return
# when it frees
# ---------------------------------------------------------------------------

# dc-west's disk fills before the first job's dispatch-time writes (as in
# scenario 6) and frees at _DISK_FREED_AT. A second, UNPINNED job is
# submitted while the disk is full.
_DISK_FREED_AT = 40.0
_DISK_FULL_WINDOW_SCHEDULE = (
    ("disk_full_window", _DISK_FULL_ARM_AT, _DISK_FULL_BUDGET_BYTES, _DISK_FREED_AT),
)
_SECOND_JOB_SUBMIT_AT = 15.0
# The manager probes its storage on its dead-node check cadence and the
# gate learns the outcome from the next manager heartbeat.
_STORAGE_PROBE_INTERVAL_SECONDS = float(Env().MANAGER_DEAD_NODE_CHECK_INTERVAL)
_HEARTBEAT_INTERVAL_SECONDS = float(Env().MANAGER_HEARTBEAT_INTERVAL)


def _run_full_disk_then_freed() -> dict:
    """Measured: the pinned job fails loudly at 2.89 and dc-west goes
    UNHEALTHY on the next heartbeat; the unpinned job submitted at 15.08 is placed in
    dc-east (running 15.25-16.5) and completes at 16.22; the disk frees
    at 40.0, the manager's next storage probe (60.0) proves the refused
    size fits, and dc-west is HEALTHY again at 60.5."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(
        coordinator, storage_schedule_by_dc={"dc-west": _DISK_FULL_WINDOW_SCHEDULE}
    )
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0, pinned_datacenters=_JOB_PLACEMENT)
    _add_soak_client(coordinator, "client-b", "sim-cli-b", 120.0, submit_at=_SECOND_JOB_SUBMIT_AT)
    return coordinator.run()


def test_placement_routes_around_a_full_disk_and_returns_when_it_frees():
    """A datacenter whose manager cannot write durably takes no new jobs
    while the condition lasts -- an unpinned job goes to the healthy
    datacenter and completes -- and it takes jobs again once its manager
    proves the storage writable."""
    results = _run_full_disk_then_freed()
    gate_log = results["sim-gate-a"]

    (_tag, pinned_status, pinned_finished) = _finished(results["client-a"])
    assert pinned_status == "failed", results["client-a"]
    west_unhealthy_times = _health_times(gate_log, "dc-west", "unhealthy")
    assert west_unhealthy_times, gate_log
    assert west_unhealthy_times[0] <= pinned_finished + _HEARTBEAT_INTERVAL_SECONDS, gate_log

    second_client_log = results["client-b"]
    assert _submitted_at(second_client_log) > west_unhealthy_times[0], second_client_log
    (_tag, second_status, _second_finished) = _finished(second_client_log)
    assert second_status == "completed", second_client_log
    assert _active_rise_times(results["worker-dc-east"]), results["worker-dc-east"]
    assert not _active_rise_times(results["worker-dc-west"]), results["worker-dc-west"]

    west_recovered_times = [
        time for time in _health_times(gate_log, "dc-west", "healthy") if time > _DISK_FREED_AT
    ]
    assert west_recovered_times, gate_log
    recovery_deadline = (
        _DISK_FREED_AT + _STORAGE_PROBE_INTERVAL_SECONDS + _HEARTBEAT_INTERVAL_SECONDS
    )
    assert west_recovered_times[0] <= recovery_deadline, (
        f"dc-west recovered at {west_recovered_times[0]}, past one probe "
        f"interval + one heartbeat after the disk freed ({recovery_deadline}): {gate_log}"
    )
    assert _final_health(gate_log) == {"dc-east": "healthy", "dc-west": "healthy"}, gate_log

    _assert_oracle_clean(results["client-a"])
    _assert_oracle_clean(second_client_log)
    _assert_no_unswapped_imports(results)


def test_full_disk_then_freed_is_replay_deterministic():
    assert _run_full_disk_then_freed() == _run_full_disk_then_freed()


# ---------------------------------------------------------------------------
# Scenario 7: LONG HORIZON — chaos, quiesce, then a clean second job
# ---------------------------------------------------------------------------

_LONG_HORIZON_CEILING = 420.0
_SECOND_SUBMIT_AT = 250.0


def _run_chaos_then_quiesce_two_jobs() -> tuple[dict, dict[str, float]]:
    """>=400 virtual seconds, two jobs in sequence: job-a is stranded
    by the total loss of its datacenter mid-execution (dc-west killed
    once its client observes it running, exactly the scenario-1 chaos;
    the kill instant is returned as ``fault_at``), the cluster quiesces for
    ~170s, then job-b submits at t=250 with free DC selection.

    Measured: job-a replays scenario 1's timeline exactly (submit 2.12,
    timeout 77.01) — the idle second client does not perturb the
    schedule. Job-b is accepted FIRST TRY at 250.08 (no rejections on
    the quiesced cluster), routed AROUND the dead datacenter to
    dc-east by free selection, and completes at 251.2 (dispatch 250.25,
    the client-visible completion ~1s later)."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_LONG_HORIZON_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 150.0, pinned_datacenters=_JOB_PLACEMENT)
    _add_soak_client(
        coordinator,
        "client-b",
        "sim-cli-b",
        150.0,
        submit_at=_SECOND_SUBMIT_AT,
    )
    fault_instants: dict[str, float] = {}
    _schedule_west_loss_once_running(coordinator, fault_instants)
    return coordinator.run(), fault_instants


def test_post_quiesce_job_completes_cleanly_after_dc_loss_chaos():
    """The liveness/convergence invariant: whatever the chaos did to
    in-flight work, a job submitted AFTER the system has re-converged
    must complete cleanly — accepted first-try, placed in the
    surviving datacenter, done within seconds."""
    results, fault_instants = _run_chaos_then_quiesce_two_jobs()
    loss_at = fault_instants["fault_at"]

    # Job-a: the scenario-1 stranding, unchanged by the second client.
    client_a_log = results["client-a"]
    submitted_a = _submitted_at(client_a_log)
    assert submitted_a < loss_at, client_a_log
    (_tag, status_a, finished_a) = _finished(client_a_log)
    assert status_a == "timeout", client_a_log
    earliest = submitted_a + _JOB_TIMEOUT_SECONDS
    latest = earliest + _TRACKER_TICK_SECONDS + _TERMINAL_SLACK_SECONDS
    assert earliest <= finished_a <= latest, client_a_log

    # Job-b: clean post-quiesce completion, routed around the dead DC.
    client_b_log = results["client-b"]
    assert not [
        entry for entry in client_b_log if entry[0] == "submit-rejected"
    ], ("a quiesced cluster must accept first try", client_b_log)
    submitted_b = _submitted_at(client_b_log)
    assert _SECOND_SUBMIT_AT <= submitted_b <= _SECOND_SUBMIT_AT + 2.0, (
        client_b_log
    )
    (_tag, status_b, finished_b) = _finished(client_b_log)
    assert status_b == "completed", client_b_log
    assert finished_b <= submitted_b + 10.0, (
        "post-quiesce completion must be prompt (measured ~1.1s after "
        f"acceptance): {client_b_log}"
    )

    # Placement: job-b ran in the SURVIVING datacenter, after the
    # quiesce point.
    east_rises = _active_rise_times(results["worker-dc-east"])
    assert east_rises and all(
        rise_time >= _SECOND_SUBMIT_AT for rise_time in east_rises
    ), results["worker-dc-east"]

    # Classification: the dead DC was detected inside the staleness
    # bracket and STAYS unhealthy through the long horizon; the
    # surviving DC ends healthy (its completion-adjacent flap heals).
    gate_log = results["sim-gate-a"]
    west_unhealthy_times = _health_times(gate_log, "dc-west", "unhealthy")
    assert west_unhealthy_times, gate_log
    detection_latency = west_unhealthy_times[0] - loss_at
    assert _DEATH_CLASSIFY_MIN <= detection_latency <= _DEATH_CLASSIFY_MAX, (
        gate_log
    )
    assert not [
        healthy_time
        for healthy_time in _health_times(gate_log, "dc-west", "healthy")
        if healthy_time > west_unhealthy_times[0]
    ], ("a dead DC must never reclassify healthy", gate_log)
    assert _final_health(gate_log) == {
        "dc-east": "healthy",
        "dc-west": "unhealthy",
    }, gate_log

    # Per-job linearizability, independently.
    _assert_oracle_clean(client_a_log)
    _assert_oracle_clean(client_b_log)
    _assert_no_unswapped_imports(results)


def test_chaos_then_quiesce_two_jobs_is_replay_deterministic():
    assert (
        _run_chaos_then_quiesce_two_jobs()
        == _run_chaos_then_quiesce_two_jobs()
    )


# ---------------------------------------------------------------------------
# Scenario 8: TWO CONCURRENT jobs; their datacenter dies mid-drain
# ---------------------------------------------------------------------------

_CONCURRENT_DURATION_SECONDS = 20.0
_CONCURRENT_KILL_AT = 13.0
_CONCURRENT_CEILING = 160.0
# Both jobs in the datacenter that dies: the scenario's premise, pinned
# (free selection co-placed them only while dc-east was still
# initializing, and the gate no longer places around an initializing DC).


def _run_concurrent_jobs_dc_loss_after_completion() -> dict:
    """Two unpinned clients submit 20s soak workflows concurrently.

    Measured placement truth: free selection CO-PLACES both jobs in
    dc-west (both accepted at 2.12; the worker's active count rises
    1 -> 2, overlap [11.5, 15.75]) — concurrent cross-DC flight does
    not exist today (pinning the two jobs to different DCs instead
    serializes the second acceptance by ~30s; see the report). Client
    completions land at 10.06 and 12.36 (~1s after each dispatch),
    while the worker keeps executing the run windows until ~21.75.

    dc-west is killed at t=13.0 — AFTER both client-observed
    completions, INSIDE the still-executing drain — so this pins
    terminal ABSORPTION: a datacenter dying with delivered results
    must not un-complete anything."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_CONCURRENT_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    coordinator.add_process(
        "client-a",
        soak_gate_dispatch_client_entry,
        "sim-cli-a",
        9500,
        ("sim-gate-a", 9000),
        _CONCURRENT_DURATION_SECONDS,
        _JOB_TIMEOUT_SECONDS,
        140.0,
        0.0,
        _JOB_PLACEMENT,
    )
    coordinator.add_process(
        "client-b",
        soak_gate_dispatch_client_entry,
        "sim-cli-b",
        9500,
        ("sim-gate-a", 9000),
        _CONCURRENT_DURATION_SECONDS,
        _JOB_TIMEOUT_SECONDS,
        140.0,
        0.0,
        _JOB_PLACEMENT,
    )
    for victim_id in _DC_VICTIMS["dc-west"]:
        coordinator.schedule_kill(victim_id, _CONCURRENT_KILL_AT)
    return coordinator.run()


def test_concurrent_jobs_keep_their_terminals_when_their_dc_dies():
    """Per-job coherence under concurrency + total DC loss: each
    client's history linearizes independently, both delivered
    terminals survive the datacenter's death, and the standby
    datacenter never executes anything."""
    results = _run_concurrent_jobs_dc_loss_after_completion()

    for client_id in ("client-a", "client-b"):
        client_log = results[client_id]
        submitted_time = _submitted_at(client_log)
        assert submitted_time < _CONCURRENT_KILL_AT, (client_id, client_log)
        (_tag, final_status, finished_time) = _finished(client_log)
        assert final_status == "completed", (client_id, client_log)
        assert finished_time < _CONCURRENT_KILL_AT, (
            "both completions were delivered BEFORE the kill — a "
            f"drifted schedule voids this scenario: {client_id} "
            f"{client_log}"
        )
        _assert_oracle_clean(client_log)

    # The standby datacenter never ran either job.
    assert not _active_rise_times(results["worker-dc-east"]), results[
        "worker-dc-east"
    ]

    # The dead datacenter converges to unhealthy and never returns
    # (its completion-adjacent flap freezes into truth: no heartbeat
    # survives to heal it); the standby DC ends healthy.
    gate_log = results["sim-gate-a"]
    west_unhealthy_times = _health_times(gate_log, "dc-west", "unhealthy")
    assert west_unhealthy_times, gate_log
    assert not [
        healthy_time
        for healthy_time in _health_times(gate_log, "dc-west", "healthy")
        if healthy_time > west_unhealthy_times[-1]
    ], gate_log
    assert _final_health(gate_log) == {
        "dc-east": "healthy",
        "dc-west": "unhealthy",
    }, gate_log

    _assert_no_unswapped_imports(results)


def test_concurrent_jobs_dc_loss_is_replay_deterministic():
    assert (
        _run_concurrent_jobs_dc_loss_after_completion()
        == _run_concurrent_jobs_dc_loss_after_completion()
    )


# ---------------------------------------------------------------------------
# Scenario 9 (FIXED BUG, pinned): gate must survive a DC dying with
# one job mid-execution and one mid-dispatch — no livelock
# ---------------------------------------------------------------------------


def test_dc_loss_with_job_mid_dispatch_must_not_livelock_the_gate():
    """FIXED-BUG PIN: with TWO in-flight jobs co-placed in dc-west —
    one executing, one accepted but not yet dispatched — killing the
    whole datacenter at t=9.6 used to spin the GATE at one frozen
    virtual instant (t=82.66357815979111, schedule-traced to
    ``JobSuspicionManager._poll_suspicion``: ``time_remaining``'s
    composed-float remainder produced a positive SUB-QUANTUM artifact
    that survived the ``remaining <= 0`` expiry check while
    ``sleep(min(interval, remaining))`` re-armed at the same quantized
    instant — 100% CPU livelock in production). The epsilon-expiry
    contract now applied in that file (sub-epsilon remainders ARE
    expiry, plus the 1ms floor as defense in depth) lets the suspicion
    expire and the run proceed: both clients reach LOUD terminals and
    the run completes to its ceiling — this test living at all is the
    regression pin."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_CONCURRENT_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    for client_id, client_host in (
        ("client-a", "sim-cli-a"),
        ("client-b", "sim-cli-b"),
    ):
        coordinator.add_process(
            client_id,
            soak_gate_dispatch_client_entry,
            client_host,
            9500,
            ("sim-gate-a", 9000),
            _CONCURRENT_DURATION_SECONDS,
            _JOB_TIMEOUT_SECONDS,
            140.0,
            0.0,
            None,
        )
    for victim_id in _DC_VICTIMS["dc-west"]:
        coordinator.schedule_kill(victim_id, 9.6)
    results = coordinator.run()

    for client_id in ("client-a", "client-b"):
        client_log = results[client_id]
        terminal_entries = [
            entry
            for entry in client_log
            if entry[0] == "job-finished"
            or (
                entry[0] == "status-seen"
                and entry[1] in ("completed", "failed", "timeout", "timed_out")
            )
        ]
        assert terminal_entries, (client_id, client_log)
        _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)
