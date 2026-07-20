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
* There is NO mid-flight cross-DC failover (AD-36 selection is
  dispatch-time only): the surviving DC never executes the stranded
  job — pinned here as the TRUE current behavior.
* The manager's completion notification to the gate
  (``_notify_gate_of_completion``) is a SINGLE 5s-timeout send and the
  manager cleans the job up even when it fails — so a gate<->manager
  partition covering that send loses the completion and the job
  resolves as gate ``timeout`` despite having run. The gate->CLIENT
  direction is redelivery-protected (terminal re-lands < 1s after a
  client-link heal) — the asymmetry is deliberate to pin.
"""

import pytest

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
_DURATION_SECONDS = 8.0
_JOB_TIMEOUT_SECONDS = 60.0

# The gate's DC-death classification mechanism (probed): 30s heartbeat
# staleness - up to one 10s heartbeat period + 0.5s health sampler.
_DEATH_CLASSIFY_MIN = 20.0
_DEATH_CLASSIFY_MAX = 31.0

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


# ---------------------------------------------------------------------------
# Scenario 1: total loss of the job's OWN datacenter mid-execution
# ---------------------------------------------------------------------------

_LOSS_AT = 9.8
_LOSS_CEILING = 140.0


def _run_stranded_dc_loss() -> dict:
    """Kill dc-west (manager + worker + both executors) at t=9.8 —
    probe-pinned to land INSIDE live execution: seed 61 free selection
    places the job in dc-west, dispatch reaches the worker at 9.5, and
    the 8s workflow executes over [9.5, 16.3].

    Measured timeline: submit 2.12, dispatch 9.5, kill 9.8, gate flips
    dc-west unhealthy 38.5, client observes terminal ``timeout`` 77.01
    (= submit + 60s job timeout + one 15s AD-34 tick - just-missed
    check at 62.0)."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0)
    for victim_id in _DC_VICTIMS["dc-west"]:
        coordinator.schedule_kill(victim_id, _LOSS_AT)
    return coordinator.run()


def test_dc_loss_strands_job_to_loud_gate_timeout():
    """A job whose datacenter dies mid-execution must end LOUDLY at the
    client — the gate's AD-34 global timeout — and the surviving
    datacenter must NOT execute it (AD-36 selection is dispatch-time
    only; pinning the absence of mid-flight failover is deliberate:
    when failover arrives, this assertion is the one it flips)."""
    results = _run_stranded_dc_loss()
    client_log = results["client-a"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < _LOSS_AT, client_log

    # The kill landed inside live execution: the client had already
    # observed ``running`` (probe: 9.62 < 9.8). The worker's own log is
    # unavailable — SIGKILLed children never report their milestones.
    running_times = [
        entry[2]
        for entry in client_log
        if entry[0] == "status-seen" and entry[1] == "running"
    ]
    assert running_times and running_times[0] < _LOSS_AT, client_log

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

    # NO cross-DC failover: the surviving datacenter never ran it.
    assert not _active_rise_times(results["worker-dc-east"]), results[
        "worker-dc-east"
    ]

    # Death classification via heartbeat staleness, inside its bound.
    gate_log = results["sim-gate-a"]
    west_unhealthy_times = _health_times(gate_log, "dc-west", "unhealthy")
    assert west_unhealthy_times, gate_log
    detection_latency = west_unhealthy_times[-1] - _LOSS_AT
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

_RESTART_AT = 9.8
_RESTART_DOWN_SECONDS = 30.0
_RESTART_CEILING = 160.0


def _run_manager_restart_mid_execution() -> dict:
    """Power-lose dc-west's manager at t=9.8 (the workflow is running on
    its worker) and reboot from the surviving durable disk at t=39.8.

    Measured timeline: dispatch 9.5, restart 9.8, first worker
    execution cycle ends 34.0 (its result had no live manager), gen-2
    boots 39.82, worker re-registered 40.32, resume RE-dispatches
    (second execution cycle 40.25-42.25), client observes completion
    41.27 — exactly-once at the client across at-least-once execution.
    dc-west dips unhealthy at 38.5 (staleness bound) and returns
    healthy at 42.5 on gen-2 heartbeats."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_RESTART_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 140.0)
    coordinator.schedule_restart(
        "manager-dc-west", _RESTART_AT, down_seconds=_RESTART_DOWN_SECONDS
    )
    return coordinator.run()


def test_job_completes_exactly_once_across_manager_restart_in_l3():
    """Manager durable resume must hold in the L3 topology: the job
    submitted through the GATE completes across the mid-execution
    manager reboot, exactly once at the client, and BEFORE the AD-34
    job timeout would have fired."""
    results = _run_manager_restart_mid_execution()
    client_log = results["client-a"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < _RESTART_AT, client_log

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    generation_two_up = _RESTART_AT + _RESTART_DOWN_SECONDS
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
    assert west_rises[0] < _RESTART_AT < west_rises[-1], results[
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
    dip_latency = west_unhealthy_times[-1] - _RESTART_AT
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

_PUSH_CUT_AT = 9.8
_PUSH_CUT_HEAL = 25.0


def _run_partition_over_completion_push() -> dict:
    """Cable-cut gate <-> manager-dc-west over [9.8, 25) — the window
    covers the manager's completion notification (~10.4) but stays
    under the 30s heartbeat-staleness bound, so classification never
    flips.

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
        latency=0.01, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0)
    coordinator.schedule_partition(
        "sim-gate-a", "manager-dc-west", _PUSH_CUT_AT, heal_time=_PUSH_CUT_HEAL
    )
    return coordinator.run()


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
    results = _run_partition_over_completion_push()
    client_log = results["client-a"]

    # The workflow genuinely ran and drained on dc-west.
    west_rises = _active_rise_times(results["worker-dc-west"])
    assert west_rises and west_rises[0] < _PUSH_CUT_AT, results[
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
    assert _PUSH_CUT_HEAL < finished_time < ad34_backstop, client_log
    assert abs(finished_time - 65.06) <= 2.0, (
        f"post-heal delivery at {finished_time} drifted from the "
        f"measured 65.06: {client_log}"
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
        latency=0.01, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0)
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
        latency=0.01, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(
        coordinator, storage_schedule_by_dc={"dc-west": _SLOW_DISK_SCHEDULE}
    )
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0)
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

_DISK_FULL_SCHEDULE = (("disk_full", 8.0, 1024),)
_DISK_FULL_ARM_AT = 8.0


def _run_disk_full_dispatch_failure() -> dict:
    """dc-west's manager disk accepts 1024 further bytes from t=8, then
    every write raises ENOSPC — armed after submission (~2.1, so the
    job is durably accepted) but before dispatch (~9.5), so the
    dispatch-time WAL appends exhaust it mid-job.

    Measured: the client observes a LOUD ``failed`` at 9.464 — ~0.2s
    after the dispatch attempt hit ENOSPC. Neither worker ever runs
    the workflow (no silent cross-DC re-route of a dispatch-time
    storage failure), and the disk-full manager keeps classifying
    HEALTHY (storage state is invisible to DC health — placement
    cannot route around a full disk; the loud failure is the
    protection)."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_LOSS_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(
        coordinator, storage_schedule_by_dc={"dc-west": _DISK_FULL_SCHEDULE}
    )
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 120.0)
    return coordinator.run()


def test_disk_full_manager_fails_job_loudly_without_cross_dc_retry():
    results = _run_disk_full_dispatch_failure()
    client_log = results["client-a"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < _DISK_FULL_ARM_AT, client_log

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "failed", client_log
    assert _DISK_FULL_ARM_AT <= finished_time <= 20.0, (
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

    # Storage exhaustion is invisible to health classification: both
    # DCs still end healthy — pinned so a future storage-aware
    # classifier flips this assertion consciously.
    assert _final_health(results["sim-gate-a"]) == {
        "dc-east": "healthy",
        "dc-west": "healthy",
    }, results["sim-gate-a"]

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_disk_full_failure_is_replay_deterministic():
    assert (
        _run_disk_full_dispatch_failure() == _run_disk_full_dispatch_failure()
    )


# ---------------------------------------------------------------------------
# Scenario 7: LONG HORIZON — chaos, quiesce, then a clean second job
# ---------------------------------------------------------------------------

_LONG_HORIZON_CEILING = 420.0
_SECOND_SUBMIT_AT = 250.0


def _run_chaos_then_quiesce_two_jobs() -> dict:
    """>=400 virtual seconds, two jobs in sequence: job-a is stranded
    by the total loss of its datacenter mid-execution (dc-west killed
    at 9.8, exactly the scenario-1 chaos), the cluster quiesces for
    ~170s, then job-b submits at t=250 with free DC selection.

    Measured: job-a replays scenario 1's timeline exactly (submit 2.12,
    timeout 77.01) — the idle second client does not perturb the
    schedule. Job-b is accepted FIRST TRY at 250.08 (no rejections on
    the quiesced cluster), routed AROUND the dead datacenter to
    dc-east by free selection, and completes at 251.2 (dispatch 250.25,
    the client-visible completion ~1s later)."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_LONG_HORIZON_CEILING, seed=_SEED
    )
    _add_multi_dc_topology(coordinator)
    _add_soak_client(coordinator, "client-a", "sim-cli-a", 150.0)
    _add_soak_client(
        coordinator,
        "client-b",
        "sim-cli-b",
        150.0,
        submit_at=_SECOND_SUBMIT_AT,
    )
    for victim_id in _DC_VICTIMS["dc-west"]:
        coordinator.schedule_kill(victim_id, _LOSS_AT)
    return coordinator.run()


def test_post_quiesce_job_completes_cleanly_after_dc_loss_chaos():
    """The liveness/convergence invariant: whatever the chaos did to
    in-flight work, a job submitted AFTER the system has re-converged
    must complete cleanly — accepted first-try, placed in the
    surviving datacenter, done within seconds."""
    results = _run_chaos_then_quiesce_two_jobs()

    # Job-a: the scenario-1 stranding, unchanged by the second client.
    client_a_log = results["client-a"]
    submitted_a = _submitted_at(client_a_log)
    assert submitted_a < _LOSS_AT, client_a_log
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
    detection_latency = west_unhealthy_times[0] - _LOSS_AT
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
        latency=0.01, max_virtual_time=_CONCURRENT_CEILING, seed=_SEED
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
        None,
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
        None,
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
# Scenario 9 (KNOWN BUG, pinned as a reproducer): gate livelock when a
# DC dies with one job mid-execution and one mid-dispatch
# ---------------------------------------------------------------------------


@pytest.mark.skip(
    reason=(
        "KNOWN BUG, ROOT-CAUSED (deterministic reproducer): with TWO "
        "in-flight jobs co-placed in dc-west — one executing, one "
        "accepted but not yet dispatched — killing the whole "
        "datacenter at t=9.6 spins the GATE at t=82.66357815979111. "
        "Schedule-traced to JobSuspicionManager._poll_suspicion "
        "(swim/detection/job_suspicion_manager.py:337, the gate's "
        "job-suspicion of the dead dc-west manager started in the "
        "AD-34 global-timeout / dead-manager broadcast aftermath): "
        "JobSuspicion.time_remaining computes max(0, timeout - "
        "elapsed) from composed floats, a positive SUB-QUANTUM "
        "remainder (the b4bc1784 epsilon-expiry class) survives the "
        "'remaining <= 0' expiry check, and sleep(min(poll_interval, "
        "remaining)) re-arms call_at at the same quantized instant "
        "forever — 100% CPU livelock in production. The fix is the "
        "established two-line shape (TIME_REMAINDER_EPSILON_SECONDS "
        "expiry predicate + the 1ms progress floor) in "
        "swim/detection/job_suspicion_manager.py — a shared-SWIM-tier "
        "file outside the gate-tier scope of this fix wave. Unskip "
        "once that lands."
    )
)
def test_dc_loss_with_job_mid_dispatch_must_not_livelock_the_gate():
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CONCURRENT_CEILING, seed=_SEED
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
