"""
K5 — the AD-26 extension surface under real workload (gateless L2),
probed then pinned. The probes found the surface ALIVE in exactly one
path and DEAD in three — these scenarios pin the living path's full
schedule and the dead paths' loud consequences.

WHAT ACTUALLY RUNS (traced + probed):

* At dispatch-accept the worker latches
  ``request_extension(reason="dispatch-accepted")``
  (``WorkerServer._handle_dispatch_execution``) and EVERY SWIM
  heartbeat re-carries that same frozen snapshot:
  ``clear_extension_request`` has ZERO callers, so the latch never
  clears. Consequences pinned here:
  - the manager's ``ExtensionTracker`` grants the FIRST piggyback
    request (30s at count 0) within one heartbeat of dispatch and
    denies every repeat (the frozen ``completed_items=0`` can never
    show progress) — grants per worker lifetime: exactly ONE;
  - the deny stream continues at heartbeat cadence FOREVER — through
    workflow completion, job termination, and on to the ceiling
    (~1/s of pure denial traffic per worker, unbounded);
  - the autonomous lookahead ``ExtensionTrigger``
    (``elapsed >= deadline x 0.75``) is DEAD CODE: its
    ``is_extension_pending`` gate reads the never-cleared latch.
* The H5/H7 witness+ledger route NEVER engages for these requests:
  ``_route_extension_through_witnesses`` falls back (workflow-context
  lookup misses for the worker-side workflow id) to the legacy
  tracker path, which records NO ledger events — the probe saw zero
  ``ext-last-code`` rows because ``ExtensionLedger`` stayed EMPTY all
  run.
* Grants never stretch the AD-34 job budget:
  ``_notify_timeout_strategies_of_extension`` gates on
  ``hasattr(strategy, "record_extension")`` (``server.py`` ~5428) but
  every strategy defines ``record_worker_extension`` — the call is
  unreachable, ``total_extensions_granted`` stays 0
  (``job-ext`` pinned flat), and the hard timeout fires on the BASE
  budget.
* A job past its hard timeout does NOT stop executing:
  ``_timeout_job`` invokes ``_cancel_running_workflows``, yet the
  worker ran the workflow to its natural end 55 virtual seconds after
  the client-observed ``timeout`` terminal (zombie execution, pinned).

Measured (probe scripts in the L2 workload series):

* BASELINE (seed 73, 30s workflow, job timeout 60): submit 7.70582,
  dispatch 7.75, grant sampled 8.5 (+30s worker deadline,
  ``ext-extended`` 30.0), first denial 9.5 then ~1/s to the 120
  ceiling (109 denials by 119.5), execution 7.75 -> 38.0, client
  completion 37.74582, ``job-ext`` flat 0, zero ledger rows.
* UDP BLACKOUT (seed 73 + ``schedule_drop_rate(worker->manager, 1.0)``
  over [6, 20)): heartbeat piggybacks are datagrams and die; the
  decision stream FREEZES — first grant AND first denial land
  together at 20.5 (heal + 0.5) — while dispatch/execution/completion
  ride TCP unaffected: the client timeline is value-identical to
  baseline (submit 7.70582, completion 37.74582) and 14s of one-way
  UDP silence stays far below the death-detection bound (no
  ``worker-lost``).
* HARD TIMEOUT (seed 113, 100s workflow, job timeout 20): grant
  sampled 15.5; ``timeout`` terminal at 60.03 — the first 30s
  unified-timeout tick past the UNEXTENDED base expiry
  (submit 15.265038 + 20; the 30.03 tick misses it) — and the worker
  drains only at 115.5 (started 15.25 + 100.25: full natural length,
  55.5 virtual seconds of zombie execution past the terminal).
"""

import pytest

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.l2_extension_demo import (
    extension_watch_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.l2_workload_demo import (
    dag_worker_entry,
    sustained_client_entry,
)
from tests.simulation.oracle import ClusterTraceOracle, JobStatusOracle

_BASELINE_SEED = 73
_BASELINE_DURATION = 30.0
_BASELINE_CEILING = 120.0
_BLACKOUT_START = 6.0
_BLACKOUT_HEAL = 20.0
_HARD_TIMEOUT_SEED = 113
_HARD_TIMEOUT_DURATION = 100.0
_HARD_TIMEOUT_JOB_BUDGET = 20.0
_HARD_TIMEOUT_CEILING = 200.0
_TIMEOUT_TICK_SECONDS = 30.0


def _build_extension_topology(
    seed: int,
    ceiling: float,
    workflow_duration_seconds: float,
    job_timeout_seconds: float,
    wait_timeout_seconds: float,
) -> SimulationCoordinator:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=ceiling, seed=seed
    )
    coordinator.add_process(
        "manager",
        extension_watch_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
    )
    coordinator.add_process(
        "worker",
        dag_worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        "client",
        sustained_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        workflow_duration_seconds,
        1,
        job_timeout_seconds,
        wait_timeout_seconds,
    )
    return coordinator


def _run_extension_baseline() -> dict:
    return _build_extension_topology(
        seed=_BASELINE_SEED,
        ceiling=_BASELINE_CEILING,
        workflow_duration_seconds=_BASELINE_DURATION,
        job_timeout_seconds=60.0,
        wait_timeout_seconds=90.0,
    ).run()


def _run_extension_udp_blackout() -> dict:
    coordinator = _build_extension_topology(
        seed=_BASELINE_SEED,
        ceiling=_BASELINE_CEILING,
        workflow_duration_seconds=_BASELINE_DURATION,
        job_timeout_seconds=60.0,
        wait_timeout_seconds=90.0,
    )
    # Datagram-only loss: heartbeat piggybacks (the ONLY extension
    # carrier) die; TCP dispatch/results/pushes are exempt by the
    # documented stream scoping.
    coordinator.schedule_drop_rate(
        "worker",
        "manager",
        1.0,
        at_time=_BLACKOUT_START,
        until_time=_BLACKOUT_HEAL,
    )
    return coordinator.run()


def _run_hard_timeout() -> dict:
    return _build_extension_topology(
        seed=_HARD_TIMEOUT_SEED,
        ceiling=_HARD_TIMEOUT_CEILING,
        workflow_duration_seconds=_HARD_TIMEOUT_DURATION,
        job_timeout_seconds=_HARD_TIMEOUT_JOB_BUDGET,
        wait_timeout_seconds=150.0,
    ).run()


def _rows(log: list, tag: str) -> list[tuple]:
    return [entry for entry in log if entry[0] == tag]


def _assert_single_grant_then_denial_stream(
    manager_log: list,
    dispatch_time: float,
    earliest_decision_time: float,
    ceiling: float,
) -> None:
    """The living path's shape: exactly ONE 30s grant (the frozen
    snapshot can never show progress again), then denial traffic at
    heartbeat cadence to the ceiling, with the H7 ledger silent."""
    grant_rows = _rows(manager_log, "ext-grants")
    assert [row[1] for row in grant_rows] == [0, 1], manager_log
    grant_time = grant_rows[1][2]
    assert earliest_decision_time <= grant_time <= (
        earliest_decision_time + 2.5
    ), manager_log
    assert grant_time >= dispatch_time, manager_log

    extended_rows = _rows(manager_log, "ext-extended")
    assert [(row[1], row[2]) for row in extended_rows] == [
        (0, 0.0),
        (30.0, grant_time),
    ], manager_log

    denial_rows = _rows(manager_log, "ext-denials")
    positive_denials = [row for row in denial_rows if row[1] > 0]
    assert positive_denials, manager_log
    assert positive_denials[0][2] >= grant_time, manager_log
    # Denial values grow monotonically by single heartbeats.
    denial_values = [row[1] for row in denial_rows]
    assert denial_values == sorted(denial_values), manager_log
    # The stream never stops: it runs to the ceiling at heartbeat
    # cadence (>= one denial per 2.5s of window on average).
    last_denial = positive_denials[-1]
    assert last_denial[2] >= ceiling - 2.0, manager_log
    denial_window = last_denial[2] - positive_denials[0][2]
    assert last_denial[1] >= denial_window * 0.4, manager_log

    # The H7 ledger never records: the witness route falls back to the
    # legacy tracker path for piggyback requests, and that path writes
    # no ledger events — zero code transitions all run.
    assert _rows(manager_log, "ext-last-code") == [], manager_log

    # Grants NEVER stretch the AD-34 job budget: the
    # hasattr("record_extension") guard can never pass.
    assert [(row[1], row[2]) for row in _rows(manager_log, "job-ext")] == [
        (0, 0.0)
    ], manager_log


def test_dispatch_time_extension_grants_once_then_denies_forever():
    results = _run_extension_baseline()
    manager_log = results["manager"]
    client_log = results["client"]
    worker_log = results["worker"]

    dispatch_rows = _rows(worker_log, "workflow-started")
    assert len(dispatch_rows) == 1, worker_log
    dispatch_time = dispatch_rows[0][2]

    _assert_single_grant_then_denial_stream(
        manager_log,
        dispatch_time=dispatch_time,
        earliest_decision_time=dispatch_time,
        ceiling=_BASELINE_CEILING,
    )

    # The workflow itself is untouched by the denial storm: full
    # execution window, clean completion, linearized history.
    finished = _rows(client_log, "job-finished")
    assert len(finished) == 1 and finished[0][1] == "completed", client_log
    executed = _rows(worker_log, "workflow-executed")
    assert len(executed) == 1, worker_log
    assert (
        _BASELINE_DURATION - 0.5
        <= executed[0][2] - dispatch_time
        <= _BASELINE_DURATION + 1.0
    ), worker_log
    # Denials CONTINUE past the completion instant — the latched
    # request outlives the workflow it was latched for.
    denial_rows = [
        row for row in _rows(manager_log, "ext-denials") if row[1] > 0
    ]
    assert denial_rows[-1][2] > finished[0][2] + 30.0, manager_log

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    trace_oracle = ClusterTraceOracle(
        worker_process_ids_by_datacenter={"sim-dc": ("worker",)},
        retry_budget=0,
    )
    assert trace_oracle.check_workflow_execution(results) == [], results
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_extension_baseline_is_replay_deterministic():
    assert _run_extension_baseline() == _run_extension_baseline()


def test_udp_blackout_freezes_extension_decisions_until_heal():
    results = _run_extension_udp_blackout()
    manager_log = results["manager"]
    client_log = results["client"]

    # NOTHING was decided during the blackout: heartbeat piggybacks
    # are datagrams, and the one-way drop killed every one of them.
    grant_rows = [row for row in _rows(manager_log, "ext-grants") if row[1] > 0]
    denial_rows = [
        row for row in _rows(manager_log, "ext-denials") if row[1] > 0
    ]
    assert grant_rows and denial_rows, manager_log
    assert grant_rows[0][2] > _BLACKOUT_HEAL, manager_log
    assert denial_rows[0][2] > _BLACKOUT_HEAL, manager_log
    # The frozen decisions land within one heartbeat + sampler tick of
    # the heal.
    assert grant_rows[0][2] <= _BLACKOUT_HEAL + 2.5, manager_log

    _assert_single_grant_then_denial_stream(
        manager_log,
        dispatch_time=_BLACKOUT_START,
        earliest_decision_time=_BLACKOUT_HEAL,
        ceiling=_BASELINE_CEILING,
    )

    # 14 seconds of one-way UDP silence stays far below the death-
    # detection bound: the worker was never reaped.
    assert _rows(manager_log, "worker-lost") == [], manager_log

    # The workload plane rode TCP the whole time: the client observed
    # the VALUE-IDENTICAL timeline of the fault-free baseline (probed:
    # submit 7.70582, completion 37.74582 in both runs).
    submitted = _rows(client_log, "job-submitted")
    finished = _rows(client_log, "job-finished")
    assert submitted == [("job-submitted", 7.70582)], client_log
    assert finished == [
        ("job-finished", "completed", 37.74582)
    ], client_log

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_udp_blackout_extension_freeze_is_replay_deterministic():
    assert _run_extension_udp_blackout() == _run_extension_udp_blackout()


def test_hard_timeout_fires_on_unextended_budget_with_zombie_execution():
    results = _run_hard_timeout()
    manager_log = results["manager"]
    client_log = results["client"]
    worker_log = results["worker"]

    dispatch_rows = _rows(worker_log, "workflow-started")
    assert len(dispatch_rows) == 1, worker_log
    dispatch_time = dispatch_rows[0][2]

    _assert_single_grant_then_denial_stream(
        manager_log,
        dispatch_time=dispatch_time,
        earliest_decision_time=dispatch_time,
        ceiling=_HARD_TIMEOUT_CEILING,
    )

    submitted = _rows(client_log, "job-submitted")
    assert len(submitted) == 1, client_log
    finished = _rows(client_log, "job-finished")
    assert len(finished) == 1 and finished[0][1] == "timeout", client_log
    # The terminal fires on the UNEXTENDED base budget (the 30s grant
    # never reached the job's tracking — the hasattr guard), quantized
    # to the unified-timeout tick.
    terminal_latency = finished[0][2] - submitted[0][1]
    assert (
        _HARD_TIMEOUT_JOB_BUDGET
        <= terminal_latency
        <= _HARD_TIMEOUT_JOB_BUDGET + 2.0 * _TIMEOUT_TICK_SECONDS
    ), client_log
    # Had the grant stretched the budget to 50s, the 60.03 tick could
    # not have fired it (elapsed 44.8 < 50) — the instant itself pins
    # the dead integration.
    assert terminal_latency < 50.0, client_log

    # ZOMBIE EXECUTION: the worker drains only at its natural end,
    # long after the client-observed terminal —
    # ``_cancel_running_workflows`` is invoked by ``_timeout_job`` but
    # the in-flight execution was not interrupted.
    executed = _rows(worker_log, "workflow-executed")
    assert len(executed) == 1, worker_log
    assert (
        _HARD_TIMEOUT_DURATION - 0.5
        <= executed[0][2] - dispatch_time
        <= _HARD_TIMEOUT_DURATION + 1.0
    ), worker_log
    assert executed[0][2] > finished[0][2] + 30.0, (
        "execution should have LONG outlived the terminal (the pinned "
        f"zombie window): {worker_log}"
    )

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_hard_timeout_zombie_execution_is_replay_deterministic():
    assert _run_hard_timeout() == _run_hard_timeout()


@pytest.mark.skip(
    reason=(
        "the autonomous lookahead ExtensionTrigger is dead code: "
        "clear_extension_request has zero callers, so the "
        "dispatch-accepted latch keeps is_extension_pending() True "
        "forever and tick() can never fire (probed: grants stay at "
        "exactly 1 for the whole run, the frozen snapshot denies every "
        "repeat). Unskip when the latch is cleared on decision "
        "processing: a workflow crossing deadline x 0.75 with real "
        "progress must then produce a SECOND grant carrying advanced "
        "progress counters."
    )
)
def test_autonomous_lookahead_trigger_fires_with_progress():
    results = _run_extension_baseline()
    grant_rows = _rows(results["manager"], "ext-grants")
    assert grant_rows[-1][1] >= 2, grant_rows


@pytest.mark.skip(
    reason=(
        "AD-34 Part 10.4.7 extension integration is unreachable: "
        "_notify_timeout_strategies_of_extension gates on "
        'hasattr(strategy, "record_extension") but strategies define '
        "record_worker_extension (server.py ~5428) — job-ext stays 0 "
        "and hard timeouts fire on the unextended base budget (probed: "
        "terminal at 60.03 with a 30s grant on the books). Unskip when "
        "the guard matches the method: total_extensions_granted must "
        "then stretch effective_timeout by each grant."
    )
)
def test_extension_grants_stretch_job_effective_timeout():
    results = _run_hard_timeout()
    job_extension_rows = _rows(results["manager"], "job-ext")
    assert job_extension_rows[-1][1] >= 30.0, job_extension_rows


@pytest.mark.skip(
    reason=(
        "a timed-out job's in-flight execution is not interrupted: "
        "_timeout_job invokes _cancel_running_workflows yet the worker "
        "ran the workflow to its natural end 55 virtual seconds past "
        "the client-observed terminal (probed: terminal 60.03, drain "
        "115.5 on seed 113). Unskip when running-workflow cancellation "
        "actually stops execution: the drain must land within a "
        "bounded window of the terminal, not at natural length."
    )
)
def test_hard_timeout_cancels_in_flight_execution():
    results = _run_hard_timeout()
    finished = _rows(results["client"], "job-finished")
    executed = _rows(results["worker"], "workflow-executed")
    assert executed[0][2] <= finished[0][2] + 10.0, (finished, executed)
