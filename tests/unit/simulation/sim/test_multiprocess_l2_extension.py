"""
K5 — the AD-26 extension surface under real workload (gateless L2),
probed then pinned. The surface is now LIVE end to end: the original
pins documented four dead paths (never-cleared latch -> perpetual
denial stream + dead autonomous trigger; a ``hasattr`` guard that kept
grants from ever stretching the AD-34 budget; a witness-route context
miss that left the H7 ledger empty; cancellation that never stopped
executors), and after the AD-26 repair wave these scenarios pin the
REPAIRED protocol's timelines — with the one sharpened residual
(executor-side graceful cancellation) pinned as loud current truth.

THE REPAIRS THESE PINS STAND ON (each traced live):

* Latch lifecycle: the manager pushes its grant/deny decision back to
  the worker (``extension_response`` TCP endpoint) after processing a
  heartbeat-piggybacked request, and the worker CLEARS its request
  latch on receipt (plus on the workflow's own termination). The
  request is one round-trip, not a permanent flag: the denial stream
  terminates, and the autonomous 0.75-lookahead ``ExtensionTrigger``
  is un-gated (it re-requests only when a progress dimension
  advanced). Delivery is self-healing — a lost response is re-carried
  by the next heartbeat and re-answered.
* Budget stretch: ``_notify_timeout_strategies_of_extension`` calls
  ``record_worker_extension`` directly (the old
  ``hasattr(strategy, "record_extension")`` guard named a method no
  strategy defines — the call was unreachable), so a granted
  extension genuinely stretches the job's AD-34 effective timeout.
* Ledger: ``_lookup_workflow_context`` resolves the SUB-workflow
  token-string id shape the dispatch actually sends (it used to
  compare against bare parent ids and always miss), so piggyback
  requests route through the H5 witness path and the H7
  ``ExtensionLedger`` records every decision (``ext-last-code`` rows).
* Cancellation is REAL, end to end: the worker's cancel path SUBMITS
  cancellation before awaiting terminal reports (it used to
  await-without-initiating — instant vacuous success) with a "2s"
  graceful window sized inside its own 5s report wait; when the
  window expires (the graceful flag only pauses TEST VU generators —
  ACTION workflows ignore it) the executor escalates to
  ``WorkflowRunner.hard_cancel`` (run task + pending VU tasks
  cancelled, completion events forced) and ``run_workflow``'s
  CancelledError path pushes the CANCELLED terminal so the worker's
  awaiting ``execute_workflow`` converges and the workflow leaves
  the active set.
* Grants stretch the LOCAL deadline too: the worker's decision
  receipt extends ``_workflow_timeout_seconds`` by the granted
  seconds, so the stuck-workflow enforcement loop honors the
  extension instead of hard-cancelling at the dispatch-time base
  (which it did the instant hard-cancel became real).

Measured timelines (seed 73 baseline/blackout, seed 113 hard-timeout):

* BASELINE (30s workflow, job timeout 60): dispatch ~7.75, ONE grant
  at 8.5 (+30s, ``job-ext`` 30.0, ledger code ``none``), the decision
  round-trip clears the latch and the denial stream TERMINATES (a
  handful of in-flight-window denials at most, none after the
  workflow drains); completion 37.74582; the trigger never fires
  (0.75 x 60s lookahead exceeds the 30s runtime).
* UDP BLACKOUT [6, 20): heartbeat piggybacks die; the decision stream
  freezes — the grant lands at 20.5 (heal + first heartbeat) — while
  dispatch/execution/completion ride TCP untouched (completion
  37.74582, value-identical to baseline).
* HARD TIMEOUT (100s workflow, job budget 20): grant at 15.5
  stretches BOTH the AD-34 budget and the local workflow deadline to
  50s; the worker's stuck-workflow enforcement cancels at the
  EXTENDED expiry (dispatch 15.25 + 50 = 65.25, next tick), the
  executor hard-stop drains the workflow at 67.5 (~52s of real
  execution — stopped, not natural-length), and the client observes
  the loud ``failed`` at 67.374678, before the AD-34 grid instant
  (90.03) — layered enforcement races, earliest wins, always loud.
  The trigger re-requests on progress (denial at 57.5 — proof it
  lives; the denial stream still terminates with the workflow).
"""

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
_BASELINE_COMPLETION = 37.74582
_BLACKOUT_START = 6.0
_BLACKOUT_HEAL = 20.0
_HARD_TIMEOUT_SEED = 113
_HARD_TIMEOUT_DURATION = 100.0
_HARD_TIMEOUT_JOB_BUDGET = 20.0
_HARD_TIMEOUT_CEILING = 200.0
_TIMEOUT_TICK_SECONDS = 30.0
_GRANT_SECONDS = 30.0


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


def _assert_repaired_grant_shape(
    manager_log: list,
    dispatch_time: float,
    earliest_decision_time: float,
    workflow_drain_time: float,
) -> float:
    """The repaired protocol's shape: exactly ONE 30s grant whose
    stretch reaches the AD-34 budget and whose decision is LEDGERED;
    the denial stream (if any heartbeats raced the latch-clear
    round-trip) is finite and DIES with the workflow. Returns the
    grant instant."""
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
        (_GRANT_SECONDS, grant_time),
    ], manager_log

    # The stretch REACHES the job budget (the hasattr fix): job-ext
    # rises to the grant seconds while the job lives (and drops back
    # only when the job's tracking state is cleaned up).
    job_extension_rows = _rows(manager_log, "job-ext")
    assert (_GRANT_SECONDS, grant_time) in [
        (row[1], row[2]) for row in job_extension_rows
    ], manager_log

    # The H7 ledger RECORDS (the witness-route id fix): the grant's
    # decision code lands as ``none``.
    ledger_rows = _rows(manager_log, "ext-last-code")
    assert ledger_rows and ledger_rows[0][1] == "none", manager_log

    # The denial stream terminates with the latch round-trip: finite,
    # and NOTHING after the workflow drained (the old behavior ran
    # ~1/s of denials to the ceiling, outliving workflow and job).
    denial_rows = [row for row in _rows(manager_log, "ext-denials") if row[1] > 0]
    if denial_rows:
        assert denial_rows[-1][1] <= 5, manager_log
        assert denial_rows[-1][2] <= workflow_drain_time + 2.0, manager_log
    return grant_time


def test_extension_round_trip_grants_once_and_denial_stream_terminates():
    results = _run_extension_baseline()
    manager_log = results["manager"]
    client_log = results["client"]
    worker_log = results["worker"]

    dispatch_rows = _rows(worker_log, "workflow-started")
    assert len(dispatch_rows) == 1, worker_log
    dispatch_time = dispatch_rows[0][2]
    drain_rows = _rows(worker_log, "workflow-executed")
    assert len(drain_rows) == 1, worker_log

    _assert_repaired_grant_shape(
        manager_log,
        dispatch_time=dispatch_time,
        earliest_decision_time=dispatch_time,
        workflow_drain_time=drain_rows[0][2],
    )

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log
    assert abs(finished[0][2] - _BASELINE_COMPLETION) <= 1.0, client_log

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_extension_baseline_is_replay_deterministic():
    assert _run_extension_baseline() == _run_extension_baseline()


def test_udp_blackout_freezes_extension_decisions_until_heal():
    """Heartbeat piggybacks are datagrams: a total worker->manager UDP
    cut freezes the DECISION stream (grant lands at heal + first
    heartbeat) while dispatch/execution/completion ride TCP untouched
    — the client timeline is value-identical to baseline."""
    results = _run_extension_udp_blackout()
    manager_log = results["manager"]
    client_log = results["client"]

    grant_rows = _rows(manager_log, "ext-grants")
    assert [row[1] for row in grant_rows] == [0, 1], manager_log
    grant_time = grant_rows[1][2]
    assert _BLACKOUT_HEAL <= grant_time <= _BLACKOUT_HEAL + 2.5, manager_log

    # The stretch and the ledger record still land, just late.
    assert (_GRANT_SECONDS, grant_time) in [
        (row[1], row[2]) for row in _rows(manager_log, "job-ext")
    ], manager_log
    ledger_rows = _rows(manager_log, "ext-last-code")
    assert ledger_rows and ledger_rows[0][1] == "none", manager_log

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log
    assert abs(finished[0][2] - _BASELINE_COMPLETION) <= 1.0, client_log

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_udp_blackout_extension_freeze_is_replay_deterministic():
    assert _run_extension_udp_blackout() == _run_extension_udp_blackout()


def test_hard_timeout_enforced_at_extended_budget():
    """The grant stretches BOTH deadline layers, and enforcement is
    REAL: with a 20s budget and one 30s grant, the workflow's local
    (worker-side) deadline extends to dispatch + 50s, the stuck-
    workflow loop cancels at the extended expiry — never the base one
    — and the executor hard-stop actually stops the in-flight run.
    Measured: dispatch 15.25, grant 15.5 (+30s -> local expiry 65.25),
    worker enforcement cancels on its next tick, the 100s workflow
    DRAINS at 67.5 (~52s of real execution — hard-stopped, not
    natural-length), and the client observes the loud ``failed`` at
    67.374678 (the workflow-cancelled -> job-failed path, arriving
    BEFORE the AD-34 grid instant at 90.03 — layered enforcement
    races, earliest wins, always loud). Pre-repair this scenario was
    the zombie pin: cancellation was vacuous (await-without-initiate,
    no hard-stop, un-stretched local deadline) and the workflow ran
    its full 100s to 115.5, ~25.5 virtual seconds past the terminal.
    """
    results = _run_hard_timeout()
    manager_log = results["manager"]
    client_log = results["client"]
    worker_log = results["worker"]

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert len(submitted) == 1, client_log
    submitted_time = submitted[0][1]

    grant_rows = _rows(manager_log, "ext-grants")
    assert [row[1] for row in grant_rows] == [0, 1], manager_log
    assert (_GRANT_SECONDS, grant_rows[1][2]) in [
        (row[1], row[2]) for row in _rows(manager_log, "job-ext")
    ], manager_log

    dispatch_rows = _rows(worker_log, "workflow-started")
    assert len(dispatch_rows) == 1, worker_log
    dispatch_time = dispatch_rows[0][2]
    extended_local_expiry = (
        dispatch_time + _HARD_TIMEOUT_JOB_BUDGET + _GRANT_SECONDS
    )

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "failed", client_log
    # Terminal rides the LOCAL extended deadline + one enforcement
    # tick + cancel/report slack — and must land BEFORE the AD-34
    # grid instant would have fired (proof the local layer, not the
    # job timeout, decided this run).
    ad34_grid_instant = (
        submitted_time
        + _HARD_TIMEOUT_JOB_BUDGET
        + _GRANT_SECONDS
        + _TIMEOUT_TICK_SECONDS
    )
    assert (
        extended_local_expiry
        <= finished[0][2]
        < ad34_grid_instant
    ), (
        f"terminal at {finished[0][2]} outside the extended-local-"
        f"enforcement bound [{extended_local_expiry}, {ad34_grid_instant}): "
        f"{client_log}"
    )
    # The BASE local deadline (dispatch + 20s = 35.25) must NOT have
    # fired: pre-repair the un-stretched enforcement killed the
    # workflow there.
    assert finished[0][2] > dispatch_time + _HARD_TIMEOUT_JOB_BUDGET + 10.0, (
        client_log
    )

    # HARD-STOP: the workflow drains promptly after the terminal —
    # never at its 100s natural length (the old zombie signature).
    drain_rows = [
        entry
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] == 0 and entry[2] > 0.0
    ]
    assert drain_rows, worker_log
    assert drain_rows[0][2] <= finished[0][2] + 5.0, (
        "worker drained late — the executor hard-stop regressed toward "
        f"natural-length zombie execution: {worker_log}"
    )
    assert drain_rows[0][2] < dispatch_time + _HARD_TIMEOUT_DURATION - 10.0, (
        worker_log
    )

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_hard_timeout_enforcement_is_replay_deterministic():
    assert _run_hard_timeout() == _run_hard_timeout()


def test_autonomous_lookahead_trigger_fires_with_progress():
    """The un-gated trigger: on the hard-timeout run (deadline 20s,
    lookahead 15s) the worker's autonomous trigger re-requests after
    the dispatch-time round-trip cleared the latch — visible as
    decision traffic strictly AFTER the initial grant settled (a
    second latch cycle at ~31.5 in the probe). The manager denies it
    (no budget for a second grant without more progress), which is
    itself proof the trigger LIVES — the old latch never cleared, so
    no second request could ever exist."""
    results = _run_hard_timeout()
    manager_log = results["manager"]

    denial_rows = [
        row for row in _rows(manager_log, "ext-denials") if row[1] > 0
    ]
    grant_rows = _rows(manager_log, "ext-grants")
    grant_time = grant_rows[1][2]
    late_decisions = [row for row in denial_rows if row[2] >= grant_time + 10.0]
    assert late_decisions, (
        "no post-grant extension traffic — the autonomous trigger is "
        f"gated again: {manager_log}"
    )


def test_hard_timeout_cancels_in_flight_execution():
    """FIXED-BUG PIN (the zombie-execution residual): cancellation now
    STOPS in-flight work. The chain that was dead end to end — the
    worker's cancel path initiates executor cancellation (it used to
    await-without-initiating: instant vacuous success), the graceful
    window ("2s", inside the worker's 5s terminal-report wait) expires
    for ACTION workflows (the graceful flag only pauses TEST VU
    generators), the executor escalates to ``WorkflowRunner.
    hard_cancel`` (cancels the run task + pending VU tasks, forces the
    completion events so awaiters converge), and ``run_workflow``'s
    CancelledError path pushes the CANCELLED terminal so the worker's
    ``execute_workflow`` returns and the workflow LEAVES the active
    set. Measured: drain at 67.5 vs client terminal 67.374678 — the
    pre-repair zombie drained at 115.5, a full natural length later."""
    results = _run_hard_timeout()
    client_log = results["client"]
    worker_log = results["worker"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    drain_rows = [
        entry
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] == 0 and entry[2] > 0.0
    ]
    assert drain_rows[0][2] <= finished[0][2] + 10.0, worker_log
