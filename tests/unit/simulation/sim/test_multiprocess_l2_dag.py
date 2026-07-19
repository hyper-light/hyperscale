"""
K3 — dependent workflow DAGs across fault boundaries (gateless L2),
probed then pinned — and the probe found the surface BROKEN at the
first step, so these scenarios pin today's LOUD degraded truth.

THE TRACED GAP (dependent workflows never dispatch): the submission
API carries ``[([], SimDagLongA), (["SimDagLongA"], SimDagShortB)]``
and the manager's ``WorkflowDispatcher`` registers B pending on A —
but the completion edge NEVER fires:

* ``ManagerServer`` constructs ``JobManager`` with no
  ``on_workflow_completed`` callback (``server.py`` ~line 431) and
  nothing in the tree calls ``set_on_workflow_completed``;
* ``JobManager.mark_workflow_completed`` therefore no-ops its
  event-driven-dispatch notification (``job_manager.py`` ~line 1315);
* ``WorkflowDispatcher.mark_workflow_completed`` — the SOLE writer of
  ``PendingWorkflow.completed_dependencies`` — has ZERO production
  callers, so the dispatch loop's readiness check
  (``dependencies <= completed_dependencies``) can never pass for a
  dependent workflow.

Consequence, measured on three seeds: A executes fully, B never
starts, and the job strands until the manager's AD-34 unified-timeout
loop (30s tick) kills it with a client-observed ``timeout`` — loud,
never silent (G2 holds), but the DAG surface is functionally dead.
The aspirational end-to-end pin is skip-marked below per the
do-not-write-failing-tests rule.

Measured timelines (probe scripts in the L2 workload series):

* CLEAN (seed 79, A=20s B=2s, job timeout 60): submit 17.945249,
  A executes 18.0 -> 38.0, B never starts, client ``timeout`` at
  90.03 (the third 30s tick: first tick with elapsed > 60).
* WORKER KILL mid-A (seed 83, kill worker + both executors at 12.0;
  A started 8.5): SWIM detection reaps the worker at 80.0 (latency
  68.0, inside the [20, 70] host-death bound), the dead-worker path
  fails A -> fails the job: client ``failed`` at 79.81 — BEFORE the
  90.03 AD-34 tick would have fired. B's documented outcome is the
  whole-job FAILED terminal (it could never dispatch anyway).
* MANAGER RESTART mid-A (seed 89, power loss at 22.0, down 30; A
  started 17.75): gen-2 boots 52.0, resumes the persisted submission
  and RE-DISPATCHES A at 52.25 while the original A — completed at
  ~37.75 but unable to deliver its result into the dead manager —
  still occupies the active set (workflows-active reaches 2: the
  at-least-once execution signature). A's re-run completes 72.5; B
  strands AGAIN on the recovered manager, and gen-2's own tracking
  times the job out at 142.03 (= gen-2 job clock + 60 + tick
  quantization: the 112.03 tick misses by 0.004s of elapsed). The
  client's 120s wait expires loudly (``wait-timed-out`` 137.730869)
  and the unbounded re-wait then observes the terminal — exactly-once.
"""

import pytest

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.l2_workload_demo import (
    dag_client_entry,
    dag_worker_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
)
from tests.simulation.oracle import ClusterTraceOracle, JobStatusOracle

_LONG_A_SECONDS = 20.0
_SHORT_B_SECONDS = 2.0
_JOB_TIMEOUT_SECONDS = 60.0
# AD-34 unified-timeout loop cadence (manager config
# ``job_timeout_check_interval_seconds``): terminal instants are
# tick-quantized, so bounds allow up to one full tick past expiry.
_TIMEOUT_TICK_SECONDS = 30.0

_KILL_SEED = 83
_KILL_AT = 12.0
_RESTART_SEED = 89
_RESTART_AT = 22.0
_RESTART_DOWN_SECONDS = 30.0
_GEN2_BOOT = _RESTART_AT + _RESTART_DOWN_SECONDS


def _build_dag_topology(
    seed: int, ceiling: float, wait_timeout_seconds: float
) -> SimulationCoordinator:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=ceiling, seed=seed
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
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
        dag_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _LONG_A_SECONDS,
        _SHORT_B_SECONDS,
        1,
        _JOB_TIMEOUT_SECONDS,
        wait_timeout_seconds,
    )
    return coordinator


def _run_clean_dag() -> dict:
    return _build_dag_topology(
        seed=79, ceiling=120.0, wait_timeout_seconds=90.0
    ).run()


def _run_dag_worker_kill() -> dict:
    coordinator = _build_dag_topology(
        seed=_KILL_SEED, ceiling=200.0, wait_timeout_seconds=120.0
    )
    coordinator.schedule_kill("worker", at_time=_KILL_AT)
    coordinator.schedule_kill("executor-sim-wkr-9009", at_time=_KILL_AT)
    coordinator.schedule_kill("executor-sim-wkr-9011", at_time=_KILL_AT)
    return coordinator.run()


def _run_dag_manager_restart() -> dict:
    coordinator = _build_dag_topology(
        seed=_RESTART_SEED, ceiling=260.0, wait_timeout_seconds=120.0
    )
    coordinator.schedule_restart(
        "manager", _RESTART_AT, down_seconds=_RESTART_DOWN_SECONDS
    )
    return coordinator.run()


def _assert_b_never_started(worker_log: list) -> None:
    """The dependent workflow must show NO execution evidence — the
    pinned strand. Any SimDagShortB appearance means the dependency
    edge started firing (flip the aspirational test below instead)."""
    b_rows = [
        entry
        for entry in worker_log
        if entry[0] in ("workflow-started", "workflow-executed")
        and entry[1] == "SimDagShortB"
    ]
    assert b_rows == [], (
        "SimDagShortB executed — the dependent-dispatch gap has been "
        f"fixed; update these pins and unskip the aspirational test: "
        f"{worker_log}"
    )


def test_dependent_workflow_strands_and_job_times_out_loudly():
    results = _run_clean_dag()
    client_log = results["client"]
    worker_log = results["worker"]

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert len(submitted) == 1, client_log
    submitted_time = submitted[0][1]

    # A executed exactly once, for its full window.
    a_started = [
        entry
        for entry in worker_log
        if entry[0] == "workflow-started" and entry[1] == "SimDagLongA"
    ]
    a_executed = [
        entry
        for entry in worker_log
        if entry[0] == "workflow-executed" and entry[1] == "SimDagLongA"
    ]
    assert len(a_started) == 1 and len(a_executed) == 1, worker_log
    execution_span = a_executed[0][2] - a_started[0][2]
    assert (
        _LONG_A_SECONDS - 0.5 <= execution_span <= _LONG_A_SECONDS + 1.0
    ), worker_log

    _assert_b_never_started(worker_log)
    # Never more than one workflow in flight: with 2 cores and vus=1
    # per workflow, a second active workflow would mean B dispatched.
    active_counts = [
        entry[1] for entry in worker_log if entry[0] == "workflows-active"
    ]
    assert max(active_counts) == 1, worker_log

    # The strand ends in the AD-34 tick-quantized LOUD timeout.
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "timeout", client_log
    terminal_latency = finished[0][2] - submitted_time
    assert (
        _JOB_TIMEOUT_SECONDS
        <= terminal_latency
        <= _JOB_TIMEOUT_SECONDS + _TIMEOUT_TICK_SECONDS + 0.5
    ), client_log

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    trace_oracle = ClusterTraceOracle(
        worker_process_ids_by_datacenter={"sim-dc": ("worker",)},
        retry_budget=1,
    )
    assert trace_oracle.check_workflow_execution(results) == [], results
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_clean_dag_strand_is_replay_deterministic():
    assert _run_clean_dag() == _run_clean_dag()


def test_worker_kill_mid_dependency_fails_job_before_ad34_tick():
    results = _run_dag_worker_kill()
    client_log = results["client"]

    # The whole worker host is gone: no result rows for any of it —
    # execution evidence for the kill run is legitimately erased.
    assert "worker" not in results
    assert "executor-sim-wkr-9009" not in results
    assert "executor-sim-wkr-9011" not in results

    manager_log = results["manager"]
    lost = [entry for entry in manager_log if entry[0] == "worker-lost"]
    assert len(lost) == 1, manager_log
    detection_latency = lost[0][1] - _KILL_AT
    # Host-death detection design bound (same bracket the committed
    # worker-kill scenario asserts): sustained SWIM silence, never
    # instant, never unbounded.
    assert 20.0 <= detection_latency <= 70.0, (
        f"death detection latency {detection_latency}s outside the "
        f"design bound: {manager_log}"
    )

    # B's documented outcome under a mid-A fault: the dead-worker path
    # fails A and with it the WHOLE JOB, loudly, BEFORE the AD-34 tick
    # (measured 79.81 < 90.03) — not a completion, never silence.
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "failed", client_log
    assert _KILL_AT + 20.0 < finished[0][2] <= _KILL_AT + 70.0, client_log

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    trace_oracle = ClusterTraceOracle(
        worker_process_ids_by_datacenter={"sim-dc": ("worker",)},
    )
    assert (
        trace_oracle.check_workflow_execution(
            results,
            killed_process_ids=(
                "worker",
                "executor-sim-wkr-9009",
                "executor-sim-wkr-9011",
            ),
        )
        == []
    ), results
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_worker_kill_mid_dependency_is_replay_deterministic():
    assert _run_dag_worker_kill() == _run_dag_worker_kill()


def test_manager_restart_mid_dependency_reexecutes_a_and_restrands_b():
    results = _run_dag_manager_restart()
    client_log = results["client"]
    worker_log = results["worker"]

    # Generations: gen-1 registered the worker before the power loss;
    # the final generation re-registered it after reboot.
    assert any(
        entry[0] == "worker-registered" for entry in results["manager.gen1"]
    ), results["manager.gen1"]
    final_manager_log = results["manager"]
    gen2_started = [
        entry for entry in final_manager_log if entry[0] == "manager-started"
    ]
    assert len(gen2_started) == 1, final_manager_log
    assert gen2_started[0][1] >= _GEN2_BOOT, final_manager_log

    # At-least-once across the restart: the resumed submission
    # re-dispatches A while the original A — completed into the dead
    # manager, result undeliverable — still occupies the active set:
    # the sampled count reaches 2, and only AFTER gen-2 boots.
    active_transitions = [
        entry for entry in worker_log if entry[0] == "workflows-active"
    ]
    peak_active = max(entry[1] for entry in active_transitions)
    assert peak_active == 2, worker_log
    first_double_active = [
        entry for entry in active_transitions if entry[1] == 2
    ][0]
    assert first_double_active[2] >= _GEN2_BOOT, worker_log

    # B strands AGAIN on the recovered manager: resume re-registers
    # the DAG, the completion edge still never fires.
    _assert_b_never_started(worker_log)

    # The client's bounded wait expires LOUDLY, the unbounded re-wait
    # then observes gen-2's own AD-34 terminal — exactly once.
    assert any(
        entry[0] == "wait-timed-out" for entry in client_log
    ), client_log
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "timeout", client_log
    # Gen-2's job clock starts at its resume; the terminal is
    # tick-quantized off that clock (measured 142.03: the 112.03 tick
    # misses expiry by 0.004s of elapsed time).
    assert (
        _GEN2_BOOT + _JOB_TIMEOUT_SECONDS
        <= finished[0][2]
        <= _GEN2_BOOT + _JOB_TIMEOUT_SECONDS + 2.0 * _TIMEOUT_TICK_SECONDS
    ), client_log

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    trace_oracle = ClusterTraceOracle(
        worker_process_ids_by_datacenter={"sim-dc": ("worker",)},
        retry_budget=1,
    )
    assert trace_oracle.check_workflow_execution(results) == [], results
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_manager_restart_mid_dependency_is_replay_deterministic():
    assert _run_dag_manager_restart() == _run_dag_manager_restart()


@pytest.mark.skip(
    reason=(
        "dependent-workflow dispatch is dead code today: ManagerServer "
        "builds JobManager without on_workflow_completed (server.py "
        "~431), set_on_workflow_completed has zero callers, and "
        "WorkflowDispatcher.mark_workflow_completed — the sole writer "
        "of PendingWorkflow.completed_dependencies — is never invoked; "
        "B strands until AD-34 kills the job (probed: timeout at 90.03 "
        "on seeds 79/83/89). Unskip when the completion edge is wired: "
        "B must then start AFTER A's terminal instant, never overlap "
        "it, and the whole job must complete exactly once."
    )
)
def test_dependent_dag_completes_in_order():
    results = _run_clean_dag()
    worker_log = results["worker"]
    a_executed = [
        entry
        for entry in worker_log
        if entry[0] == "workflow-executed" and entry[1] == "SimDagLongA"
    ]
    b_started = [
        entry
        for entry in worker_log
        if entry[0] == "workflow-started" and entry[1] == "SimDagShortB"
    ]
    assert a_executed and b_started, worker_log
    # The dependency gate, not core contention, must order B after A
    # (2 cores, vus=1 each — cores were free the whole time).
    assert b_started[0][2] >= a_executed[0][2] - 0.25, worker_log
    finished = [
        entry for entry in results["client"] if entry[0] == "job-finished"
    ]
    assert finished == [("job-finished", "completed", finished[0][2])]
