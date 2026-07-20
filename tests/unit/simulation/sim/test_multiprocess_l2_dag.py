"""
K3 — dependent workflow DAGs across fault boundaries (gateless L2),
probed then pinned. THE DEPENDENCY EDGE IS NOW LIVE: these scenarios
originally pinned the dead surface (``on_workflow_completed`` was never
wired, so B stranded until AD-34 killed the job); after the completion-
edge wiring plus the workflow-duration/overload fix, the DAG lifecycle
runs end to end and these scenarios pin ITS timelines instead.

THE TWO FIXES THESE PINS STAND ON (both traced live):

* Completion-edge wiring: ``ManagerServer`` registers
  ``_handle_workflow_terminal_for_dispatch`` as the ``JobManager``'s
  ``on_workflow_completed`` callback — success unblocks dependents via
  ``WorkflowDispatcher.mark_workflow_completed`` (the sole writer of
  ``completed_dependencies``); failure cascade-fails every transitive
  dependent in the dispatcher AND mirrors each at the job level, so a
  never-dispatchable dependent counts toward ``workflows_failed`` and
  the job reaches a truthful terminal promptly.
* Duration-is-not-latency: the worker used to record each workflow's
  ENTIRE DURATION as an overload-detector latency sample; any workflow
  over 2s (the absolute overload bound) flipped its worker to
  ``overloaded`` at drain — permanently, since a worker the manager
  stops routing to never produces another sample. B's dispatch then
  starved in ``allocate_cores`` against a full-capacity-but-
  "overloaded" worker (traced: allocation returned empty for the full
  30s timeout with 2 free cores and routing ROUTE). With the sample
  removed, the worker stays HEALTHY and B dispatches the instant its
  dependency completes.

Measured timelines (seed 79/83/89, A=20s B=2s, job timeout 60):

* CLEAN (seed 79): submit 17.945249, A executes [18.0, 38.0], B
  dispatches ON the completion edge (worker-started 38.0 sample ->
  B active by 38.25), B executes ~[38.25, 40.25], client observes
  ``completed`` at 40.025249 — dependency ordering enforced by the
  edge, not by core contention (2 cores, vus=1 each: cores were free
  for B the whole time A ran).
* WORKER KILL mid-A (seed 83, kill worker + both executors at 12.0):
  SWIM death detection inside the [20, 70] host-death bound, the
  dead-worker path fails A -> the cascade fails B at the job level ->
  the whole job reaches a LOUD ``failed`` terminal in one sweep.
* MANAGER RESTART mid-A (seed 89, power loss at 22.0, down 30): gen-2
  boots 52.0, resumes the persisted submission and re-dispatches A
  (at-least-once: the first A completed into the dead manager,
  undeliverable); A's re-run completes, the edge fires on gen-2, B
  dispatches and completes — exactly-once client outcome ACROSS a
  manager reboot with a live dependency graph.
"""

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

_CLEAN_SEED = 79
# Probed: A executes [18.0, 38.0]; B rides the completion edge and the
# client sees completed at 40.025249 (= A-drain + B duration + push).
_CLEAN_COMPLETION = 40.025249
_CLEAN_COMPLETION_CEILING = 42.0

_KILL_SEED = 83
_KILL_AT = 12.0
_RESTART_SEED = 89
_RESTART_AT = 22.0
_RESTART_DOWN_SECONDS = 30.0
_GEN2_BOOT = _RESTART_AT + _RESTART_DOWN_SECONDS  # 52.0


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
        seed=_CLEAN_SEED, ceiling=120.0, wait_timeout_seconds=90.0
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


def _workflow_rows(worker_log: list, tag: str, name: str) -> list[tuple]:
    return [
        entry
        for entry in worker_log
        if entry[0] == tag and entry[1] == name
    ]


def test_dependent_dag_completes_in_order():
    """The K3 mission invariant, LIVE: B must start only after A's
    terminal instant — gated by the dependency edge, not core
    contention (cores were free throughout A's run) — and the whole
    job completes exactly once at the probed instant."""
    results = _run_clean_dag()
    client_log = results["client"]
    worker_log = results["worker"]

    a_executed = _workflow_rows(worker_log, "workflow-executed", "SimDagLongA")
    b_started = _workflow_rows(worker_log, "workflow-started", "SimDagShortB")
    b_executed = _workflow_rows(worker_log, "workflow-executed", "SimDagShortB")
    assert len(a_executed) == 1, worker_log
    assert len(b_started) == 1 and len(b_executed) == 1, worker_log

    # Ordering: B's start is sampled at-or-after A's drain sample (the
    # 0.25s watcher may catch both transitions in one sample window).
    assert b_started[0][2] >= a_executed[0][2] - 0.25, worker_log

    # B rides the edge IMMEDIATELY — a start later than one dispatch
    # round past A's drain would mean the edge regressed to polling.
    assert b_started[0][2] <= a_executed[0][2] + 2.0, worker_log

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log
    assert (
        _CLEAN_COMPLETION - 0.5
        <= finished[0][2]
        <= _CLEAN_COMPLETION_CEILING
    ), (
        f"DAG completion at {finished[0][2]} outside the design bound "
        f"(measured {_CLEAN_COMPLETION}): {client_log}"
    )

    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    trace_oracle = ClusterTraceOracle(
        worker_process_ids_by_datacenter={"sim-dc": ("worker",)},
        retry_budget=1,
    )
    assert trace_oracle.check_workflow_execution(results) == [], results
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_clean_dag_is_replay_deterministic():
    assert _run_clean_dag() == _run_clean_dag()


def test_worker_kill_mid_dependency_cascade_fails_job_loudly():
    """A fails on worker death inside the detection bound; the cascade
    must fail B at the JOB level in the same sweep — a LOUD ``failed``
    terminal, never a strand to AD-34 for work that deterministically
    cannot run."""
    results = _run_dag_worker_kill()
    client_log = results["client"]

    assert "worker" not in results
    assert "executor-sim-wkr-9009" not in results
    assert "executor-sim-wkr-9011" not in results

    manager_log = results["manager"]
    lost = [entry for entry in manager_log if entry[0] == "worker-lost"]
    assert len(lost) == 1, manager_log
    detection_latency = lost[0][1] - _KILL_AT
    assert 20.0 <= detection_latency <= 70.0, (
        f"death detection latency {detection_latency}s outside the "
        f"design bound: {manager_log}"
    )

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "failed", client_log
    # The terminal rides death detection (+ failure sweep), never the
    # AD-34 job timeout: it must land inside the detection bracket's
    # tail, well before submit + 60s + a 30s tick could fire.
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


def test_manager_restart_mid_dependency_completes_dag_on_gen2():
    """Manager power loss mid-A: gen-2 resumes the persisted
    submission, A re-executes (at-least-once — the first run's result
    died with gen-1), the dependency edge fires ON THE RECOVERED
    manager, B dispatches and completes: exactly-once client outcome
    across a reboot with a live dependency graph."""
    results = _run_dag_manager_restart()
    client_log = results["client"]
    worker_log = results["worker"]

    assert any(
        entry[0] == "worker-registered" for entry in results["manager.gen1"]
    ), results["manager.gen1"]
    final_manager_log = results["manager"]
    gen2_started = [
        entry for entry in final_manager_log if entry[0] == "manager-started"
    ]
    assert len(gen2_started) == 1, final_manager_log
    assert gen2_started[0][1] >= _GEN2_BOOT, final_manager_log

    # At-least-once for A across the reboot: the re-dispatched instance
    # overlaps the first (whose result died with gen-1, still occupying
    # the active set), so the sampled active count reaches 2 strictly
    # after gen-2's boot. The NAME watcher logs no second
    # workflow-started row — the name never left the active set.
    a_started = _workflow_rows(worker_log, "workflow-started", "SimDagLongA")
    assert len(a_started) == 1, worker_log
    assert a_started[0][2] < _RESTART_AT, worker_log
    double_active = [
        entry
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] == 2
    ]
    assert double_active and double_active[0][2] >= _GEN2_BOOT, worker_log

    # B rides the edge on gen-2, exactly once, after A's re-run drains.
    a_executed = _workflow_rows(worker_log, "workflow-executed", "SimDagLongA")
    b_started = _workflow_rows(worker_log, "workflow-started", "SimDagShortB")
    assert len(b_started) == 1, worker_log
    assert b_started[0][2] >= a_executed[-1][2] - 0.25, worker_log

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log
    # Design bound: gen-2 boot + resume re-dispatch + A's full 20s +
    # B's 2s + push/apply slack.
    assert (
        _GEN2_BOOT + _LONG_A_SECONDS
        < finished[0][2]
        <= _GEN2_BOOT + _LONG_A_SECONDS + _SHORT_B_SECONDS + 6.0
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
