"""
AD-54 workflow lifecycle, observed end to end under multi-process SIM.

The manager's ``JobManager`` records every workflow's lifecycle in its
``WorkflowLifecycleStateMachine``. These scenarios run real jobs through
a real manager and workers and judge the recorded histories with
``WorkflowLifecycleOracle``: every applied edge in the AD-54 table and
none refused, continuous histories, nothing after an absorbing state, an
observable FAILED only on the retry path's same-instant chain, status
equal to the state's projection at every sample, and every record
released with its job (on a job leader, at completion).

* CLEAN: one ping workflow runs PENDING -> DISPATCHED -> RUNNING ->
  COMPLETED.
* WORKER LOSS: the only worker dies while running the workflow and a
  second worker joins later (the worker-retry topology): the workflow
  fails on the lost worker, takes the retry chain FAILED ->
  FAILED_CANCELING_DEPENDENTS -> FAILED_READY_FOR_RETRY -> PENDING at one
  instant, and completes on the late worker.
* DAG: ``SimDagShortB`` depends on ``SimDagLongA``; B leaves PENDING only
  after A completed.
* DAG, A's EXECUTOR KILLED mid-run: the worker fails A (a pool exit fails
  its active workflows); A ends FAILED and the cascade fails B straight
  from PENDING -- B never dispatches -- and the job ends ``failed``
  before its timeout could.
* DISPATCH EXHAUSTION: the only worker refuses the workflow itself and the
  job allows one retry per workflow (AD-44): the first refusal spends the
  budget and the workflow takes the retry chain back to PENDING; the
  second ends it FAILED, and the job fails with the cause promptly --
  never at its AD-34 timeout.
* DAG, A's WORKER LOST: the worker running A dies; A takes the retry chain
  onto a worker that joined later and completes there, and B leaves
  PENDING only after A's retry completed.
* LOSSES SPEND THE BUDGET: with one retry allowed per workflow, losing the
  worker that runs it is charged (an unexplained death): the first loss
  retries it, the second fails it for good, loudly, with the cause.
* DAG CANCELLED mid-A: A turns CANCELLING and is CANCELLED once its worker
  stopped it; B, waiting on A, goes straight from PENDING to CANCELLED;
  the client hears the cancellation complete exactly once.
* A LONG JOB MAKING PROGRESS IS NOT STUCK (AD-34): a workflow completing
  actions every half second runs five times the stuck threshold and
  completes -- progress is the work advancing, not only lifecycle moves or
  extensions (a job past the threshold used to be timed out as stuck).

Every scenario also ends with the dispatcher holding nothing for the job:
the one job teardown releases its queue entries and dispatch loop with
the job. Each scenario has a replay twin.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.l2_workload_demo import (
    dag_client_entry,
    dag_worker_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)
from tests.simulation.harness.sim.multiprocess.workflow_lifecycle_demo import (
    budgeted_client_entry,
    dag_cancelling_client_entry,
    lifecycle_manager_entry,
    refusing_worker_entry,
    steady_client_entry,
)
from tests.simulation.oracle import WorkflowLifecycleOracle

_CLEAN_SEED = 23
_CLEAN_CEILING = 40.0

_RETRY_SEED = 23
_RETRY_CEILING = 90.0
# 0.25 after worker-a's activation (probed 7.5, 2026-10-04; 11.75 while a
# lone manager waited out a full pre-vote and vote wait for a majority its
# own vote already made); worker-b starts 13s after the loss, as before.
_KILL_AT = 7.75
_WORKER_B_START = _KILL_AT + 13.0

_DAG_SEED = 79
_DAG_CEILING = 120.0
_LONG_A_SECONDS = 20.0
_SHORT_B_SECONDS = 2.0
_DAG_JOB_TIMEOUT_SECONDS = 60.0
_DAG_WAIT_TIMEOUT_SECONDS = 90.0
# Mid-A, 7s in as before: the clean DAG runs A over [1.75, 21.75]
# (probed 2026-10-04; [18.0, 38.0] while a lone manager waited out its
# election).
_DAG_A_STARTS_AT = 1.75
_EXECUTOR_KILL_AT = _DAG_A_STARTS_AT + 7.0

_EXHAUSTION_SEED = 23
_EXHAUSTION_CEILING = 60.0
_EXHAUSTION_RETRY_BUDGET_PER_WORKFLOW = 1
_EXHAUSTION_WORKFLOW_SECONDS = 2.0
# The AD-34 deadline the failure must beat by far.
_EXHAUSTION_JOB_TIMEOUT_SECONDS = 50.0

# A's worker dies mid-A; the retry target joins after A dispatched. A
# starts at 3.5 with two workers (probed 2026-10-04); worker-b joins 2s
# and the loss lands 7s into A, as before.
_DAG_LOSS_CEILING = 260.0
_DAG_LOSS_A_STARTS_AT = 3.5
_DAG_LOSS_WORKER_B_START = _DAG_LOSS_A_STARTS_AT + 2.0
_DAG_LOSS_KILL_AT = _DAG_LOSS_A_STARTS_AT + 7.0
_DAG_LOSS_JOB_TIMEOUT_SECONDS = 200.0
_DAG_LOSS_WAIT_TIMEOUT_SECONDS = 230.0

# Cancel the DAG five seconds after it is seen running: mid-A.
_DAG_CANCEL_CEILING = 90.0
_DAG_CANCEL_AFTER_RUNNING_SECONDS = 5.0
_DAG_CANCEL_OBSERVE_SECONDS = 20.0

# A steady workflow five times longer than the stuck threshold, checked
# every two seconds.
_STEADY_SEED = 23
_STEADY_STUCK_THRESHOLD_SECONDS = 10.0
_STEADY_TIMEOUT_CHECK_INTERVAL_SECONDS = 2.0
_STEADY_WORKFLOW_SECONDS = 50.0
_STEADY_JOB_TIMEOUT_SECONDS = 150.0
_STEADY_CEILING = 120.0

# Two losses of the worker running the workflow, one retry allowed.
_LOSSES_SEED = 23
_LOSSES_CEILING = 600.0
_LOSSES_RETRY_BUDGET_PER_WORKFLOW = 1
_LOSSES_WORKFLOW_SECONDS = 300.0
_LOSSES_JOB_TIMEOUT_SECONDS = 560.0
_LOSSES_WORKER_B_START = 20.0
_LOSSES_FIRST_KILL_AT = 30.0

_RETRY_CHAIN = ["failed", "failed_canceling_deps", "failed_ready", "pending"]


def _run_clean() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CLEAN_CEILING, seed=_CLEAN_SEED
    )
    coordinator.add_process(
        "manager", lifecycle_manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )
    return coordinator.run()


def _run_worker_loss() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_RETRY_CEILING, seed=_RETRY_SEED
    )
    coordinator.add_process(
        "manager", lifecycle_manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker-a", worker_entry, "sim-wkr-a", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "worker-b",
        worker_entry,
        "sim-wkr-b",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
        _WORKER_B_START,
    )
    coordinator.add_process(
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )
    coordinator.schedule_kill("worker-a", at_time=_KILL_AT)
    coordinator.schedule_kill("executor-sim-wkr-a-9009", at_time=_KILL_AT)
    coordinator.schedule_kill("executor-sim-wkr-a-9011", at_time=_KILL_AT)
    return coordinator.run()


def _build_dag() -> SimulationCoordinator:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_DAG_CEILING, seed=_DAG_SEED
    )
    coordinator.add_process(
        "manager", lifecycle_manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker", dag_worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
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
        _DAG_JOB_TIMEOUT_SECONDS,
        _DAG_WAIT_TIMEOUT_SECONDS,
    )
    return coordinator


def _run_dag() -> dict:
    return _build_dag().run()


def _run_dag_executor_kill() -> dict:
    coordinator = _build_dag()
    coordinator.schedule_kill("executor-sim-wkr-9009", at_time=_EXECUTOR_KILL_AT)
    return coordinator.run()


def _run_dispatch_exhaustion() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_EXHAUSTION_CEILING, seed=_EXHAUSTION_SEED
    )
    coordinator.add_process(
        "manager", lifecycle_manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker", refusing_worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "client",
        budgeted_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _EXHAUSTION_RETRY_BUDGET_PER_WORKFLOW,
        _EXHAUSTION_WORKFLOW_SECONDS,
        _EXHAUSTION_JOB_TIMEOUT_SECONDS,
        _EXHAUSTION_CEILING,
    )
    return coordinator.run()


def _run_dag_worker_loss() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_DAG_LOSS_CEILING, seed=_DAG_SEED
    )
    coordinator.add_process(
        "manager", lifecycle_manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker-a", dag_worker_entry, "sim-wkr-a", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "worker-b",
        dag_worker_entry,
        "sim-wkr-b",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
        _DAG_LOSS_WORKER_B_START,
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
        _DAG_LOSS_JOB_TIMEOUT_SECONDS,
        _DAG_LOSS_WAIT_TIMEOUT_SECONDS,
    )
    for process_id in ("worker-a", "executor-sim-wkr-a-9009", "executor-sim-wkr-a-9011"):
        coordinator.schedule_kill(process_id, at_time=_DAG_LOSS_KILL_AT)
    return coordinator.run()


def _run_dag_cancel() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_DAG_CANCEL_CEILING, seed=_DAG_SEED
    )
    coordinator.add_process(
        "manager", lifecycle_manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker", dag_worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "client",
        dag_cancelling_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _LONG_A_SECONDS,
        _SHORT_B_SECONDS,
        _DAG_CANCEL_AFTER_RUNNING_SECONDS,
        _DAG_JOB_TIMEOUT_SECONDS,
        _DAG_CANCEL_OBSERVE_SECONDS,
    )
    return coordinator.run()


def _run_steady_long_job() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_STEADY_CEILING, seed=_STEADY_SEED
    )
    coordinator.add_process(
        "manager",
        lifecycle_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        {
            "JOB_STUCK_THRESHOLD": _STEADY_STUCK_THRESHOLD_SECONDS,
            "JOB_TIMEOUT_CHECK_INTERVAL": _STEADY_TIMEOUT_CHECK_INTERVAL_SECONDS,
        },
    )
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "client",
        steady_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _STEADY_WORKFLOW_SECONDS,
        _STEADY_JOB_TIMEOUT_SECONDS,
        _STEADY_CEILING,
    )
    return coordinator.run()


def _run_losses_spending_the_budget(second_kill_at: float, worker_c_start: float) -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_LOSSES_CEILING, seed=_LOSSES_SEED
    )
    coordinator.add_process(
        "manager", lifecycle_manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    for process_id, host, start_at in (
        ("worker-a", "sim-wkr-a", 0.0),
        ("worker-b", "sim-wkr-b", _LOSSES_WORKER_B_START),
        ("worker-c", "sim-wkr-c", worker_c_start),
    ):
        coordinator.add_process(
            process_id, dag_worker_entry, host, 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2, start_at
        )
    coordinator.add_process(
        "client",
        budgeted_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _LOSSES_RETRY_BUDGET_PER_WORKFLOW,
        _LOSSES_WORKFLOW_SECONDS,
        _LOSSES_JOB_TIMEOUT_SECONDS,
        _LOSSES_CEILING,
    )
    for process_id in ("worker-a", "executor-sim-wkr-a-9009", "executor-sim-wkr-a-9011"):
        coordinator.schedule_kill(process_id, at_time=_LOSSES_FIRST_KILL_AT)
    for process_id in ("worker-b", "executor-sim-wkr-b-9009", "executor-sim-wkr-b-9011"):
        coordinator.schedule_kill(process_id, at_time=second_kill_at)
    return coordinator.run()


def _finished_status(client_log: list) -> str:
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    return finished[0][1]


def _states_entered(history: list[tuple[str | None, str, float]]) -> list[str]:
    return [to_value for _from_value, to_value, _at_time in history]


def _assert_dispatcher_released(manager_log: list) -> None:
    for tag in ("dispatcher-pending", "dispatch-loops"):
        counts = [entry[1] for entry in manager_log if entry[0] == tag]
        assert counts and max(counts) >= 1 and counts[-1] == 0, (tag, counts)


def test_a_clean_workflow_walks_the_lifecycle_once():
    results = _run_clean()
    manager_log = results["manager"]
    oracle = WorkflowLifecycleOracle()

    assert oracle.check_manager_log(manager_log) == [], manager_log
    assert _finished_status(results["client"]) == "completed"
    _assert_dispatcher_released(manager_log)

    histories = oracle.workflow_histories(manager_log)
    assert list(histories) == [(0, "SimPingWorkflow")], histories
    assert _states_entered(histories[(0, "SimPingWorkflow")]) == [
        "pending",
        "dispatched",
        "running",
        "completed",
    ]


def test_clean_lifecycle_is_replay_deterministic():
    assert _run_clean() == _run_clean()


def test_a_workflow_lost_with_its_worker_takes_the_retry_chain_and_completes():
    results = _run_worker_loss()
    manager_log = results["manager"]
    oracle = WorkflowLifecycleOracle()

    assert oracle.check_manager_log(manager_log) == [], manager_log
    assert _finished_status(results["client"]) == "completed"
    _assert_dispatcher_released(manager_log)

    history = oracle.workflow_histories(manager_log)[(0, "SimPingWorkflow")]
    entered = _states_entered(history)
    # One loss, one retry: the chain appears exactly once, after the
    # first dispatch, and the retried run ends the history.
    chain_starts = [
        index
        for index in range(len(entered))
        if entered[index : index + len(_RETRY_CHAIN)] == _RETRY_CHAIN
    ]
    assert len(chain_starts) == 1, entered
    chain_start = chain_starts[0]
    # The kill can land before the first progress report (the workflow
    # never observed RUNNING on the lost worker) or after it.
    assert entered[:chain_start] in (
        ["pending", "dispatched"],
        ["pending", "dispatched", "running"],
    ), entered
    assert entered[chain_start + len(_RETRY_CHAIN) :] == [
        "dispatched",
        "running",
        "completed",
    ], entered
    # The failure follows the kill; the retry's dispatch waits for the
    # late worker.
    assert history[chain_start][2] > _KILL_AT, history
    assert history[chain_start + len(_RETRY_CHAIN)][2] > _WORKER_B_START, history


def test_worker_loss_lifecycle_is_replay_deterministic():
    assert _run_worker_loss() == _run_worker_loss()


def test_a_dependent_leaves_pending_only_after_its_dependency_completed():
    results = _run_dag()
    manager_log = results["manager"]
    oracle = WorkflowLifecycleOracle()

    assert oracle.check_manager_log(manager_log) == [], manager_log
    assert _finished_status(results["client"]) == "completed"
    _assert_dispatcher_released(manager_log)

    histories = oracle.workflow_histories(manager_log)
    dependency = histories[(0, "SimDagLongA")]
    dependent = histories[(0, "SimDagShortB")]
    for history in (dependency, dependent):
        assert _states_entered(history) == ["pending", "dispatched", "running", "completed"], history

    dependency_completed_at = dependency[-1][2]
    dependent_dispatched_at = dependent[1][2]
    assert dependent_dispatched_at >= dependency_completed_at, (dependency, dependent)


def test_dag_lifecycle_is_replay_deterministic():
    assert _run_dag() == _run_dag()


def test_a_failed_dependency_fails_its_pending_dependent_in_the_same_cascade():
    results = _run_dag_executor_kill()
    manager_log = results["manager"]
    oracle = WorkflowLifecycleOracle()

    assert oracle.check_manager_log(manager_log) == [], manager_log
    assert _finished_status(results["client"]) == "failed"
    _assert_dispatcher_released(manager_log)

    histories = oracle.workflow_histories(manager_log)
    dependency = histories[(0, "SimDagLongA")]
    dependent = histories[(0, "SimDagShortB")]
    assert _states_entered(dependency) == ["pending", "dispatched", "running", "failed"], dependency
    # B never dispatched: the cascade took it from PENDING to FAILED.
    assert _states_entered(dependent) == ["pending", "failed"], dependent
    dependency_failed_at = dependency[-1][2]
    assert dependency_failed_at > _EXECUTOR_KILL_AT, dependency
    assert dependent[-1][2] >= dependency_failed_at, (dependency, dependent)

    # The job closed on the failure, not on its timeout.
    client_log = results["client"]
    submitted_at = next(entry[1] for entry in client_log if entry[0] == "job-submitted")
    finished_at = next(entry[2] for entry in client_log if entry[0] == "job-finished")
    assert finished_at < submitted_at + _DAG_JOB_TIMEOUT_SECONDS, client_log


def test_dag_executor_kill_lifecycle_is_replay_deterministic():
    assert _run_dag_executor_kill() == _run_dag_executor_kill()


def test_a_workflow_every_worker_refuses_fails_loudly_once_its_retry_budget_is_spent():
    results = _run_dispatch_exhaustion()
    manager_log = results["manager"]
    oracle = WorkflowLifecycleOracle()

    assert oracle.check_manager_log(manager_log) == [], manager_log
    client_log = results["client"]
    assert _finished_status(client_log) == "failed"
    # The job's errors name the spent budget and the workers' refusal.
    assert ("failure-cause", True, True) in client_log, client_log
    _assert_dispatcher_released(manager_log)

    # A budget of one: the first refusal spends it, the retry is refused
    # too, and nothing is sent again.
    refusals = [entry for entry in results["worker"] if entry[0] == "dispatch-refused"]
    assert len(refusals) == _EXHAUSTION_RETRY_BUDGET_PER_WORKFLOW + 1, results["worker"]

    history = oracle.workflow_histories(manager_log)[(0, "SimL2SustainedWorkflow")]
    assert _states_entered(history) == [
        "pending",
        "dispatched",
        *_RETRY_CHAIN,
        "dispatched",
        "failed",
    ], history

    submitted_at = next(entry[1] for entry in client_log if entry[0] == "job-submitted")
    finished_at = next(entry[2] for entry in client_log if entry[0] == "job-finished")
    assert finished_at < submitted_at + _EXHAUSTION_JOB_TIMEOUT_SECONDS, client_log


def test_dispatch_exhaustion_is_replay_deterministic():
    assert _run_dispatch_exhaustion() == _run_dispatch_exhaustion()


def test_a_dependency_lost_with_its_worker_retries_before_its_dependent_dispatches():
    results = _run_dag_worker_loss()
    manager_log = results["manager"]
    oracle = WorkflowLifecycleOracle()

    assert oracle.check_manager_log(manager_log) == [], manager_log
    assert _finished_status(results["client"]) == "completed"
    _assert_dispatcher_released(manager_log)

    histories = oracle.workflow_histories(manager_log)
    dependency = histories[(0, "SimDagLongA")]
    dependent = histories[(0, "SimDagShortB")]
    entered = _states_entered(dependency)
    chain_starts = [
        index
        for index in range(len(entered))
        if entered[index : index + len(_RETRY_CHAIN)] == _RETRY_CHAIN
    ]
    assert len(chain_starts) == 1, entered
    assert dependency[chain_starts[0]][2] > _DAG_LOSS_KILL_AT, dependency
    assert entered[chain_starts[0] + len(_RETRY_CHAIN) :] == ["dispatched", "running", "completed"], entered

    assert _states_entered(dependent) == ["pending", "dispatched", "running", "completed"], dependent
    assert dependent[1][2] >= dependency[-1][2], (dependency, dependent)

    # The retried A ran on the late worker.
    late_starts = [entry for entry in results["worker-b"] if entry[0] == "workflow-started"]
    assert [entry[1] for entry in late_starts][:1] == ["SimDagLongA"], results["worker-b"]


def test_dag_worker_loss_lifecycle_is_replay_deterministic():
    assert _run_dag_worker_loss() == _run_dag_worker_loss()


def test_a_cancelled_dag_cancels_the_running_dependency_and_the_waiting_dependent():
    results = _run_dag_cancel()
    manager_log = results["manager"]
    client_log = results["client"]
    oracle = WorkflowLifecycleOracle()

    # A cancelled job is kept until the retention sweep: its records stay.
    assert oracle.check_manager_log(manager_log, expect_released=False) == [], manager_log
    assert ("cancel-response", True) in [entry[:2] for entry in client_log], client_log
    assert _finished_status(client_log) == "cancelled"
    pushes = [entry for entry in client_log if entry[0] == "cancellation-push"]
    assert [entry[1] for entry in pushes] == [True], client_log

    histories = oracle.workflow_histories(manager_log)
    dependency = histories[(0, "SimDagLongA")]
    dependent = histories[(0, "SimDagShortB")]
    assert _states_entered(dependency) == [
        "pending",
        "dispatched",
        "running",
        "cancelling",
        "cancelled",
    ], dependency
    assert _states_entered(dependent) == ["pending", "cancelling", "cancelled"], dependent

    # Nothing of the job is dispatched after the cancellation.
    _assert_dispatcher_released(manager_log)


def test_dag_cancel_lifecycle_is_replay_deterministic():
    assert _run_dag_cancel() == _run_dag_cancel()


def test_a_long_job_making_progress_is_not_declared_stuck():
    results = _run_steady_long_job()
    manager_log = results["manager"]
    client_log = results["client"]
    oracle = WorkflowLifecycleOracle()

    assert oracle.check_manager_log(manager_log) == [], manager_log
    assert _finished_status(client_log) == "completed", client_log
    history = oracle.workflow_histories(manager_log)[(0, "SimSteadyWorkflow")]
    assert _states_entered(history) == ["pending", "dispatched", "running", "completed"], history

    # It ran past the stuck threshold -- the point -- and completed.
    running_at = history[2][2]
    completed_at = history[3][2]
    assert completed_at - running_at > _STEADY_STUCK_THRESHOLD_SECONDS, history


def test_steady_long_job_is_replay_deterministic():
    assert _run_steady_long_job() == _run_steady_long_job()
