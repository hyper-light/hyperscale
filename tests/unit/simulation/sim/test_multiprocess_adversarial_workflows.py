"""
Adversarial workflows under multi-process SIM (SCENARIOS §7 "Panic,
infinite loop (timeout-killed), giant memory allocation (OOM-killed)"),
over ``adversarial_workflow_demo``: one gateless manager, one two-core
worker, and a client that submits the adversary, awaits its terminal,
then submits a plain ``SimPingWorkflow`` job.

* A step that raises: the job ends FAILED with an error, and the next job
  completes.
* A step that never ends (it swallows every cancellation, as a step stuck
  in a loop does): the job's AD-34 timeout ends it -- at the worker's
  execution-timeout check, one check interval past the timeout -- FAILED
  with an error naming the timeout, and the next job completes.
* A workflow whose memory grows without bound: the AD-41 enforcer kills
  it once its estimate is past the job's memory budget -- after the
  reading crosses the budget, before any other bound (the job timeout,
  the workflow's duration) could end it -- FAILED with an error naming
  the budget, and the next job completes.

"The next job completes" is held to a bound: one ping round past the
worker's cancellation windows. Before the fix in
``nodes/worker/cancellation.py`` (the workflow's name is read before the
TaskRunner cancel, whose cleanup drops it), a cancelled workflow's
executors were never told to stop and ran it to its natural end, so the
next job waited behind it until its own timeout failed it.

Every scenario has a replay twin.
"""

import pytest

from hyperscale.distributed.env import Env
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.adversarial_workflow_demo import (
    WATCH_INTERVAL_SECONDS,
    adversarial_client_entry,
    guarded_manager_entry,
    observed_worker_entry,
)

_SEED = 71
_LINK_LATENCY_SECONDS = 0.01
_WORKER_CORES = 2
_TICK_SECONDS = 1.0
_ENV = Env()
# A result's way back -- the dispatch's answer, the executor's result, the
# worker's final result, the manager's push to the client -- with room for
# one reordered hop each way (as test_multiprocess_fanout's round).
_DELIVERY_SECONDS = 10 * _LINK_LATENCY_SECONDS
# SimPingWorkflow: one 0.5s action, plus its delivery.
_PING_ROUND_SECONDS = 0.5 + _DELIVERY_SECONDS
# The worker's cancellation: a 2s graceful window
# (``WorkerCancellationHandler._run_remote_cancellation``), then the
# executor's hard-cancel convergence wait of at most 5s
# (``RemoteGraphController.cancel_workflow_background``).
_GRACEFUL_CANCEL_SECONDS = 2.0
_HARD_CANCEL_CONVERGENCE_SECONDS = 5.0
_FOLLOW_UP_BOUND_SECONDS = _GRACEFUL_CANCEL_SECONDS + _HARD_CANCEL_CONVERGENCE_SECONDS + _PING_ROUND_SECONDS

# The never-ending step: a job timeout the step outlives by its duration.
_LOOP_JOB_TIMEOUT_SECONDS = 20.0
_LOOP_DURATION_SECONDS = 2 * _LOOP_JOB_TIMEOUT_SECONDS

# The memory hog: the job's budget is the environment's default memory
# limit, and the scripted reading crosses it three warning graces after
# the hog starts -- long enough for the enforcer to warn before it kills.
_MEMORY_BUDGET_BYTES = _ENV.RESOURCE_GUARD_MAX_MEMORY_BYTES
_BUDGET_CROSSED_AFTER_SECONDS = 3 * _ENV.RESOURCE_GUARD_WARNING_GRACE_SECONDS
_HOG_GROWTH_BYTES_PER_SECOND = _MEMORY_BUDGET_BYTES / _BUDGET_CROSSED_AFTER_SECONDS
_HOG_JOB_TIMEOUT_SECONDS = 3 * _BUDGET_CROSSED_AFTER_SECONDS
_HOG_DURATION_SECONDS = 2 * _HOG_JOB_TIMEOUT_SECONDS

_RAISE_DURATION_SECONDS = 2.0
_RAISE_JOB_TIMEOUT_SECONDS = 30.0


def _run(
    adversary_kind: str,
    duration_seconds: float,
    job_timeout_seconds: float,
    max_memory_bytes: int | None,
    hog_growth_bytes_per_second: float,
) -> dict:
    """Run the adversary, then the follow-up, to a ceiling past both."""
    ceiling = job_timeout_seconds + _FOLLOW_UP_BOUND_SECONDS + job_timeout_seconds
    coordinator = SimulationCoordinator(latency=_LINK_LATENCY_SECONDS, max_virtual_time=ceiling, seed=_SEED)
    manager_address = ("sim-mgr", 9000)
    coordinator.add_process("manager", guarded_manager_entry, "sim-mgr", 9000, 9001, "sim-dc")
    coordinator.add_process(
        "worker",
        observed_worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        manager_address,
        _WORKER_CORES,
        hog_growth_bytes_per_second,
    )
    coordinator.add_process(
        "client",
        adversarial_client_entry,
        "sim-cli",
        9500,
        manager_address,
        adversary_kind,
        duration_seconds,
        _TICK_SECONDS,
        job_timeout_seconds,
        max_memory_bytes,
    )
    return coordinator.run()


def _run_raising() -> dict:
    return _run("raises", _RAISE_DURATION_SECONDS, _RAISE_JOB_TIMEOUT_SECONDS, None, 0.0)


def _run_never_ending() -> dict:
    return _run("never-ends", _LOOP_DURATION_SECONDS, _LOOP_JOB_TIMEOUT_SECONDS, None, 0.0)


def _run_memory_hog() -> dict:
    return _run(
        "exhausts-memory",
        _HOG_DURATION_SECONDS,
        _HOG_JOB_TIMEOUT_SECONDS,
        _MEMORY_BUDGET_BYTES,
        _HOG_GROWTH_BYTES_PER_SECOND,
    )


def _row(log: list, tag: str, label: str) -> tuple:
    rows = [row for row in log if row[:2] == (tag, label)]
    assert len(rows) == 1, (tag, label, log)
    return rows[0]


def _first_dispatch_at(worker_log: list) -> float:
    """The first watch sample showing a workflow active: the adversary's
    dispatch happened at or before it, and at most one watch interval
    earlier."""
    return next(row[3] for row in worker_log if row[0] == "worker-state" and row[1] > 0)


def _assert_follow_up_completes_promptly(client_log: list, adversary_kind: str) -> None:
    """The cluster runs the next job normally once the adversary is gone."""
    _tag, _label, _status, adversary_finished_at = _row(client_log, "job-finished", adversary_kind)
    _tag, _label, follow_up_status, follow_up_finished_at = _row(client_log, "job-finished", "follow-up")
    assert follow_up_status == "completed", client_log
    assert _row(client_log, "job-errors", "follow-up")[2:] == (False, False, False, False), client_log
    assert follow_up_finished_at - adversary_finished_at <= _FOLLOW_UP_BOUND_SECONDS, client_log


def _assert_worker_idle_at_end(worker_log: list) -> None:
    """No workflow left active and every core free."""
    final_state = [row for row in worker_log if row[0] == "worker-state"][-1]
    assert final_state[1:3] == (0, _WORKER_CORES), worker_log


def _assert_no_unswapped_imports(results: dict) -> None:
    for process_id, process_log in results.items():
        audit_rows = [
            row for row in (process_log or []) if isinstance(row, tuple) and row[:1] == ("determinism-audit-unswapped",)
        ]
        assert not audit_rows, (process_id, audit_rows)


def test_a_raising_step_fails_its_job_loudly_and_the_next_job_completes():
    results = _run_raising()
    client_log = results["client"]

    _tag, _label, status, _finished_at = _row(client_log, "job-finished", "raises")
    assert status == "failed", client_log
    has_error, _names_raised, names_timeout, names_memory = _row(client_log, "job-errors", "raises")[2:]
    assert has_error and not names_timeout and not names_memory, client_log
    assert not [row for row in results["manager"] if row[0] == "resource-kill"], results["manager"]

    _assert_follow_up_completes_promptly(client_log, "raises")
    _assert_worker_idle_at_end(results["worker"])
    _assert_no_unswapped_imports(results)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "hyperscale/core/jobs/graphs/remote_graph_manager.py:1171 raises a bare "
        "'No results returned' when every executor failed, dropping the errors the "
        "controller kept per executor (remote_graph_controller.py:1066 _errors): the "
        "job's error never names what the step raised. core/jobs is peer-owned."
    ),
)
def test_a_raising_steps_error_reaches_the_client():
    client_log = _run_raising()["client"]
    assert _row(client_log, "job-errors", "raises")[3] is True, client_log


def test_a_raising_step_is_replay_deterministic():
    assert _run_raising() == _run_raising()


def test_a_never_ending_step_is_ended_by_the_job_timeout_and_the_next_job_completes():
    results = _run_never_ending()
    client_log = results["client"]

    _tag, _label, submitted_at = _row(client_log, "job-submitted", "never-ends")
    _tag, _label, status, finished_at = _row(client_log, "job-finished", "never-ends")
    assert status == "failed", client_log
    has_error, _names_raised, names_timeout, names_memory = _row(client_log, "job-errors", "never-ends")[2:]
    assert has_error and names_timeout and not names_memory, client_log

    # AD-34: never before the timeout, and within one execution-timeout
    # check of it (plus delivery) after the dispatch started its clock.
    dispatched_by = _first_dispatch_at(results["worker"])
    assert submitted_at + _LOOP_JOB_TIMEOUT_SECONDS <= finished_at, (submitted_at, finished_at)
    latest = dispatched_by + _LOOP_JOB_TIMEOUT_SECONDS + _ENV.WORKER_ORPHAN_CHECK_INTERVAL + _DELIVERY_SECONDS
    assert finished_at <= latest, (finished_at, latest, results["worker"])

    _assert_follow_up_completes_promptly(client_log, "never-ends")
    _assert_worker_idle_at_end(results["worker"])
    _assert_no_unswapped_imports(results)


def test_a_never_ending_step_is_replay_deterministic():
    assert _run_never_ending() == _run_never_ending()


def test_a_memory_hog_is_killed_at_its_budget_and_the_next_job_completes():
    results = _run_memory_hog()
    client_log = results["client"]
    manager_log = results["manager"]

    _tag, _label, submitted_at = _row(client_log, "job-submitted", "exhausts-memory")
    _tag, _label, status, finished_at = _row(client_log, "job-finished", "exhausts-memory")
    assert status == "failed", client_log
    has_error, _names_raised, names_timeout, names_memory = _row(client_log, "job-errors", "exhausts-memory")[2:]
    assert has_error and names_memory and not names_timeout, client_log

    # AD-41: one kill, never before the reading crosses the budget (the
    # worker first sees the hog no earlier than its dispatch, at most one
    # watch interval before the first active sample), and before the job
    # timeout or the workflow's duration could have ended it.
    kill_times = [row[1] for row in manager_log if row[0] == "resource-kill"]
    assert len(kill_times) == 1, manager_log
    budget_crossed_no_earlier_than = (
        _first_dispatch_at(results["worker"]) - WATCH_INTERVAL_SECONDS + _BUDGET_CROSSED_AFTER_SECONDS
    )
    assert budget_crossed_no_earlier_than <= kill_times[0] < submitted_at + _HOG_JOB_TIMEOUT_SECONDS, (
        kill_times,
        budget_crossed_no_earlier_than,
    )
    assert kill_times[0] <= finished_at <= kill_times[0] + _FOLLOW_UP_BOUND_SECONDS, (kill_times, finished_at)

    _assert_follow_up_completes_promptly(client_log, "exhausts-memory")
    _assert_worker_idle_at_end(results["worker"])
    _assert_no_unswapped_imports(results)


def test_a_memory_hog_is_replay_deterministic():
    assert _run_memory_hog() == _run_memory_hog()
