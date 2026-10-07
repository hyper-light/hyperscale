"""
Sustained-rate submission under multi-process SIM (SCENARIOS §7
"Sustained. 10 jobs/s for 60 s. Steady-state queueing."), over
``sustained_submission_demo``: one gateless manager and four four-core
workers (``fanout_demo``'s, observed the same way), and one client that,
once its first job is accepted, submits a ``SimPingWorkflow`` job (one
half-second action on two VUs: two cores) every tenth of a virtual second
for sixty -- 600 jobs.

The cluster runs eight such jobs at once and each takes a round (the
action plus its dispatch and result), so the offered load is
``rate * round / 8`` = 0.75 of capacity: steady state, no backlog.

* Paced: job ``k`` is submitted exactly ``k / rate`` after the anchor.
* Exactly once: every job is accepted once and completes once, at the
  manager (AD-54 lifecycle, by job ordinal) and at the client.
* Steady-state queueing: no job waits more than one round behind others
  (every sojourn within two rounds), so the jobs in flight stay within
  Little's bound (rate x that sojourn) for the whole window -- nothing
  accumulates -- and no submission is refused.
* Drained: once the window ends and the jobs are swept, the manager holds
  no job, lifecycle record, dispatch queue entry, dispatch loop or per-job
  Raft group, and its and the workers' tables hold what a run of a sixth
  of the jobs leaves behind, plus at most one entry per peer -- nothing
  scales with the job count.

The run has a replay twin.
"""

import math

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.fanout_demo import (
    WATCH_INTERVAL_SECONDS,
    fanout_manager_entry,
    fanout_worker_entry,
)
from tests.simulation.harness.sim.multiprocess.sustained_submission_demo import paced_client_entry
from tests.simulation.oracle import WorkflowLifecycleOracle

_SEED = 59
_LINK_LATENCY_SECONDS = 0.01
_JOBS_PER_SECOND = 10.0
_WINDOW_SECONDS = 60.0
_TOTAL_JOBS = round(_JOBS_PER_SECOND * _WINDOW_SECONDS)
_WORKERS = 4
_CORES_PER_WORKER = 4
# SimPingWorkflow: one 0.5s action on two VUs, which take two cores.
_ACTION_SECONDS = 0.5
_CORES_PER_JOB = 2
_CONCURRENT_JOBS = _WORKERS * (_CORES_PER_WORKER // _CORES_PER_JOB)
# A round beyond the action: dispatch, the executor taking the workflow,
# its result, the ack, the client push -- room for one reordered hop each
# way (as test_multiprocess_fanout's round).
_ROUND_SECONDS = _ACTION_SECONDS + 10 * _LINK_LATENCY_SECONDS
_OFFERED_LOAD = _JOBS_PER_SECOND * _ROUND_SECONDS / _CONCURRENT_JOBS
_SOJOURN_BOUND_SECONDS = 2 * _ROUND_SECONDS
_IN_FLIGHT_BOUND = math.ceil(_JOBS_PER_SECOND * _SOJOURN_BOUND_SECONDS)
_JOB_RETENTION_SECONDS = 5.0
_JOB_CLEANUP_INTERVAL_SECONDS = 2.0
_JOB_TIMEOUT_SECONDS = 120.0
# Room for the cluster to form and accept the first job (the fanout
# scenarios measured every worker registered by 3.5s), the window, the
# last jobs' rounds, and the sweep after them with its watch.
_FORMATION_ALLOWANCE_SECONDS = 6.0
_CEILING = (
    _FORMATION_ALLOWANCE_SECONDS
    + _WINDOW_SECONDS
    + _SOJOURN_BOUND_SECONDS
    + _JOB_RETENTION_SECONDS
    + 2 * _JOB_CLEANUP_INTERVAL_SECONDS
    + 2 * WATCH_INTERVAL_SECONDS
)

_WORKER_HOSTS = [f"sim-wkr-{index}" for index in range(_WORKERS)]


def _run_sustained(window_seconds: float = _WINDOW_SECONDS) -> dict:
    ceiling = _CEILING - _WINDOW_SECONDS + window_seconds
    coordinator = SimulationCoordinator(latency=_LINK_LATENCY_SECONDS, max_virtual_time=ceiling, seed=_SEED)
    manager_address = ("sim-mgr", 9000)
    coordinator.add_process(
        "manager",
        fanout_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        _JOB_RETENTION_SECONDS,
        _JOB_CLEANUP_INTERVAL_SECONDS,
    )
    for worker_host in _WORKER_HOSTS:
        coordinator.add_process(
            worker_host,
            fanout_worker_entry,
            worker_host,
            9000,
            9001,
            "sim-dc",
            manager_address,
            _CORES_PER_WORKER,
        )
    coordinator.add_process(
        "client",
        paced_client_entry,
        "sim-cli",
        9500,
        manager_address,
        _JOBS_PER_SECOND,
        window_seconds,
        _JOB_TIMEOUT_SECONDS,
    )
    return coordinator.run()


def _times_by_ordinal(log: list, tag: str) -> dict[int, float]:
    return {row[1]: row[-1] for row in log if row[0] == tag}


def _counts(log: list, tag: str) -> list[tuple[int, float]]:
    return [(row[1], row[2]) for row in log if row[0] == tag]


def _in_flight_peak(submitted: dict[int, float], finished: dict[int, float]) -> int:
    """The most jobs submitted and not yet finished at any one instant."""
    events = sorted([(at_time, 1) for at_time in submitted.values()] + [(at_time, -1) for at_time in finished.values()])
    in_flight = 0
    peak = 0
    for _at_time, change in events:
        in_flight += change
        peak = max(peak, in_flight)
    return peak


def _assert_paced(client_log: list, submitted: dict[int, float]) -> None:
    """Job ``k`` went out ``k / rate`` after the anchor -- the virtual clock
    keeps the schedule exactly (rows round to the microsecond)."""
    anchor = next(row[1] for row in client_log if row[0] == "anchor")
    for ordinal in range(1, _TOTAL_JOBS):
        assert math.isclose(submitted[ordinal], anchor + ordinal / _JOBS_PER_SECOND, abs_tol=1e-6), (ordinal, anchor)


def _assert_exactly_once(results: dict, client_log: list) -> None:
    manager_log = results["manager"]
    oracle = WorkflowLifecycleOracle()
    assert oracle.check_manager_log(manager_log) == [], manager_log
    histories = oracle.workflow_histories(manager_log)
    assert len(histories) == _TOTAL_JOBS, len(histories)
    for history in histories.values():
        assert [to_value for _from, to_value, _at in history] == ["pending", "dispatched", "running", "completed"]
    finished = [row for row in client_log if row[0] == "job-finished"]
    assert sorted((row[1], row[2]) for row in finished) == [(ordinal, "completed") for ordinal in range(_TOTAL_JOBS)]
    worker_runs = sum(len([row for row in results[host] if row[0] == "dispatch-run"]) for host in _WORKER_HOSTS)
    assert worker_runs == _TOTAL_JOBS, worker_runs


def _drained_table_entries(results: dict) -> tuple[int, list[int]]:
    """The manager's node-table entries at the end, and each worker's."""
    worker_entries = sorted(_counts(results[host], "state-entries")[-1][0] for host in _WORKER_HOSTS)
    return _counts(results["manager"], "state-entries")[-1][0], worker_entries


def _assert_drained(results: dict) -> None:
    """Nothing per job is left, and the drained tables hold no more than a
    run of a sixth of the jobs leaves, plus at most one entry per peer a
    node tracks (a worker's AD-23 backpressure level for its manager, say,
    set only once load signalled it): nothing scales with the job count."""
    for tag in ("jobs", "lifecycle-records", "dispatcher-pending", "dispatch-loops", "raft-groups"):
        assert _counts(results["manager"], tag)[-1][0] == 0, (tag, _counts(results["manager"], tag))
    manager_entries, worker_entries = _drained_table_entries(results)
    twin_manager_entries, twin_worker_entries = _drained_table_entries(_run_sustained(_WINDOW_SECONDS / 6))
    assert manager_entries <= twin_manager_entries + _WORKERS, (manager_entries, twin_manager_entries)
    for entries, twin_entries in zip(worker_entries, twin_worker_entries):
        # A worker's one peer: the manager.
        assert entries <= twin_entries + 1, (worker_entries, twin_worker_entries)


def test_ten_jobs_a_second_for_a_minute_hold_a_steady_state():
    assert _OFFERED_LOAD < 1.0, _OFFERED_LOAD
    results = _run_sustained()
    client_log = results["client"]
    submitted = _times_by_ordinal(client_log, "job-submitted")
    finished = _times_by_ordinal(client_log, "job-finished")

    assert len(submitted) == _TOTAL_JOBS, len(submitted)
    _assert_paced(client_log, submitted)
    _assert_exactly_once(results, client_log)

    # Steady state: once paced, nothing refused, nothing waits more than a
    # round behind others, and the in-flight count holds Little's bound.
    assert not [row for row in client_log if row[0] == "submit-rejected" and row[1] > 0], client_log
    sojourns = {ordinal: finished[ordinal] - submitted[ordinal] for ordinal in range(1, _TOTAL_JOBS)}
    slowest = max(sojourns, key=sojourns.__getitem__)
    assert sojourns[slowest] <= _SOJOURN_BOUND_SECONDS, (slowest, sojourns[slowest])
    paced_submitted = {ordinal: submitted[ordinal] for ordinal in range(1, _TOTAL_JOBS)}
    paced_finished = {ordinal: finished[ordinal] for ordinal in range(1, _TOTAL_JOBS)}
    assert _in_flight_peak(paced_submitted, paced_finished) <= _IN_FLIGHT_BOUND
    # The manager's dispatch queue holds no more than is in flight.
    assert max(count for count, _at in _counts(results["manager"], "dispatcher-pending")) <= _IN_FLIGHT_BOUND

    _assert_drained(results)


def test_sustained_submission_is_replay_deterministic():
    assert _run_sustained() == _run_sustained()
