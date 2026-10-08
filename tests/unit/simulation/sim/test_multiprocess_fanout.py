"""
Fanout 10x and 100x under multi-process SIM (``fanout_demo``): one
gateless manager, four two-core workers, and five clients each submitting
its share of the jobs at one instant -- 10 jobs in all, then 100. Every
job is one ``SimPingWorkflow`` (a single half-second action on two VUs:
two cores, so each worker runs one at a time).

* Exactly once: the manager dispatches and completes every job's workflow
  once (AD-54 lifecycle, by job ordinal), the workers run exactly as many
  dispatches as there are jobs, none of them twice, and every client sees
  each of its jobs complete once.
* Fair, no starvation: the workers share the jobs evenly (within one),
  and every job completes within the rounds the cluster needs for all of
  them -- each round a dispatch, the action, and its result -- so none
  waits behind the others for longer than the whole batch takes.
* No unbounded growth: once the jobs are swept, the manager holds no job,
  lifecycle record, dispatch queue entry, dispatch loop or per-job Raft
  group, and the manager's and workers' tables hold no more than they did
  idle, plus at most one entry per peer they track -- never one per job.

The 10x run has a replay twin.
"""

import math

import pytest

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.fanout_demo import (
    WATCH_INTERVAL_SECONDS,
    fanout_client_entry,
    fanout_manager_entry,
    fanout_worker_entry,
)
from tests.simulation.oracle import WorkflowLifecycleOracle

_SEED = 53
_LINK_LATENCY_SECONDS = 0.01
_WORKERS = 4
_CORES_PER_WORKER = 2
_CLIENTS = 5
# SimPingWorkflow: one 0.5s action on two VUs, which take two cores.
_ACTION_SECONDS = 0.5
_CORES_PER_WORKFLOW = 2
_CONCURRENT_WORKFLOWS = _WORKERS * (_CORES_PER_WORKER // _CORES_PER_WORKFLOW)
# One round on a worker beyond the action itself: the dispatch and its
# answer, the executor taking the workflow, its result and the ack --
# eight link latencies (probed 2026-10-05: 0.58s a round); ten allow one
# reordered hop each way.
_ROUND_OVERHEAD_SECONDS = 10 * _LINK_LATENCY_SECONDS
_ROUND_SECONDS = _ACTION_SECONDS + _ROUND_OVERHEAD_SECONDS
# Every worker has registered by then (probed: the last at 3.5).
_SUBMIT_AT = 6.0
# Completed jobs kept 5s, swept every 2s.
_JOB_RETENTION_SECONDS = 5.0
_JOB_CLEANUP_INTERVAL_SECONDS = 2.0
_JOB_TIMEOUT_SECONDS = 300.0

_WORKER_HOSTS = [f"sim-wkr-{index}" for index in range(_WORKERS)]
_CLIENT_HOSTS = [f"sim-cli-{index}" for index in range(_CLIENTS)]


def _ceiling(jobs: int) -> float:
    """Room for every round, the sweep after the last, and its watch."""
    return (
        _SUBMIT_AT
        + _rounds(jobs) * _ROUND_SECONDS
        + _JOB_RETENTION_SECONDS
        + 2 * _JOB_CLEANUP_INTERVAL_SECONDS
        + 2 * WATCH_INTERVAL_SECONDS
    )


def _rounds(jobs: int) -> int:
    return math.ceil(jobs / _CONCURRENT_WORKFLOWS)


# What a worker keeps per manager: the manager itself, and its last
# backpressure level and delay.
_ENTRIES_PER_MANAGER = 3


def _run_fanout(jobs: int) -> dict:
    coordinator = SimulationCoordinator(
        latency=_LINK_LATENCY_SECONDS, max_virtual_time=_ceiling(jobs), seed=_SEED
    )
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
    for client_host in _CLIENT_HOSTS:
        coordinator.add_process(
            client_host,
            fanout_client_entry,
            client_host,
            9500,
            manager_address,
            jobs // _CLIENTS,
            _SUBMIT_AT,
            _JOB_TIMEOUT_SECONDS,
            _JOB_TIMEOUT_SECONDS,
        )
    return coordinator.run()


def _rows(log: list, tag: str) -> list[tuple]:
    return [entry for entry in log if entry[0] == tag]


def _counts(log: list, tag: str) -> list[tuple[int, float]]:
    return [(entry[1], entry[2]) for entry in _rows(log, tag)]


def _idle_then_final(log: list, tag: str) -> tuple[int, int]:
    """The count before the jobs were submitted, and the last one."""
    counts = _counts(log, tag)
    idle = [count for count, at_time in counts if at_time < _SUBMIT_AT][-1]
    return idle, counts[-1][0]


@pytest.mark.parametrize("jobs", [10, 100])
def test_every_job_of_a_fanout_completes_exactly_once_fairly_and_leaves_nothing_behind(jobs: int):
    results = _run_fanout(jobs)
    manager_log = results["manager"]
    oracle = WorkflowLifecycleOracle()

    # Exactly once -- at the manager, per job ordinal.
    assert oracle.check_manager_log(manager_log) == [], manager_log
    histories = oracle.workflow_histories(manager_log)
    assert len(histories) == jobs, sorted(histories)
    for history in histories.values():
        assert [to_value for _from, to_value, _at in history] == [
            "pending",
            "dispatched",
            "running",
            "completed",
        ], history

    # -- at the workers: as many runs as jobs, none run twice.
    worker_runs = {
        worker_host: _rows(results[worker_host], "dispatch-run") for worker_host in _WORKER_HOSTS
    }
    for worker_host, runs in worker_runs.items():
        assert [distinct for _tag, distinct, _at in runs] == list(range(1, len(runs) + 1)), (worker_host, runs)
    assert sum(len(runs) for runs in worker_runs.values()) == jobs, worker_runs

    # -- at the clients: each job accepted once and completed once.
    last_completed_at = 0.0
    for client_host in _CLIENT_HOSTS:
        client_log = results[client_host]
        accepted = _rows(client_log, "job-accepted")
        finished = _rows(client_log, "job-finished")
        assert sorted(ordinal for _tag, ordinal, _at in accepted) == list(range(jobs // _CLIENTS)), client_log
        assert sorted((ordinal, status) for _tag, ordinal, status, _at in finished) == [
            (ordinal, "completed") for ordinal in range(jobs // _CLIENTS)
        ], client_log
        last_completed_at = max(last_completed_at, *(at_time for *_rest, at_time in finished))

    # Fair: the workers share the jobs evenly.
    runs_per_worker = [len(runs) for runs in worker_runs.values()]
    assert max(runs_per_worker) - min(runs_per_worker) <= 1, runs_per_worker

    # No starvation: every job is done within the rounds the batch needs.
    assert last_completed_at <= _SUBMIT_AT + _rounds(jobs) * _ROUND_SECONDS, (
        last_completed_at,
        _rounds(jobs),
    )

    # No unbounded growth.
    for tag in ("jobs", "lifecycle-records", "dispatcher-pending", "dispatch-loops", "raft-groups"):
        counts = _counts(manager_log, tag)
        assert counts[-1][0] == 0, (tag, counts)
    # ManagerState tracks workers, not clients: past its idle count it may
    # hold one more entry per worker (a worker's table filled by a report
    # that came after the idle count), never one per job.
    manager_idle, manager_final = _idle_then_final(manager_log, "state-entries")
    assert manager_final <= manager_idle + _WORKERS, (manager_idle, manager_final)
    for worker_host in _WORKER_HOSTS:
        worker_idle, worker_final = _idle_then_final(results[worker_host], "state-entries")
        # Its one peer, the manager, and nothing per job: the manager's
        # entry, and the backpressure level and delay it last signalled
        # (AD-23; NONE included, forgotten with the manager).
        assert worker_final <= worker_idle + _ENTRIES_PER_MANAGER, (worker_host, worker_idle, worker_final)


def test_fanout_is_replay_deterministic():
    assert _run_fanout(10) == _run_fanout(10)
