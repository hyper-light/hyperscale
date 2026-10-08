"""
L5 -- a rejection storm against a manager at its highest overload level,
under multi-process SIM (``rejection_storm_demo``).

A gateless manager with two two-core workers runs an admitted job (a
steady workflow) when its host saturates at ``_SATURATED_AT``: the
host's CPU reading (scripted -- SIM has no CPU) goes past the AD-18
OVERLOADED threshold, and a storm of submissions -- three clients, eight
concurrent back-to-back ``submit_job`` loops each -- arrives against it
until the host is relieved at ``_RELIEVED_AT``. The admitted job's
workflow ends mid-overload. After the relief a late client submits once.

* The level: the manager is OVERLOADED within one resource sample of the
  saturation, holds it until the relief, and is healthy again within its
  de-escalation hysteresis -- the only level changes of the run.
* It sheds with hints: every submission the transport saw while
  overloaded was refused (AD-24) with a retry-after, and none made a job
  or an idempotency entry.
* The storm is paced by those hints: no client sends more submissions in
  any retry-after window than it has submitters -- none spins.
* Live: no node is declared dead and both workers stay registered and
  healthy, though the overloaded manager refused their heartbeats,
  progress and final results.
* Admitted work completes: the admitted job's final result, refused
  while the manager was overloaded, is delivered once it recovers -- the
  workers' resend budget is far shorter than the overload, and a refusal
  spends none of it -- and every job the storm got admitted after the
  recovery completes.
* Bounded: the manager's per-client tables hold no more clients than
  talk to it, and the workers hold no undelivered result at the end.
* It recovers: the late client's submission is accepted at once.

Replay-deterministic.
"""

from hyperscale.distributed.env import Env
from hyperscale.distributed.reliability.overload_config import OverloadConfig
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.rejection_storm_demo import (
    WATCH_INTERVAL_SECONDS,
    late_client_entry,
    overloaded_manager_entry,
    storm_client_entry,
    tuned_worker_entry,
)
from tests.simulation.harness.sim.multiprocess.workflow_lifecycle_demo import (
    steady_client_entry,
)

_ENV = Env()
_OVERLOAD = OverloadConfig()
_SEED = 41
_LINK_LATENCY_SECONDS = 0.01
_SAMPLE_INTERVAL_SECONDS = _ENV.OVERLOAD_SAMPLE_INTERVAL_SECONDS

# The host's CPU: half the BUSY threshold before and after, saturated
# between. Memory stays as low.
_CALM_PERCENT = _OVERLOAD.cpu_thresholds[0] * 100 / 2
_SATURATED_PERCENT = 100.0
_SATURATED_AT = 10.0
_RELIEVED_AT = 30.0
_CPU_STEPS = [(0.0, _CALM_PERCENT), (_SATURATED_AT, _SATURATED_PERCENT), (_RELIEVED_AT, _CALM_PERCENT)]
# Escalation is immediate at the first sample after the saturation;
# de-escalation takes ``hysteresis_samples`` consecutive calm samples,
# the first of them up to one interval after the relief. Each is seen on
# the manager's next watch.
_OVERLOADED_BY = _SATURATED_AT + _SAMPLE_INTERVAL_SECONDS + WATCH_INTERVAL_SECONDS
_RECOVERED_BY = (
    _RELIEVED_AT + (_OVERLOAD.hysteresis_samples + 1) * _SAMPLE_INTERVAL_SECONDS + WATCH_INTERVAL_SECONDS
)

# The storm arrives once the manager is overloaded and stops at the relief.
_STORM_CLIENTS = 3
_SUBMITTERS_PER_CLIENT = 8
_STORM_START = _OVERLOADED_BY + WATCH_INTERVAL_SECONDS
_STORM_END = _RELIEVED_AT
_STORM_JOB_TIMEOUT_SECONDS = 60.0

# Admitted before the saturation; its workflow ends mid-overload (it runs
# from ~1.6s, probed 2026-10-05).
_STEADY_WORKFLOW_SECONDS = 15.0
_STEADY_JOB_TIMEOUT_SECONDS = 120.0
# A resend budget far shorter than the overload: two resends from a one
# second base (spent, the result was dropped for good).
_WORKER_ENV = {"WORKER_RESULT_MAX_RETRIES": 2, "WORKER_RESULT_RETRY_BASE_DELAY": 1.0}
# A worker resends held results on a fixed five-second loop
# (``WorkerServer._run_pending_result_retry_loop``).
_PENDING_RESULT_RETRY_PERIOD_SECONDS = 5.0

_LATE_SUBMIT_AT = _RECOVERED_BY
_CEILING = 90.0

_WORKERS = [f"sim-wkr-{index}" for index in range(2)]
_STORM_HOSTS = [f"sim-cli-storm-{index}" for index in range(_STORM_CLIENTS)]
# Every process that talks TCP to the manager.
_MANAGER_PEERS = len(_WORKERS) + len(_STORM_HOSTS) + 2


def _run_storm() -> dict:
    coordinator = SimulationCoordinator(latency=_LINK_LATENCY_SECONDS, max_virtual_time=_CEILING, seed=_SEED)
    manager_address = ("sim-mgr", 9000)
    coordinator.add_process(
        "manager", overloaded_manager_entry, "sim-mgr", 9000, 9001, "sim-dc", _CPU_STEPS, _CALM_PERCENT
    )
    for worker_host in _WORKERS:
        coordinator.add_process(
            worker_host, tuned_worker_entry, worker_host, 9000, 9001, "sim-dc", manager_address, 2, _WORKER_ENV
        )
    coordinator.add_process(
        "steady",
        steady_client_entry,
        "sim-cli-steady",
        9500,
        manager_address,
        _STEADY_WORKFLOW_SECONDS,
        _STEADY_JOB_TIMEOUT_SECONDS,
        _CEILING,
    )
    for storm_host in _STORM_HOSTS:
        coordinator.add_process(
            storm_host,
            storm_client_entry,
            storm_host,
            9500,
            manager_address,
            _SUBMITTERS_PER_CLIENT,
            _STORM_START,
            _STORM_END,
            _STORM_JOB_TIMEOUT_SECONDS,
            _CEILING,
        )
    coordinator.add_process(
        "late",
        late_client_entry,
        "sim-cli-late",
        9500,
        manager_address,
        _LATE_SUBMIT_AT,
        _STORM_JOB_TIMEOUT_SECONDS,
        _CEILING,
    )
    return coordinator.run()


def _rows(log: list, tag: str) -> list[tuple]:
    return [entry for entry in log if entry[0] == tag]


def _overloaded_window(manager_log: list) -> tuple[float, float]:
    """When the manager turned OVERLOADED and when it turned healthy again
    -- asserting those are its only level changes."""
    levels = [(state, at_time) for _tag, state, at_time in _rows(manager_log, "overload")]
    assert [state for state, _ in levels] == ["healthy", "overloaded", "healthy"], levels
    return levels[1][1], levels[2][1]


def test_a_storm_at_the_highest_overload_level_is_shed_with_hints_and_the_cluster_recovers():
    results = _run_storm()
    manager_log = results["manager"]

    # The level.
    overloaded_at, recovered_at = _overloaded_window(manager_log)
    assert _SATURATED_AT < overloaded_at <= _OVERLOADED_BY, overloaded_at
    assert _RELIEVED_AT < recovered_at <= _RECOVERED_BY, recovered_at

    # Shed with hints: refused, every one, while overloaded -- and nothing
    # of the refused submissions stays behind.
    admissions = _rows(manager_log, "submission-admission")
    overloaded_admissions = [entry for entry in admissions if entry[1] == "overloaded"]
    assert len(overloaded_admissions) >= _STORM_CLIENTS * _SUBMITTERS_PER_CLIENT, admissions
    assert all(
        not allowed and retry_after > 0 for _tag, _level, allowed, retry_after, _host, _at in overloaded_admissions
    ), overloaded_admissions
    for tag in ("jobs", "idempotency-entries"):
        counts = _rows(manager_log, tag)
        at_overload = [count for _tag, count, at_time in counts if at_time <= overloaded_at][-1]
        # Watched: the level changed within one watch interval before each.
        during_overload = [
            count
            for _tag, count, at_time in counts
            if overloaded_at < at_time < recovered_at - WATCH_INTERVAL_SECONDS
        ]
        assert all(count <= at_overload for count in during_overload), (tag, counts)

    # Paced by the hints: per client, never more submissions inside one
    # retry-after window than it has submitters.
    for storm_host in _STORM_HOSTS:
        refusals = sorted(
            (at_time, retry_after)
            for _tag, _level, allowed, retry_after, host, at_time in overloaded_admissions
            if host == storm_host
        )
        for first_at, retry_after in refusals:
            in_window = [at_time for at_time, _hint in refusals if first_at <= at_time < first_at + retry_after]
            assert len(in_window) <= _SUBMITTERS_PER_CLIENT, (storm_host, first_at, in_window)

    # Live.
    assert not _rows(manager_log, "node-dead"), manager_log
    worker_counts = [count for _tag, count, _at in _rows(manager_log, "worker-count")]
    assert worker_counts == [0, 1, 2], worker_counts
    assert [count for _tag, count, _at in _rows(manager_log, "unhealthy-workers")] == [0], manager_log
    refused_handlers = {handler for _tag, handler, _count, _at in _rows(manager_log, "refused")}
    assert "workflow_final_result" in refused_handlers, refused_handlers

    # Admitted work completes: the job whose result the overload refused,
    # once the manager recovers -- at the worker's next resend, and on to
    # the client (a few link latencies, within one watch interval).
    steady_log = results["steady"]
    steady_finished_rows = _rows(steady_log, "job-finished")
    assert len(steady_finished_rows) == 1, steady_log
    steady_finished = steady_finished_rows[0]
    assert steady_finished[1] == "completed", steady_log
    assert (
        recovered_at - WATCH_INTERVAL_SECONDS
        < steady_finished[2]
        <= recovered_at + _PENDING_RESULT_RETRY_PERIOD_SECONDS + WATCH_INTERVAL_SECONDS
    ), (
        recovered_at,
        steady_log,
    )
    storm_accepted = 0
    for storm_host in _STORM_HOSTS:
        storm_log = results[storm_host]
        accepted = [entry for entry in _rows(storm_log, "storm-call") if entry[1] == "accepted"]
        storm_accepted += len(accepted)
        assert all(at_time >= recovered_at - WATCH_INTERVAL_SECONDS for _tag, _outcome, at_time in accepted), storm_log
        finished = _rows(storm_log, "storm-job-finished")
        assert [status for _tag, status, _at in finished] == ["completed"] * len(accepted), storm_log
    assert storm_accepted > 0, results

    # Bounded.
    for _tag, *table_sizes, _at in _rows(manager_log, "rate-limit-clients"):
        assert max(table_sizes) <= _MANAGER_PEERS, manager_log
    # One idempotency entry per accepted job -- the steady one, the late one
    # and the storm's -- and no more.
    idempotency_counts = [count for _tag, count, _at in _rows(manager_log, "idempotency-entries")]
    assert max(idempotency_counts) == storm_accepted + 2, idempotency_counts
    for worker_host in _WORKERS:
        assert _rows(results[worker_host], "pending-results")[-1][1] == 0, results[worker_host]

    # Recovered: the late submission is accepted at once.
    late_log = results["late"]
    assert not _rows(late_log, "submit-rejected"), late_log
    (late_finished,) = _rows(late_log, "job-finished")
    assert late_finished[1] == "completed", late_log


def test_rejection_storm_is_replay_deterministic():
    assert _run_storm() == _run_storm()
