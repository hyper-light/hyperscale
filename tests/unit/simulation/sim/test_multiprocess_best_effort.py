"""
AD-44 best-effort completion over the L3 two-datacenter topology
(``test_multiprocess_dc_loss``): one gate, dc-east and dc-west each a
real manager + 2-core worker, a client submitting one two-datacenter job
through the gate. dc-west is killed outright (manager, worker, both
executors) while the job runs in it.

Pinned outcomes:
* ``best_effort, min_dcs=1``: the job completes as soon as dc-east does
  -- ``min_dcs_reached`` -- naming dc-west as the datacenter it stopped
  waiting for; nowhere near the job timeout.
* ``best_effort, min_dcs=2, deadline=D``: dc-east alone cannot satisfy
  it, so the job completes with dc-east's result once D passes
  (``deadline_expired``), within one deadline-check interval.
* not best-effort (control): the job waits for dc-west until the gate's
  AD-34 global timeout -- a loud ``timeout``.
* No datacenter's own terminal is the job's: with no fault, a job not in
  best-effort mode finishes only when both datacenters reported, and the
  client never sees a terminal status before the job's (the gate
  relayed dc-east's final status as the job's, so the client finished
  -- with one datacenter's numbers -- while dc-west still ran).

Timelines (seed 61, probe-measured): both datacenters are first
classified healthy once their managers heartbeat the gate, so the client
submits after two heartbeat periods (20.0); submit 20.08, dispatch to
both 20.17, execution 20.25-21.5, dc-east's final result at the gate
21.21. dc-west is killed at 20.6, inside its execution.
"""

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager.config import create_manager_config_from_env
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.soak_job_demo import (
    soak_gate_dispatch_client_entry,
)
from tests.unit.simulation.sim.test_multiprocess_dc_loss import (
    _DC_VICTIMS,
    _DURATION_SECONDS,
    _JOB_TIMEOUT_SECONDS,
    _SEED,
    _TERMINAL_SLACK_SECONDS,
    _TRACKER_TICK_SECONDS,
    _add_multi_dc_topology,
    _assert_no_unswapped_imports,
    _assert_oracle_clean,
    _finished,
    _submitted_at,
)

_JOB_DATACENTERS = 2
# Both managers have heartbeated the gate (healthy, with capacity) by the
# end of their second heartbeat period; earlier, dc-east is still
# INITIALIZING and the job is placed in dc-west alone.
_SUBMIT_AT = 2 * create_manager_config_from_env("sim-mgr", 9000, 9001, Env()).gate_heartbeat_interval_seconds
_LOSS_AT = 20.6
_DEADLINE_SECONDS = 20.0
_DEADLINE_CHECK_INTERVAL_SECONDS = Env().BEST_EFFORT_DEADLINE_CHECK_INTERVAL
_CEILING = _SUBMIT_AT + _JOB_TIMEOUT_SECONDS + _TRACKER_TICK_SECONDS + 15.0
_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "timeout"})


def _run(
    *,
    best_effort: bool,
    min_dcs: int = 0,
    deadline_seconds: float = 0.0,
    lose_west: bool = True,
) -> dict:
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=_CEILING, seed=_SEED)
    _add_multi_dc_topology(coordinator)
    coordinator.add_process(
        "client-a",
        soak_gate_dispatch_client_entry,
        "sim-cli-a",
        9500,
        ("sim-gate-a", 9000),
        _DURATION_SECONDS,
        _JOB_TIMEOUT_SECONDS,
        _CEILING,
        _SUBMIT_AT,
        None,
        _JOB_DATACENTERS,
        best_effort,
        min_dcs,
        deadline_seconds,
    )
    if lose_west:
        for victim_id in _DC_VICTIMS["dc-west"]:
            coordinator.schedule_kill(victim_id, _LOSS_AT)
    return coordinator.run()


def _best_effort_outcome(client_log: list) -> tuple[str, tuple[str, ...]]:
    (outcome,) = [entry for entry in client_log if entry[0] == "best-effort-outcome"]
    return outcome[1], outcome[2]


def _active_window(worker_log: list) -> tuple[float, float]:
    """When the worker started running the job and when it went idle."""
    changes = [(entry[1], entry[2]) for entry in worker_log if entry[0] == "workflows-active"]
    started = next(time for count, time in changes if count > 0)
    idle = next(time for count, time in changes if count == 0 and time > started)
    return started, idle


def _statuses_seen_before_finish(client_log: list) -> list[str]:
    finished_time = _finished(client_log)[2]
    return [
        entry[1]
        for entry in client_log
        if entry[0] == "status-seen" and entry[2] < finished_time
    ]


def _run_min_one_with_loss() -> dict:
    return _run(best_effort=True, min_dcs=1)


def test_best_effort_completes_with_the_surviving_datacenter():
    results = _run_min_one_with_loss()
    client_log = results["client-a"]

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    assert _best_effort_outcome(client_log) == (
        "best_effort: min_dcs_reached (1/1)",
        ("dc-west",),
    ), client_log

    # Completed with dc-east's own completion -- not by any timeout.
    east_started, east_idle = _active_window(results["worker-dc-east"])
    assert east_started < _LOSS_AT, results["worker-dc-east"]
    assert east_started < finished_time <= east_idle + _TERMINAL_SLACK_SECONDS, (
        finished_time,
        results["worker-dc-east"],
    )
    assert not set(_statuses_seen_before_finish(client_log)) & _TERMINAL_STATUSES, client_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_best_effort_min_one_is_replay_deterministic():
    assert _run_min_one_with_loss() == _run_min_one_with_loss()


def test_best_effort_deadline_completes_with_what_reported():
    results = _run(best_effort=True, min_dcs=2, deadline_seconds=_DEADLINE_SECONDS)
    client_log = results["client-a"]

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    assert _best_effort_outcome(client_log) == (
        "best_effort: deadline_expired (completed: 1)",
        ("dc-west",),
    ), client_log

    # The deadline counts from dispatch, which follows submission; the
    # periodic check completes the job within one interval after it.
    earliest = _submitted_at(client_log) + _DEADLINE_SECONDS
    latest = earliest + _DEADLINE_CHECK_INTERVAL_SECONDS + _TERMINAL_SLACK_SECONDS
    assert earliest <= finished_time <= latest, (earliest, finished_time, latest, client_log)
    assert not set(_statuses_seen_before_finish(client_log)) & _TERMINAL_STATUSES, client_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_without_best_effort_a_lost_datacenter_holds_the_job_to_its_timeout():
    results = _run(best_effort=False)
    client_log = results["client-a"]

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "timeout", client_log
    earliest = _submitted_at(client_log) + _JOB_TIMEOUT_SECONDS
    latest = earliest + _TRACKER_TICK_SECONDS + _TERMINAL_SLACK_SECONDS
    assert earliest <= finished_time <= latest, (earliest, finished_time, latest, client_log)
    # dc-east finished long before; its final status was not the job's.
    assert not set(_statuses_seen_before_finish(client_log)) & _TERMINAL_STATUSES, client_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_multi_datacenter_job_finishes_only_when_every_datacenter_reported():
    results = _run(best_effort=False, lose_west=False)
    client_log = results["client-a"]

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    for worker_id in ("worker-dc-east", "worker-dc-west"):
        started, _idle = _active_window(results[worker_id])
        assert started < finished_time, (worker_id, results[worker_id])
    assert not set(_statuses_seen_before_finish(client_log)) & _TERMINAL_STATUSES, client_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)
