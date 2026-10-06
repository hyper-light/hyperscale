"""
A job's footprint across a peered manager tier, end to end under
multi-process SIM: three peered ``ManagerServer`` children, a worker, and a
client submitting to the manager tier.

Measured before the fix, after the job completed every member kept its
per-job Raft group forever (``destroy_job_raft`` had no caller, and RPCs
re-created groups on arrival), and both followers kept the job itself
forever: the leader removed the job without telling peers it was
terminal, and a follower's hydrated terminal job never got
``completed_at`` — which the retention sweep requires.

Measured too: the leader's dispatcher kept the job's queue entry for the
process's lifetime -- completion removed the job from the JobManager, and
the retention sweep that also cleaned the dispatcher walks only the
JobManager's jobs.

Pinned: the leader drops the job, its group and its dispatcher state at
completion; followers
drop the group as soon as the leader's terminal sync lands and the job at
their retention sweep; every member ends with nothing held. Bounds come
from the mechanism: the leader's removal follows the client's terminal by
the completion exchanges still running (one watcher sample of slack), the
follower's group goes in the same sync that precedes the leader's removal
(one sample), the follower's job within retention + one cleanup interval
+ one sample.
"""

from tests.simulation.harness.sim.multiprocess.peered_manager_demo import (
    LINK_LATENCY_SECONDS,
    PEERED_MANAGERS,
    WATCH_INTERVAL_SECONDS,
    run_peered_manager_job,
)

_CEILING = 90.0
_JOB_RETENTION_SECONDS = 10.0
_JOB_CLEANUP_INTERVAL_SECONDS = 2.0
# The client finishes on the job's final result; the leader's completion
# then still runs the final result's reply leg, the terminal status
# push's round trip, and the terminal sync's round trip to its peers
# (measured with a 5ms watcher: client 8.465, leader removal 8.515).
_COMPLETION_LEGS_AFTER_CLIENT_TERMINAL = 5


def _run_peered_job() -> dict:
    return run_peered_manager_job(
        _CEILING, _JOB_RETENTION_SECONDS, _JOB_CLEANUP_INTERVAL_SECONDS
    )


def _transitions(log: list, tag: str) -> list[tuple[int, float]]:
    return [(entry[1], entry[2]) for entry in log if entry[0] == tag]


def _first_time_at(log: list, tag: str, count: int) -> float:
    return next(time for value, time in _transitions(log, tag) if value == count)


def test_every_member_releases_the_job_and_its_raft_group():
    results = _run_peered_job()

    finished = [entry for entry in results["client"] if entry[0] == "job-finished"]
    assert [entry[1] for entry in finished] == ["completed"], results["client"]
    finished_time = finished[0][2]

    manager_logs = {host: results[host] for host, _, _ in PEERED_MANAGERS}
    for host, log in manager_logs.items():
        assert _transitions(log, "jobs")[-1][0] == 0, (host, log)
        assert _transitions(log, "raft-groups")[-1][0] == 0, (host, log)
        assert max(value for value, _ in _transitions(log, "raft-groups")) == 1, (host, log)

    # Every member ends at zero jobs (asserted above); the leader is the
    # one that released its job first — at completion, not at a sweep.
    job_released_at = {
        host: [time for value, time in _transitions(log, "jobs") if value == 0][-1]
        for host, log in manager_logs.items()
    }
    leader_host = min(job_released_at, key=job_released_at.__getitem__)
    leader_removed_at = job_released_at[leader_host]
    assert leader_removed_at <= (
        finished_time
        + _COMPLETION_LEGS_AFTER_CLIENT_TERMINAL * LINK_LATENCY_SECONDS
        + WATCH_INTERVAL_SECONDS
    )

    # The dispatcher's per-job state leaves with the job. It was held for
    # the process's lifetime: the retention sweep that cleaned it walks the
    # JobManager's jobs, which the job had already left at completion
    # (measured: the leader's queue entry stayed at 1 to the run's end).
    for host, log in manager_logs.items():
        assert _transitions(log, "dispatcher-pending")[-1][0] == 0, (host, log)
        assert _transitions(log, "dispatch-loops")[-1][0] == 0, (host, log)
    leader_pending = _transitions(manager_logs[leader_host], "dispatcher-pending")
    assert max(value for value, _ in leader_pending) >= 1, leader_pending
    assert [time for value, time in leader_pending if value == 0][-1] <= (
        leader_removed_at + WATCH_INTERVAL_SECONDS
    ), leader_pending

    for host, log in manager_logs.items():
        if host == leader_host:
            continue
        group_released_at = _first_time_at(log, "raft-groups", 0)

        assert group_released_at <= leader_removed_at + WATCH_INTERVAL_SECONDS, (host, log)
        assert job_released_at[host] <= (
            finished_time
            + _JOB_RETENTION_SECONDS
            + _JOB_CLEANUP_INTERVAL_SECONDS
            + WATCH_INTERVAL_SECONDS
        ), (host, log)


def test_peered_job_lifecycle_is_replay_deterministic():
    assert _run_peered_job() == _run_peered_job()
