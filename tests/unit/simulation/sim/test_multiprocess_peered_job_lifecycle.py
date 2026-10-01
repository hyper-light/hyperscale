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

Pinned: the leader drops the job and its group at completion; followers
drop the group as soon as the leader's terminal sync lands and the job at
their retention sweep; every member ends with nothing held. Bounds come
from the mechanism: the follower's group goes in the same sync that
precedes the leader's removal (one watcher sample of slack), the
follower's job within retention + one cleanup interval + one sample.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    multi_manager_client_entry,
)
from tests.simulation.harness.sim.multiprocess.peered_manager_demo import (
    WATCH_INTERVAL_SECONDS,
    peered_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)

_CEILING = 90.0
_JOB_RETENTION_SECONDS = 10.0
_JOB_CLEANUP_INTERVAL_SECONDS = 2.0
_MANAGERS = [(f"sim-mgr-{name}", 9000, 9001) for name in "abc"]


def _run_peered_job() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=23
    )
    for host, tcp_port, udp_port in _MANAGERS:
        coordinator.add_process(
            host,
            peered_manager_entry,
            host,
            tcp_port,
            udp_port,
            "sim-dc",
            [(peer, peer_tcp) for peer, peer_tcp, _ in _MANAGERS if peer != host],
            [(peer, peer_udp) for peer, _, peer_udp in _MANAGERS if peer != host],
            _JOB_RETENTION_SECONDS,
            _JOB_CLEANUP_INTERVAL_SECONDS,
        )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        (_MANAGERS[0][0], _MANAGERS[0][1]),
        2,
    )
    coordinator.add_process(
        "client",
        multi_manager_client_entry,
        "sim-cli",
        9500,
        [(host, tcp_port) for host, tcp_port, _ in _MANAGERS],
    )
    return coordinator.run()


def _transitions(log: list, tag: str) -> list[tuple[int, float]]:
    return [(entry[1], entry[2]) for entry in log if entry[0] == tag]


def _first_time_at(log: list, tag: str, count: int) -> float:
    return next(time for value, time in _transitions(log, tag) if value == count)


def test_every_member_releases_the_job_and_its_raft_group():
    results = _run_peered_job()

    finished = [entry for entry in results["client"] if entry[0] == "job-finished"]
    assert [entry[1] for entry in finished] == ["completed"], results["client"]
    finished_time = finished[0][2]

    manager_logs = {host: results[host] for host, _, _ in _MANAGERS}
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
    assert leader_removed_at <= finished_time + WATCH_INTERVAL_SECONDS

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
