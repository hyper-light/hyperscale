"""
The full node pair under multi-process SIM: a real ``ManagerServer`` and
a real ``WorkerServer`` as coordinator children.

Everything runs at once, all of it production code: the manager's SWIM +
Raft self-election and background loops; the worker's full ``start()`` —
executor pool spawned as coordinator children mid-run (``ProcessSpawner``
seam), pool leader handshake over the datagram boundary, TCP
registration with the manager over the stream boundary, SWIM probes both
ways, background loops on virtual timers. The run is bounded by the
coordinator's virtual-time ceiling (probe cycles never quiesce) and
asserted on milestones + replay.

Milestones are ``(tag, virtual_time)`` only — identities and nonces are
legitimately fresh per run; the *schedule* is what must replay.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

_CEILING = 30.0


def _run_pair() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=11
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    return coordinator.run()


def _times(log: list, tag: str) -> list:
    return [entry[1] for entry in log if entry[0] == tag]


def test_worker_and_manager_run_end_to_end_under_sim():
    results = _run_pair()

    # The worker's pool executors were admitted as coordinator children
    # (ports derive from udp 9001: local leader 9005, executors 9009/9011).
    assert "executor-sim-wkr-9009" in results
    assert "executor-sim-wkr-9011" in results

    manager_log = results["manager"]
    worker_log = results["worker"]

    # Both nodes complete their full production start() on virtual time.
    (manager_started,) = _times(manager_log, "manager-started")
    (worker_started,) = _times(worker_log, "worker-started")
    assert 0.0 <= manager_started < _CEILING
    assert 0.0 <= worker_started < _CEILING

    # The worker registered with the manager (manager-side registry) and
    # observed the manager healthy (worker-side registry) before the
    # ceiling — the TCP registration + SWIM paths completed end to end.
    (worker_registered,) = _times(manager_log, "worker-registered")
    (manager_healthy,) = _times(worker_log, "manager-healthy")
    assert manager_started <= worker_registered < _CEILING
    assert worker_started <= manager_healthy < _CEILING


def test_worker_manager_pair_is_replay_deterministic():
    assert _run_pair() == _run_pair()
