"""
``LocalServerPool`` under multi-process SIM: executors are coordinator
child processes.

One coordinator child runs the production pool-leader
``RemoteGraphController`` plus a ``LocalServerPool`` whose
``process_spawner`` seam points at the child's context. ``run_pool``
therefore requests its executors from the ``SimulationCoordinator``
instead of a ``ProcessPoolExecutor`` — each executor is a REAL OS
process admitted at the window barrier, running the unchanged production
lifecycle (``run_server``: start server, connect back to the leader over
the encrypted UDP handshake, ``acknowledge_start``, serve) on a lockstep
``SimulationLoop``.

Asserts the pool's executors all acknowledge at a deterministic virtual
time, that the coordinator tracked them as first-class children, and
byte-identical replay across two full runs.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.server_pool_demo import (
    pool_leader_entry,
)


def _run_pool() -> dict:
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=30.0)
    coordinator.add_process("worker-node", pool_leader_entry, "sim-pool", 100, 2)
    return coordinator.run()


def test_local_server_pool_spawns_executors_as_coordinator_children():
    results = _run_pool()

    # Both executors were admitted as coordinator children (their ids are
    # derived from their worker addresses) and survived to shutdown.
    assert "executor-sim-pool-101" in results
    assert "executor-sim-pool-102" in results

    # Timeline: executors admitted at 0.0 send their connect handshake at
    # 0.0 (leader handles it at 0.01, replies land 0.02), acknowledge at
    # 0.02 (leader records both at 0.03), and the leader's 0.05 poll tick
    # observes the full pool.
    assert results["worker-node"] == [
        (
            "workers-acknowledged",
            ["sim-pool:101", "sim-pool:102"],
            0.05,
        ),
    ]


def test_local_server_pool_under_sim_is_replay_deterministic():
    assert _run_pool() == _run_pool()
