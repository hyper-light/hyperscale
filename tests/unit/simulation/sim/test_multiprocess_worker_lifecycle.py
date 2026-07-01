"""
The worker node's pool bring-up under multi-process SIM.

One coordinator child runs the production ``WorkerLifecycleManager``
sequence from ``WorkerServer.start()`` — pool setup, ``RemoteGraphManager``
leader start, ``run_pool``, and the production ``connect_to_workers``
wait path (``wait_for_workers`` + leader-side ``connect_client`` to each
executor). The seams flow exactly as ``WorkerServer`` threads them:
``transport_factory`` through the lifecycle manager to the
``RemoteGraphManager``'s leader controller, ``process_spawner`` to the
``LocalServerPool`` so the executors are admitted as coordinator child
processes mid-run.

Asserts the full bring-up completes at a deterministic virtual time
with every executor acknowledged, that the SIM exit-code snapshot is
empty by design (executors are coordinator children, not pool
subprocesses), and byte-identical replay across two full runs.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.worker_lifecycle_demo import (
    worker_lifecycle_entry,
)


def _run_worker_lifecycle() -> dict:
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=30.0)
    coordinator.add_process(
        "worker-node", worker_lifecycle_entry, "sim-worker", 199, 200, 2
    )
    return coordinator.run()


def test_worker_lifecycle_pool_bring_up_under_sim():
    results = _run_worker_lifecycle()

    # local_udp = 200 + 2**2 = 204; executor ports = 208, 210 — derived
    # by the production get_worker_ips math, spawned as coordinator
    # children by the pool.
    assert "executor-sim-worker-208" in results
    assert "executor-sim-worker-210" in results

    (tag, acknowledged, exitcodes, virtual_time) = results["worker-node"][0]
    assert tag == "pool-connected"
    assert acknowledged == ["sim-worker:208", "sim-worker:210"]
    assert exitcodes == {}
    # Executors admitted at 0.0 handshake by 0.02 and acknowledge by
    # 0.03; the production wait path then connects back to each executor
    # (one more round trip) and completes at a fixed virtual instant.
    assert virtual_time == 0.05


def test_worker_lifecycle_under_sim_is_replay_deterministic():
    assert _run_worker_lifecycle() == _run_worker_lifecycle()
