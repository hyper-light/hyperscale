"""
Multi-process deterministic simulation: coordinator correctness + replay.

Spawns two REAL OS processes, each running its own ``SimulationLoop``,
and drives them through the ``SimulationCoordinator``. Process A sends a
``ping`` to process B at virtual time 5.0; B replies ``pong``. With the
coordinator's fixed 0.5s cross-process latency, B must see the ping at
5.5 and A must see the pong at 6.0 — virtual time is globally coherent
across the process boundary. Running the whole thing twice must yield
byte-identical per-process logs (replay-determinism across processes).
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.demo_endpoints import ping_pong_entry


def _run_ping_pong() -> dict:
    coordinator = SimulationCoordinator(latency=0.5)
    coordinator.add_process("A", ping_pong_entry, ("proc", 1), ("proc", 2), "ping")
    coordinator.add_process("B", ping_pong_entry, ("proc", 2), ("proc", 1), "pong")
    return coordinator.run()


def test_message_crosses_processes_at_correct_virtual_time():
    results = _run_ping_pong()

    assert results["A"] == [
        (5.0, "send", "ping", ("proc", 2)),
        (6.0, "recv", "pong", ("proc", 2)),
    ]
    assert results["B"] == [
        (5.5, "recv", "ping", ("proc", 1)),
    ]


def test_multi_process_run_is_replay_deterministic():
    first = _run_ping_pong()
    second = _run_ping_pong()
    assert first == second
