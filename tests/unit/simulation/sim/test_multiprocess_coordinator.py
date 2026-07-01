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
from tests.simulation.harness.sim.multiprocess.demo_endpoints import (
    ping_pong_entry,
    repeating_ping_entry,
    spawning_parent_entry,
)


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


def _run_dynamic_spawn() -> dict:
    coordinator = SimulationCoordinator(latency=0.5)
    coordinator.add_process(
        "parent", spawning_parent_entry, ("proc", 1), "late", ("proc", 2), 3.0
    )
    return coordinator.run()


def test_dynamically_spawned_child_joins_at_global_virtual_time():
    """A child admitted mid-run (the ``ProcessSpawner`` seam) starts its
    clock at the admitting barrier's global time — 3.0, not 0.0 — and
    exchanges messages coherently in both directions."""
    results = _run_dynamic_spawn()

    assert results["parent"] == [
        (3.5, "recv", "ping", ("proc", 2)),
    ]
    assert results["late"] == [
        ("started", 3.0),
        (3.0, "send", "ping", ("proc", 1)),
        (4.0, "recv", "pong", ("proc", 1)),
    ]


def test_dynamic_spawn_is_replay_deterministic():
    assert _run_dynamic_spawn() == _run_dynamic_spawn()


def _run_kill() -> dict:
    coordinator = SimulationCoordinator(latency=0.5)
    coordinator.add_process(
        "sender", repeating_ping_entry, ("proc", 1), ("proc", 2), 1.0
    )
    coordinator.add_process("receiver", ping_pong_entry, ("proc", 2), ("proc", 1), "pong")
    coordinator.schedule_kill("sender", at_time=3.5)
    return coordinator.run()


def test_killed_process_goes_silent_at_its_virtual_instant():
    """A process killed at K executes nothing at or after K: the
    receiver sees exactly the heartbeats sent strictly before the kill
    (1.0, 2.0, 3.0 -> delivered 1.5, 2.5, 3.5), its replies toward the
    dead sender drop silently, and the victim — otherwise immortal —
    produces no RESULT."""
    results = _run_kill()

    assert "sender" not in results
    assert results["receiver"] == [
        (1.5, "recv", "ping", ("proc", 1)),
        (2.5, "recv", "ping", ("proc", 1)),
        (3.5, "recv", "ping", ("proc", 1)),
    ]


def test_kill_is_replay_deterministic():
    assert _run_kill() == _run_kill()
