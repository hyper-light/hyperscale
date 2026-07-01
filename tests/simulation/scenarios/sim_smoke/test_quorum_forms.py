"""
Phase 6d end-to-end SIM smoke: three real managers form quorum.

The first fully in-process cluster scenario. Three production
``ManagerServer`` instances — with their entire SWIM membership +
Lifeguard + Raft leader-election machinery and every background loop —
run under the ``SimulationLoop`` sharing one ``InProcessTransport``.
They discover each other over (fake) UDP, run an election in virtual
time, and converge on a single agreed leader with quorum, at zero
wall-clock cost and with no OS sockets or banned event-loop operations.

This is the proof that the SIM stack composes end to end: transport
seam (Phase 6d) + DI kwarg threading through the node servers + the
``VirtualClock`` / ``SeededRandom`` / ``swap_defaults`` machinery + the
logging kill-switch. Everything the REAL cluster does at the consensus
layer, the SIM cluster does deterministically in one process.

Convergence budget: leader election is pre-vote (~2s) + election
timeout (5–7s, jittered) per attempt, plus SWIM discovery; 60 virtual
seconds comfortably covers a couple of election cycles. Because time is
virtual, those 60s cost effectively nothing in wall-clock — the whole
test runs in well under a second.
"""

import asyncio

import pytest

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.manager.server import ManagerServer
from tests.simulation.harness.sim import SimulationRuntime


def _manager_cluster_config() -> list[dict[str, int]]:
    """Three managers, each with a distinct TCP/UDP port pair."""
    return [
        {"tcp": 9000, "udp": 9001},
        {"tcp": 9002, "udp": 9003},
        {"tcp": 9004, "udp": 9005},
    ]


def _build_managers(
    runtime: SimulationRuntime,
    configs: list[dict[str, int]],
    host: str = "127.0.0.1",
    dc_id: str = "sim-dc",
) -> list[ManagerServer]:
    """Cross-wire each manager's peer set (all others, excluding self)."""
    managers: list[ManagerServer] = []
    for my in configs:
        peer_tcp = [(host, cfg["tcp"]) for cfg in configs if cfg["tcp"] != my["tcp"]]
        peer_udp = [(host, cfg["udp"]) for cfg in configs if cfg["udp"] != my["udp"]]
        managers.append(
            ManagerServer(
                host,
                my["tcp"],
                my["udp"],
                Env(),
                dc_id=dc_id,
                seed_managers=peer_tcp,
                manager_udp_peers=peer_udp,
                **runtime.sim_kwargs(),
            )
        )
    return managers


def test_three_managers_form_quorum_under_sim():
    """Three managers elect exactly one leader, agreed by all, with quorum."""
    runtime = SimulationRuntime(seed=1)
    try:
        managers = _build_managers(runtime, _manager_cluster_config())

        async def scenario():
            # start() is non-blocking: it brings up listeners and spawns
            # the election / raft / probe background loops, then returns.
            await asyncio.gather(*[manager.start() for manager in managers])
            # Advance virtual time to let the election converge.
            await asyncio.sleep(60.0)
            leader_indices = [
                index
                for index, manager in enumerate(managers)
                if manager.is_leader()
            ]
            quorum = [manager._has_quorum_available() for manager in managers]
            agreed_leader = {manager.get_current_leader() for manager in managers}
            return leader_indices, quorum, agreed_leader

        leader_indices, quorum, agreed_leader = runtime.run(scenario())

        # Exactly one manager considers itself leader.
        assert len(leader_indices) == 1, (
            f"expected exactly one leader, got indices {leader_indices}"
        )
        # Every manager has quorum available.
        assert all(quorum), f"not all managers have quorum: {quorum}"
        # All three managers agree on the same (single) leader address.
        assert len(agreed_leader) == 1 and None not in agreed_leader, (
            f"managers disagree on the leader: {agreed_leader}"
        )
    finally:
        runtime.close()


def test_quorum_forms_in_zero_wall_time():
    """The 60 virtual seconds of convergence cost ~no real time — proof
    the virtual clock, not wall-clock waiting, drives the election."""
    import time as wall_time

    runtime = SimulationRuntime(seed=7)
    try:
        managers = _build_managers(runtime, _manager_cluster_config())

        async def scenario():
            await asyncio.gather(*[manager.start() for manager in managers])
            await asyncio.sleep(60.0)
            return sum(1 for manager in managers if manager.is_leader())

        wall_start = wall_time.monotonic()
        leader_count = runtime.run(scenario())
        wall_elapsed = wall_time.monotonic() - wall_start

        assert leader_count == 1
        # 60s of virtual election time must not translate to real waiting.
        assert wall_elapsed < 10.0, (
            f"election took {wall_elapsed:.1f}s wall — virtual time is not "
            "driving the clock"
        )
    finally:
        runtime.close()
