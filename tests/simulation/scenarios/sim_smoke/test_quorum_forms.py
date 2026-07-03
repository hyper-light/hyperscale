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
    """Three managers converge on ONE leader and HOLD it — stable
    across a window, not merely at a single snapshot.

    The earlier version asserted leadership at one instant (t=60). That
    passed *by luck* over a churning cluster: the DC leader oscillated
    every election-timeout period (the leader's byte-identical
    heartbeats were dropped by the SWIM content-hash duplicate
    suppressor, so follower leases renewed once per term and then
    expired → continuous re-election), and t=60 happened to land on a
    converged instant. Sampling across a window makes the churn a
    failure instead of a coin flip, and asserts the real property: a
    single, agreed, STABLE leader.
    """
    runtime = SimulationRuntime(seed=1)
    try:
        managers = _build_managers(runtime, _manager_cluster_config())

        async def scenario():
            # start() is non-blocking: it brings up listeners and spawns
            # the election / raft / probe background loops, then returns.
            await asyncio.gather(*[manager.start() for manager in managers])
            # Let the election converge, then SAMPLE leadership across a
            # window so any oscillation is caught, not snapshot-hidden.
            await asyncio.sleep(30.0)
            samples = []
            for _ in range(7):
                await asyncio.sleep(5.0)
                samples.append(
                    (
                        tuple(
                            index
                            for index, manager in enumerate(managers)
                            if manager.is_leader()
                        ),
                        frozenset(
                            manager.get_current_leader() for manager in managers
                        ),
                        all(manager._has_quorum_available() for manager in managers),
                    )
                )
            return samples

        samples = runtime.run(scenario())

        for leader_indices, agreed_leader, has_quorum in samples:
            # Exactly one manager considers itself leader, at EVERY sample.
            assert len(leader_indices) == 1, (
                f"expected exactly one self-leader at every sample, "
                f"got {leader_indices} in {samples}"
            )
            # All three agree on that single leader (no None), at every sample.
            assert len(agreed_leader) == 1 and None not in agreed_leader, (
                f"managers disagree on the leader: {agreed_leader} in {samples}"
            )
            assert has_quorum, f"not all managers have quorum in {samples}"

        # The SAME leader across the whole window — stable, not churning.
        distinct_leaders = {agreed for _, agreed, _ in samples}
        assert len(distinct_leaders) == 1, (
            f"DC leadership oscillated across the window: {samples}"
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
