"""
AD-39 clock fencing over the multi-process coordinator, per tier: three
peered managers in one datacenter (client submitting to the manager
tier), or three peered gates fronting one datacenter (client submitting
through one gate). One node's wall clock is stepped by a skew schedule
(``VirtualClock.set_wall_offset``, the NTP-step model).

Every manager or gate logs transitions of: whether its clock is fenced, whether
it is the datacenter (SWIM) leader, how many per-job Raft groups it
leads, and whether it advertises accepting jobs -- booleans and counts
only, inside the replay contract.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
from pathlib import Path

from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .chaos_cluster_demo import apply_clock_skew_schedule
from .gate_cluster_demo import multi_gate_manager_entry
from .gate_ledger_demo import GATE_HOSTS
from .job_dispatch_demo import gate_dispatch_client_entry, multi_manager_client_entry
from .peered_manager_demo import PEERED_MANAGERS, _env
from .simulation_coordinator import SimulationCoordinator
from .worker_manager_demo import worker_entry

WATCH_INTERVAL_SECONDS = 0.1


def clock_fence_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    peer_tcp_addresses,
    peer_udp_addresses,
    clock_skew_schedule,
) -> None:
    """Manager child whose wall clock follows ``clock_skew_schedule``
    (``("wall_skew", at, delta_seconds)`` steps); logs ``("fenced",
    bool, t)``, ``("dc-leader", bool, t)``, ``("raft-leading", count,
    t)`` and ``("accepting", bool, t)`` transitions."""
    apply_clock_skew_schedule(context, clock_skew_schedule)
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        seed_managers=list(peer_tcp_addresses),
        manager_udp_peers=list(peer_udp_addresses),
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    async def watch() -> None:
        last_values: dict[str, bool | int] = {}
        while True:
            values = {
                "fenced": manager._clock_offset_monitor.is_fenced,
                "dc-leader": manager.is_leader(),
                "raft-leading": sum(
                    node.is_leader() for node in manager._raft.consensus._nodes.values()
                ),
                "accepting": manager._is_accepting_jobs(),
            }
            for tag, value in values.items():
                if last_values.get(tag) != value:
                    last_values[tag] = value
                    log.append((tag, value, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch())


def run_clock_fence_scenario(
    max_virtual_time: float,
    skewed_host: str,
    clock_skew_schedule: list[tuple[str, float, float]],
    job_timeout_seconds: float = 30.0,
    wait_timeout_seconds: float = 45.0,
    seed: int = 23,
) -> dict:
    """Three peered managers (``skewed_host`` follows the skew schedule),
    one worker seeded at the first manager, and a client submitting one
    job to the tier, under ``seed``. Returns every child's log."""
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=max_virtual_time, seed=seed)
    for host, tcp_port, udp_port in PEERED_MANAGERS:
        coordinator.add_process(
            host,
            clock_fence_manager_entry,
            host,
            tcp_port,
            udp_port,
            "sim-dc",
            [(peer, peer_tcp) for peer, peer_tcp, _ in PEERED_MANAGERS if peer != host],
            [(peer, peer_udp) for peer, _, peer_udp in PEERED_MANAGERS if peer != host],
            clock_skew_schedule if host == skewed_host else [],
        )
    seed_host, seed_tcp, _ = PEERED_MANAGERS[0]
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", (seed_host, seed_tcp), 2
    )
    coordinator.add_process(
        "client",
        multi_manager_client_entry,
        "sim-cli",
        9500,
        [(host, tcp_port) for host, tcp_port, _ in PEERED_MANAGERS],
        False,
        job_timeout_seconds,
        wait_timeout_seconds,
    )
    return coordinator.run()


def clock_fence_gate_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_managers,
    datacenter_manager_udp,
    gate_tcp_peers,
    gate_udp_peers,
    clock_skew_schedule,
) -> None:
    """Gate child whose wall clock follows ``clock_skew_schedule``; logs
    ``("fenced", bool, t)``, ``("gate-leader", bool, t)`` and
    ``("raft-leading", count, t)`` transitions."""
    apply_clock_skew_schedule(context, clock_skew_schedule)
    gate = GateServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id="global",
        datacenter_managers=datacenter_managers,
        datacenter_manager_udp=datacenter_manager_udp,
        gate_peers=gate_tcp_peers,
        gate_udp_peers=gate_udp_peers,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await gate.start()
        log.append(("gate-started", round(context.loop.time(), 6)))
        # The Raft integration exists once start() ran.
        context.loop.create_task(watch())

    async def watch() -> None:
        last_values: dict[str, bool | int] = {}
        while True:
            values = {
                "fenced": gate._clock_offset_monitor.is_fenced,
                "gate-leader": gate.is_leader(),
                "raft-leading": sum(
                    node.is_leader() for node in gate._raft.consensus._nodes.values()
                ),
            }
            for tag, value in values.items():
                if last_values.get(tag) != value:
                    last_values[tag] = value
                    log.append((tag, value, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())


def run_gate_clock_fence_scenario(
    max_virtual_time: float,
    skewed_host: str,
    clock_skew_schedule: list[tuple[str, float, float]],
) -> dict:
    """Three peered gates (``skewed_host`` follows the skew schedule)
    fronting one datacenter (manager + worker), and a client submitting
    one job through gate-b. Returns every child's log."""
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=max_virtual_time, seed=43)
    datacenter_managers = {"sim-dc": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"sim-dc": [("sim-mgr", 9001)]}
    for gate_host in GATE_HOSTS:
        peer_hosts = [host for host in GATE_HOSTS if host != gate_host]
        coordinator.add_process(
            gate_host,
            clock_fence_gate_entry,
            gate_host,
            9000,
            9001,
            datacenter_managers,
            datacenter_manager_udp,
            [(peer_host, 9000) for peer_host in peer_hosts],
            [(peer_host, 9001) for peer_host in peer_hosts],
            clock_skew_schedule if gate_host == skewed_host else [],
        )
    coordinator.add_process(
        "manager",
        multi_gate_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        [(gate_host, 9000) for gate_host in GATE_HOSTS],
        [(gate_host, 9001) for gate_host in GATE_HOSTS],
    )
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "client", gate_dispatch_client_entry, "sim-cli", 9500, (GATE_HOSTS[1], 9000)
    )
    return coordinator.run()
