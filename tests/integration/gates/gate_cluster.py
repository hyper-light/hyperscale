"""
Builders for the gate tier's in-process integration tests: gates that
know their peer gates and each datacenter's managers, managers that know
their datacenter peers and the gates, and workers seeded with their
datacenter's managers. Every node runs on loopback with its TCP port
reserved by the caller (UDP is TCP + 1) and its Env from ``node_env``, so
everything it writes lands in the test's own directory.
"""

import asyncio
import pathlib
from collections.abc import Iterable

from hyperscale.distributed.nodes import GateServer, ManagerServer, WorkerServer
from tests.integration.in_process_nodes import LOCALHOST, node_env

__all__ = [
    "new_gate",
    "new_gates",
    "new_manager",
    "new_managers",
    "new_workers",
    "start_nodes",
    "tcp_addresses",
    "udp_addresses",
]


def tcp_addresses(tcp_ports: Iterable[int]) -> list[tuple[str, int]]:
    """The loopback TCP address of each node."""
    return [(LOCALHOST, tcp_port) for tcp_port in tcp_ports]


def udp_addresses(tcp_ports: Iterable[int]) -> list[tuple[str, int]]:
    """The loopback UDP (SWIM) address of each node: its TCP port + 1."""
    return [(LOCALHOST, tcp_port + 1) for tcp_port in tcp_ports]


def new_gate(
    node_directory: pathlib.Path,
    gate_port: int,
    cluster_gate_ports: list[int],
    datacenter_manager_ports: dict[str, list[int]],
    **env_overrides: str | int | float,
) -> GateServer:
    """The gate on ``gate_port``, peered with every other gate of
    ``cluster_gate_ports`` and configured with every datacenter's managers."""
    peer_ports = [peer_port for peer_port in cluster_gate_ports if peer_port != gate_port]
    return GateServer(
        host=LOCALHOST,
        tcp_port=gate_port,
        udp_port=gate_port + 1,
        env=node_env(node_directory, **env_overrides),
        datacenter_managers={
            datacenter_id: tcp_addresses(manager_ports)
            for datacenter_id, manager_ports in datacenter_manager_ports.items()
        },
        datacenter_manager_udp={
            datacenter_id: udp_addresses(manager_ports)
            for datacenter_id, manager_ports in datacenter_manager_ports.items()
        },
        gate_peers=tcp_addresses(peer_ports),
        gate_udp_peers=udp_addresses(peer_ports),
    )


def new_gates(
    node_directory: pathlib.Path,
    gate_ports: list[int],
    datacenter_manager_ports: dict[str, list[int]],
    **env_overrides: str | int | float,
) -> list[GateServer]:
    """One gate per TCP port in ``gate_ports``, each peered with every
    other gate and configured with every datacenter's managers."""
    return [
        new_gate(node_directory, gate_port, gate_ports, datacenter_manager_ports, **env_overrides)
        for gate_port in gate_ports
    ]


def new_manager(
    node_directory: pathlib.Path,
    datacenter_id: str,
    manager_port: int,
    datacenter_manager_ports: list[int],
    gate_ports: list[int],
    **env_overrides: str | int | float,
) -> ManagerServer:
    """The manager of ``datacenter_id`` on ``manager_port``, peered with
    the datacenter's other managers and registering with every gate in
    ``gate_ports``."""
    peer_ports = [peer_port for peer_port in datacenter_manager_ports if peer_port != manager_port]
    return ManagerServer(
        host=LOCALHOST,
        tcp_port=manager_port,
        udp_port=manager_port + 1,
        env=node_env(node_directory, **env_overrides),
        dc_id=datacenter_id,
        manager_peers=tcp_addresses(peer_ports),
        manager_udp_peers=udp_addresses(peer_ports),
        gate_addrs=tcp_addresses(gate_ports),
        gate_udp_addrs=udp_addresses(gate_ports),
    )


def new_managers(
    node_directory: pathlib.Path,
    datacenter_id: str,
    manager_ports: list[int],
    gate_ports: list[int],
    **env_overrides: str | int | float,
) -> list[ManagerServer]:
    """One manager of ``datacenter_id`` per TCP port in ``manager_ports``,
    each peered with the datacenter's other managers and registering with
    every gate in ``gate_ports``."""
    return [
        new_manager(node_directory, datacenter_id, manager_port, manager_ports, gate_ports, **env_overrides)
        for manager_port in manager_ports
    ]


def new_workers(
    node_directory: pathlib.Path,
    datacenter_id: str,
    worker_ports: list[int],
    manager_ports: list[int],
    total_cores: int,
    **env_overrides: str | int | float,
) -> list[WorkerServer]:
    """One worker of ``datacenter_id`` with ``total_cores`` cores per TCP
    port in ``worker_ports`` (each reserved with room for its executors),
    seeded with every manager of its datacenter."""
    return [
        WorkerServer(
            host=LOCALHOST,
            tcp_port=worker_port,
            udp_port=worker_port + 1,
            env=node_env(node_directory, **env_overrides),
            dc_id=datacenter_id,
            total_cores=total_cores,
            seed_managers=tcp_addresses(manager_ports),
        )
        for worker_port in worker_ports
    ]


async def start_nodes(nodes: Iterable[GateServer | ManagerServer | WorkerServer], within_seconds: float) -> None:
    """Start every node concurrently, each bounded by ``within_seconds``;
    every start runs to its end even when one fails, and the first failure
    is raised after all ran (the caller's teardown then stops them all)."""
    start_results = await asyncio.gather(
        *[asyncio.wait_for(node.start(), timeout=within_seconds) for node in nodes],
        return_exceptions=True,
    )
    if failures := [result for result in start_results if isinstance(result, BaseException)]:
        raise failures[0]
