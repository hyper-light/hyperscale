"""
Builders for the manager integration tests: managers of one datacenter
seeded with each other (and optionally with gates), gates configured with
every datacenter's managers, and workers seeded with a datacenter's
managers. Every node listens on loopback; a node's UDP port is its TCP
port + 1.
"""

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import GateServer, ManagerServer, WorkerServer
from tests.integration.in_process_nodes import LOCALHOST

# Bounds on what the nodes do on their own: a node's start() (bind, join
# its seeds, begin election) and its graceful stop.
NODE_START_SECONDS = 60.0
NODE_STOP_SECONDS = 30.0
# The stop the tests use to simulate a crash: no leave broadcast, a short
# drain, so the cluster must detect the loss itself.
CRASH_DRAIN_SECONDS = 0.5
QUIET_LOG_LEVEL = "error"
# The request timeout the discovery scenarios ran with.
REQUEST_TIMEOUT = "5s"
GATE_DATACENTER_ID = "global"

__all__ = [
    "CRASH_DRAIN_SECONDS",
    "GATE_DATACENTER_ID",
    "NODE_START_SECONDS",
    "NODE_STOP_SECONDS",
    "QUIET_LOG_LEVEL",
    "REQUEST_TIMEOUT",
    "new_gate",
    "new_manager",
    "new_worker",
]


def new_manager(
    env: Env,
    tcp_port: int,
    datacenter_manager_ports: list[int],
    datacenter_id: str,
    gate_ports: list[int] | None = None,
) -> ManagerServer:
    """A manager of ``datacenter_id`` seeded with every other manager in
    ``datacenter_manager_ports`` (TCP and SWIM UDP) and told of the gates
    at ``gate_ports``."""
    return ManagerServer(
        host=LOCALHOST,
        tcp_port=tcp_port,
        udp_port=tcp_port + 1,
        env=env,
        dc_id=datacenter_id,
        seed_managers=[(LOCALHOST, peer_port) for peer_port in datacenter_manager_ports if peer_port != tcp_port],
        manager_udp_peers=[
            (LOCALHOST, peer_port + 1) for peer_port in datacenter_manager_ports if peer_port != tcp_port
        ],
        gate_addrs=[(LOCALHOST, gate_port) for gate_port in gate_ports] if gate_ports else None,
    )


def new_gate(
    env: Env,
    tcp_port: int,
    gate_ports: list[int],
    manager_ports_by_datacenter: dict[str, list[int]],
) -> GateServer:
    """A gate peered with every other gate in ``gate_ports`` and configured
    with every datacenter's managers (TCP and SWIM UDP)."""
    return GateServer(
        host=LOCALHOST,
        tcp_port=tcp_port,
        udp_port=tcp_port + 1,
        env=env,
        dc_id=GATE_DATACENTER_ID,
        datacenter_managers={
            datacenter_id: [(LOCALHOST, manager_port) for manager_port in manager_ports]
            for datacenter_id, manager_ports in manager_ports_by_datacenter.items()
        },
        datacenter_manager_udp={
            datacenter_id: [(LOCALHOST, manager_port + 1) for manager_port in manager_ports]
            for datacenter_id, manager_ports in manager_ports_by_datacenter.items()
        },
        gate_peers=[(LOCALHOST, peer_port) for peer_port in gate_ports if peer_port != tcp_port],
        gate_udp_peers=[(LOCALHOST, peer_port + 1) for peer_port in gate_ports if peer_port != tcp_port],
    )


def new_worker(
    env: Env,
    tcp_port: int,
    total_cores: int,
    datacenter_id: str,
    manager_ports: list[int],
) -> WorkerServer:
    """A worker of ``datacenter_id`` with ``total_cores`` executor cores,
    seeded with every manager in ``manager_ports``."""
    return WorkerServer(
        host=LOCALHOST,
        tcp_port=tcp_port,
        udp_port=tcp_port + 1,
        env=env,
        dc_id=datacenter_id,
        seed_managers=[(LOCALHOST, manager_port) for manager_port in manager_ports],
        total_cores=total_cores,
    )
