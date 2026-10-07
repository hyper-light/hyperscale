"""
The cluster the comprehensive gate scenarios run against: three gates in
front of two datacenters, each with three managers and two workers, and a
client submitting through the gates. Every node runs in-process on
loopback, with its logs under the test's own directory.
"""

import asyncio
import pathlib
from dataclasses import dataclass, field

from hyperscale.distributed.models import DatacenterHealth
from hyperscale.distributed.nodes import GateServer, ManagerServer, WorkerServer
from hyperscale.distributed.nodes.client import HyperscaleClient
from tests.integration.cli.node_processes import reserve_port_blocks, worker_port_span
from tests.integration.in_process_nodes import LOCALHOST, NODE_PORT_BLOCK, node_env, stop_nodes, wait_until

GATE_COUNT = 3
DATACENTER_IDS = ("DC-A", "DC-B")
MANAGERS_PER_DATACENTER = 3
WORKERS_PER_DATACENTER = 2
CORES_PER_WORKER = 2
# Bounds on what the nodes do on their own: the original script slept
# 15s for gate and manager leaders to settle and 10s for workers to
# register; these bounds are generous multiples of those.
CLUSTER_FORMATION_SECONDS = 60.0
WORKER_REGISTRATION_SECONDS = 60.0
SHUTDOWN_SECONDS = 30.0
SUBMITTABLE_DATACENTER_HEALTH = frozenset({DatacenterHealth.HEALTHY.value, DatacenterHealth.BUSY.value})

type ClusterNode = GateServer | ManagerServer | WorkerServer


@dataclass
class ComprehensiveCluster:
    """Every node of the scenario cluster, the client submitting through
    its gates, and the nodes still running (those a scenario stopped are
    no longer stopped again at teardown)."""

    gates: list[GateServer]
    managers_by_datacenter: dict[str, list[ManagerServer]]
    workers_by_datacenter: dict[str, list[WorkerServer]]
    client: HyperscaleClient
    running_nodes: list[ClusterNode] = field(default_factory=list)
    client_running: bool = False

    @property
    def all_managers(self) -> list[ManagerServer]:
        return [manager for managers in self.managers_by_datacenter.values() for manager in managers]

    @property
    def all_workers(self) -> list[WorkerServer]:
        return [worker for workers in self.workers_by_datacenter.values() for worker in workers]

    def gate_leader(self) -> GateServer | None:
        return next((gate for gate in self.gates if gate.is_leader()), None)

    def datacenter_leader(self, datacenter_id: str) -> ManagerServer | None:
        return next((manager for manager in self.managers_by_datacenter[datacenter_id] if manager.is_leader()), None)

    async def start_nodes(self, nodes: list[ClusterNode]) -> None:
        """Start ``nodes`` concurrently; every node that started is stopped
        at teardown even when another failed to start, whose failure is
        then raised."""
        start_results = await asyncio.gather(*[node.start() for node in nodes], return_exceptions=True)
        self.running_nodes.extend(
            node for node, result in zip(nodes, start_results) if not isinstance(result, BaseException)
        )
        if failures := [result for result in start_results if isinstance(result, BaseException)]:
            raise failures[0]

    async def stop_node(self, node: ClusterNode) -> None:
        """Stop one node mid-scenario without announcing its leave (a
        crash, as the scenarios simulate one)."""
        self.running_nodes.remove(node)
        await asyncio.wait_for(node.stop(drain_timeout=0.1, broadcast_leave=False), timeout=SHUTDOWN_SECONDS)


def build_comprehensive_cluster(node_directory: pathlib.Path) -> ComprehensiveCluster:
    """Construct (not start) every node and the client, on ports reserved
    in one call so no two blocks overlap."""
    manager_count = len(DATACENTER_IDS) * MANAGERS_PER_DATACENTER
    worker_count = len(DATACENTER_IDS) * WORKERS_PER_DATACENTER
    reserved_ports = reserve_port_blocks(
        [NODE_PORT_BLOCK] * (GATE_COUNT + manager_count + 1)
        + [NODE_PORT_BLOCK + worker_port_span(CORES_PER_WORKER)] * worker_count
    )
    gate_ports = reserved_ports[:GATE_COUNT]
    manager_ports = reserved_ports[GATE_COUNT : GATE_COUNT + manager_count]
    client_port = reserved_ports[GATE_COUNT + manager_count]
    worker_ports = reserved_ports[GATE_COUNT + manager_count + 1 :]
    env = node_env(node_directory, MERCURY_SYNC_REQUEST_TIMEOUT="5s", MERCURY_SYNC_LOG_LEVEL="error")

    manager_ports_by_datacenter = {
        datacenter_id: manager_ports[index * MANAGERS_PER_DATACENTER : (index + 1) * MANAGERS_PER_DATACENTER]
        for index, datacenter_id in enumerate(DATACENTER_IDS)
    }
    worker_ports_by_datacenter = {
        datacenter_id: worker_ports[index * WORKERS_PER_DATACENTER : (index + 1) * WORKERS_PER_DATACENTER]
        for index, datacenter_id in enumerate(DATACENTER_IDS)
    }
    gate_tcp_addresses = [(LOCALHOST, port) for port in gate_ports]
    gate_udp_addresses = [(LOCALHOST, port + 1) for port in gate_ports]

    gates = [
        GateServer(
            host=LOCALHOST,
            tcp_port=port,
            udp_port=port + 1,
            env=env,
            gate_peers=[address for address in gate_tcp_addresses if address[1] != port],
            gate_udp_peers=[address for address in gate_udp_addresses if address[1] != port + 1],
            datacenter_managers={
                datacenter_id: [(LOCALHOST, manager_port) for manager_port in ports]
                for datacenter_id, ports in manager_ports_by_datacenter.items()
            },
            datacenter_manager_udp={
                datacenter_id: [(LOCALHOST, manager_port + 1) for manager_port in ports]
                for datacenter_id, ports in manager_ports_by_datacenter.items()
            },
        )
        for port in gate_ports
    ]
    managers_by_datacenter = {
        datacenter_id: [
            ManagerServer(
                host=LOCALHOST,
                tcp_port=port,
                udp_port=port + 1,
                env=env,
                dc_id=datacenter_id,
                manager_peers=[(LOCALHOST, peer_port) for peer_port in ports if peer_port != port],
                manager_udp_peers=[(LOCALHOST, peer_port + 1) for peer_port in ports if peer_port != port],
                gate_addrs=gate_tcp_addresses,
                gate_udp_addrs=gate_udp_addresses,
            )
            for port in ports
        ]
        for datacenter_id, ports in manager_ports_by_datacenter.items()
    }
    workers_by_datacenter = {
        datacenter_id: [
            WorkerServer(
                host=LOCALHOST,
                tcp_port=port,
                udp_port=port + 1,
                env=env,
                dc_id=datacenter_id,
                total_cores=CORES_PER_WORKER,
                seed_managers=[
                    (LOCALHOST, manager_port) for manager_port in manager_ports_by_datacenter[datacenter_id]
                ],
            )
            for port in ports
        ]
        for datacenter_id, ports in worker_ports_by_datacenter.items()
    }
    client = HyperscaleClient(host=LOCALHOST, port=client_port, env=env, gates=gate_tcp_addresses)
    return ComprehensiveCluster(
        gates=gates,
        managers_by_datacenter=managers_by_datacenter,
        workers_by_datacenter=workers_by_datacenter,
        client=client,
    )


async def start_comprehensive_cluster(cluster: ComprehensiveCluster) -> None:
    """Start gates, then managers, until a gate leader and a leader per
    datacenter hold; then workers, until each datacenter's leader has
    registered all of its workers and the gate leader classifies every
    datacenter as able to take a job; then the client."""
    await cluster.start_nodes(list(cluster.gates))
    await cluster.start_nodes(cluster.all_managers)
    await wait_until(
        lambda: cluster.gate_leader() is not None
        and all(cluster.datacenter_leader(datacenter_id) is not None for datacenter_id in DATACENTER_IDS),
        within_seconds=CLUSTER_FORMATION_SECONDS,
        description="a gate leader and a manager leader in every datacenter",
    )

    await cluster.start_nodes(cluster.all_workers)
    await wait_until(
        lambda: all(
            (leader := cluster.datacenter_leader(datacenter_id)) is not None
            and all(leader._manager_state.has_worker(worker._node_id.full) for worker in workers)
            for datacenter_id, workers in cluster.workers_by_datacenter.items()
        ),
        within_seconds=WORKER_REGISTRATION_SECONDS,
        description="every datacenter's leader manager registering all of its workers",
    )
    await wait_until(
        lambda: (gate_leader := cluster.gate_leader()) is not None
        and all(
            gate_leader._classify_datacenter_health(datacenter_id).health in SUBMITTABLE_DATACENTER_HEALTH
            for datacenter_id in DATACENTER_IDS
        ),
        within_seconds=WORKER_REGISTRATION_SECONDS,
        description="the gate leader classifying every datacenter healthy or busy",
    )

    await cluster.client.start()
    cluster.client_running = True


async def stop_comprehensive_cluster(cluster: ComprehensiveCluster) -> None:
    """Stop the client, then every node still running; the nodes are
    stopped even when the client's stop fails."""
    try:
        if cluster.client_running:
            await asyncio.wait_for(cluster.client.stop(), timeout=SHUTDOWN_SECONDS)
    finally:
        await stop_nodes(cluster.running_nodes, within_seconds=SHUTDOWN_SECONDS)
