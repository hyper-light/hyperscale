import asyncio

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer

from .test_cluster import TestCluster
from ..specs.cluster_spec import ClusterSpec


def _build_datacenter_ids(dc_count: int) -> list[str]:
    return [f"DC-{chr(65 + index)}" for index in range(dc_count)]


class ClusterFactory:
    def __init__(self) -> None:
        self._env = None

    async def create_cluster(self, spec: ClusterSpec) -> TestCluster:
        if spec.nodes:
            raise ValueError("Node-level cluster specs are not supported yet")
        env_overrides = spec.env_overrides or {}
        self._env = Env(**env_overrides)
        cluster = TestCluster(config=spec)
        datacenter_ids = _build_datacenter_ids(spec.dc_count)
        gate_tcp_ports = [
            spec.base_gate_tcp + (index * 2) for index in range(spec.gate_count)
        ]
        gate_udp_ports = [
            spec.base_gate_tcp + (index * 2) + 1 for index in range(spec.gate_count)
        ]
        manager_ports: dict[str, list[tuple[int, int]]] = {}
        port_offset = 0
        for datacenter_id in datacenter_ids:
            manager_ports[datacenter_id] = []
            for _ in range(spec.managers_per_dc):
                tcp_port = spec.base_manager_tcp + port_offset
                udp_port = tcp_port + 1
                manager_ports[datacenter_id].append((tcp_port, udp_port))
                port_offset += 2
        worker_ports: dict[str, list[tuple[int, int]]] = {}
        port_offset = 0
        for datacenter_id in datacenter_ids:
            worker_ports[datacenter_id] = []
            for _ in range(spec.workers_per_dc):
                tcp_port = spec.base_worker_tcp + port_offset
                udp_port = tcp_port + 1
                worker_ports[datacenter_id].append((tcp_port, udp_port))
                port_offset += 2
        datacenter_managers_tcp: dict[str, list[tuple[str, int]]] = {}
        datacenter_managers_udp: dict[str, list[tuple[str, int]]] = {}
        for datacenter_id in datacenter_ids:
            datacenter_managers_tcp[datacenter_id] = [
                ("127.0.0.1", tcp_port) for tcp_port, _ in manager_ports[datacenter_id]
            ]
            datacenter_managers_udp[datacenter_id] = [
                ("127.0.0.1", udp_port) for _, udp_port in manager_ports[datacenter_id]
            ]
        all_gate_tcp = [("127.0.0.1", port) for port in gate_tcp_ports]
        all_gate_udp = [("127.0.0.1", port) for port in gate_udp_ports]
        for gate_index in range(spec.gate_count):
            tcp_port = gate_tcp_ports[gate_index]
            udp_port = gate_udp_ports[gate_index]
            peer_tcp = [addr for addr in all_gate_tcp if addr[1] != tcp_port]
            peer_udp = [addr for addr in all_gate_udp if addr[1] != udp_port]
            gate = GateServer(
                host="127.0.0.1",
                tcp_port=tcp_port,
                udp_port=udp_port,
                env=self._env,
                gate_peers=peer_tcp,
                gate_udp_peers=peer_udp,
                datacenter_managers=datacenter_managers_tcp,
                datacenter_manager_udp=datacenter_managers_udp,
            )
            cluster.gates.append(gate)
        for datacenter_id in datacenter_ids:
            cluster.managers[datacenter_id] = []
            dc_manager_tcp = [
                ("127.0.0.1", tcp_port) for tcp_port, _ in manager_ports[datacenter_id]
            ]
            dc_manager_udp = [
                ("127.0.0.1", udp_port) for _, udp_port in manager_ports[datacenter_id]
            ]
            for manager_index in range(spec.managers_per_dc):
                tcp_port, udp_port = manager_ports[datacenter_id][manager_index]
                peer_tcp = [addr for addr in dc_manager_tcp if addr[1] != tcp_port]
                peer_udp = [addr for addr in dc_manager_udp if addr[1] != udp_port]
                manager = ManagerServer(
                    host="127.0.0.1",
                    tcp_port=tcp_port,
                    udp_port=udp_port,
                    env=self._env,
                    dc_id=datacenter_id,
                    manager_peers=peer_tcp,
                    manager_udp_peers=peer_udp,
                    gate_addrs=all_gate_tcp,
                    gate_udp_addrs=all_gate_udp,
                )
                cluster.managers[datacenter_id].append(manager)
        for datacenter_id in datacenter_ids:
            cluster.workers[datacenter_id] = []
            seed_managers = [
                ("127.0.0.1", tcp_port) for tcp_port, _ in manager_ports[datacenter_id]
            ]
            for worker_index in range(spec.workers_per_dc):
                tcp_port, udp_port = worker_ports[datacenter_id][worker_index]
                worker = WorkerServer(
                    host="127.0.0.1",
                    tcp_port=tcp_port,
                    udp_port=udp_port,
                    env=self._env,
                    dc_id=datacenter_id,
                    total_cores=spec.cores_per_worker,
                    seed_managers=seed_managers,
                )
                cluster.workers[datacenter_id].append(worker)
        await asyncio.gather(*[gate.start() for gate in cluster.gates])
        await asyncio.gather(
            *[manager.start() for manager in cluster.get_all_managers()]
        )
        await asyncio.sleep(spec.stabilization_seconds)
        await asyncio.gather(*[worker.start() for worker in cluster.get_all_workers()])
        await asyncio.sleep(spec.worker_registration_seconds)
        cluster.client = HyperscaleClient(
            host="127.0.0.1",
            port=spec.client_port,
            env=self._env,
            gates=all_gate_tcp,
        )
        await cluster.client.start()
        return cluster

    async def teardown_cluster(self, cluster: TestCluster) -> None:
        if cluster.client:
            await cluster.client.stop()
        for worker in cluster.get_all_workers():
            await worker.stop(drain_timeout=0.5, broadcast_leave=False)
        for manager in cluster.get_all_managers():
            await manager.stop(drain_timeout=0.5, broadcast_leave=False)
        for gate in cluster.gates:
            await gate.stop(drain_timeout=0.5, broadcast_leave=False)
        await asyncio.sleep(1.0)
