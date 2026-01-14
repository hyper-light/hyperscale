import asyncio

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer

from tests.framework.runtime.test_cluster import TestCluster
from tests.framework.specs.cluster_spec import ClusterSpec
from tests.framework.specs.node_spec import NodeSpec


def _build_datacenter_ids(dc_count: int) -> list[str]:
    return [f"DC-{chr(65 + index)}" for index in range(dc_count)]


def _group_node_specs(
    node_specs: list[NodeSpec],
) -> tuple[list[NodeSpec], list[NodeSpec], list[NodeSpec]]:
    gate_specs: list[NodeSpec] = []
    manager_specs: list[NodeSpec] = []
    worker_specs: list[NodeSpec] = []
    for node_spec in node_specs:
        if node_spec.node_type == "gate":
            gate_specs.append(node_spec)
        elif node_spec.node_type == "manager":
            manager_specs.append(node_spec)
        elif node_spec.node_type == "worker":
            worker_specs.append(node_spec)
        else:
            raise ValueError(f"Unknown node_type '{node_spec.node_type}'")
    return gate_specs, manager_specs, worker_specs


class ClusterFactory:
    def __init__(self) -> None:
        self._env: Env | None = None

    async def create_cluster(self, spec: ClusterSpec) -> TestCluster:
        if spec.nodes:
            return await self._create_from_nodes(spec)
        return await self._create_from_counts(spec)

    async def _create_from_counts(self, spec: ClusterSpec) -> TestCluster:
        env_overrides = dict(spec.env_overrides or {})
        env_overrides.setdefault("WORKER_MAX_CORES", spec.cores_per_worker)
        self._env = Env.model_validate(env_overrides)
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
                udp_port = tcp_port + spec.worker_udp_offset
                worker_ports[datacenter_id].append((tcp_port, udp_port))
                port_offset += spec.worker_port_stride
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
        await self._start_cluster(cluster, spec, all_gate_tcp)
        return cluster

    async def _create_from_nodes(self, spec: ClusterSpec) -> TestCluster:
        node_specs = spec.nodes or []
        gate_specs, manager_specs, worker_specs = _group_node_specs(node_specs)
        if not gate_specs:
            raise ValueError("Node specs must include at least one gate")
        self._env = Env.model_validate(spec.env_overrides or {})
        cluster = TestCluster(config=spec)
        datacenter_ids = sorted(
            {
                node_spec.dc_id
                for node_spec in manager_specs + worker_specs
                if node_spec.dc_id
            }
        )
        manager_tcp_addrs: dict[str, list[tuple[str, int]]] = {
            datacenter_id: [] for datacenter_id in datacenter_ids
        }
        manager_udp_addrs: dict[str, list[tuple[str, int]]] = {
            datacenter_id: [] for datacenter_id in datacenter_ids
        }
        for manager_spec in manager_specs:
            datacenter_id = manager_spec.dc_id
            if not datacenter_id:
                raise ValueError("Manager node specs require dc_id")
            manager_tcp_addrs[datacenter_id].append(
                (manager_spec.host, manager_spec.tcp_port)
            )
            manager_udp_addrs[datacenter_id].append(
                (manager_spec.host, manager_spec.udp_port)
            )
        all_gate_tcp = [
            (gate_spec.host, gate_spec.tcp_port) for gate_spec in gate_specs
        ]
        all_gate_udp = [
            (gate_spec.host, gate_spec.udp_port) for gate_spec in gate_specs
        ]
        for gate_spec in gate_specs:
            gate_env = self._build_env(spec, gate_spec.env_overrides)
            gate_peers = gate_spec.gate_peers or [
                addr
                for addr in all_gate_tcp
                if addr != (gate_spec.host, gate_spec.tcp_port)
            ]
            gate_udp_peers = gate_spec.gate_udp_peers or [
                addr
                for addr in all_gate_udp
                if addr != (gate_spec.host, gate_spec.udp_port)
            ]
            gate = GateServer(
                host=gate_spec.host,
                tcp_port=gate_spec.tcp_port,
                udp_port=gate_spec.udp_port,
                env=gate_env,
                gate_peers=gate_peers,
                gate_udp_peers=gate_udp_peers,
                datacenter_managers=manager_tcp_addrs,
                datacenter_manager_udp=manager_udp_addrs,
            )
            cluster.gates.append(gate)
        for datacenter_id in datacenter_ids:
            cluster.managers[datacenter_id] = []
            cluster.workers[datacenter_id] = []
        for manager_spec in manager_specs:
            datacenter_id = manager_spec.dc_id
            if not datacenter_id:
                raise ValueError("Manager node specs require dc_id")
            manager_env = self._build_env(spec, manager_spec.env_overrides)
            dc_manager_tcp = manager_tcp_addrs[datacenter_id]
            dc_manager_udp = manager_udp_addrs[datacenter_id]
            manager_peers = manager_spec.manager_peers or [
                addr
                for addr in dc_manager_tcp
                if addr != (manager_spec.host, manager_spec.tcp_port)
            ]
            manager_udp_peers = manager_spec.manager_udp_peers or [
                addr
                for addr in dc_manager_udp
                if addr != (manager_spec.host, manager_spec.udp_port)
            ]
            manager = ManagerServer(
                host=manager_spec.host,
                tcp_port=manager_spec.tcp_port,
                udp_port=manager_spec.udp_port,
                env=manager_env,
                dc_id=datacenter_id,
                manager_peers=manager_peers,
                manager_udp_peers=manager_udp_peers,
                gate_addrs=all_gate_tcp,
                gate_udp_addrs=all_gate_udp,
            )
            cluster.managers[datacenter_id].append(manager)
        for worker_spec in worker_specs:
            datacenter_id = worker_spec.dc_id
            if not datacenter_id:
                raise ValueError("Worker node specs require dc_id")
            worker_env = self._build_env(spec, worker_spec.env_overrides)
            seed_managers = worker_spec.seed_managers or manager_tcp_addrs.get(
                datacenter_id, []
            )
            if not seed_managers:
                raise ValueError(
                    f"Worker node requires seed managers for '{datacenter_id}'"
                )
            total_cores = worker_spec.total_cores or spec.cores_per_worker
            worker = WorkerServer(
                host=worker_spec.host,
                tcp_port=worker_spec.tcp_port,
                udp_port=worker_spec.udp_port,
                env=worker_env,
                dc_id=datacenter_id,
                total_cores=total_cores,
                seed_managers=seed_managers,
            )
            if datacenter_id not in cluster.workers:
                cluster.workers[datacenter_id] = []
            cluster.workers[datacenter_id].append(worker)
        await self._start_cluster(cluster, spec, all_gate_tcp)
        return cluster

    def _build_env(
        self, spec: ClusterSpec, node_overrides: dict[str, object] | None
    ) -> Env:
        env_overrides = dict(spec.env_overrides or {})
        if node_overrides:
            env_overrides.update(node_overrides)
        return Env.model_validate(env_overrides)

    async def _start_cluster(
        self,
        cluster: TestCluster,
        spec: ClusterSpec,
        gate_addrs: list[tuple[str, int]],
    ) -> None:
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
            gates=gate_addrs,
        )
        await cluster.client.start()

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
