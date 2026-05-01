"""
ClusterHarness — the user-facing async context manager.

Builds a real cluster of `GateServer` / `ManagerServer` / `WorkerServer`
instances from a `ClusterSpec`, hands the supervised lifetime to a
`Supervisor`, and exposes accessors scenarios use to inspect or
manipulate nodes.

Phase 1 deliverable: REAL execution mode only. SIM mode is a Phase 6
configuration of the same harness; the API is shaped for it now (see
`mode` parameter and `__aenter__` branching) but raises NotImplementedError
until the production-side Clock/Random/Transport refactor lands.
"""

import asyncio
from dataclasses import dataclass, field

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer

from tests.simulation.harness.cluster_spec import ClusterSpec
from tests.simulation.harness.dc_spec import DCSpec
from tests.simulation.harness.env_overrides import EnvOverrides
from tests.simulation.harness.execution_mode import ExecutionMode
from tests.simulation.harness.port_allocator import PortAllocator
from tests.simulation.harness.server_handle import ServerHandle, ServerKind
from tests.simulation.harness.supervisor import Supervisor
from tests.simulation.harness.worker_ports import WorkerPorts


@dataclass(slots=True)
class ClusterHarness:
    """Async context manager that builds and reaps a cluster of real servers.

    Usage:

        async with ClusterHarness(spec) as cluster:
            # cluster.gates, cluster.managers, cluster.workers are populated.
            # supervisor has already snapshotted baseline pids and reaped any
            # zombies from prior runs.
            ...
    """

    spec: ClusterSpec
    mode: ExecutionMode = ExecutionMode.REAL
    fail_on_async_leak: bool = True
    stabilization_seconds: float | None = None
    """Wall-clock pause after starting all servers; uses spec.timeouts default if None."""

    _supervisor: Supervisor = field(init=False)
    _ports: PortAllocator = field(init=False)
    _handles_by_id: dict[str, ServerHandle] = field(init=False, default_factory=dict)
    _gates: list[ServerHandle] = field(init=False, default_factory=list)
    _managers_by_dc: dict[str, list[ServerHandle]] = field(init=False, default_factory=dict)
    _workers_by_dc: dict[str, list[ServerHandle]] = field(init=False, default_factory=dict)
    _entered: bool = field(init=False, default=False)

    async def __aenter__(self) -> "ClusterHarness":
        if self.mode is ExecutionMode.SIM:
            raise NotImplementedError(
                "SIM mode requires the Clock/Random/Transport refactor (Phases 5–6); "
                "use ExecutionMode.REAL until then."
            )

        self._ports = PortAllocator(host=self.spec.host, base_port=self.spec.base_port)
        self._supervisor = Supervisor(
            timeouts=self.spec.timeouts,
            ports=self._ports,
            fail_on_async_leak=self.fail_on_async_leak,
        )
        await self._supervisor.__aenter__()

        try:
            self._build_servers()
            await self._start_servers()
            await self._stabilize()
        except BaseException:
            await self._supervisor.shutdown()
            raise

        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            await self._supervisor.shutdown()
        finally:
            errors = self._supervisor.cleanup_errors
            if errors and exc_type is None:
                # Surface cleanup errors when the scenario itself succeeded;
                # otherwise the original exception is already informative.
                joined = "\n  - ".join(errors)
                raise RuntimeError(f"harness cleanup reported errors:\n  - {joined}")

    @property
    def supervisor(self) -> Supervisor:
        return self._supervisor

    @property
    def gates(self) -> list[ServerHandle]:
        return list(self._gates)

    def managers(self, dc_id: str) -> list[ServerHandle]:
        return list(self._managers_by_dc.get(dc_id, []))

    def workers(self, dc_id: str) -> list[ServerHandle]:
        return list(self._workers_by_dc.get(dc_id, []))

    def handle(self, node_id: str) -> ServerHandle:
        return self._handles_by_id[node_id]

    def all_handles(self) -> list[ServerHandle]:
        return self._supervisor.server_handles

    def _build_servers(self) -> None:
        """Allocate ports and construct (but do not start) every server."""
        # Allocate gate ports first so peer addresses are known before manager
        # construction; managers carry `gate_addrs`/`gate_udp_addrs`.
        gate_specs = [self._allocate_gate_addrs(idx) for idx in range(self.spec.gates)]
        gate_tcp = [(self.spec.host, tcp) for tcp, _udp in gate_specs]
        gate_udp = [(self.spec.host, udp) for _tcp, udp in gate_specs]

        manager_addrs_by_dc: dict[str, list[tuple[int, int]]] = {}
        for dc_id, dc_spec in self.spec.datacenters.items():
            manager_addrs_by_dc[dc_id] = [
                self._allocate_pair() for _ in range(dc_spec.managers)
            ]

        worker_addrs_by_dc: dict[str, list[tuple[int, int]]] = {}
        for dc_id, dc_spec in self.spec.datacenters.items():
            worker_addrs_by_dc[dc_id] = [
                self._allocate_pair() for _ in range(dc_spec.workers)
            ]
            # Reserve the derived `udp + cores ** 2` per worker so a future
            # worker we allocate after this one cannot collide with it.
            for _tcp, udp in worker_addrs_by_dc[dc_id]:
                self._ports.reserve_range(0)  # placeholder; range below
                # Mark the derived port as reserved by binding it; do this
                # with the same mechanism so the post-teardown verifier
                # checks it too.
                derived = udp + dc_spec.cores_per_worker ** 2
                self._reserve_specific(derived)

        self._build_gates(gate_specs, manager_addrs_by_dc)
        self._build_managers(
            manager_addrs_by_dc=manager_addrs_by_dc,
            gate_tcp=gate_tcp,
            gate_udp=gate_udp,
        )
        self._build_workers(
            worker_addrs_by_dc=worker_addrs_by_dc,
            manager_addrs_by_dc=manager_addrs_by_dc,
        )

    def _build_gates(
        self,
        gate_specs: list[tuple[int, int]],
        manager_addrs_by_dc: dict[str, list[tuple[int, int]]],
    ) -> None:
        if not gate_specs:
            return
        datacenter_managers = {
            dc_id: [(self.spec.host, tcp) for tcp, _udp in addrs]
            for dc_id, addrs in manager_addrs_by_dc.items()
        }
        datacenter_manager_udp = {
            dc_id: [(self.spec.host, udp) for _tcp, udp in addrs]
            for dc_id, addrs in manager_addrs_by_dc.items()
        }
        all_gate_tcp = [(self.spec.host, tcp) for tcp, _udp in gate_specs]
        all_gate_udp = [(self.spec.host, udp) for _tcp, udp in gate_specs]
        for index, (tcp, udp) in enumerate(gate_specs):
            node_id = f"global.gate.{index}"
            peer_tcp = [addr for addr in all_gate_tcp if addr != (self.spec.host, tcp)]
            peer_udp = [addr for addr in all_gate_udp if addr != (self.spec.host, udp)]
            env = self._build_env(node_id=node_id, dc_id="global", dc_spec=None)
            gate = GateServer(
                host=self.spec.host,
                tcp_port=tcp,
                udp_port=udp,
                env=env,
                dc_id="global",
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
                gate_peers=peer_tcp,
                gate_udp_peers=peer_udp,
            )
            handle = ServerHandle(
                node_id=node_id,
                kind=ServerKind.GATE,
                dc_id="global",
                host=self.spec.host,
                tcp_port=tcp,
                udp_port=udp,
                instance=gate,
            )
            self._handles_by_id[node_id] = handle
            self._gates.append(handle)
            self._supervisor.register_server(handle)

    def _build_managers(
        self,
        manager_addrs_by_dc: dict[str, list[tuple[int, int]]],
        gate_tcp: list[tuple[str, int]],
        gate_udp: list[tuple[str, int]],
    ) -> None:
        for dc_id, addrs in manager_addrs_by_dc.items():
            dc_spec = self.spec.datacenters[dc_id]
            self._managers_by_dc[dc_id] = []
            for index, (tcp, udp) in enumerate(addrs):
                node_id = f"{dc_id}.manager.{index}"
                peer_tcp = [
                    (self.spec.host, t) for (t, _u) in addrs if t != tcp
                ]
                peer_udp = [
                    (self.spec.host, u) for (_t, u) in addrs if u != udp
                ]
                env = self._build_env(node_id=node_id, dc_id=dc_id, dc_spec=dc_spec)
                manager = ManagerServer(
                    host=self.spec.host,
                    tcp_port=tcp,
                    udp_port=udp,
                    env=env,
                    dc_id=dc_id,
                    gate_addrs=gate_tcp or None,
                    gate_udp_addrs=gate_udp or None,
                    seed_managers=peer_tcp or None,
                    manager_udp_peers=peer_udp or None,
                )
                handle = ServerHandle(
                    node_id=node_id,
                    kind=ServerKind.MANAGER,
                    dc_id=dc_id,
                    host=self.spec.host,
                    tcp_port=tcp,
                    udp_port=udp,
                    instance=manager,
                )
                self._handles_by_id[node_id] = handle
                self._managers_by_dc[dc_id].append(handle)
                self._supervisor.register_server(handle)

    def _build_workers(
        self,
        worker_addrs_by_dc: dict[str, list[tuple[int, int]]],
        manager_addrs_by_dc: dict[str, list[tuple[int, int]]],
    ) -> None:
        for dc_id, addrs in worker_addrs_by_dc.items():
            dc_spec = self.spec.datacenters[dc_id]
            self._workers_by_dc[dc_id] = []
            seed_managers = [
                (self.spec.host, tcp) for tcp, _udp in manager_addrs_by_dc[dc_id]
            ]
            for index, (tcp, udp) in enumerate(addrs):
                node_id = f"{dc_id}.worker.{index}"
                env = self._build_env(
                    node_id=node_id,
                    dc_id=dc_id,
                    dc_spec=dc_spec,
                    worker_cores=dc_spec.cores_per_worker,
                )
                worker = WorkerServer(
                    host=self.spec.host,
                    tcp_port=tcp,
                    udp_port=udp,
                    env=env,
                    dc_id=dc_id,
                    seed_managers=seed_managers,
                )
                handle = ServerHandle(
                    node_id=node_id,
                    kind=ServerKind.WORKER,
                    dc_id=dc_id,
                    host=self.spec.host,
                    tcp_port=tcp,
                    udp_port=udp,
                    instance=worker,
                    worker_ports=WorkerPorts.for_worker(
                        tcp=tcp, udp=udp, cores=dc_spec.cores_per_worker
                    ),
                )
                self._handles_by_id[node_id] = handle
                self._workers_by_dc[dc_id].append(handle)
                self._supervisor.register_server(handle)

    async def _start_servers(self) -> None:
        # Start order matters: gates first (so managers can register with
        # them), then managers (so workers can register), then workers.
        for kind, handles in (
            (ServerKind.GATE, self._gates),
            (
                ServerKind.MANAGER,
                [h for hs in self._managers_by_dc.values() for h in hs],
            ),
            (
                ServerKind.WORKER,
                [h for hs in self._workers_by_dc.values() for h in hs],
            ),
        ):
            if not handles:
                continue
            await asyncio.gather(*(handle.instance.start() for handle in handles))
            for handle in handles:
                handle.started = True
                if kind is ServerKind.WORKER:
                    self._supervisor.start_worker_pid_tracking(handle)

    async def _stabilize(self) -> None:
        budget = self.stabilization_seconds
        if budget is None:
            budget = self.spec.timeouts.stabilization_default
        # Phase 1: real wall-clock sleep. Phase 2 replaces this with
        # condition predicates (`wait_until(has_quorum and has_workers)`).
        if budget > 0:
            await asyncio.sleep(budget)

    def _allocate_pair(self) -> tuple[int, int]:
        return self._ports.reserve_pair()

    def _allocate_gate_addrs(self, _index: int) -> tuple[int, int]:
        return self._ports.reserve_pair()

    def _reserve_specific(self, port: int) -> None:
        """Reserve a specific port we have already implicitly committed to.

        Used for worker-derived ports (`udp + cores ** 2`). The PortAllocator
        does not know about these by default; this teaches it so the
        post-teardown verifier checks them too.
        """
        # Reach into PortAllocator's reserved set deliberately. Adding a public
        # method would invite misuse from scenario code.
        if not self._ports._is_bindable(port):  # type: ignore[attr-defined]
            from tests.simulation.harness.errors import PortConflictError

            raise PortConflictError(
                f"derived worker port {port} is already held by another process"
            )
        self._ports._reserved.add(port)  # type: ignore[attr-defined]

    def _build_env(
        self,
        node_id: str,
        dc_id: str,
        dc_spec: DCSpec | None,
        worker_cores: int | None = None,
    ) -> Env:
        """Compose Env from cluster + DC + per-node overrides + worker cores."""
        layered = self._layered_overrides(dc_spec=dc_spec, node_id=node_id)
        kwargs: dict[str, object] = {}
        if layered.request_timeout is not None:
            kwargs["MERCURY_SYNC_REQUEST_TIMEOUT"] = layered.request_timeout
        if layered.log_level is not None:
            kwargs["MERCURY_SYNC_LOG_LEVEL"] = layered.log_level
        if layered.recovery_jitter_min is not None:
            kwargs["RECOVERY_JITTER_MIN"] = layered.recovery_jitter_min
        if layered.recovery_jitter_max is not None:
            kwargs["RECOVERY_JITTER_MAX"] = layered.recovery_jitter_max
        if worker_cores is not None:
            kwargs["WORKER_MAX_CORES"] = layered.worker_max_cores or worker_cores
        return Env(**kwargs)

    def _layered_overrides(
        self, dc_spec: DCSpec | None, node_id: str
    ) -> EnvOverrides:
        """Merge env overrides: cluster < dc < per-node, last writer wins."""
        result = self.spec.env
        if dc_spec is not None and dc_spec.env is not None:
            result = _merge(result, dc_spec.env)
        per_node = self.spec.per_node_env.get(node_id)
        if per_node is not None:
            result = _merge(result, per_node)
        return result


def _merge(base: EnvOverrides, overlay: EnvOverrides) -> EnvOverrides:
    """Layer `overlay` on `base`; non-None overlay fields win."""
    return EnvOverrides(
        request_timeout=overlay.request_timeout
        if overlay.request_timeout is not None
        else base.request_timeout,
        log_level=overlay.log_level
        if overlay.log_level is not None
        else base.log_level,
        connect_timeout_seconds=overlay.connect_timeout_seconds
        if overlay.connect_timeout_seconds is not None
        else base.connect_timeout_seconds,
        worker_max_cores=overlay.worker_max_cores
        if overlay.worker_max_cores is not None
        else base.worker_max_cores,
        recovery_jitter_min=overlay.recovery_jitter_min
        if overlay.recovery_jitter_min is not None
        else base.recovery_jitter_min,
        recovery_jitter_max=overlay.recovery_jitter_max
        if overlay.recovery_jitter_max is not None
        else base.recovery_jitter_max,
    )
