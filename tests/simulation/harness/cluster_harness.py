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
import pathlib
from collections.abc import Callable
from dataclasses import dataclass, field

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer

from tests.simulation.harness.cluster_spec import ClusterSpec
from tests.simulation.harness.conditions import (
    lhm_at_baseline,
    manager_has_n_peers,
    manager_has_n_swim_confirmed_workers,
    manager_has_n_workers,
    wait_until,
    worker_subprocesses_alive,
)
from tests.simulation.harness.dc_spec import DCSpec
from tests.simulation.harness.diagnostics import DiagnosticDumper
from tests.simulation.harness.env_overrides import EnvOverrides
from tests.simulation.harness.execution_mode import ExecutionMode
from tests.simulation.harness.fault_matrix import FaultMatrix
from tests.simulation.harness import fault_transport
from tests.simulation.harness.invariants import (
    InvariantChecker,
    LivenessInvariant,
    SafetyInvariant,
    at_most_one_job_leader_per_job,
    cluster_membership_progress,
)
from tests.simulation.harness.port_allocator import PortAllocator
from tests.simulation.harness.server_handle import ServerHandle, ServerKind
from tests.simulation.harness.submission import WorkloadSpec
from tests.simulation.harness.supervisor import Supervisor
from tests.simulation.harness.worker_ports import WorkerPorts
from tests.simulation.harness.workload import WorkloadDriver


_DEFAULT_ARTIFACTS_ROOT = pathlib.Path(__file__).resolve().parents[1] / "_artifacts"


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
    """Hard ceiling on cluster stabilization. ``None`` uses
    ``spec.timeouts.stabilization_default``. The harness uses condition
    predicates (membership / worker registration / subprocess spawn) to
    return as soon as steady state is reached, capped by this budget."""
    scenario_name: str = "anonymous"
    """Used for diagnostic dump path: _artifacts/<scenario>/<timestamp>/."""
    artifacts_root: pathlib.Path = field(default_factory=lambda: _DEFAULT_ARTIFACTS_ROOT)
    extra_safety_invariants: list[SafetyInvariant] = field(default_factory=list)
    extra_liveness_invariants: list[LivenessInvariant] = field(default_factory=list)

    _supervisor: Supervisor = field(init=False)
    _ports: PortAllocator = field(init=False)
    _diagnostics: DiagnosticDumper = field(init=False)
    _invariants: InvariantChecker = field(init=False)
    _faults: FaultMatrix = field(init=False)
    _handles_by_id: dict[str, ServerHandle] = field(init=False, default_factory=dict)
    _gates: list[ServerHandle] = field(init=False, default_factory=list)
    _managers_by_dc: dict[str, list[ServerHandle]] = field(init=False, default_factory=dict)
    _workers_by_dc: dict[str, list[ServerHandle]] = field(init=False, default_factory=dict)
    _next_worker_index_by_dc: dict[str, int] = field(init=False, default_factory=dict)
    _expected_worker_count_by_dc: dict[str, int] = field(init=False, default_factory=dict)
    _entered: bool = field(init=False, default=False)

    async def __aenter__(self) -> "ClusterHarness":
        if self.mode is ExecutionMode.SIM:
            raise NotImplementedError(
                "SIM mode requires the Clock/Random/Transport refactor (Phases 5–6); "
                "use ExecutionMode.REAL until then."
            )

        self._ports = PortAllocator(host=self.spec.host, base_port=self.spec.base_port)
        self._expected_worker_count_by_dc = {
            dc_id: dc_spec.workers
            for dc_id, dc_spec in self.spec.datacenters.items()
        }
        self._supervisor = Supervisor(
            timeouts=self.spec.timeouts,
            ports=self._ports,
            fail_on_async_leak=self.fail_on_async_leak,
        )
        self._diagnostics = DiagnosticDumper(
            artifacts_root=self.artifacts_root,
            scenario=self.scenario_name,
            harness=self,
        )
        self._invariants = InvariantChecker(
            harness=self,
            poll_interval=self.spec.timeouts.invariant_poll_interval,
            on_violation=self._on_invariant_violation,
        )
        self._invariants.add_safety(at_most_one_job_leader_per_job())
        self._invariants.add_liveness(
            cluster_membership_progress(
                staleness_budget=self.spec.timeouts.stabilization_default,
            )
        )
        for safety in self.extra_safety_invariants:
            self._invariants.add_safety(safety)
        for liveness in self.extra_liveness_invariants:
            self._invariants.add_liveness(liveness)

        self._faults = FaultMatrix(harness=self)

        await self._supervisor.__aenter__()

        try:
            self._build_servers()
            await self._start_servers()
            # Phase 4: install FaultInjectingTransport on every started
            # server so partition/delay/drop rules take effect for any
            # subsequent send. Servers without the wrapper would still
            # be reachable from rule-blocked peers.
            fault_transport.install(self)
            await self._invariants.start()
            await self._stabilize()
        except BaseException:
            await self._invariants.stop()
            await self._supervisor.shutdown()
            raise

        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._invariants.stop()
        invariant_violation = self._invariants.violation
        try:
            await self._supervisor.shutdown()
        finally:
            errors = self._supervisor.cleanup_errors
            if invariant_violation is not None and exc_type is None:
                raise invariant_violation
            if errors and exc_type is None:
                joined = "\n  - ".join(errors)
                raise RuntimeError(f"harness cleanup reported errors:\n  - {joined}")

    async def dump_diagnostics(self, reason: str = "manual") -> None:
        """Write a complete diagnostic snapshot. Safe to call any time after __aenter__."""
        await self._diagnostics.dump(reason=reason)

    def workload(self, spec: WorkloadSpec) -> WorkloadDriver:
        """Construct a `WorkloadDriver` bound to this cluster.

        The driver allocates a fresh client port from the supervisor's
        port allocator and is intended to be used as ``async with``.
        Each call returns a new driver — workloads do not share state
        across the same cluster lifetime.
        """
        client_port = self._ports.reserve_pair()[0]
        return WorkloadDriver(
            harness=self,
            spec=spec,
            client_port=client_port,
        )

    async def _on_invariant_violation(self, reason: str) -> None:
        await self._diagnostics.dump(reason=f"invariant: {reason}")

    @property
    def supervisor(self) -> Supervisor:
        return self._supervisor

    @property
    def faults(self) -> FaultMatrix:
        """Fault-injection primitives for this harness (Phase 3)."""
        return self._faults

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

    def expected_worker_count(self, dc_id: str) -> int:
        """Return the scenario's current expected registered worker count."""
        return self._expected_worker_count_by_dc.get(
            dc_id, self.spec.datacenters[dc_id].workers
        )

    def set_expected_worker_count(self, dc_id: str, count: int) -> None:
        """Update the worker-count target for intentional membership churn."""
        if dc_id not in self.spec.datacenters:
            raise KeyError(f"unknown datacenter {dc_id!r}")
        if count < 0:
            raise ValueError("expected worker count cannot be negative")
        self._expected_worker_count_by_dc[dc_id] = count
        self._invariants.reset_liveness("ClusterMembershipProgress")

    async def add_worker(self, dc_id: str) -> ServerHandle:
        """Construct, start, and register one additional worker in ``dc_id``."""
        if dc_id not in self.spec.datacenters:
            raise KeyError(f"unknown datacenter {dc_id!r}")
        dc_spec = self.spec.datacenters[dc_id]
        tcp, udp = self._ports.reserve_worker_block(
            cores=dc_spec.cores_per_worker,
            block_size=dc_spec.worker_port_block_size,
        )
        worker_index = self._next_worker_index_by_dc.get(
            dc_id, len(self._workers_by_dc.get(dc_id, []))
        )
        handle = self._build_worker_handle(
            dc_id=dc_id,
            dc_spec=dc_spec,
            index=worker_index,
            tcp=tcp,
            udp=udp,
        )
        self._next_worker_index_by_dc[dc_id] = worker_index + 1
        self._handles_by_id[handle.node_id] = handle
        self._workers_by_dc.setdefault(dc_id, []).append(handle)
        self._expected_worker_count_by_dc[dc_id] = max(
            self.expected_worker_count(dc_id),
            len(self._workers_by_dc[dc_id]),
        )
        self._supervisor.register_server(handle)
        await handle.instance.start()
        handle.started = True
        self._supervisor.start_worker_pid_tracking(handle)
        fault_transport.reinstall_for(handle, self)
        return handle

    def address_to_node_id(
        self, address: tuple[str, int], *, kind: str = "tcp"
    ) -> str | None:
        """Look up the harness-managed node owning ``address``.

        Used by ``FaultInjectingTransport`` to resolve the (src, dst)
        pair for fault-rule matching. Returns ``None`` for addresses
        outside the harness — most commonly the
        ``HyperscaleClient`` port the workload driver opens, which
        is intentionally not subject to partition rules (the harness
        treats the client as an external entity).

        ``kind`` is ``"tcp"`` or ``"udp"`` so the lookup uses the
        right port table; the same handle exposes both.
        """
        for handle in self.all_handles():
            if address[0] != handle.host:
                continue
            if kind == "tcp" and address[1] == handle.tcp_port:
                return handle.node_id
            if kind == "udp" and address[1] == handle.udp_port:
                return handle.node_id
        return None

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

        # Each worker owns a 500-port block: TCP at block base, UDP at
        # block+10, plus headroom for the derived `udp + cores ** 2`
        # subprocess UDP and any helper ports the local pool spawns.
        # Mirrors the stride pattern used by the integration tests
        # (see tests/integration/gates/test_gate_cross_dc_dispatch.py).
        worker_addrs_by_dc: dict[str, list[tuple[int, int]]] = {}
        for dc_id, dc_spec in self.spec.datacenters.items():
            worker_addrs_by_dc[dc_id] = [
                self._ports.reserve_worker_block(
                    cores=dc_spec.cores_per_worker,
                    block_size=dc_spec.worker_port_block_size,
                )
                for _ in range(dc_spec.workers)
            ]

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

            def _build_gate(
                _node_id=node_id, _tcp=tcp, _udp=udp, _peer_tcp=peer_tcp,
                _peer_udp=peer_udp,
            ) -> GateServer:
                env = self._build_env(node_id=_node_id, dc_id="global", dc_spec=None)
                return GateServer(
                    host=self.spec.host,
                    tcp_port=_tcp,
                    udp_port=_udp,
                    env=env,
                    dc_id="global",
                    datacenter_managers=datacenter_managers,
                    datacenter_manager_udp=datacenter_manager_udp,
                    gate_peers=_peer_tcp,
                    gate_udp_peers=_peer_udp,
                )

            gate = _build_gate()
            handle = ServerHandle(
                node_id=node_id,
                kind=ServerKind.GATE,
                dc_id="global",
                host=self.spec.host,
                tcp_port=tcp,
                udp_port=udp,
                instance=gate,
                builder=_build_gate,
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

                def _build_manager(
                    _node_id=node_id, _dc_id=dc_id, _dc_spec=dc_spec,
                    _tcp=tcp, _udp=udp, _peer_tcp=peer_tcp, _peer_udp=peer_udp,
                ) -> ManagerServer:
                    env = self._build_env(
                        node_id=_node_id, dc_id=_dc_id, dc_spec=_dc_spec,
                    )
                    return ManagerServer(
                        host=self.spec.host,
                        tcp_port=_tcp,
                        udp_port=_udp,
                        env=env,
                        dc_id=_dc_id,
                        gate_addrs=gate_tcp or None,
                        gate_udp_addrs=gate_udp or None,
                        seed_managers=_peer_tcp or None,
                        manager_udp_peers=_peer_udp or None,
                    )

                manager = _build_manager()
                handle = ServerHandle(
                    node_id=node_id,
                    kind=ServerKind.MANAGER,
                    dc_id=dc_id,
                    host=self.spec.host,
                    tcp_port=tcp,
                    udp_port=udp,
                    instance=manager,
                    builder=_build_manager,
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
            for index, (tcp, udp) in enumerate(addrs):
                handle = self._build_worker_handle(
                    dc_id=dc_id,
                    dc_spec=dc_spec,
                    index=index,
                    tcp=tcp,
                    udp=udp,
                    seed_managers=[
                        (self.spec.host, manager_tcp)
                        for manager_tcp, _manager_udp in manager_addrs_by_dc[dc_id]
                    ],
                )
                self._handles_by_id[handle.node_id] = handle
                self._workers_by_dc[dc_id].append(handle)
                self._supervisor.register_server(handle)
            self._next_worker_index_by_dc[dc_id] = len(addrs)

    def _build_worker_handle(
        self,
        dc_id: str,
        dc_spec: DCSpec,
        index: int,
        tcp: int,
        udp: int,
        seed_managers: list[tuple[str, int]] | None = None,
    ) -> ServerHandle:
        node_id = f"{dc_id}.worker.{index}"
        manager_seeds = seed_managers or [
            (manager.host, manager.tcp_port)
            for manager in self._managers_by_dc.get(dc_id, [])
        ]

        def _build_worker(
            _node_id=node_id, _dc_id=dc_id, _dc_spec=dc_spec,
            _tcp=tcp, _udp=udp, _seed_managers=manager_seeds,
        ) -> WorkerServer:
            env = self._build_env(
                node_id=_node_id,
                dc_id=_dc_id,
                dc_spec=_dc_spec,
                worker_cores=_dc_spec.cores_per_worker,
            )
            return WorkerServer(
                host=self.spec.host,
                tcp_port=_tcp,
                udp_port=_udp,
                env=env,
                dc_id=_dc_id,
                seed_managers=_seed_managers,
            )

        worker = _build_worker()
        return ServerHandle(
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
            builder=_build_worker,
        )

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
        """Wait until the cluster is in steady state, capped at the budget.

        For each DC: every manager has discovered its peers and has every
        worker registered, and every worker's subprocess pool has spawned
        at least one tracked PID. Returns as soon as all hold; raises
        ``ConditionTimeoutError`` (with a diagnostic dump already written)
        on budget exhaustion.
        """
        budget = self.stabilization_seconds
        if budget is None:
            budget = self.spec.timeouts.stabilization_default
        if budget <= 0:
            return

        labelled: list[tuple[str, Callable[[], bool]]] = []
        for dc_id, dc_spec in self.spec.datacenters.items():
            managers = self._managers_by_dc.get(dc_id, [])
            workers = self._workers_by_dc.get(dc_id, [])
            for manager in managers:
                tag = f"{dc_id}/manager/{manager.node_id}"
                labelled.append(
                    (f"{tag}/peers", manager_has_n_peers(manager, dc_spec.managers - 1))
                )
                labelled.append(
                    (f"{tag}/workers", manager_has_n_workers(manager, dc_spec.workers))
                )
                # SWIM-confirmation is required for fault-injection
                # tests: AD-29 forbids UNCONFIRMED→SUSPECT transitions,
                # so a fault injected in the window between worker
                # registration (TCP) and SWIM confirmation (first
                # successful UDP probe round) results in suspicion
                # being silently skipped — and detection blowing past
                # any documented budget. This predicate enforces full
                # SWIM-tier readiness, the same invariant
                # ``manager_has_n_peers`` already enforces for
                # manager-peer relationships.
                labelled.append(
                    (
                        f"{tag}/swim_confirmed_workers",
                        manager_has_n_swim_confirmed_workers(manager, dc_spec.workers),
                    )
                )
                # LHM-quiescence is the actual readiness invariant the
                # downstream tests assume — registered+reachable is
                # weaker than "in steady state with self-health at
                # baseline." Without this gate, spin-up jitter can
                # leave LHM elevated when the harness declares ready,
                # and the test's detection-budget assertions (which
                # assume LHM=0) silently inflate.
                if dc_spec.stabilization_lhm_max_score is not None:
                    labelled.append(
                        (
                            f"{tag}/lhm_baseline",
                            lhm_at_baseline(
                                manager,
                                max_score=dc_spec.stabilization_lhm_max_score,
                            ),
                        )
                    )
            for worker in workers:
                tag = f"{dc_id}/worker/{worker.node_id}"
                labelled.append(
                    (f"{tag}/subprocesses", worker_subprocesses_alive(self, worker))
                )
                if dc_spec.stabilization_lhm_max_score is not None:
                    labelled.append(
                        (
                            f"{tag}/lhm_baseline",
                            lhm_at_baseline(
                                worker,
                                max_score=dc_spec.stabilization_lhm_max_score,
                            ),
                        )
                    )

        if not labelled:
            return

        # Capture the most-recent set of unsatisfied predicates so the
        # timeout message and diagnostic dump can name the laggards
        # directly. A single composite boolean is not enough when the
        # stabilization gate spans managers, workers, SWIM confirmation,
        # subprocess pools, and LHM baseline checks.
        failing_labels: list[str] = []

        def _composite() -> bool:
            fails: list[str] = []
            for label, pred in labelled:
                try:
                    holds = pred()
                except Exception:
                    holds = False
                if not holds:
                    fails.append(label)
            failing_labels[:] = fails
            return not fails

        async def _on_fail() -> None:
            reason = "stabilization timeout"
            if failing_labels:
                reason = f"{reason}: unsatisfied predicates={failing_labels}"
            await self._diagnostics.dump(reason=reason)

        await wait_until(
            _composite,
            timeout=budget,
            poll=0.5,
            description=f"cluster stabilizes ({len(labelled)} predicates)",
            on_fail=_on_fail,
            failure_detail=lambda: (
                f"unsatisfied predicates={failing_labels}" if failing_labels else ""
            ),
        )

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
        if layered.max_workers_per_manager is not None:
            kwargs["MAX_WORKERS_PER_MANAGER"] = layered.max_workers_per_manager
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
        max_workers_per_manager=overlay.max_workers_per_manager
        if overlay.max_workers_per_manager is not None
        else base.max_workers_per_manager,
        recovery_jitter_min=overlay.recovery_jitter_min
        if overlay.recovery_jitter_min is not None
        else base.recovery_jitter_min,
        recovery_jitter_max=overlay.recovery_jitter_max
        if overlay.recovery_jitter_max is not None
        else base.recovery_jitter_max,
    )
