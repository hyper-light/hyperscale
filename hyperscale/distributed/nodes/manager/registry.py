"""
Manager registry for worker, gate, and peer management.

Provides centralized registration and tracking of workers, gates,
and peer managers.
"""

from types import MappingProxyType
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    WorkerRegistration,
    GateInfo,
    ManagerInfo,
)
from hyperscale.distributed.swim.core import ErrorStats, CircuitState
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerDebug

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

# AD-17 dispatch bucket per worker health state; "overloaded" (and any
# unknown state) has no bucket, so those workers are excluded like unhealthy.
_HEALTH_STATE_BUCKETS: MappingProxyType[str, str] = MappingProxyType(
    {
        "healthy": "healthy",
        "busy": "busy",
        "stressed": "degraded",
        "degraded": "degraded",
    }
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.jobs.worker_pool import WorkerPool
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


class ManagerRegistry:
    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner: "TaskRunner",
        on_worker_unregistered: Callable[[str], None],
    ) -> None:
        """
        Args:
            on_worker_unregistered: Hears each worker this registry
                forgets, so state kept beside it (AD-26 extension tracking)
                is forgotten with it.
        """
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner
        self._on_worker_unregistered = on_worker_unregistered
        self._worker_pool: "WorkerPool | None" = None

    def set_worker_pool(self, worker_pool: "WorkerPool") -> None:
        self._worker_pool = worker_pool

    async def register_worker(
        self,
        registration: WorkerRegistration,
    ) -> None:
        """
        Register a worker with this manager.

        Args:
            registration: Worker registration details
        """
        worker_id = registration.node.node_id

        tcp_addr = (registration.node.host, registration.node.port)
        udp_addr = (registration.node.host, registration.node.udp_port)

        # Evict any stale worker_id currently sharing either address. SWIM
        # death detection can lag a hard kill + restart, so the previous
        # process's node_id may still be in ``_workers`` when the rebuilt
        # worker re-registers at the same TCP/UDP ports. Without this,
        # the registry accumulates one entry per restart cycle even though
        # only one live process exists at the address — the kill/restart
        # churn test eventually pushes ``get_worker_count()`` above the
        # actual cluster size and ``wait_until(count <= 1)`` never fires
        # because the count never drops back down.
        for addr in (tcp_addr, udp_addr):
            self._evict_stale_worker_at(addr, worker_id)

        self._state._workers[worker_id] = registration
        self._state._worker_addr_to_id[tcp_addr] = worker_id
        self._state._worker_addr_to_id[udp_addr] = worker_id
        # (Re-)registration discharges any outstanding eviction-notice
        # obligation (two-sided deregistration).
        self._state.clear_eviction_notice(worker_id)

        # Initialize circuit breaker for this worker
        if worker_id not in self._state._worker_circuits:
            self._state._worker_circuits[worker_id] = ErrorStats(
                max_errors=self._config.circuit_breaker_max_errors,
                window_seconds=self._config.circuit_breaker_window_seconds,
                half_open_after=self._config.circuit_breaker_half_open_after_seconds,
            )

        await self._logger.log(
            ServerInfo(
                message=f"Worker {worker_id[:8]}... registered with {registration.total_cores} cores",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def _evict_stale_worker_at(self, addr: tuple[str, int], worker_id: str) -> None:
        """Unregister a different worker_id still mapped to ``addr`` (restart churn)."""
        existing_worker_id = self._state._worker_addr_to_id.get(addr)
        if existing_worker_id is not None and existing_worker_id != worker_id:
            self.unregister_worker(existing_worker_id)

    def _worker_progress_keys(self, worker_id: str) -> list[tuple[str, str]]:
        """The (job_id, worker_id) progress keys recorded for ``worker_id``."""
        return [
            key for key in self._state._worker_job_last_progress if key[1] == worker_id
        ]

    def unregister_worker(self, worker_id: str) -> None:
        """
        Unregister a worker from this manager.

        Args:
            worker_id: Worker node ID to unregister
        """
        registration = self._state._workers.pop(worker_id, None)
        if registration:
            tcp_addr = (registration.node.host, registration.node.port)
            udp_addr = (registration.node.host, registration.node.udp_port)
            self._state._worker_addr_to_id.pop(tcp_addr, None)
            self._state._worker_addr_to_id.pop(udp_addr, None)

        self._state._worker_circuits.pop(worker_id, None)
        self._state._worker_deadlines.pop(worker_id, None)
        self._state._worker_unhealthy_since.pop(worker_id, None)
        self._state._worker_health_states.pop(worker_id, None)
        self._state._worker_latency_samples.pop(worker_id, None)
        self._state._dispatch_semaphores.pop(worker_id, None)

        progress_keys_to_remove = self._worker_progress_keys(worker_id)
        for key in progress_keys_to_remove:
            self._state._worker_job_last_progress.pop(key, None)

        self._on_worker_unregistered(worker_id)

    def get_worker(self, worker_id: str) -> WorkerRegistration | None:
        """Get worker registration by ID."""
        return self._state._workers.get(worker_id)

    def get_worker_by_addr(self, addr: tuple[str, int]) -> WorkerRegistration | None:
        """Get worker registration by address."""
        worker_id = self._state._worker_addr_to_id.get(addr)
        return self._state._workers.get(worker_id) if worker_id else None

    def get_all_workers(self) -> dict[str, WorkerRegistration]:
        """Get all registered workers."""
        return dict(self._state._workers)

    def get_healthy_worker_ids(self) -> set[str]:
        """Get IDs of workers not marked unhealthy."""
        unhealthy = set(self._state._worker_unhealthy_since.keys())
        return set(self._state._workers.keys()) - unhealthy

    def update_worker_health_state(
        self,
        worker_id: str,
        health_state: str,
    ) -> tuple[str | None, str]:
        if worker_id not in self._state._workers:
            return (None, health_state)

        previous_state = self.get_worker_health_state(worker_id)
        return (previous_state, health_state)

    def get_worker_health_state(self, worker_id: str) -> str:
        if self._worker_pool:
            worker = self._worker_pool._workers.get(worker_id)
            if worker:
                return worker.overload_state
        return "healthy"

    def get_worker_health_state_counts(self) -> dict[str, int]:
        if self._worker_pool:
            return self._worker_pool.get_worker_health_state_counts()

        counts = {"healthy": 0, "busy": 0, "stressed": 0, "overloaded": 0}
        unhealthy_ids = set(self._state._worker_unhealthy_since.keys())

        for worker_id in self._state._workers:
            if worker_id in unhealthy_ids:
                continue

            health_state = self._state._worker_health_states.get(worker_id, "healthy")
            if health_state in counts:
                counts[health_state] += 1
            else:
                counts["healthy"] += 1

        return counts

    def get_workers_by_health_bucket(
        self,
        cores_required: int = 1,
    ) -> dict[str, list[WorkerRegistration]]:
        """
        Bucket workers by health state for AD-17 smart dispatch.

        Returns workers grouped by health: healthy > busy > degraded.
        Workers marked as unhealthy or with open circuit breakers are excluded.
        Workers within each bucket are sorted by available capacity (descending).

        Args:
            cores_required: Minimum cores required

        Returns:
            Dict with keys "healthy", "busy", "degraded" containing lists of workers
        """
        buckets: dict[str, list[WorkerRegistration]] = {
            "healthy": [],
            "busy": [],
            "degraded": [],
        }

        # Get workers not marked as dead/unhealthy
        unhealthy_ids = set(self._state._worker_unhealthy_since.keys())

        for worker_id, worker in self._state._workers.items():
            if not self._worker_dispatchable(worker_id, worker, unhealthy_ids, cores_required):
                continue

            self._place_in_health_bucket(buckets, worker_id, worker)

        # Sort each bucket by capacity (total_cores descending)
        self._sort_buckets_by_capacity(buckets)

        return buckets

    @staticmethod
    def _unhealthy_worker_excluded(
        worker_id: str,
        circuit: ErrorStats | None,
        unhealthy_ids: set[str],
    ) -> bool:
        """An unhealthy worker stays dispatchable only while its circuit is half-open (AD-17)."""
        return worker_id in unhealthy_ids and (
            not circuit or circuit.circuit_state != CircuitState.HALF_OPEN
        )

    @staticmethod
    def _circuit_blocks_dispatch(circuit: ErrorStats | None) -> bool:
        """Whether the worker's circuit breaker is open."""
        return circuit and circuit.is_open()

    def _worker_dispatchable(
        self,
        worker_id: str,
        worker: WorkerRegistration,
        unhealthy_ids: set[str],
        cores_required: int,
    ) -> bool:
        """AD-17 eligibility: not unhealthy (unless half-open), circuit closed, enough cores."""
        circuit = self._state._worker_circuits.get(worker_id)

        if self._unhealthy_worker_excluded(worker_id, circuit, unhealthy_ids):
            return False

        if self._circuit_blocks_dispatch(circuit):
            return False

        # Skip workers without capacity
        return worker.total_cores >= cores_required

    def _place_in_health_bucket(
        self,
        buckets: dict[str, list[WorkerRegistration]],
        worker_id: str,
        worker: WorkerRegistration,
    ) -> None:
        """Append the worker to its health bucket; "overloaded" workers are excluded (treated like unhealthy)."""
        health_state = self.get_worker_health_state(worker_id)

        if (bucket_name := _HEALTH_STATE_BUCKETS.get(health_state)) is not None:
            buckets[bucket_name].append(worker)

    @staticmethod
    def _sort_buckets_by_capacity(buckets: dict[str, list[WorkerRegistration]]) -> None:
        """Sort each bucket by capacity (total_cores descending)."""
        for bucket_name in buckets:
            buckets[bucket_name].sort(
                key=lambda w: w.total_cores,
                reverse=True,
            )

    async def register_gate(self, gate_info: GateInfo) -> None:
        """
        Register a gate with this manager.

        Args:
            gate_info: Gate information
        """
        tcp_addr = (gate_info.tcp_host, gate_info.tcp_port)
        udp_addr = (gate_info.udp_host, gate_info.udp_port)
        stale_gate_ids = [
            gate_id
            for gate_id, known_gate in self._state._known_gates.items()
            if gate_id != gate_info.node_id
            and (
                (known_gate.tcp_host, known_gate.tcp_port) == tcp_addr
                or (known_gate.udp_host, known_gate.udp_port) == udp_addr
            )
        ]
        for stale_gate_id in stale_gate_ids:
            self.unregister_gate(stale_gate_id)

        self._state._known_gates[gate_info.node_id] = gate_info
        self._state._healthy_gate_ids.add(gate_info.node_id)

        await self._logger.log(
            ServerInfo(
                message=f"Gate {gate_info.node_id[:8]}... registered",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def unregister_gate(self, gate_id: str) -> None:
        """
        Unregister a gate from this manager.

        Args:
            gate_id: Gate node ID to unregister
        """
        gate_info = self._state._known_gates.pop(gate_id, None)
        self._state._healthy_gate_ids.discard(gate_id)
        self._state._gate_unhealthy_since.pop(gate_id, None)
        self._state.remove_gate_lock(gate_id)

        if gate_info is not None:
            stale_udp_addrs = self._stale_gate_udp_addrs(gate_info)
            for udp_addr in stale_udp_addrs:
                self._state._gate_udp_to_tcp.pop(udp_addr, None)

    def _stale_gate_udp_addrs(self, gate_info: GateInfo) -> list[tuple[str, int]]:
        """UDP addresses still mapped to the forgotten gate's TCP address."""
        return [
            udp_addr
            for udp_addr, tcp_addr in self._state._gate_udp_to_tcp.items()
            if tcp_addr == (gate_info.tcp_host, gate_info.tcp_port)
        ]

    def get_gate(self, gate_id: str) -> GateInfo | None:
        """Get gate info by ID."""
        return self._state._known_gates.get(gate_id)

    def get_healthy_gates(self) -> list[GateInfo]:
        """Get all healthy gates."""
        return [
            gate
            for gate_id, gate in self._state._known_gates.items()
            if gate_id in self._state._healthy_gate_ids
        ]

    def mark_gate_unhealthy(self, gate_id: str) -> None:
        """Mark a gate as unhealthy."""
        self._state._healthy_gate_ids.discard(gate_id)
        if gate_id not in self._state._gate_unhealthy_since:
            self._state._gate_unhealthy_since[gate_id] = _DEFAULT_CLOCK.monotonic()

    def mark_gate_healthy(self, gate_id: str) -> None:
        """Mark a gate as healthy."""
        if gate_id in self._state._known_gates:
            self._state._healthy_gate_ids.add(gate_id)
            self._state._gate_unhealthy_since.pop(gate_id, None)

    async def register_manager_peer(self, peer_info: ManagerInfo) -> None:
        """
        Register a manager peer.

        Args:
            peer_info: Manager peer information
        """
        tcp_addr = (peer_info.tcp_host, peer_info.tcp_port)
        udp_addr = (peer_info.udp_host, peer_info.udp_port)
        stale_peer_ids = self._stale_manager_peer_ids(peer_info.node_id, tcp_addr, udp_addr)
        for stale_peer_id in stale_peer_ids:
            self.unregister_manager_peer(stale_peer_id)

        self._state._known_manager_peers[peer_info.node_id] = peer_info

        await self._logger.log(
            ServerDebug(
                message=f"Manager peer {peer_info.node_id[:8]}... registered",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    @staticmethod
    def _is_stale_manager_peer(
        peer_id: str,
        known_peer: ManagerInfo,
        node_id: str,
        tcp_addr: tuple[str, int],
        udp_addr: tuple[str, int],
    ) -> bool:
        """Whether a different known peer occupies either address of the registering peer."""
        return peer_id != node_id and (
            (known_peer.tcp_host, known_peer.tcp_port) == tcp_addr
            or (known_peer.udp_host, known_peer.udp_port) == udp_addr
        )

    def _stale_manager_peer_ids(
        self,
        node_id: str,
        tcp_addr: tuple[str, int],
        udp_addr: tuple[str, int],
    ) -> list[str]:
        """Other peer ids still registered at the registering peer's TCP or UDP address."""
        return [
            peer_id
            for peer_id, known_peer in self._state._known_manager_peers.items()
            if self._is_stale_manager_peer(peer_id, known_peer, node_id, tcp_addr, udp_addr)
        ]

    def unregister_manager_peer(self, peer_id: str) -> None:
        """
        Unregister a manager peer.

        Args:
            peer_id: Peer node ID to unregister
        """
        peer_info = self._state._known_manager_peers.pop(peer_id, None)
        if peer_info:
            tcp_addr = (peer_info.tcp_host, peer_info.tcp_port)
            self._state._active_manager_peers.discard(tcp_addr)
            self._state.remove_peer_lock(tcp_addr)
        self._state._active_manager_peer_ids.discard(peer_id)
        self._state._manager_peer_unhealthy_since.pop(peer_id, None)
        self._state._peer_manager_health_states.pop(peer_id, None)
        self._state._registered_with_managers.discard(peer_id)
        self._state.remove_peer_latency_samples(peer_id)

    def get_manager_peer(self, peer_id: str) -> ManagerInfo | None:
        """Get manager peer info by ID."""
        return self._state._known_manager_peers.get(peer_id)

    def get_active_manager_peers(self) -> list[ManagerInfo]:
        """Get all active manager peers."""
        return [
            peer
            for peer_id, peer in self._state._known_manager_peers.items()
            if peer_id in self._state._active_manager_peer_ids
        ]
