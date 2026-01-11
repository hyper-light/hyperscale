"""
Manager registry for worker, gate, and peer management.

Provides centralized registration and tracking of workers, gates,
and peer managers.
"""

import time
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import (
    WorkerRegistration,
    GateInfo,
    ManagerInfo,
)
from hyperscale.distributed_rewrite.swim.core import ErrorStats
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerDebug

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.logging.hyperscale_logger import Logger


class ManagerRegistry:
    """
    Manages registration and tracking of workers, gates, and peer managers.

    Centralizes all registration logic and provides accessor methods
    for retrieving healthy/active nodes.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

    def register_worker(
        self,
        registration: WorkerRegistration,
    ) -> None:
        """
        Register a worker with this manager.

        Args:
            registration: Worker registration details
        """
        worker_id = registration.node.node_id
        self._state._workers[worker_id] = registration

        tcp_addr = (registration.node.host, registration.node.tcp_port)
        udp_addr = (registration.node.host, registration.node.udp_port)
        self._state._worker_addr_to_id[tcp_addr] = worker_id
        self._state._worker_addr_to_id[udp_addr] = worker_id

        # Initialize circuit breaker for this worker
        if worker_id not in self._state._worker_circuits:
            self._state._worker_circuits[worker_id] = ErrorStats(
                max_errors=5,
                window_seconds=60.0,
                half_open_after=30.0,
            )

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Worker {worker_id[:8]}... registered with {registration.node.total_cores} cores",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def unregister_worker(self, worker_id: str) -> None:
        """
        Unregister a worker from this manager.

        Args:
            worker_id: Worker node ID to unregister
        """
        registration = self._state._workers.pop(worker_id, None)
        if registration:
            tcp_addr = (registration.node.host, registration.node.tcp_port)
            udp_addr = (registration.node.host, registration.node.udp_port)
            self._state._worker_addr_to_id.pop(tcp_addr, None)
            self._state._worker_addr_to_id.pop(udp_addr, None)

        self._state._worker_circuits.pop(worker_id, None)
        self._state._dispatch_semaphores.pop(worker_id, None)
        self._state._worker_deadlines.pop(worker_id, None)
        self._state._worker_unhealthy_since.pop(worker_id, None)

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

    def register_gate(self, gate_info: GateInfo) -> None:
        """
        Register a gate with this manager.

        Args:
            gate_info: Gate information
        """
        self._state._known_gates[gate_info.node_id] = gate_info
        self._state._healthy_gate_ids.add(gate_info.node_id)

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Gate {gate_info.node_id[:8]}... registered",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def unregister_gate(self, gate_id: str) -> None:
        """
        Unregister a gate from this manager.

        Args:
            gate_id: Gate node ID to unregister
        """
        self._state._known_gates.pop(gate_id, None)
        self._state._healthy_gate_ids.discard(gate_id)
        self._state._gate_unhealthy_since.pop(gate_id, None)

    def get_gate(self, gate_id: str) -> GateInfo | None:
        """Get gate info by ID."""
        return self._state._known_gates.get(gate_id)

    def get_healthy_gates(self) -> list[GateInfo]:
        """Get all healthy gates."""
        return [
            gate for gate_id, gate in self._state._known_gates.items()
            if gate_id in self._state._healthy_gate_ids
        ]

    def mark_gate_unhealthy(self, gate_id: str) -> None:
        """Mark a gate as unhealthy."""
        self._state._healthy_gate_ids.discard(gate_id)
        if gate_id not in self._state._gate_unhealthy_since:
            self._state._gate_unhealthy_since[gate_id] = time.monotonic()

    def mark_gate_healthy(self, gate_id: str) -> None:
        """Mark a gate as healthy."""
        if gate_id in self._state._known_gates:
            self._state._healthy_gate_ids.add(gate_id)
            self._state._gate_unhealthy_since.pop(gate_id, None)

    def register_manager_peer(self, peer_info: ManagerInfo) -> None:
        """
        Register a manager peer.

        Args:
            peer_info: Manager peer information
        """
        self._state._known_manager_peers[peer_info.node_id] = peer_info

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Manager peer {peer_info.node_id[:8]}... registered",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

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
        self._state._active_manager_peer_ids.discard(peer_id)
        self._state._manager_peer_unhealthy_since.pop(peer_id, None)

    def get_manager_peer(self, peer_id: str) -> ManagerInfo | None:
        """Get manager peer info by ID."""
        return self._state._known_manager_peers.get(peer_id)

    def get_active_manager_peers(self) -> list[ManagerInfo]:
        """Get all active manager peers."""
        return [
            peer for peer_id, peer in self._state._known_manager_peers.items()
            if peer_id in self._state._active_manager_peer_ids
        ]

    def get_active_peer_count(self) -> int:
        """Get count of active peers (including self)."""
        return len(self._state._active_manager_peers) + 1
