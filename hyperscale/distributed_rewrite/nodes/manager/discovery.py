"""
Manager discovery module.

Handles discovery service integration for worker and peer manager selection
per AD-28 specifications.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.distributed_rewrite.discovery import DiscoveryService
    from hyperscale.logging import Logger


class ManagerDiscoveryCoordinator:
    """
    Coordinates discovery service for worker and peer selection (AD-28).

    Handles:
    - Worker discovery service management
    - Peer manager discovery service management
    - Failure decay and maintenance loops
    - Locality-aware selection
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
        worker_discovery: "DiscoveryService",
        peer_discovery: "DiscoveryService",
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._worker_discovery = worker_discovery
        self._peer_discovery = peer_discovery

    def add_worker(
        self,
        worker_id: str,
        host: str,
        port: int,
        datacenter_id: str,
    ) -> None:
        """
        Add a worker to discovery service.

        Args:
            worker_id: Worker node ID
            host: Worker host
            port: Worker TCP port
            datacenter_id: Worker's datacenter
        """
        self._worker_discovery.add_peer(
            peer_id=worker_id,
            host=host,
            port=port,
            role="worker",
            datacenter_id=datacenter_id,
        )

    def remove_worker(self, worker_id: str) -> None:
        """
        Remove a worker from discovery service.

        Args:
            worker_id: Worker node ID
        """
        self._worker_discovery.remove_peer(worker_id)

    def add_peer_manager(
        self,
        peer_id: str,
        host: str,
        port: int,
        datacenter_id: str,
    ) -> None:
        """
        Add a peer manager to discovery service.

        Args:
            peer_id: Peer manager node ID
            host: Peer host
            port: Peer TCP port
            datacenter_id: Peer's datacenter
        """
        self._peer_discovery.add_peer(
            peer_id=peer_id,
            host=host,
            port=port,
            role="manager",
            datacenter_id=datacenter_id,
        )

    def remove_peer_manager(self, peer_id: str) -> None:
        """
        Remove a peer manager from discovery service.

        Args:
            peer_id: Peer manager node ID
        """
        self._peer_discovery.remove_peer(peer_id)

    def select_worker(self, exclude: set[str] | None = None) -> str | None:
        """
        Select a worker using EWMA-based selection.

        Args:
            exclude: Set of worker IDs to exclude

        Returns:
            Selected worker ID or None if none available
        """
        return self._worker_discovery.select_peer(exclude=exclude)

    def select_peer_manager(self, exclude: set[str] | None = None) -> str | None:
        """
        Select a peer manager using EWMA-based selection.

        Args:
            exclude: Set of peer IDs to exclude

        Returns:
            Selected peer ID or None if none available
        """
        return self._peer_discovery.select_peer(exclude=exclude)

    def record_worker_success(self, worker_id: str, latency_ms: float) -> None:
        """
        Record successful interaction with worker.

        Args:
            worker_id: Worker node ID
            latency_ms: Interaction latency
        """
        self._worker_discovery.record_success(worker_id, latency_ms)

    def record_worker_failure(self, worker_id: str) -> None:
        """
        Record failed interaction with worker.

        Args:
            worker_id: Worker node ID
        """
        self._worker_discovery.record_failure(worker_id)

    def record_peer_success(self, peer_id: str, latency_ms: float) -> None:
        """
        Record successful interaction with peer.

        Args:
            peer_id: Peer node ID
            latency_ms: Interaction latency
        """
        self._peer_discovery.record_success(peer_id, latency_ms)

    def record_peer_failure(self, peer_id: str) -> None:
        """
        Record failed interaction with peer.

        Args:
            peer_id: Peer node ID
        """
        self._peer_discovery.record_failure(peer_id)

    async def start_maintenance_loop(self) -> None:
        """
        Start the discovery maintenance loop.

        Runs periodic failure decay and cleanup.
        """
        self._state._discovery_maintenance_task = asyncio.create_task(
            self._maintenance_loop()
        )

    async def stop_maintenance_loop(self) -> None:
        """Stop the discovery maintenance loop."""
        if self._state._discovery_maintenance_task:
            self._state._discovery_maintenance_task.cancel()
            try:
                await self._state._discovery_maintenance_task
            except asyncio.CancelledError:
                pass
            self._state._discovery_maintenance_task = None

    async def _maintenance_loop(self) -> None:
        """
        Background loop for discovery maintenance.

        Decays failure counts and removes stale entries.
        """
        interval = self._config.discovery_failure_decay_interval_seconds

        while True:
            try:
                await asyncio.sleep(interval)

                # Decay failure counts
                self._worker_discovery.decay_failures()
                self._peer_discovery.decay_failures()

                self._task_runner.run(
                    self._logger.log,
                    ServerDebug(
                        message="Discovery maintenance completed",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )

            except asyncio.CancelledError:
                break
            except Exception as maintenance_error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Discovery maintenance error: {maintenance_error}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )

    def get_discovery_metrics(self) -> dict:
        """Get discovery-related metrics."""
        return {
            "worker_peer_count": self._worker_discovery.peer_count(),
            "manager_peer_count": self._peer_discovery.peer_count(),
        }
