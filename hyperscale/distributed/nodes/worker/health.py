"""
Worker health integration module.

Handles SWIM callbacks, health embedding, and overload detection
integration for worker health reporting.
"""

import time
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from .registry import WorkerRegistry
    from .backpressure import WorkerBackpressureManager


class WorkerHealthIntegration:
    """
    Integrates health monitoring for worker.

    Handles SWIM callbacks for node join/dead events,
    health state embedding for gossip, and coordinates
    with backpressure manager for overload detection.
    """

    def __init__(
        self,
        registry: "WorkerRegistry",
        backpressure_manager: "WorkerBackpressureManager",
        logger: "Logger",
    ) -> None:
        """
        Initialize health integration.

        Args:
            registry: WorkerRegistry for manager tracking
            backpressure_manager: WorkerBackpressureManager for overload state
            logger: Logger instance for logging
        """
        self._registry = registry
        self._backpressure_manager = backpressure_manager
        self._logger = logger

        # Callbacks for external handlers
        self._on_manager_failure: Callable[[str], None] | None = None
        self._on_manager_recovery: Callable[[str], None] | None = None

    def set_failure_callback(self, callback: Callable[[str], None]) -> None:
        """Set callback for manager failure events."""
        self._on_manager_failure = callback

    def set_recovery_callback(self, callback: Callable[[str], None]) -> None:
        """Set callback for manager recovery events."""
        self._on_manager_recovery = callback

    def on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """
        SWIM callback when a node is marked as DEAD.

        Dispatches to async handler for proper lock coordination.

        Args:
            node_addr: UDP address of the dead node
        """
        # Find which manager this address belongs to
        if manager_id := self._registry.find_manager_by_udp_addr(node_addr):
            if self._on_manager_failure:
                self._on_manager_failure(manager_id)

    def on_node_join(self, node_addr: tuple[str, int]) -> None:
        """
        SWIM callback when a node joins or rejoins the cluster.

        Dispatches to async handler for proper jitter and lock coordination.

        Args:
            node_addr: UDP address of the joining node
        """
        # Find which manager this address belongs to
        if manager_id := self._registry.find_manager_by_udp_addr(node_addr):
            if self._on_manager_recovery:
                self._on_manager_recovery(manager_id)

    def get_health_embedding(self) -> dict[str, Any]:
        """
        Get health data for SWIM state embedding.

        Returns worker health state for gossip propagation.

        Returns:
            Dictionary with health embedding data
        """
        return {
            "overload_state": self._backpressure_manager.get_overload_state_str(),
            "timestamp": time.monotonic(),
        }

    def is_healthy(self) -> bool:
        """
        Check if worker is currently healthy.

        Returns:
            True if worker is not overloaded
        """
        return not self._backpressure_manager.is_overloaded()

    def get_health_status(self) -> dict:
        """
        Get comprehensive health status.

        Returns:
            Dictionary with health metrics
        """
        return {
            "healthy": self.is_healthy(),
            "overload_state": self._backpressure_manager.get_overload_state_str(),
            "backpressure_level": self._backpressure_manager.get_max_backpressure_level().value,
            "backpressure_delay_ms": self._backpressure_manager.get_backpressure_delay_ms(),
            "healthy_managers": len(self._registry._healthy_manager_ids),
            "known_managers": len(self._registry._known_managers),
            "primary_manager": self._registry._primary_manager_id,
        }
