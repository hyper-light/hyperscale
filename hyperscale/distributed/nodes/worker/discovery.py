"""
Worker discovery service manager (AD-28).

Handles discovery service integration for adaptive peer selection; the
discovery maintenance (DNS, failure decay) runs in the worker's
background loops.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hyperscale.distributed.discovery import DiscoveryService
    from hyperscale.logging import Logger


class WorkerDiscoveryManager:
    """
    Manages discovery service integration for worker.

    Provides adaptive peer selection using Power of Two Choices
    with EWMA-based load tracking and locality preferences (AD-28).
    """

    def __init__(
        self,
        discovery_service: "DiscoveryService",
        logger: "Logger",
    ) -> None:
        """
        Initialize discovery manager.

        Args:
            discovery_service: DiscoveryService instance for peer selection
            logger: Logger instance for logging
        """
        self._discovery_service: "DiscoveryService" = discovery_service
        self._logger: "Logger" = logger

    def select_best_manager(
        self,
        key: str,
        healthy_manager_ids: set[str],
    ) -> str | None:
        """
        Select a manager for ``key`` using adaptive selection (AD-28).

        Weighted rendezvous ranking, then Power of Two Choices on EWMA
        latency, restricted to ``healthy_manager_ids``. Discovery peers are
        keyed by manager node id (registration adds them that way).

        Args:
            key: Rendezvous key (the worker's own node id spreads workers
                across managers deterministically)
            healthy_manager_ids: Manager node ids eligible for selection

        Returns:
            The selected manager node id, or None if none is eligible
        """
        selection = self._discovery_service.select_peer_with_filter(
            key,
            healthy_manager_ids.__contains__,
        )
        return selection.peer_id if selection is not None else None

    def record_success(self, manager_id: str, latency_ms: float) -> None:
        """Feed a successful round trip into the manager's EWMA latency."""
        self._discovery_service.record_success(manager_id, latency_ms)

    def record_failure(self, manager_id: str) -> None:
        """Record a failed interaction with a manager."""
        self._discovery_service.record_failure(manager_id)
