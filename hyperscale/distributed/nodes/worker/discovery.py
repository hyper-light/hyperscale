"""
Worker discovery service manager (AD-28).

Handles discovery service integration and maintenance loop
for adaptive peer selection and DNS-based discovery.
"""

import asyncio
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
        failure_decay_interval: float = 60.0,
    ) -> None:
        """
        Initialize discovery manager.

        Args:
            discovery_service: DiscoveryService instance for peer selection
            logger: Logger instance for logging
            failure_decay_interval: Interval for decaying failure counts
        """
        self._discovery_service: "DiscoveryService" = discovery_service
        self._logger: "Logger" = logger
        self._failure_decay_interval: float = failure_decay_interval
        self._running: bool = False

    async def run_maintenance_loop(self) -> None:
        """
        Background loop for discovery service maintenance (AD-28).

        Periodically:
        - Runs DNS discovery for new managers
        - Decays failure counts to allow recovery
        - Cleans up expired DNS cache entries
        """
        self._running = True
        while self._running:
            try:
                await asyncio.sleep(self._failure_decay_interval)

                # Decay failure counts to allow peers to recover
                self._discovery_service.decay_failures()

                # Clean up expired DNS cache entries
                self._discovery_service.cleanup_expired_dns()

                # Optionally discover new peers via DNS (if configured)
                if self._discovery_service.config.dns_names:
                    await self._discovery_service.discover_peers()

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def stop(self) -> None:
        """Stop the maintenance loop."""
        self._running = False

    def select_best_manager(
        self,
        key: str,
        healthy_manager_ids: set[str],
    ) -> tuple[str, int] | None:
        """
        Select the best manager for a given key using adaptive selection (AD-28).

        Uses Power of Two Choices with EWMA for load-aware selection,
        with locality preferences if configured.

        Args:
            key: Key for consistent selection (e.g., workflow_id)
            healthy_manager_ids: Set of healthy manager IDs to consider

        Returns:
            Tuple of (host, port) for the selected manager, or None if unavailable
        """

        def is_healthy(peer_id: str) -> bool:
            return peer_id in healthy_manager_ids

        selection = self._discovery_service.select_peer_with_filter(key, is_healthy)
        if not selection:
            return None

        # Parse host:port from selection
        if ":" in selection:
            host, port_str = selection.rsplit(":", 1)
            return (host, int(port_str))

        return None

    def record_success(self, peer_addr: tuple[str, int]) -> None:
        """Record a successful interaction with a peer."""
        peer_id = f"{peer_addr[0]}:{peer_addr[1]}"
        self._discovery_service.record_success(peer_id)

    def record_failure(self, peer_addr: tuple[str, int]) -> None:
        """Record a failed interaction with a peer."""
        peer_id = f"{peer_addr[0]}:{peer_addr[1]}"
        self._discovery_service.record_failure(peer_id)
