"""
Worker discovery service manager (AD-28).

Handles discovery service integration and maintenance loop
for adaptive peer selection and DNS-based discovery.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logging_models import ServerWarning

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

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
                await _DEFAULT_CLOCK.sleep(self._failure_decay_interval)

                # Decay failure counts to allow peers to recover
                self._discovery_service.decay_failures()

                # Clean up expired DNS cache entries
                self._discovery_service.cleanup_expired_dns()

                # Optionally discover new peers via DNS (if configured)
                if self._discovery_service.config.dns_names:
                    await self._discovery_service.discover_peers()

            except asyncio.CancelledError:
                break
            except Exception as maintenance_error:
                dns_names = (
                    self._discovery_service.config.dns_names
                    if self._discovery_service.config
                    else []
                )
                await self._logger.log(
                    ServerWarning(
                        message=(
                            f"Discovery maintenance loop error: {maintenance_error} "
                            f"(dns_names={dns_names}, decay_interval={self._failure_decay_interval}s)"
                        ),
                        node_host="worker",
                        node_port=0,
                        node_id="discovery",
                    )
                )

    def stop(self) -> None:
        """Stop the maintenance loop."""
        self._running = False

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
