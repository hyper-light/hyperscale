"""
Worker background loops module.

Consolidates all periodic background tasks for WorkerServer:
- Dead manager reaping
- Orphan workflow checking
- Discovery maintenance
- Progress flushing
- Overload detection polling

Extracted from worker_impl.py for modularity.
"""

import asyncio
import time
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerWarning,
    ServerError,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from hyperscale.distributed.discovery import DiscoveryService
    from .registry import WorkerRegistry
    from .state import WorkerState
    from .backpressure import WorkerBackpressureManager


class WorkerBackgroundLoops:
    """
    Manages background loops for worker server.

    Runs periodic maintenance tasks including:
    - Dead manager reaping (AD-28)
    - Orphan workflow checking (Section 2.7)
    - Discovery maintenance (AD-28)
    - Progress buffer flushing (AD-37)
    """

    def __init__(
        self,
        registry: "WorkerRegistry",
        state: "WorkerState",
        discovery_service: "DiscoveryService",
        logger: "Logger | None" = None,
        backpressure_manager: "WorkerBackpressureManager | None" = None,
    ) -> None:
        """
        Initialize background loops manager.

        Args:
            registry: WorkerRegistry for manager tracking
            state: WorkerState for workflow tracking
            discovery_service: DiscoveryService for peer management
            logger: Logger instance
            backpressure_manager: Optional backpressure manager
        """
        self._registry = registry
        self._state = state
        self._discovery_service = discovery_service
        self._logger = logger
        self._backpressure_manager = backpressure_manager
        self._running = False

        # Loop intervals (can be overridden via config)
        self._dead_manager_reap_interval = 60.0
        self._dead_manager_check_interval = 10.0
        self._orphan_grace_period = 120.0
        self._orphan_check_interval = 10.0
        self._discovery_failure_decay_interval = 60.0
        self._progress_flush_interval = 0.5

    def configure(
        self,
        dead_manager_reap_interval: float = 60.0,
        dead_manager_check_interval: float = 10.0,
        orphan_grace_period: float = 120.0,
        orphan_check_interval: float = 10.0,
        discovery_failure_decay_interval: float = 60.0,
        progress_flush_interval: float = 0.5,
    ) -> None:
        """
        Configure loop intervals.

        Args:
            dead_manager_reap_interval: Time before reaping dead managers
            dead_manager_check_interval: Interval for checking dead managers
            orphan_grace_period: Grace period before cancelling orphan workflows
            orphan_check_interval: Interval for checking orphan workflows
            discovery_failure_decay_interval: Interval for decaying failure counts
            progress_flush_interval: Interval for flushing progress buffer
        """
        self._dead_manager_reap_interval = dead_manager_reap_interval
        self._dead_manager_check_interval = dead_manager_check_interval
        self._orphan_grace_period = orphan_grace_period
        self._orphan_check_interval = orphan_check_interval
        self._discovery_failure_decay_interval = discovery_failure_decay_interval
        self._progress_flush_interval = progress_flush_interval

    async def run_dead_manager_reap_loop(
        self,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
        is_running: callable,
    ) -> None:
        """
        Reap managers that have been unhealthy for too long.

        Args:
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            task_runner_run: Function to run async tasks
            is_running: Function to check if worker is running
        """
        self._running = True
        while is_running() and self._running:
            try:
                await asyncio.sleep(self._dead_manager_check_interval)

                current_time = time.monotonic()
                managers_to_reap: list[str] = []

                for manager_id, unhealthy_since in list(
                    self._registry._manager_unhealthy_since.items()
                ):
                    if (
                        current_time - unhealthy_since
                        >= self._dead_manager_reap_interval
                    ):
                        managers_to_reap.append(manager_id)

                for manager_id in managers_to_reap:
                    manager_info = self._registry.get_manager(manager_id)
                    manager_addr = None
                    if manager_info:
                        manager_addr = (manager_info.tcp_host, manager_info.tcp_port)

                    # Remove from all tracking structures
                    self._registry._known_managers.pop(manager_id, None)
                    self._registry._healthy_manager_ids.discard(manager_id)
                    self._registry._manager_unhealthy_since.pop(manager_id, None)
                    self._registry._manager_circuits.pop(manager_id, None)

                    # Remove from discovery service
                    self._discovery_service.remove_peer(manager_id)

                    # Clean up address-based circuit breaker
                    if manager_addr:
                        self._registry._manager_addr_circuits.pop(manager_addr, None)

                    if self._logger:
                        task_runner_run(
                            self._logger.log,
                            ServerInfo(
                                message=f"Reaped dead manager {manager_id} after {self._dead_manager_reap_interval}s",
                                node_host=node_host,
                                node_port=node_port,
                                node_id=node_id_short,
                            ),
                        )

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def run_orphan_check_loop(
        self,
        cancel_workflow: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
        is_running: callable,
    ) -> None:
        """
        Check for and cancel orphaned workflows (Section 2.7).

        Orphaned workflows are those whose job leader manager failed
        and haven't received a transfer notification within grace period.

        Args:
            cancel_workflow: Function to cancel a workflow
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            is_running: Function to check if worker is running
        """
        self._running = True
        while is_running() and self._running:
            try:
                await asyncio.sleep(self._orphan_check_interval)

                workflows_to_cancel: list[tuple[str, str]] = []

                for workflow_id, orphan_timestamp in list(
                    self._state._orphaned_workflows.items()
                ):
                    elapsed = time.monotonic() - orphan_timestamp
                    if elapsed >= self._orphan_grace_period:
                        workflows_to_cancel.append(
                            (workflow_id, "orphan_grace_period_expired")
                        )

                for workflow_id, elapsed in self._state.get_stuck_workflows():
                    if workflow_id not in self._state._orphaned_workflows:
                        workflows_to_cancel.append(
                            (
                                workflow_id,
                                f"execution_timeout_exceeded ({elapsed:.1f}s)",
                            )
                        )

                for workflow_id, reason in workflows_to_cancel:
                    self._state._orphaned_workflows.pop(workflow_id, None)

                    if workflow_id not in self._state._active_workflows:
                        continue

                    if self._logger:
                        await self._logger.log(
                            ServerWarning(
                                message=f"Cancelling workflow {workflow_id[:8]}... - {reason}",
                                node_host=node_host,
                                node_port=node_port,
                                node_id=node_id_short,
                            )
                        )

                    success, errors = await cancel_workflow(workflow_id, reason)

                    if not success or errors:
                        if self._logger:
                            await self._logger.log(
                                ServerError(
                                    message=f"Error cancelling orphaned workflow {workflow_id[:8]}...: {errors}",
                                    node_host=node_host,
                                    node_port=node_port,
                                    node_id=node_id_short,
                                )
                            )

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def run_discovery_maintenance_loop(
        self,
        is_running: callable,
    ) -> None:
        """
        Maintain discovery service state (AD-28).

        Periodically:
        - Decays failure counts to allow recovery
        - Cleans up expired DNS cache entries
        - Discovers new peers via DNS if configured

        Args:
            is_running: Function to check if worker is running
        """
        self._running = True
        while is_running() and self._running:
            try:
                await asyncio.sleep(self._discovery_failure_decay_interval)

                # Decay failure counts
                self._discovery_service.decay_failures()

                # Clean up expired DNS cache
                self._discovery_service.cleanup_expired_dns()

                # Discover new peers via DNS if configured
                if self._discovery_service.config.dns_names:
                    await self._discovery_service.discover_peers()

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def run_progress_flush_loop(
        self,
        send_progress_to_job_leader: callable,
        aggregate_progress_by_job: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
        is_running: callable,
        get_healthy_managers: callable,
    ) -> None:
        """
        Flush buffered progress updates to managers (AD-37).

        Respects backpressure signals:
        - NONE: Flush all updates immediately
        - THROTTLE: Add delay between flushes
        - BATCH: Aggregate by job, send fewer updates
        - REJECT: Drop non-critical updates entirely

        Args:
            send_progress_to_job_leader: Function to send progress to job leader
            aggregate_progress_by_job: Function to aggregate progress by job
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            is_running: Function to check if worker is running
            get_healthy_managers: Function to get healthy manager IDs
        """
        self._running = True
        while is_running() and self._running:
            try:
                # Calculate effective flush interval based on backpressure
                effective_interval = self._progress_flush_interval
                if self._backpressure_manager:
                    delay_ms = self._backpressure_manager.get_backpressure_delay_ms()
                    if delay_ms > 0:
                        effective_interval += delay_ms / 1000.0

                await asyncio.sleep(effective_interval)

                # Check backpressure level
                if self._backpressure_manager:
                    # REJECT level: drop all updates
                    if self._backpressure_manager.should_reject_updates():
                        await self._state.clear_progress_buffer()
                        continue

                # Get and clear buffer atomically
                updates = await self._state.flush_progress_buffer()
                if not updates:
                    continue

                # BATCH level: aggregate by job
                if (
                    self._backpressure_manager
                    and self._backpressure_manager.should_batch_only()
                ):
                    updates = aggregate_progress_by_job(updates)

                # Send updates if we have healthy managers
                if get_healthy_managers():
                    for workflow_id, progress in updates.items():
                        await send_progress_to_job_leader(progress)

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def stop(self) -> None:
        """Stop all background loops."""
        self._running = False
