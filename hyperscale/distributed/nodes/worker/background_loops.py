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
from typing import TYPE_CHECKING, Awaitable, Callable

from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerWarning,
    ServerError,
)

from hyperscale.distributed.health.extension_tracker import ExtensionTracker
from hyperscale.distributed.runtime import Clock, RealClock, RunTask
from hyperscale.distributed.models import WorkflowProgress


_DEFAULT_CLOCK: Clock = RealClock()

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
        self._registry: "WorkerRegistry" = registry
        self._state: "WorkerState" = state
        self._discovery_service: "DiscoveryService" = discovery_service
        self._logger: "Logger | None" = logger
        self._backpressure_manager: "WorkerBackpressureManager | None" = (
            backpressure_manager
        )
        self._running: bool = False

        # Loop intervals (can be overridden via config)
        self._dead_manager_reap_interval: float = 60.0
        self._dead_manager_check_interval: float = 10.0
        self._orphan_grace_period: float = 120.0
        self._orphan_check_interval: float = 10.0
        self._orphan_extension_min_grant: float = 1.0
        self._orphan_extension_max_extensions: int = 5
        # The AD-26 extensions each orphaned workflow was granted.
        self._orphan_extensions: dict[str, ExtensionTracker] = {}
        self._discovery_failure_decay_interval: float = 60.0
        self._progress_flush_interval: float = 0.5

    def configure(
        self,
        dead_manager_reap_interval: float = 60.0,
        dead_manager_check_interval: float = 10.0,
        orphan_grace_period: float = 120.0,
        orphan_check_interval: float = 10.0,
        discovery_failure_decay_interval: float = 60.0,
        progress_flush_interval: float = 0.5,
        orphan_extension_min_grant: float = 1.0,
        orphan_extension_max_extensions: int = 5,
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
        self._orphan_extension_min_grant = orphan_extension_min_grant
        self._orphan_extension_max_extensions = orphan_extension_max_extensions
        self._discovery_failure_decay_interval = discovery_failure_decay_interval
        self._progress_flush_interval = progress_flush_interval

    async def run_dead_manager_reap_loop(
        self,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: RunTask,
        is_running: Callable[[], bool],
        is_seed_manager: Callable[[str], bool] | None = None,
    ) -> None:
        """
        Reap managers that have been unhealthy for too long.

        Args:
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            task_runner_run: Function to run async tasks
            is_running: Function to check if worker is running
            is_seed_manager: ``manager_id -> bool`` predicate. Managers
                whose addresses match a configured seed are never reaped
                — reaping them strands the worker's bootstrap path: the
                SWIM ``_on_node_join`` recovery callback can no longer
                map the seed's address to a known manager_id when the
                seed rejoins, so the worker never re-registers. Pass
                ``None`` to disable the exemption (legacy callers /
                test scaffolds).
        """
        self._running = True
        while is_running() and self._running:
            try:
                await _DEFAULT_CLOCK.sleep(self._dead_manager_check_interval)

                current_time = _DEFAULT_CLOCK.monotonic()
                managers_to_reap: list[str] = []

                for manager_id, unhealthy_since in list(
                    self._registry._manager_unhealthy_since.items()
                ):
                    if (
                        current_time - unhealthy_since
                        >= self._dead_manager_reap_interval
                    ):
                        if is_seed_manager is not None and is_seed_manager(manager_id):
                            continue
                        managers_to_reap.append(manager_id)

                for manager_id in managers_to_reap:
                    manager_info = self._registry.get_manager(manager_id)
                    manager_addr = None
                    if manager_info:
                        manager_addr = (manager_info.tcp_host, manager_info.tcp_port)

                    self._registry.remove_manager_state(manager_id, manager_addr)
                    self._discovery_service.remove_peer(manager_id)

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
            except Exception as error:
                if self._logger:
                    task_runner_run(
                        self._logger.log,
                        ServerWarning(
                            message=f"Error in dead_manager_reap_loop: {error}",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        ),
                    )

    async def run_orphan_check_loop(
        self,
        cancel_workflow: Callable[[str, str], Awaitable[tuple[bool, list[str]]]],
        node_host: str,
        node_port: int,
        node_id_short: str,
        is_running: Callable[[], bool],
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
                await _DEFAULT_CLOCK.sleep(self._orphan_check_interval)

                workflows_to_cancel: list[tuple[str, str]] = []
                # Extensions of workflows no longer orphaned (their new
                # leader was found, or they ended) go with them.
                for extended_workflow_id in [
                    workflow_id
                    for workflow_id in self._orphan_extensions
                    if workflow_id not in self._state._orphaned_workflows
                ]:
                    del self._orphan_extensions[extended_workflow_id]

                # The grace: the cluster's replacement of a dead leader
                # (derived), or the longest rescue seen here if longer.
                grace = max(self._orphan_grace_period, self._state.longest_orphan_rescue_seconds)
                now = _DEFAULT_CLOCK.monotonic()
                heartbeats = self._state.manager_heartbeats_received
                for workflow_id, orphan_timestamp in list(
                    self._state._orphaned_workflows.items()
                ):
                    tracker = self._orphan_extensions.get(workflow_id)
                    extended = tracker.total_extended if tracker is not None else 0.0
                    if now - orphan_timestamp < grace + extended:
                        continue
                    # AD-26: managers still heartbeating this worker since
                    # the last grant (or the orphaning) are a cluster that
                    # can still take the job over -- extend, decaying. An
                    # isolated worker has nothing to wait for.
                    last_heartbeats = (
                        tracker.last_completed_items
                        if tracker is not None and tracker.last_completed_items is not None
                        else self._state.orphan_heartbeat_baseline(workflow_id)
                    )
                    if heartbeats > last_heartbeats:
                        if tracker is None:
                            tracker = ExtensionTracker(
                                worker_id=workflow_id,
                                base_deadline=grace,
                                min_grant=self._orphan_extension_min_grant,
                                max_extensions=self._orphan_extension_max_extensions,
                            )
                            self._orphan_extensions[workflow_id] = tracker
                        granted, _grant, _denial, _warning = tracker.request_extension(
                            "orphaned: awaiting the job's new leader",
                            current_progress=float(heartbeats),
                            completed_items=heartbeats,
                        )
                        if granted:
                            continue
                    workflows_to_cancel.append(
                        (
                            workflow_id,
                            f"orphan_grace_period_expired (waited {now - orphan_timestamp:.1f}s; "
                            f"grace {grace:.1f}s + {extended:.1f}s extended)",
                        )
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
                    self._state.drop_orphan(workflow_id)
                    self._orphan_extensions.pop(workflow_id, None)

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
            except Exception as error:
                if self._logger:
                    await self._logger.log(
                        ServerWarning(
                            message=f"Error in orphan_check_loop: {error}",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        )
                    )

    async def run_discovery_maintenance_loop(
        self,
        is_running: Callable[[], bool],
        register_with_manager: Callable[[tuple[str, int]], Awaitable[bool]],
    ) -> None:
        """
        Maintain discovery service state (AD-28).

        Each round, at once and then every decay interval:
        - Discovers managers via DNS if configured, and registers with them
          while this worker knows no healthy manager (any one answers with
          the whole manager cohort)
        - Decays failure counts to allow recovery
        - Cleans up expired DNS cache entries

        Args:
            is_running: Function to check if worker is running
            register_with_manager: Registers this worker with a manager
        """
        self._running = True
        while is_running() and self._running:
            try:
                await self._join_through_dns(register_with_manager)

                await _DEFAULT_CLOCK.sleep(self._discovery_failure_decay_interval)

                # Decay failure counts
                self._discovery_service.decay_failures()

                # Clean up expired DNS cache
                self._discovery_service.cleanup_expired_dns()

            except asyncio.CancelledError:
                break
            except Exception as error:
                if self._logger:
                    await self._logger.log(
                        ServerWarning(
                            message=f"Error in discovery_maintenance_loop: {error}",
                            node_host="worker",
                            node_port=0,
                            node_id="worker",
                        )
                    )

    async def _join_through_dns(
        self,
        register_with_manager: Callable[[tuple[str, int]], Awaitable[bool]],
    ) -> None:
        """Discover managers through the configured DNS names and, while
        this worker knows no healthy manager, register with every one."""
        if not self._discovery_service.config.dns_names:
            return

        await self._discovery_service.discover_peers()
        if self._registry.get_healthy_manager_tcp_addrs():
            return

        await asyncio.gather(
            *(
                register_with_manager(manager_addr)
                for manager_addr in self._discovery_service.get_dns_peer_addresses()
            )
        )

    async def run_progress_flush_loop(
        self,
        send_progress_to_job_leader: Callable[[WorkflowProgress], Awaitable[bool]],
        aggregate_progress_by_job: Callable[[dict[str, WorkflowProgress]], dict[str, WorkflowProgress]],
        node_host: str,
        node_port: int,
        node_id_short: str,
        is_running: Callable[[], bool],
        get_healthy_managers: Callable[[], set[str]],
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

                await _DEFAULT_CLOCK.sleep(effective_interval)

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
            except Exception as error:
                if self._logger:
                    await self._logger.log(
                        ServerWarning(
                            message=f"Error in progress_flush_loop: {error}",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        )
                    )

    def stop(self) -> None:
        """Stop all background loops."""
        self._running = False
