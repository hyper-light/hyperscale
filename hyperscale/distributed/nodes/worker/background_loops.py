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
        while self._should_run(is_running):
            if not await self._dead_manager_reap_iteration(
                node_host,
                node_port,
                node_id_short,
                task_runner_run,
                is_seed_manager,
            ):
                break

    def _should_run(self, is_running: Callable[[], bool]) -> bool:
        """Whether the worker and these loops are both still running."""
        return is_running() and self._running

    async def _dead_manager_reap_iteration(
        self,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: RunTask,
        is_seed_manager: Callable[[str], bool] | None,
    ) -> bool:
        """One dead-manager reap pass; False once cancelled, an error logged and the loop kept."""
        try:
            await self._reap_dead_managers(
                node_host,
                node_port,
                node_id_short,
                task_runner_run,
                is_seed_manager,
            )
            return True
        except asyncio.CancelledError:
            return False
        except Exception as error:
            self._schedule_loop_warning(
                task_runner_run,
                f"Error in dead_manager_reap_loop: {error}",
                node_host,
                node_port,
                node_id_short,
            )
            return True

    def _schedule_loop_warning(
        self,
        task_runner_run: RunTask,
        message: str,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Log a loop warning through the task runner, when a logger is set."""
        if self._logger:
            task_runner_run(
                self._logger.log,
                ServerWarning(
                    message=message,
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                ),
            )

    async def _reap_dead_managers(
        self,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: RunTask,
        is_seed_manager: Callable[[str], bool] | None,
    ) -> None:
        """After the check interval, forget every non-seed manager unhealthy past the reap interval (AD-28)."""
        await _DEFAULT_CLOCK.sleep(self._dead_manager_check_interval)

        current_time = _DEFAULT_CLOCK.monotonic()
        managers_to_reap = self._managers_due_for_reaping(current_time, is_seed_manager)

        for manager_id in managers_to_reap:
            self._reap_manager(manager_id, node_host, node_port, node_id_short, task_runner_run)

    def _managers_due_for_reaping(
        self,
        current_time: float,
        is_seed_manager: Callable[[str], bool] | None,
    ) -> list[str]:
        """Managers unhealthy for the reap interval, seeds excepted."""
        managers_to_reap: list[str] = []

        for manager_id, unhealthy_since in list(
            self._registry._manager_unhealthy_since.items()
        ):
            if self._is_reapable(manager_id, unhealthy_since, current_time, is_seed_manager):
                managers_to_reap.append(manager_id)
        return managers_to_reap

    def _is_reapable(
        self,
        manager_id: str,
        unhealthy_since: float,
        current_time: float,
        is_seed_manager: Callable[[str], bool] | None,
    ) -> bool:
        """Whether a non-seed manager has been unhealthy for the reap interval."""
        return (
            current_time - unhealthy_since
            >= self._dead_manager_reap_interval
        ) and not self._is_exempt_seed(manager_id, is_seed_manager)

    @staticmethod
    def _is_exempt_seed(manager_id: str, is_seed_manager: Callable[[str], bool] | None) -> bool:
        """Whether ``manager_id`` is a configured seed, which is never reaped."""
        return is_seed_manager is not None and is_seed_manager(manager_id)

    def _reap_manager(
        self,
        manager_id: str,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: RunTask,
    ) -> None:
        """Forget a dead manager in the registry and discovery, and log it."""
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
        while self._should_run(is_running):
            if not await self._orphan_check_iteration(cancel_workflow, node_host, node_port, node_id_short):
                break

    async def _orphan_check_iteration(
        self,
        cancel_workflow: Callable[[str, str], Awaitable[tuple[bool, list[str]]]],
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> bool:
        """One orphan check pass; False once cancelled, an error logged and the loop kept."""
        try:
            await self._check_orphans(cancel_workflow, node_host, node_port, node_id_short)
            return True
        except asyncio.CancelledError:
            return False
        except Exception as error:
            await self._log_loop_warning(
                f"Error in orphan_check_loop: {error}",
                node_host,
                node_port,
                node_id_short,
            )
            return True

    async def _log_loop_warning(
        self,
        message: str,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Log a loop warning, when a logger is set."""
        if self._logger:
            await self._logger.log(
                ServerWarning(
                    message=message,
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                )
            )

    async def _check_orphans(
        self,
        cancel_workflow: Callable[[str, str], Awaitable[tuple[bool, list[str]]]],
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """After the check interval, cancel orphans past their grace and workflows past their timeout."""
        await _DEFAULT_CLOCK.sleep(self._orphan_check_interval)

        # Extensions of workflows no longer orphaned (their new
        # leader was found, or they ended) go with them.
        self._forget_rescued_orphan_extensions()

        workflows_to_cancel = self._expired_orphans()
        workflows_to_cancel.extend(self._stuck_workflows_to_cancel())

        for workflow_id, reason in workflows_to_cancel:
            await self._cancel_orphan(workflow_id, reason, cancel_workflow, node_host, node_port, node_id_short)

    def _forget_rescued_orphan_extensions(self) -> None:
        """Drop the AD-26 extensions of workflows no longer orphaned."""
        for extended_workflow_id in self._rescued_orphan_extension_ids():
            del self._orphan_extensions[extended_workflow_id]

    def _rescued_orphan_extension_ids(self) -> list[str]:
        """Extended workflows that are no longer orphaned."""
        return [
            workflow_id
            for workflow_id in self._orphan_extensions
            if workflow_id not in self._state._orphaned_workflows
        ]

    def _expired_orphans(self) -> list[tuple[str, str]]:
        """Orphaned workflows past their grace that earned no extension, with why each is cancelled."""
        workflows_to_cancel: list[tuple[str, str]] = []
        # The grace: the cluster's replacement of a dead leader
        # (derived), or the longest rescue seen here if longer.
        grace = max(self._orphan_grace_period, self._state.longest_orphan_rescue_seconds)
        now = _DEFAULT_CLOCK.monotonic()
        heartbeats = self._state.manager_heartbeats_received
        for workflow_id, orphan_timestamp in list(
            self._state._orphaned_workflows.items()
        ):
            if (
                reason := self._orphan_cancellation_reason(workflow_id, orphan_timestamp, grace, now, heartbeats)
            ) is not None:
                workflows_to_cancel.append((workflow_id, reason))
        return workflows_to_cancel

    def _orphan_cancellation_reason(
        self,
        workflow_id: str,
        orphan_timestamp: float,
        grace: float,
        now: float,
        heartbeats: int,
    ) -> str | None:
        """Why an orphan is cancelled now; None while its grace (plus extensions) runs or it is extended."""
        tracker = self._orphan_extensions.get(workflow_id)
        extended = self._orphan_extended_seconds(tracker)
        if now - orphan_timestamp < grace + extended:
            return None
        if self._extend_orphan_grace(workflow_id, tracker, grace, heartbeats):
            return None
        return (
            f"orphan_grace_period_expired (waited {now - orphan_timestamp:.1f}s; "
            f"grace {grace:.1f}s + {extended:.1f}s extended)"
        )

    @staticmethod
    def _orphan_extended_seconds(tracker: ExtensionTracker | None) -> float:
        """The seconds of AD-26 extension an orphan was granted."""
        return tracker.total_extended if tracker is not None else 0.0

    def _extend_orphan_grace(
        self,
        workflow_id: str,
        tracker: ExtensionTracker | None,
        grace: float,
        heartbeats: int,
    ) -> bool:
        """Whether an orphan earns a decaying AD-26 extension from managers still heartbeating."""
        # AD-26: managers still heartbeating this worker since
        # the last grant (or the orphaning) are a cluster that
        # can still take the job over -- extend, decaying. An
        # isolated worker has nothing to wait for.
        last_heartbeats = self._orphan_heartbeat_mark(workflow_id, tracker)
        if heartbeats > last_heartbeats:
            return self._grant_orphan_extension(workflow_id, tracker, grace, heartbeats)
        return False

    def _orphan_heartbeat_mark(self, workflow_id: str, tracker: ExtensionTracker | None) -> int:
        """Manager heartbeats counted at the orphan's last grant, else at its orphaning."""
        return (
            tracker.last_completed_items
            if tracker is not None and tracker.last_completed_items is not None
            else self._state.orphan_heartbeat_baseline(workflow_id)
        )

    def _grant_orphan_extension(
        self,
        workflow_id: str,
        tracker: ExtensionTracker | None,
        grace: float,
        heartbeats: int,
    ) -> bool:
        """Request an orphan extension, creating its tracker on first use; whether it was granted."""
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
        return granted

    def _stuck_workflows_to_cancel(self) -> list[tuple[str, str]]:
        """Workflows past their execution timeout (orphans excepted), with why each is cancelled."""
        return [
            (
                workflow_id,
                f"execution_timeout_exceeded ({elapsed:.1f}s)",
            )
            for workflow_id, elapsed in self._state.get_stuck_workflows()
            if workflow_id not in self._state._orphaned_workflows
        ]

    async def _cancel_orphan(
        self,
        workflow_id: str,
        reason: str,
        cancel_workflow: Callable[[str, str], Awaitable[tuple[bool, list[str]]]],
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Stop tracking a workflow as orphaned and cancel it while it is still active."""
        self._state.drop_orphan(workflow_id)
        self._orphan_extensions.pop(workflow_id, None)

        if workflow_id not in self._state._active_workflows:
            return

        await self._log_loop_warning(
            f"Cancelling workflow {workflow_id[:8]}... - {reason}",
            node_host,
            node_port,
            node_id_short,
        )

        success, errors = await cancel_workflow(workflow_id, reason)

        await self._report_orphan_cancel_result(workflow_id, success, errors, node_host, node_port, node_id_short)

    async def _report_orphan_cancel_result(
        self,
        workflow_id: str,
        success: bool,
        errors: list[str],
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Log an orphan cancellation that failed or reported errors."""
        if not success or errors:
            await self._log_orphan_cancel_failure(workflow_id, errors, node_host, node_port, node_id_short)

    async def _log_orphan_cancel_failure(
        self,
        workflow_id: str,
        errors: list[str],
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Log a failed orphan cancellation, when a logger is set."""
        if self._logger:
            await self._logger.log(
                ServerError(
                    message=f"Error cancelling orphaned workflow {workflow_id[:8]}...: {errors}",
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
        while self._should_run(is_running):
            if not await self._discovery_maintenance_iteration(register_with_manager):
                break

    async def _discovery_maintenance_iteration(
        self,
        register_with_manager: Callable[[tuple[str, int]], Awaitable[bool]],
    ) -> bool:
        """One discovery maintenance round; False once cancelled, an error logged and the loop kept."""
        try:
            await self._join_through_dns(register_with_manager)

            await _DEFAULT_CLOCK.sleep(self._discovery_failure_decay_interval)

            # Decay failure counts
            self._discovery_service.decay_failures()

            # Clean up expired DNS cache
            self._discovery_service.cleanup_expired_dns()
            return True

        except asyncio.CancelledError:
            return False
        except Exception as error:
            await self._log_loop_warning(
                f"Error in discovery_maintenance_loop: {error}",
                "worker",
                0,
                "worker",
            )
            return True

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

        await self._register_with_dns_peers(register_with_manager)

    async def _register_with_dns_peers(
        self,
        register_with_manager: Callable[[tuple[str, int]], Awaitable[bool]],
    ) -> None:
        """Register with every manager DNS discovery found, concurrently."""
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
        while self._should_run(is_running):
            if not await self._progress_flush_iteration(
                send_progress_to_job_leader,
                aggregate_progress_by_job,
                node_host,
                node_port,
                node_id_short,
                get_healthy_managers,
            ):
                break

    async def _progress_flush_iteration(
        self,
        send_progress_to_job_leader: Callable[[WorkflowProgress], Awaitable[bool]],
        aggregate_progress_by_job: Callable[[dict[str, WorkflowProgress]], dict[str, WorkflowProgress]],
        node_host: str,
        node_port: int,
        node_id_short: str,
        get_healthy_managers: Callable[[], set[str]],
    ) -> bool:
        """One progress flush (AD-37); False once cancelled, an error logged and the loop kept."""
        try:
            await self._flush_progress_once(
                send_progress_to_job_leader,
                aggregate_progress_by_job,
                get_healthy_managers,
            )
            return True
        except asyncio.CancelledError:
            return False
        except Exception as error:
            await self._log_loop_warning(
                f"Error in progress_flush_loop: {error}",
                node_host,
                node_port,
                node_id_short,
            )
            return True

    async def _flush_progress_once(
        self,
        send_progress_to_job_leader: Callable[[WorkflowProgress], Awaitable[bool]],
        aggregate_progress_by_job: Callable[[dict[str, WorkflowProgress]], dict[str, WorkflowProgress]],
        get_healthy_managers: Callable[[], set[str]],
    ) -> None:
        """Wait the backpressure-adjusted interval, then drop (REJECT) or send the buffered progress."""
        # Calculate effective flush interval based on backpressure
        effective_interval = self._effective_flush_interval()

        await _DEFAULT_CLOCK.sleep(effective_interval)

        # Check backpressure level
        if self._rejects_progress_updates():
            # REJECT level: drop all updates
            await self._state.clear_progress_buffer()
            return

        await self._send_flushed_progress(
            send_progress_to_job_leader,
            aggregate_progress_by_job,
            get_healthy_managers,
        )

    def _effective_flush_interval(self) -> float:
        """The flush interval plus any AD-23 backpressure delay (THROTTLE)."""
        effective_interval = self._progress_flush_interval
        if self._backpressure_manager:
            delay_ms = self._backpressure_manager.get_backpressure_delay_ms()
            if delay_ms > 0:
                effective_interval += delay_ms / 1000.0
        return effective_interval

    def _rejects_progress_updates(self) -> bool:
        """Whether backpressure is at the REJECT level."""
        return self._backpressure_manager and self._backpressure_manager.should_reject_updates()

    def _batches_progress_updates(self) -> bool:
        """Whether backpressure is at the BATCH level."""
        return (
            self._backpressure_manager
            and self._backpressure_manager.should_batch_only()
        )

    async def _send_flushed_progress(
        self,
        send_progress_to_job_leader: Callable[[WorkflowProgress], Awaitable[bool]],
        aggregate_progress_by_job: Callable[[dict[str, WorkflowProgress]], dict[str, WorkflowProgress]],
        get_healthy_managers: Callable[[], set[str]],
    ) -> None:
        """Take the progress buffer and deliver it."""
        # Get and clear buffer atomically
        updates = await self._state.flush_progress_buffer()
        if not updates:
            return

        await self._deliver_progress_updates(
            updates,
            send_progress_to_job_leader,
            aggregate_progress_by_job,
            get_healthy_managers,
        )

    async def _deliver_progress_updates(
        self,
        updates: dict[str, WorkflowProgress],
        send_progress_to_job_leader: Callable[[WorkflowProgress], Awaitable[bool]],
        aggregate_progress_by_job: Callable[[dict[str, WorkflowProgress]], dict[str, WorkflowProgress]],
        get_healthy_managers: Callable[[], set[str]],
    ) -> None:
        """Aggregate by job under BATCH backpressure, then send while a manager is healthy."""
        # BATCH level: aggregate by job
        if self._batches_progress_updates():
            updates = aggregate_progress_by_job(updates)

        # Send updates if we have healthy managers
        if get_healthy_managers():
            await self._send_progress_updates(updates, send_progress_to_job_leader)

    @staticmethod
    async def _send_progress_updates(
        updates: dict[str, WorkflowProgress],
        send_progress_to_job_leader: Callable[[WorkflowProgress], Awaitable[bool]],
    ) -> None:
        """Send each update to its job leader, in order."""
        for workflow_id, progress in updates.items():
            await send_progress_to_job_leader(progress)

    def stop(self) -> None:
        """Stop all background loops."""
        self._running = False
