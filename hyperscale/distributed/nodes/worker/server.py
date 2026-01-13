"""
Worker server composition root.

Thin orchestration layer that wires all worker modules together.
All business logic is delegated to specialized modules.
"""

import asyncio
import time

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    psutil = None
    HAS_PSUTIL = False

from hyperscale.distributed.swim import HealthAwareServer, WorkerStateEmbedder
from hyperscale.distributed.env import Env
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    WorkerState as WorkerStateEnum,
    WorkerStateSnapshot,
    WorkflowProgress,
    WorkerHeartbeat,
)
from hyperscale.distributed.jobs import CoreAllocator
from hyperscale.distributed.resources import ProcessResourceMonitor
from hyperscale.distributed.protocol.version import (
    NodeCapabilities,
    NegotiatedCapabilities,
)
from hyperscale.distributed.server import tcp
from hyperscale.logging import Logger
from hyperscale.logging.config import DurabilityMode
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    WorkerExtensionRequested,
    WorkerHealthcheckReceived,
    WorkerStarted,
    WorkerStopping,
)

from .config import WorkerConfig
from .state import WorkerState
from .registry import WorkerRegistry
from .execution import WorkerExecutor
from .sync import WorkerStateSync
from .health import WorkerHealthIntegration
from .backpressure import WorkerBackpressureManager
from .discovery import WorkerDiscoveryManager
from .lifecycle import WorkerLifecycleManager
from .registration import WorkerRegistrationHandler
from .heartbeat import WorkerHeartbeatHandler
from .progress import WorkerProgressReporter
from .workflow_executor import WorkerWorkflowExecutor
from .cancellation import WorkerCancellationHandler
from .background_loops import WorkerBackgroundLoops
from .handlers import (
    WorkflowDispatchHandler,
    WorkflowCancelHandler,
    JobLeaderTransferHandler,
    WorkflowProgressHandler,
    StateSyncHandler,
)


class WorkerServer(HealthAwareServer):
    """
    Worker node composition root.

    Wires all worker modules together and delegates to them.
    Inherits networking from HealthAwareServer.
    """

    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "default",
        seed_managers: list[tuple[str, int]] | None = None,
    ) -> None:
        """
        Initialize worker server.

        Args:
            host: Host address to bind
            tcp_port: TCP port for data operations
            udp_port: UDP port for SWIM healthchecks
            env: Environment configuration
            dc_id: Datacenter identifier
            seed_managers: Initial manager addresses for registration
        """
        # Build config from env
        self._config = WorkerConfig.from_env(env, host, tcp_port, udp_port, dc_id)
        self._env = env
        self._seed_managers = seed_managers or []

        # Core capacity
        self._total_cores = self._config.total_cores
        self._core_allocator = CoreAllocator(self._total_cores)

        # Centralized runtime state (single source of truth)
        self._worker_state = WorkerState(self._core_allocator)

        self._resource_monitor = ProcessResourceMonitor()

        # Initialize modules (will be fully wired after super().__init__)
        self._registry = WorkerRegistry(
            logger=None,
            recovery_jitter_min=env.RECOVERY_JITTER_MIN,
            recovery_jitter_max=env.RECOVERY_JITTER_MAX,
            recovery_semaphore_size=env.RECOVERY_SEMAPHORE_SIZE,
        )

        self._backpressure_manager = WorkerBackpressureManager(
            state=self._worker_state,
            logger=None,
            registry=self._registry,
            throttle_delay_ms=env.WORKER_BACKPRESSURE_THROTTLE_DELAY_MS,
            batch_delay_ms=env.WORKER_BACKPRESSURE_BATCH_DELAY_MS,
            reject_delay_ms=env.WORKER_BACKPRESSURE_REJECT_DELAY_MS,
        )

        self._executor = WorkerExecutor(
            core_allocator=self._core_allocator,
            logger=None,
            state=self._worker_state,
            progress_update_interval=self._config.progress_update_interval,
            progress_flush_interval=self._config.progress_flush_interval,
            backpressure_manager=self._backpressure_manager,
        )

        self._state_sync = WorkerStateSync()

        self._health_integration = WorkerHealthIntegration(
            registry=self._registry,
            backpressure_manager=self._backpressure_manager,
            logger=None,
        )

        # AD-28: Enhanced DNS Discovery
        static_seeds = [f"{host}:{port}" for host, port in self._seed_managers]
        discovery_config = env.get_discovery_config(
            node_role="worker",
            static_seeds=static_seeds,
        )
        self._discovery_service = DiscoveryService(discovery_config)

        self._discovery_manager = WorkerDiscoveryManager(
            discovery_service=self._discovery_service,
            logger=None,
        )

        # New modular components
        self._lifecycle_manager = WorkerLifecycleManager(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            total_cores=self._total_cores,
            env=env,
            logger=None,
        )

        # Initialize after we have discovery service
        self._registration_handler: WorkerRegistrationHandler | None = None
        self._heartbeat_handler: WorkerHeartbeatHandler | None = None
        self._progress_reporter: WorkerProgressReporter | None = None
        self._workflow_executor: WorkerWorkflowExecutor | None = None
        self._cancellation_handler_impl: WorkerCancellationHandler | None = None
        self._background_loops: WorkerBackgroundLoops | None = None

        # Runtime state (delegate to _worker_state)
        self._active_workflows: dict[str, WorkflowProgress] = (
            self._worker_state._active_workflows
        )
        self._workflow_tokens: dict[str, str] = self._worker_state._workflow_tokens
        self._workflow_cancel_events: dict[str, asyncio.Event] = (
            self._worker_state._workflow_cancel_events
        )
        self._workflow_job_leader: dict[str, tuple[str, int]] = (
            self._worker_state._workflow_job_leader
        )
        self._workflow_fence_tokens: dict[str, int] = (
            self._worker_state._workflow_fence_tokens
        )
        self._pending_workflows: list = self._worker_state._pending_workflows
        self._orphaned_workflows: dict[str, float] = (
            self._worker_state._orphaned_workflows
        )

        # Section 8: Job leadership transfer (delegate to state)
        self._job_leader_transfer_locks: dict[str, asyncio.Lock] = (
            self._worker_state._job_leader_transfer_locks
        )
        self._job_fence_tokens: dict[str, int] = self._worker_state._job_fence_tokens
        self._pending_transfers: dict = self._worker_state._pending_transfers

        # Negotiated capabilities (AD-25)
        self._negotiated_capabilities: NegotiatedCapabilities | None = None
        self._node_capabilities = NodeCapabilities.current(node_version="")

        # Background tasks
        self._progress_flush_task: asyncio.Task | None = None
        self._dead_manager_reap_task: asyncio.Task | None = None
        self._cancellation_poll_task: asyncio.Task | None = None
        self._orphan_check_task: asyncio.Task | None = None
        self._discovery_maintenance_task: asyncio.Task | None = None
        self._overload_poll_task: asyncio.Task | None = None

        # Debounced cores notification (AD-38 fix: single in-flight task, coalesced updates)
        self._pending_cores_notification: int | None = None
        self._cores_notification_task: asyncio.Task | None = None

        # Event logger for crash forensics (AD-47)
        self._event_logger: Logger | None = None

        # Create state embedder for SWIM
        state_embedder = WorkerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_worker_state=lambda: self._get_worker_state().value,
            get_available_cores=lambda: self._core_allocator.available_cores,
            get_queue_depth=lambda: len(self._pending_workflows),
            get_cpu_percent=self._get_cpu_percent,
            get_memory_percent=self._get_memory_percent,
            get_state_version=lambda: self._state_sync.state_version,
            get_active_workflows=lambda: {
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
            on_manager_heartbeat=self._handle_manager_heartbeat,
            get_tcp_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_health_accepting_work=lambda: self._get_worker_state()
            in (WorkerStateEnum.HEALTHY, WorkerStateEnum.DEGRADED),
            get_health_throughput=self._executor.get_throughput,
            get_health_expected_throughput=self._executor.get_expected_throughput,
            get_health_overload_state=self._backpressure_manager.get_overload_state_str,
            get_extension_requested=lambda: self._worker_state._extension_requested,
            get_extension_reason=lambda: self._worker_state._extension_reason,
            get_extension_current_progress=lambda: self._worker_state._extension_current_progress,
            get_extension_completed_items=lambda: self._worker_state._extension_completed_items,
            get_extension_total_items=lambda: self._worker_state._extension_total_items,
            get_extension_estimated_completion=lambda: self._worker_state._extension_estimated_completion,
            get_extension_active_workflow_count=lambda: len(self._active_workflows),
        )

        # Initialize parent HealthAwareServer
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
            node_role="worker",
            state_embedder=state_embedder,
        )

        # Initialize components that need discovery service
        self._registration_handler = WorkerRegistrationHandler(
            registry=self._registry,
            discovery_service=self._discovery_service,
            logger=self._udp_logger,
            node_capabilities=self._node_capabilities,
        )

        self._heartbeat_handler = WorkerHeartbeatHandler(
            registry=self._registry,
            logger=self._udp_logger,
        )

        self._progress_reporter = WorkerProgressReporter(
            registry=self._registry,
            state=self._worker_state,
            logger=self._udp_logger,
        )

        self._workflow_executor = WorkerWorkflowExecutor(
            core_allocator=self._core_allocator,
            state=self._worker_state,
            lifecycle=self._lifecycle_manager,
            backpressure_manager=self._backpressure_manager,
            env=env,
            logger=self._udp_logger,
        )

        self._cancellation_handler_impl = WorkerCancellationHandler(
            state=self._worker_state,
            logger=self._udp_logger,
            poll_interval=self._config.cancellation_poll_interval_seconds,
        )

        self._background_loops = WorkerBackgroundLoops(
            registry=self._registry,
            state=self._worker_state,
            discovery_service=self._discovery_service,
            logger=self._udp_logger,
            backpressure_manager=self._backpressure_manager,
        )

        # Configure background loops
        self._background_loops.configure(
            dead_manager_reap_interval=self._config.dead_manager_reap_interval_seconds,
            dead_manager_check_interval=self._config.dead_manager_check_interval_seconds,
            orphan_grace_period=self._config.orphan_grace_period_seconds,
            orphan_check_interval=self._config.orphan_check_interval_seconds,
            discovery_failure_decay_interval=self._config.discovery_failure_decay_interval_seconds,
            progress_flush_interval=self._config.progress_flush_interval_seconds,
        )

        # Wire logger to modules after parent init
        self._wire_logger_to_modules()

        # Set resource getters for backpressure
        self._backpressure_manager.set_resource_getters(
            self._get_cpu_percent,
            self._get_memory_percent,
        )

        # Register SWIM callbacks
        self.register_on_node_dead(self._health_integration.on_node_dead)
        self.register_on_node_join(self._health_integration.on_node_join)
        self._health_integration.set_failure_callback(self._on_manager_failure)
        self._health_integration.set_recovery_callback(self._on_manager_recovery)

        # AD-29: Register peer confirmation callback to activate managers only after
        # successful SWIM communication (probe/ack or heartbeat reception)
        self.register_on_peer_confirmed(self._on_peer_confirmed)

        # Set up heartbeat callbacks
        self._heartbeat_handler.set_callbacks(
            on_new_manager_discovered=self._on_new_manager_discovered,
            on_job_leadership_update=self._on_job_leadership_update,
        )

        # Initialize handlers
        self._dispatch_handler = WorkflowDispatchHandler(self)
        self._cancel_handler = WorkflowCancelHandler(self)
        self._transfer_handler = JobLeaderTransferHandler(self)
        self._progress_handler = WorkflowProgressHandler(self)
        self._sync_handler = StateSyncHandler(self)

    def _wire_logger_to_modules(self) -> None:
        """Wire logger to all modules after parent init."""
        self._registry._logger = self._udp_logger
        self._executor._logger = self._udp_logger
        self._backpressure_manager._logger = self._udp_logger
        self._health_integration._logger = self._udp_logger
        self._discovery_manager._logger = self._udp_logger
        self._lifecycle_manager._logger = self._udp_logger

    @property
    def node_info(self) -> NodeInfo:
        """Get this worker's node info."""
        return NodeInfo(
            node_id=self._node_id.full,
            role=NodeRole.WORKER.value,
            host=self._host,
            port=self._tcp_port,
            datacenter=self._node_id.datacenter,
            version=self._state_sync.state_version,
            udp_port=self._udp_port,
        )

    # =========================================================================
    # Module Accessors (for backward compatibility)
    # =========================================================================

    @property
    def _known_managers(self) -> dict[str, ManagerInfo]:
        """Backward compatibility - delegate to registry."""
        return self._registry._known_managers

    @property
    def _healthy_manager_ids(self) -> set[str]:
        """Backward compatibility - delegate to registry."""
        return self._registry._healthy_manager_ids

    @property
    def _primary_manager_id(self) -> str | None:
        """Backward compatibility - delegate to registry."""
        return self._registry._primary_manager_id

    @_primary_manager_id.setter
    def _primary_manager_id(self, value: str | None) -> None:
        """Backward compatibility - delegate to registry."""
        self._registry._primary_manager_id = value

    @property
    def _transfer_metrics_received(self) -> int:
        """Transfer metrics received - delegate to state."""
        return self._worker_state._transfer_metrics_received

    @property
    def _transfer_metrics_accepted(self) -> int:
        """Transfer metrics accepted - delegate to state."""
        return self._worker_state._transfer_metrics_accepted

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    async def start(self, timeout: float | None = None) -> None:
        """Start the worker server."""
        # Setup logging config
        self._lifecycle_manager.setup_logging_config()

        # Start parent server
        await super().start()

        if self._config.event_log_dir is not None:
            self._event_logger = Logger()
            self._event_logger.configure(
                name="worker_events",
                path=str(self._config.event_log_dir / "events.jsonl"),
                durability=DurabilityMode.FLUSH,
                log_format="json",
                retention_policy={
                    "max_size": "50MB",
                    "max_age": "24h",
                },
            )
            await self._event_logger.log(
                WorkerStarted(
                    message="Worker started",
                    node_id=self._node_id.full,
                    node_host=self._host,
                    node_port=self._tcp_port,
                    manager_host=self._seed_managers[0][0]
                    if self._seed_managers
                    else None,
                    manager_port=self._seed_managers[0][1]
                    if self._seed_managers
                    else None,
                ),
                name="worker_events",
            )

            self._workflow_executor.set_event_logger(self._event_logger)

        # Update node capabilities
        self._node_capabilities = self._lifecycle_manager.get_node_capabilities(
            self._node_id.full
        )
        self._registration_handler.set_node_capabilities(self._node_capabilities)

        # Start monitors
        await self._lifecycle_manager.start_monitors(
            self._node_id.datacenter,
            self._node_id.full,
        )

        # Setup server pool
        await self._lifecycle_manager.setup_server_pool()

        # Initialize remote manager
        remote_manager = await self._lifecycle_manager.initialize_remote_manager(
            self._updates_controller,
            self._config.progress_update_interval,
        )

        # Set remote manager for cancellation
        self._cancellation_handler_impl.set_remote_manager(remote_manager)

        # Start remote manager
        await self._lifecycle_manager.start_remote_manager()

        # Run worker pool
        await self._lifecycle_manager.run_worker_pool()

        # Connect to workers
        await self._lifecycle_manager.connect_to_workers(timeout)

        # Set core availability callback
        self._lifecycle_manager.set_on_cores_available(self._on_cores_available)

        # Register with all seed managers
        for manager_addr in self._seed_managers:
            await self._register_with_manager(manager_addr)

        # Join SWIM cluster with all known managers for healthchecks
        for manager_info in list(self._registry._known_managers.values()):
            manager_udp_addr = (manager_info.udp_host, manager_info.udp_port)
            self.join([manager_udp_addr])

        # Start SWIM probe cycle
        self.start_probe_cycle()

        # Start background loops
        await self._start_background_loops()

        await self._udp_logger.log(
            ServerInfo(
                message=f"Worker started with {self._total_cores} cores",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def stop(
        self, drain_timeout: float = 5, broadcast_leave: bool = True
    ) -> None:
        """Stop the worker server gracefully."""
        self._running = False

        await self._log_worker_stopping()
        await self._stop_background_loops()
        await self._cancel_cores_notification_task()
        self._stop_modules()
        await self._cancel_all_active_workflows()
        await self._shutdown_lifecycle_components()
        await super().stop(drain_timeout, broadcast_leave)

        await self._udp_logger.log(
            ServerInfo(
                message="Worker stopped",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _log_worker_stopping(self) -> None:
        if self._event_logger is None:
            return
        await self._event_logger.log(
            WorkerStopping(
                message="Worker stopping",
                node_id=self._node_id.full,
                node_host=self._host,
                node_port=self._tcp_port,
                reason="graceful_shutdown",
            ),
            name="worker_events",
        )
        await self._event_logger.close()

    async def _cancel_cores_notification_task(self) -> None:
        if not self._cores_notification_task or self._cores_notification_task.done():
            return
        self._cores_notification_task.cancel()
        try:
            await self._cores_notification_task
        except asyncio.CancelledError:
            pass

    def _stop_modules(self) -> None:
        self._backpressure_manager.stop()
        self._executor.stop()
        if self._cancellation_handler_impl:
            self._cancellation_handler_impl.stop()
        if self._background_loops:
            self._background_loops.stop()

    async def _cancel_all_active_workflows(self) -> None:
        for workflow_id in list(self._workflow_tokens.keys()):
            await self._cancel_workflow(workflow_id, "server_shutdown")

    async def _shutdown_lifecycle_components(self) -> None:
        await self._lifecycle_manager.shutdown_remote_manager()
        await self._lifecycle_manager.stop_monitors(
            self._node_id.datacenter,
            self._node_id.full,
        )
        await self._lifecycle_manager.shutdown_server_pool()
        await self._lifecycle_manager.kill_child_processes()

    def abort(self):
        """Abort the worker server immediately."""
        self._running = False

        # Cancel background tasks synchronously
        self._lifecycle_manager.cancel_background_tasks_sync()

        if self._cores_notification_task and not self._cores_notification_task.done():
            self._cores_notification_task.cancel()

        # Abort modules
        self._lifecycle_manager.abort_monitors()
        self._lifecycle_manager.abort_remote_manager()
        self._lifecycle_manager.abort_server_pool()

        # Abort parent server
        super().abort()

    async def _start_background_loops(self) -> None:
        self._progress_flush_task = self._create_background_task(
            self._background_loops.run_progress_flush_loop(
                send_progress_to_job_leader=self._send_progress_to_job_leader,
                aggregate_progress_by_job=self._aggregate_progress_by_job,
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                is_running=lambda: self._running,
                get_healthy_managers=lambda: self._registry._healthy_manager_ids,
            ),
            "progress_flush",
        )
        self._lifecycle_manager.add_background_task(self._progress_flush_task)

        self._dead_manager_reap_task = self._create_background_task(
            self._background_loops.run_dead_manager_reap_loop(
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                task_runner_run=self._task_runner.run,
                is_running=lambda: self._running,
            ),
            "dead_manager_reap",
        )
        self._lifecycle_manager.add_background_task(self._dead_manager_reap_task)

        self._cancellation_poll_task = self._create_background_task(
            self._cancellation_handler_impl.run_cancellation_poll_loop(
                get_manager_addr=self._registry.get_primary_manager_tcp_addr,
                is_circuit_open=lambda: (
                    self._registry.is_circuit_open(self._primary_manager_id)
                    if self._primary_manager_id
                    else False
                ),
                send_tcp=self.send_tcp,
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                task_runner_run=self._task_runner.run,
                is_running=lambda: self._running,
            ),
            "cancellation_poll",
        )
        self._lifecycle_manager.add_background_task(self._cancellation_poll_task)

        self._orphan_check_task = self._create_background_task(
            self._background_loops.run_orphan_check_loop(
                cancel_workflow=self._cancel_workflow,
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                is_running=lambda: self._running,
            ),
            "orphan_check",
        )
        self._lifecycle_manager.add_background_task(self._orphan_check_task)

        self._discovery_maintenance_task = self._create_background_task(
            self._background_loops.run_discovery_maintenance_loop(
                is_running=lambda: self._running,
            ),
            "discovery_maintenance",
        )
        self._lifecycle_manager.add_background_task(self._discovery_maintenance_task)

        self._overload_poll_task = self._create_background_task(
            self._backpressure_manager.run_overload_poll_loop(),
            "overload_poll",
        )
        self._lifecycle_manager.add_background_task(self._overload_poll_task)

        self._resource_sample_task = self._create_background_task(
            self._run_resource_sample_loop(),
            "resource_sample",
        )
        self._lifecycle_manager.add_background_task(self._resource_sample_task)

    async def _run_resource_sample_loop(self) -> None:
        while self._running:
            try:
                await self._resource_monitor.sample()
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1.0)

    async def _stop_background_loops(self) -> None:
        """Stop all background loops."""
        await self._lifecycle_manager.cancel_background_tasks()

    # =========================================================================
    # State Methods
    # =========================================================================

    def _get_worker_state(self) -> WorkerStateEnum:
        """Determine current worker state."""
        if not self._running:
            return WorkerStateEnum.OFFLINE
        if self._degradation.current_level.value >= 3:
            return WorkerStateEnum.DRAINING
        if self._degradation.current_level.value >= 2:
            return WorkerStateEnum.DEGRADED
        return WorkerStateEnum.HEALTHY

    async def _increment_version(self) -> int:
        return await self._state_sync.increment_version()

    def _get_state_snapshot(self) -> WorkerStateSnapshot:
        """Get a complete state snapshot."""
        return WorkerStateSnapshot(
            node_id=self._node_id.full,
            state=self._get_worker_state().value,
            total_cores=self._total_cores,
            available_cores=self._core_allocator.available_cores,
            version=self._state_sync.state_version,
            active_workflows=dict(self._active_workflows),
        )

    def _get_heartbeat(self) -> WorkerHeartbeat:
        """
        Build a WorkerHeartbeat with current state.

        This is the same data that gets embedded in SWIM messages via
        WorkerStateEmbedder, but available for other uses like diagnostics
        or explicit TCP status updates if needed.
        """
        return WorkerHeartbeat(
            node_id=self._node_id.full,
            state=self._get_worker_state().value,
            available_cores=self._core_allocator.available_cores,
            queue_depth=len(self._pending_workflows),
            cpu_percent=self._get_cpu_percent(),
            memory_percent=self._get_memory_percent(),
            version=self._state_sync.state_version,
            active_workflows={
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
            extension_requested=self._worker_state._extension_requested,
            extension_reason=self._worker_state._extension_reason,
            extension_current_progress=self._worker_state._extension_current_progress,
            extension_completed_items=self._worker_state._extension_completed_items,
            extension_total_items=self._worker_state._extension_total_items,
            extension_estimated_completion=self._worker_state._extension_estimated_completion,
            extension_active_workflow_count=len(self._active_workflows),
        )

    def request_extension(
        self,
        reason: str,
        progress: float = 0.0,
        completed_items: int = 0,
        total_items: int = 0,
        estimated_completion: float = 0.0,
    ) -> None:
        """
        Request a deadline extension via heartbeat piggyback (AD-26).

        This sets the extension request fields in the worker's heartbeat,
        which will be processed by the manager when the next heartbeat is
        received. This is more efficient than a separate TCP call for
        extension requests.

        AD-26 Issue 4: Supports absolute metrics (completed_items, total_items)
        which are preferred over relative progress for robustness.

        Args:
            reason: Human-readable reason for the extension request.
            progress: Monotonic progress value (not clamped to 0-1). Must strictly
                increase between extension requests for approval. Prefer completed_items.
            completed_items: Absolute count of completed items (preferred metric).
            total_items: Total items to complete.
            estimated_completion: Estimated seconds until workflow completion.
        """
        self._worker_state._extension_requested = True
        self._worker_state._extension_reason = reason
        self._worker_state._extension_current_progress = max(0.0, progress)
        self._worker_state._extension_completed_items = completed_items
        self._worker_state._extension_total_items = total_items
        self._worker_state._extension_estimated_completion = estimated_completion
        active_workflow_count = len(self._active_workflows)
        self._worker_state._extension_active_workflow_count = active_workflow_count

        if self._event_logger is not None:
            self._task_runner.run(
                self._event_logger.log,
                WorkerExtensionRequested(
                    message=f"Extension requested: {reason}",
                    node_id=self._node_id.full,
                    node_host=self._host,
                    node_port=self._tcp_port,
                    reason=reason,
                    estimated_completion_seconds=estimated_completion,
                    active_workflow_count=active_workflow_count,
                ),
                "worker_events",
            )

    def clear_extension_request(self) -> None:
        """
        Clear the extension request after it's been processed.

        Called when the worker completes its task or the manager has
        processed the extension request.
        """
        self._worker_state._extension_requested = False
        self._worker_state._extension_reason = ""
        self._worker_state._extension_current_progress = 0.0
        self._worker_state._extension_completed_items = 0
        self._worker_state._extension_total_items = 0
        self._worker_state._extension_estimated_completion = 0.0
        self._worker_state._extension_active_workflow_count = 0

    async def get_core_assignments(self) -> dict[int, str | None]:
        """Get a copy of the current core assignments."""
        return await self._core_allocator.get_core_assignments()

    # =========================================================================
    # Lock Helpers (Section 8)
    # =========================================================================

    async def _get_job_transfer_lock(self, job_id: str) -> asyncio.Lock:
        return await self._worker_state.get_or_create_job_transfer_lock(job_id)

    async def _validate_transfer_fence_token(
        self, job_id: str, new_fence_token: int
    ) -> tuple[bool, str]:
        current_token = await self._worker_state.get_job_fence_token(job_id)
        if new_fence_token <= current_token:
            return (
                False,
                f"Stale fence token: received {new_fence_token}, current {current_token}",
            )
        return (True, "")

    def _validate_transfer_manager(self, new_manager_id: str) -> tuple[bool, str]:
        """Validate that the new manager is known."""
        if new_manager_id not in self._registry._known_managers:
            return (False, f"Unknown manager: {new_manager_id} not in known managers")
        return (True, "")

    async def _check_pending_transfer_for_job(
        self, job_id: str, workflow_id: str
    ) -> None:
        """
        Check if there's a pending transfer for a job when a new workflow arrives (Section 8.3).

        Called after a workflow is dispatched to see if a leadership transfer
        arrived before the workflow did.
        """
        pending = self._pending_transfers.get(job_id)
        if pending is None:
            return

        if self._is_pending_transfer_expired(pending):
            del self._pending_transfers[job_id]
            return

        if workflow_id not in pending.workflow_ids:
            return

        await self._apply_pending_transfer(job_id, workflow_id, pending)
        self._cleanup_pending_transfer_if_complete(job_id, workflow_id, pending)

    def _is_pending_transfer_expired(self, pending) -> bool:
        current_time = time.monotonic()
        pending_transfer_ttl = self._config.pending_transfer_ttl_seconds
        return current_time - pending.received_at > pending_transfer_ttl

    async def _apply_pending_transfer(
        self, job_id: str, workflow_id: str, pending
    ) -> None:
        job_lock = await self._get_job_transfer_lock(job_id)
        async with job_lock:
            self._workflow_job_leader[workflow_id] = pending.new_manager_addr
            self._job_fence_tokens[job_id] = pending.fence_token

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Applied pending transfer for workflow {workflow_id[:8]}... to job {job_id[:8]}...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    def _cleanup_pending_transfer_if_complete(
        self, job_id: str, workflow_id: str, pending
    ) -> None:
        remaining_workflows = [
            wf_id
            for wf_id in pending.workflow_ids
            if wf_id not in self._active_workflows and wf_id != workflow_id
        ]
        if not remaining_workflows:
            del self._pending_transfers[job_id]

    # =========================================================================
    # Registration Methods
    # =========================================================================

    async def _register_with_manager(self, manager_addr: tuple[str, int]) -> bool:
        """Register this worker with a manager."""
        return await self._registration_handler.register_with_manager(
            manager_addr=manager_addr,
            node_info=self.node_info,
            total_cores=self._total_cores,
            available_cores=self._core_allocator.available_cores,
            memory_mb=self._get_memory_mb(),
            available_memory_mb=self._get_available_memory_mb(),
            cluster_id=self._env.MERCURY_SYNC_CLUSTER_ID,
            environment_id=self._env.MERCURY_SYNC_ENVIRONMENT_ID,
            send_func=self._send_registration,
        )

    async def _send_registration(
        self,
        manager_addr: tuple[str, int],
        data: bytes,
        timeout: float = 5.0,
    ) -> bytes | Exception:
        """Send registration data to manager."""
        try:
            response, _ = await self.send_tcp(
                manager_addr,
                "worker_registration",
                data,
                timeout=timeout,
            )
            return response
        except Exception as error:
            return error

    def _get_memory_mb(self) -> int:
        """Get total memory in MB."""
        if not HAS_PSUTIL:
            return 0
        return int(psutil.virtual_memory().total / (1024 * 1024))

    def _get_available_memory_mb(self) -> int:
        """Get available memory in MB."""
        if not HAS_PSUTIL:
            return 0
        return int(psutil.virtual_memory().available / (1024 * 1024))

    # =========================================================================
    # Callbacks
    # =========================================================================

    def _on_manager_failure(self, manager_id: str) -> None:
        """Handle manager failure callback."""
        self._task_runner.run(self._handle_manager_failure_async, manager_id)

    def _on_manager_recovery(self, manager_id: str) -> None:
        """Handle manager recovery callback."""
        self._task_runner.run(self._handle_manager_recovery_async, manager_id)

    async def _handle_manager_failure_async(self, manager_id: str) -> None:
        """Handle manager failure - mark workflows as orphaned."""
        await self._registry.mark_manager_unhealthy(manager_id)

        if self._primary_manager_id == manager_id:
            await self._registry.select_new_primary_manager()

        self._mark_manager_workflows_orphaned(manager_id)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager {manager_id[:8]}... failed, affected workflows marked as orphaned",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    def _mark_manager_workflows_orphaned(self, manager_id: str) -> None:
        manager_info = self._registry.get_manager(manager_id)
        if not manager_info:
            return

        manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
        for workflow_id, leader_addr in list(self._workflow_job_leader.items()):
            if leader_addr == manager_addr:
                self._worker_state.mark_workflow_orphaned(workflow_id)

    async def _handle_manager_recovery_async(self, manager_id: str) -> None:
        """Handle manager recovery - mark as healthy."""
        self._registry.mark_manager_healthy(manager_id)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager {manager_id[:8]}... recovered",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    def _on_peer_confirmed(self, peer: tuple[str, int]) -> None:
        """
        Add confirmed peer to active peer sets (AD-29).

        Called when a peer is confirmed via successful SWIM communication.
        This is the ONLY place where managers should be added to _healthy_manager_ids,
        ensuring failure detection only applies to managers we've communicated with.

        Args:
            peer: The UDP address of the confirmed peer (manager).
        """
        for manager_id, manager_info in self._registry._known_managers.items():
            if (manager_info.udp_host, manager_info.udp_port) == peer:
                self._registry._healthy_manager_ids.add(manager_id)
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"AD-29: Manager {manager_id[:8]}... confirmed via SWIM, added to healthy set",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
                break

    async def _handle_manager_heartbeat(
        self, heartbeat, source_addr: tuple[str, int]
    ) -> None:
        """Handle manager heartbeat from SWIM."""
        if self._event_logger is not None:
            await self._event_logger.log(
                WorkerHealthcheckReceived(
                    message=f"Healthcheck from {source_addr[0]}:{source_addr[1]}",
                    node_id=self._node_id.full,
                    node_host=self._host,
                    node_port=self._tcp_port,
                    source_host=source_addr[0],
                    source_port=source_addr[1],
                ),
                name="worker_events",
            )

        self._heartbeat_handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=source_addr,
            confirm_peer=self.confirm_peer,
            node_host=self._host,
            node_port=self._tcp_port,
            node_id_short=self._node_id.short,
            task_runner_run=self._task_runner.run,
        )

    def _on_new_manager_discovered(self, manager_addr: tuple[str, int]) -> None:
        """Handle discovery of new manager via heartbeat."""
        self._task_runner.run(self._register_with_manager, manager_addr)

    def _on_job_leadership_update(
        self,
        job_leaderships: list[str],
        manager_addr: tuple[str, int],
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
    ) -> None:
        """Handle job leadership claims from heartbeat."""
        # Check each active workflow to see if this manager leads its job
        for workflow_id, progress in list(self._active_workflows.items()):
            job_id = progress.job_id
            if job_id in job_leaderships:
                current_leader = self._workflow_job_leader.get(workflow_id)
                if current_leader != manager_addr:
                    self._workflow_job_leader[workflow_id] = manager_addr
                    self._worker_state.clear_workflow_orphaned(workflow_id)
                    task_runner_run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"Job leader update via SWIM: workflow {workflow_id[:8]}... "
                            f"job {job_id[:8]}... -> {manager_addr}",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        ),
                    )

    def _on_cores_available(self, available_cores: int) -> None:
        """Handle cores becoming available - notify manager (debounced)."""
        if not self._should_notify_cores_available(available_cores):
            return

        self._pending_cores_notification = available_cores
        self._ensure_cores_notification_task_running()

    def _should_notify_cores_available(self, available_cores: int) -> bool:
        return self._running and available_cores > 0

    def _ensure_cores_notification_task_running(self) -> None:
        task_not_running = (
            self._cores_notification_task is None
            or self._cores_notification_task.done()
        )
        if task_not_running:
            self._cores_notification_task = self._create_background_task(
                self._flush_cores_notification(), "cores_notification"
            )

    async def _flush_cores_notification(self) -> None:
        """Send pending cores notifications to manager, coalescing rapid updates."""
        while self._pending_cores_notification is not None and self._running:
            cores_to_send = self._pending_cores_notification
            self._pending_cores_notification = None

            await self._notify_manager_cores_available(cores_to_send)

    async def _notify_manager_cores_available(self, available_cores: int) -> None:
        """Send core availability notification to manager."""
        manager_addr = self._registry.get_primary_manager_tcp_addr()
        if not manager_addr:
            return

        try:
            heartbeat = self._get_heartbeat()
            await self.send_tcp(
                manager_addr,
                "worker_heartbeat",
                heartbeat.dump(),
                timeout=1.0,
            )
        except Exception as error:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Failed to notify manager of core availability: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    # =========================================================================
    # Dispatch Execution
    # =========================================================================

    async def _handle_dispatch_execution(
        self, dispatch, addr: tuple[str, int], allocation_result
    ) -> bytes:
        """Handle the execution phase of a workflow dispatch."""
        result = await self._workflow_executor.handle_dispatch_execution(
            dispatch=dispatch,
            dispatching_addr=addr,
            allocated_cores=allocation_result.allocated_cores,
            task_runner_run=self._task_runner.run,
            increment_version=self._increment_version,
            node_id_full=self._node_id.full,
            node_host=self._host,
            node_port=self._tcp_port,
        )

        # Section 8.3: Check for pending transfers that arrived before this dispatch
        await self._check_pending_transfer_for_job(
            dispatch.job_id, dispatch.workflow_id
        )

        return result

    def _cleanup_workflow_state(self, workflow_id: str) -> None:
        """Cleanup workflow state on failure."""
        self._worker_state.remove_active_workflow(workflow_id)

    # =========================================================================
    # Cancellation
    # =========================================================================

    async def _cancel_workflow(
        self, workflow_id: str, reason: str
    ) -> tuple[bool, list[str]]:
        """Cancel a workflow and clean up resources."""
        success, errors = await self._cancellation_handler_impl.cancel_workflow(
            workflow_id=workflow_id,
            reason=reason,
            task_runner_cancel=self._task_runner.cancel,
            increment_version=self._increment_version,
        )

        # Push cancellation complete to manager (fire-and-forget via task runner)
        progress = self._active_workflows.get(workflow_id)
        if progress and progress.job_id:
            self._task_runner.run(
                self._progress_reporter.send_cancellation_complete,
                progress.job_id,
                workflow_id,
                success,
                errors,
                time.time(),
                self._node_id.full,
                self.send_tcp,
                self._host,
                self._tcp_port,
                self._node_id.short,
            )

        return (success, errors)

    async def get_workflows_on_cores(self, core_indices: list[int]) -> set[str]:
        """Get workflows running on specific cores."""
        return await self._core_allocator.get_workflows_on_cores(core_indices)

    async def stop_workflows_on_cores(
        self,
        core_indices: list[int],
        reason: str = "core_stop",
    ) -> list[str]:
        """Stop all workflows running on specific cores (hierarchical stop)."""
        workflows = await self.get_workflows_on_cores(core_indices)
        stopped = []

        for workflow_id in workflows:
            success, _ = await self._cancel_workflow(workflow_id, reason)
            if success:
                stopped.append(workflow_id)

        return stopped

    # =========================================================================
    # Progress Reporting
    # =========================================================================

    async def _send_progress_to_job_leader(self, progress: WorkflowProgress) -> bool:
        """Send progress update to job leader."""
        return await self._progress_reporter.send_progress_to_job_leader(
            progress=progress,
            send_tcp=self.send_tcp,
            node_host=self._host,
            node_port=self._tcp_port,
            node_id_short=self._node_id.short,
        )

    def _aggregate_progress_by_job(
        self, updates: dict[str, WorkflowProgress]
    ) -> dict[str, WorkflowProgress]:
        """Aggregate progress updates by job for BATCH mode."""
        if not updates:
            return updates

        by_job: dict[str, list[WorkflowProgress]] = {}
        for workflow_id, progress in updates.items():
            job_id = progress.job_id
            if job_id not in by_job:
                by_job[job_id] = []
            by_job[job_id].append(progress)

        aggregated: dict[str, WorkflowProgress] = {}
        for job_id, job_updates in by_job.items():
            if len(job_updates) == 1:
                aggregated[job_updates[0].workflow_id] = job_updates[0]
            else:
                best_update = max(job_updates, key=lambda p: p.completed_count)
                aggregated[best_update.workflow_id] = best_update

        return aggregated

    async def _report_active_workflows_to_managers(self) -> None:
        """Report all active workflows to all healthy managers."""
        if not self._registry._healthy_manager_ids:
            return

        for workflow_id, progress in list(self._active_workflows.items()):
            try:
                await self._progress_reporter.send_progress_to_all_managers(
                    progress=progress,
                    send_tcp=self.send_tcp,
                )
            except Exception:
                pass

    # =========================================================================
    # Environment Property (for tcp_dispatch.py)
    # =========================================================================

    @property
    def env(self) -> Env:
        """Get the environment configuration."""
        return self._env

    # =========================================================================
    # State Version Property (for tcp_state_sync.py)
    # =========================================================================

    @property
    def _state_version(self) -> int:
        """Get current state version - delegate to state sync."""
        return self._state_sync.state_version

    # =========================================================================
    # Resource Helpers
    # =========================================================================

    def _get_cpu_percent(self) -> float:
        """Get CPU utilization percentage from Kalman-filtered monitor."""
        metrics = self._resource_monitor.get_last_metrics()
        if metrics is not None:
            return metrics.cpu_percent
        return 0.0

    def _get_memory_percent(self) -> float:
        """Get memory utilization percentage from Kalman-filtered monitor."""
        metrics = self._resource_monitor.get_last_metrics()
        if metrics is not None:
            return metrics.memory_percent
        return 0.0

    # =========================================================================
    # TCP Handlers - Delegate to handler classes
    # =========================================================================

    @tcp.receive()
    async def workflow_dispatch(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Handle workflow dispatch request."""
        return await self._dispatch_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def cancel_workflow(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Handle workflow cancellation request."""
        return await self._cancel_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def job_leader_worker_transfer(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Handle job leadership transfer notification."""
        return await self._transfer_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def state_sync_request(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Handle state sync request."""
        return await self._sync_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def workflow_status_query(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Handle workflow status query."""
        active_ids = list(self._active_workflows.keys())
        return ",".join(active_ids).encode("utf-8")

    @tcp.handle("manager_register")
    async def handle_manager_register(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """
        Handle registration request from a manager.

        This enables bidirectional registration: managers can proactively
        register with workers they discover via state sync from peer managers.
        This speeds up cluster formation.
        """
        return self._registration_handler.process_manager_registration(
            data=data,
            node_id_full=self._node_id.full,
            total_cores=self._total_cores,
            available_cores=self._core_allocator.available_cores,
            add_unconfirmed_peer=self.add_unconfirmed_peer,
            add_to_probe_scheduler=self.add_to_probe_scheduler,
        )

    @tcp.handle("worker_register")
    async def handle_worker_register(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """
        Handle registration response from manager - populate known managers.

        This handler processes RegistrationResponse when managers push registration
        acknowledgments to workers.
        """
        accepted, primary_manager_id = (
            self._registration_handler.process_registration_response(
                data=data,
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                add_unconfirmed_peer=self.add_unconfirmed_peer,
                add_to_probe_scheduler=self.add_to_probe_scheduler,
            )
        )

        if accepted and primary_manager_id:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Registration accepted, primary manager: {primary_manager_id[:8]}...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

        return data


__all__ = ["WorkerServer"]
