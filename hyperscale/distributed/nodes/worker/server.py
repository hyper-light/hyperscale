"""
Worker server composition root.

Thin orchestration layer that wires all worker modules together.
All business logic is delegated to specialized modules.
"""

import asyncio

from hyperscale.distributed.cluster import ClusterJoinError
from hyperscale.distributed.cluster.cluster_view_cache import ClusterViewCache
from hyperscale.distributed.cluster.cluster_watch_follower import ClusterWatchFollower
from hyperscale.distributed.swim import HealthAwareServer, WorkerStateEmbedder
from hyperscale.distributed.swim.health.graceful_degradation import DegradationLevel
from hyperscale.distributed.env import Env
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.models import (
    HealthcheckExtensionResponse,
    NodeInfo,
    NodeRole,
    ManagerInfo,
    ManagerHeartbeat,
    PendingTransfer,
    WorkerEvictionNotice,
    WorkerEvictionNoticeAck,
    WorkerState as WorkerStateEnum,
    WorkerStateSnapshot,
    WorkflowDispatch,
    WorkflowFinalResult,
    WorkflowProgress,
    WorkflowStatus,
    WorkerHeartbeat,
)
from hyperscale.distributed.jobs import AllocationResult, CoreAllocator
from hyperscale.distributed.resources import ProcessResourceMonitor
from hyperscale.distributed.resources.workflow_resource_tracker import WorkflowResourceTracker
from hyperscale.distributed.protocol.version import (
    NodeCapabilities,
    NegotiatedCapabilities,
)
from hyperscale.distributed.server import tcp
from hyperscale.distributed.runtime import (
    Clock,
    ProcessSpawner,
    Random,
    RealSystemResources,
    SystemResources,
    TransportFactory,
)

# Module-level machine-telemetry seam: borrowed; swap_defaults
# rebinds it under SIM so registration payloads read a constant
# machine instead of live psutil.
_DEFAULT_SYSTEM_RESOURCES: SystemResources = RealSystemResources()
from hyperscale.logging import Logger, LogLevel
from hyperscale.logging.config import DurabilityMode
from hyperscale.logging.hyperscale_logging_models import (
    ClusterWatchConnectivityChanged,
    ServerDebug,
    ServerInfo,
    ServerWarning,
    WorkerExtensionDecision,
    WorkerExtensionRequested,
    WorkerHealthcheckReceived,
    WorkerStarted,
    WorkerStopping,
)

from .config import WorkerConfig
from .extension_trigger import (
    ExtensionTrigger,
    ExtensionTriggerConfig,
)
from .models import WorkflowRuntimeState
from .state import WorkerState
from .registry import WorkerRegistry
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
    WorkflowThrottleHandler,
    CancelJobWorkflowsHandler,
    JobLeaderTransferHandler,
    StateSyncHandler,
)
from .cluster_connection import WorkerClusterConnection
from hyperscale.ui.interface_updates_controller import (InterfaceUpdatesController)
from collections.abc import Callable
from hyperscale.distributed.runtime import SendTcp, RunTask


SERVER_SHUTDOWN_CANCELLATION_REASON = "server_shutdown"


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
        total_cores: int | None = None,
        *,
        clock: Clock | None = None,
        random_source: Random | None = None,
        transport_factory: TransportFactory | None = None,
        process_spawner: ProcessSpawner | None = None,
        incarnation_storage_dir: str | None = None,
    ) -> None:
        """
        Initialize worker server.

        Args:
            host: Host address to bind
            tcp_port: TCP port for data operations
            udp_port: UDP port for SWIM healthchecks
            env: Environment configuration
            dc_id: Datacenter identifier
            seed_managers: Initial manager addresses for registration.
                Empty (the default) boots the worker standalone; it then
                joins a manager when an operator join request names one.
            total_cores: Executor core count. ``None`` defers to
                ``WORKER_MAX_CORES`` and then the physical core count.
        """
        # Build config from env
        self._config: WorkerConfig = WorkerConfig.from_env(
            env,
            host,
            tcp_port,
            udp_port,
            dc_id,
            total_cores=total_cores,
        )
        self._env: Env = env
        self._seed_managers: list[tuple[str, int]] = seed_managers or []

        # Core capacity
        self._total_cores: int = self._config.total_cores
        self._core_allocator: CoreAllocator = CoreAllocator(self._total_cores)

        # Centralized runtime state (single source of truth)
        self._worker_state: WorkerState = WorkerState(
            self._core_allocator,
            throughput_interval_seconds=self._config.throughput_interval_seconds,
            completion_times_max_samples=self._config.completion_times_max_samples,
        )
        # Manager UDP addr -> last SWIM incarnation observed. A jump of
        # at least the tracker's rejoin bump is the RESTART signature
        # (the manager's persisted incarnation store bumps on every
        # boot): the new generation has an empty worker registry, so we
        # must re-register even though we never declared it dead.
        self._manager_incarnations_seen: dict[tuple[str, int], int] = {}
        self._stopping: bool = False

        self._resource_monitor: ProcessResourceMonitor = ProcessResourceMonitor()

        # Initialize modules (will be fully wired after super().__init__)
        self._registry: WorkerRegistry = WorkerRegistry(
            logger=None,
            recovery_jitter_min=env.RECOVERY_JITTER_MIN,
            recovery_jitter_max=env.RECOVERY_JITTER_MAX,
            recovery_semaphore_size=env.RECOVERY_SEMAPHORE_SIZE,
            circuit_breaker_config=env.get_circuit_breaker_config(),
            # Resolved at call time: the discovery manager is built below.
            select_manager=lambda healthy_manager_ids: (
                self._discovery_manager.select_best_manager(
                    self._node_id.full,
                    healthy_manager_ids,
                )
            ),
        )

        self._backpressure_manager: WorkerBackpressureManager = (
            WorkerBackpressureManager(
                state=self._worker_state,
                logger=None,
                registry=self._registry,
                throttle_delay_ms=env.WORKER_BACKPRESSURE_THROTTLE_DELAY_MS,
                batch_delay_ms=env.WORKER_BACKPRESSURE_BATCH_DELAY_MS,
                reject_delay_ms=env.WORKER_BACKPRESSURE_REJECT_DELAY_MS,
            )
        )

        self._state_sync: WorkerStateSync = WorkerStateSync()

        self._health_integration: WorkerHealthIntegration = WorkerHealthIntegration(
            registry=self._registry,
            backpressure_manager=self._backpressure_manager,
            logger=None,
        )

        # AD-28: Enhanced DNS Discovery. A worker started without seed
        # managers is a valid, first-class topology: it boots standalone
        # and waits for an operator join request (``hyperscale join``)
        # naming the manager to register with. DiscoveryConfig refuses
        # an empty seed list unless dynamic registration is allowed, so
        # fall back to it in that case (the same solo-node pattern
        # ManagerDiscovery and GateServer use); managers learned at
        # runtime are added through ``DiscoveryService.add_peer``.
        static_seeds: list[str] = [
            f"{host}:{port}" for host, port in self._seed_managers
        ]
        discovery_config = env.get_discovery_config(
            node_role="worker",
            static_seeds=static_seeds,
            allow_dynamic_registration=not static_seeds,
        )
        self._discovery_service: DiscoveryService = DiscoveryService(discovery_config)

        self._discovery_manager: WorkerDiscoveryManager = WorkerDiscoveryManager(
            discovery_service=self._discovery_service,
            logger=None,
        )

        # New modular components. The Phase 6 SIM seams flow through the
        # lifecycle manager to the pool leader (``RemoteGraphManager`` ->
        # ``RemoteGraphController``) and the executor pool
        # (``LocalServerPool``): under SIM the leader transacts over the
        # simulation transport and the executors are spawned as
        # simulation-coordinator child processes; in REAL mode both are
        # ``None`` and nothing changes.
        self._lifecycle_manager: WorkerLifecycleManager = WorkerLifecycleManager(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            total_cores=self._total_cores,
            env=env,
            logger=None,
            transport_factory=transport_factory,
            process_spawner=process_spawner,
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
        self._pending_workflows: list[WorkflowDispatch] = (
            self._worker_state._pending_workflows
        )
        self._orphaned_workflows: dict[str, float] = (
            self._worker_state._orphaned_workflows
        )

        # Section 8: Job leadership transfer (delegate to state)
        self._job_leader_transfer_locks: dict[str, asyncio.Lock] = (
            self._worker_state._job_leader_transfer_locks
        )
        self._job_fence_tokens: dict[str, int] = self._worker_state._job_fence_tokens
        self._pending_transfers: dict[str, PendingTransfer] = (
            self._worker_state._pending_transfers
        )

        # Negotiated capabilities (AD-25)
        self._negotiated_capabilities: NegotiatedCapabilities | None = None
        self._node_capabilities: NodeCapabilities = NodeCapabilities.current(
            node_version=""
        )

        # Background tasks
        self._progress_flush_task: asyncio.Task | None = None
        self._dead_manager_reap_task: asyncio.Task | None = None
        self._cancellation_poll_task: asyncio.Task | None = None
        self._orphan_check_task: asyncio.Task | None = None
        self._discovery_maintenance_task: asyncio.Task | None = None
        self._overload_poll_task: asyncio.Task | None = None
        self._pending_result_retry_task: asyncio.Task | None = None
        self._worker_pool_health_task: asyncio.Task | None = None
        # Event-driven seed-recovery task. Started when
        # ``_healthy_manager_ids`` transitions to empty and cancelled
        # when it transitions back to non-empty. Re-issues TCP
        # ``worker_register`` against configured seed_managers with
        # exponential backoff until one accepts. Required because in
        # the all-managers-die-then-quorum-returns scenario neither
        # restarted manager has worker state to share via SWIM gossip
        # — the worker must initiate recovery from its side.
        self._manager_seed_recovery_task: asyncio.Task | None = None
        self._known_worker_pool_process_ids: set[int] = set()
        # Phase H4 — autonomous extension trigger background task
        self._extension_trigger_task: asyncio.Task | None = None
        self._extension_trigger: ExtensionTrigger = ExtensionTrigger(
            active_runtimes_provider=self._iter_active_workflow_runtimes,
            deadline_provider=self._worker_state.get_workflow_timeout,
            is_extension_pending=lambda: self._worker_state._extension_requested,
            request_extension=self.request_extension,
            config=ExtensionTriggerConfig.from_env_values(
                poll_interval_str=env.HYPERSCALE_EXTENSION_TRIGGER_INTERVAL,
                lookahead_fraction=env.HYPERSCALE_EXTENSION_LOOKAHEAD_FRACTION,
            ),
        )
        # Drop trigger bookkeeping for any workflow that finishes via
        # any path (success / failure / cancel / orphan-eviction).
        # WorkerState.remove_active_workflow fires every termination,
        # so registering here covers all of them uniformly.
        self._worker_state.register_workflow_termination_callback(
            self._extension_trigger.forget_workflow
        )
        # A pending extension request for a workflow that terminated is
        # moot on every termination path: clear the latch so the next
        # heartbeat stops carrying it (otherwise it rides until the
        # manager's decision push lands — an avoidable denial round)
        # and the trigger is free to serve the remaining workflows.
        self._worker_state.register_workflow_termination_callback(
            self._clear_extension_request_for_workflow
        )

        # Debounced cores notification (AD-38 fix: single in-flight task, coalesced updates)
        self._pending_cores_notification: int | None = None
        self._cores_notification_task: asyncio.Task | None = None

        # Event logger for crash forensics (AD-47)
        self._event_logger: Logger | None = None


        self._updates_controller: InterfaceUpdatesController = (
            InterfaceUpdatesController()
        )

        # Create state embedder for SWIM
        state_embedder = WorkerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_worker_state=lambda: self._get_worker_state().value,
            get_available_cores=lambda: self._core_allocator.available_cores,
            get_total_cores=lambda: self._core_allocator.total_cores,
            get_queue_depth=lambda: len(self._pending_workflows),
            get_cpu_percent=self._get_cpu_percent,
            get_memory_percent=self._get_memory_percent,
            get_state_version=lambda: self._state_sync.state_version,
            get_active_workflows=lambda: {
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
            get_cores_version=lambda: self._core_allocator.availability_version,
            on_manager_heartbeat=self._handle_manager_heartbeat,
            get_tcp_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_health_accepting_work=lambda: self._get_worker_state()
            in (WorkerStateEnum.HEALTHY, WorkerStateEnum.DEGRADED),
            get_health_throughput=self._worker_state.get_throughput,
            get_health_expected_throughput=self._worker_state.get_expected_throughput,
            get_health_overload_state=self._backpressure_manager.get_overload_state_str,
            get_extension_requested=lambda: self._worker_state._extension_requested,
            get_extension_reason=lambda: self._worker_state._extension_reason,
            get_extension_current_progress=lambda: self._worker_state._extension_current_progress,
            get_extension_completed_items=lambda: self._worker_state._extension_completed_items,
            get_extension_total_items=lambda: self._worker_state._extension_total_items,
            get_extension_estimated_completion=lambda: self._worker_state._extension_estimated_completion,
            get_extension_active_workflow_count=lambda: len(self._active_workflows),
            # Phase H3 — multi-dimensional progress snapshot piggyback
            get_extension_step_transitions=lambda: self._worker_state._extension_step_transitions,
            get_extension_actions_completed=lambda: self._worker_state._extension_actions_completed,
            get_extension_snapshot_time=lambda: self._worker_state._extension_snapshot_time,
            get_extension_workflow_id=lambda: self._worker_state._extension_workflow_id,
            # AD-19 addendum (Phase D): uniform LHM gossip
            get_lhm_score=lambda: self._local_health.score,
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
            clock=clock,
            random_source=random_source,
            transport_factory=transport_factory,
            incarnation_storage_dir=incarnation_storage_dir,
        )

        # Initialize components that need discovery service
        self._registration_handler: WorkerRegistrationHandler = (
            WorkerRegistrationHandler(
                registry=self._registry,
                discovery_service=self._discovery_service,
                logger=self._udp_logger,
                node_capabilities=self._node_capabilities,
            )
        )

        self._heartbeat_handler: WorkerHeartbeatHandler = WorkerHeartbeatHandler(
            registry=self._registry,
            logger=self._udp_logger,
        )

        self._progress_reporter: WorkerProgressReporter = WorkerProgressReporter(
            registry=self._registry,
            state=self._worker_state,
            config=self._config,
            logger=self._udp_logger,
        )

        self._workflow_executor: WorkerWorkflowExecutor = WorkerWorkflowExecutor(
            core_allocator=self._core_allocator,
            state=self._worker_state,
            lifecycle=self._lifecycle_manager,
            backpressure_manager=self._backpressure_manager,
            env=env,
            logger=self._udp_logger,
            resource_tracker=WorkflowResourceTracker(
                clock=self._clock,
                total_memory_bytes=self._resource_monitor.total_memory_bytes,
            ),
            task_runner=self._task_runner,
            execution_update_wait_seconds=self._config.execution_update_wait_seconds,
        )

        self._cancellation_handler_impl: WorkerCancellationHandler = (
            WorkerCancellationHandler(
                state=self._worker_state,
                logger=self._udp_logger,
                poll_interval=self._config.cancellation_poll_interval_seconds,
                query_timeout=self._config.tcp_timeout_short_seconds,
                cancel_timeout=self._config.workflow_cancel_timeout_seconds,
            )
        )

        self._background_loops: WorkerBackgroundLoops = WorkerBackgroundLoops(
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
            orphan_extension_min_grant=self._config.orphan_extension_min_grant_seconds,
            orphan_extension_max_extensions=self._config.orphan_extension_max_extensions,
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
            on_new_manager_discovered=self._register_with_manager,
            on_job_leadership_update=self._on_job_leadership_update,
        )

        # Initialize handlers
        self._dispatch_handler: WorkflowDispatchHandler = WorkflowDispatchHandler(self)
        self._cancel_handler: WorkflowCancelHandler = WorkflowCancelHandler(self)
        self._throttle_handler: WorkflowThrottleHandler = WorkflowThrottleHandler(self)
        self._cancel_job_handler: CancelJobWorkflowsHandler = (
            CancelJobWorkflowsHandler(self)
        )
        self._transfer_handler: JobLeaderTransferHandler = JobLeaderTransferHandler(
            self
        )
        self._sync_handler: StateSyncHandler = StateSyncHandler(self)

        # Cluster-membership invariant owner. Every registry path that
        # mutates ``_healthy_manager_ids`` (mark_healthy /
        # mark_unhealthy / remove_manager_state /
        # register-response processing) signals this component via
        # ``update()`` so the worker re-establishes connectivity
        # against its seed list when isolated.
        self._cluster_connection: WorkerClusterConnection = WorkerClusterConnection(
            seed_manager_tcp_addrs=self._seed_managers,
            register_with_manager=self._register_with_manager,
            get_lhm_multiplier=lambda: self._local_health.get_multiplier(),
            get_healthy_manager_ids=lambda: set(
                self._registry._healthy_manager_ids
            ),
            mark_manager_unhealthy=self._registry.mark_manager_unhealthy,
            invalidate_tcp_client=self._invalidate_tcp_client_transport,
            task_runner=self._task_runner,
            logger=self._udp_logger,
            node_host=self._host,
            node_port=self._tcp_port,
            node_id_short=self._node_id.short,
            liveness_check_interval_seconds=(
                self._env.WORKER_CLUSTER_LIVENESS_CHECK_INTERVAL
            ),
            heartbeat_staleness_threshold_seconds=(
                self._env.WORKER_CLUSTER_HEARTBEAT_STALENESS_THRESHOLD
            ),
            rejoin_base_backoff_seconds=(
                self._env.WORKER_CLUSTER_REJOIN_BASE_BACKOFF
            ),
            rejoin_jitter_min_seconds=self._config.recovery_jitter_min_seconds,
            rejoin_jitter_max_seconds=self._config.recovery_jitter_max_seconds,
        )
        # The datacenter's manager membership as last observed (AD-52
        # section 10): its cohort becomes the rejoin seeds.
        manager_request_timeout_seconds = self._env.WORKER_TCP_TIMEOUT_STANDARD
        self._manager_membership_view = ClusterViewCache(
            poll_wait_seconds=self._env.CLUSTER_WATCH_WAIT_SECONDS,
            request_timeout_seconds=manager_request_timeout_seconds,
        )
        self._manager_membership_watch = ClusterWatchFollower(
            self._manager_membership_view,
            seeds=lambda: self._cluster_connection.seed_manager_tcp_addrs,
            send_watch=self._send_manager_membership_watch,
            clock=self._clock,
            poll_wait_seconds=self._env.CLUSTER_WATCH_WAIT_SECONDS,
            request_timeout_seconds=manager_request_timeout_seconds,
            on_view_changed=lambda view: self._cluster_connection.adopt_cohort(view.cohort) if view.cohort else None,
            on_disconnected_changed=lambda disconnected: self._udp_logger.log(
                ClusterWatchConnectivityChanged(
                    message=(
                        "Lost the datacenter's manager membership: rejoining from the cohort last observed"
                        if disconnected
                        else "Following the datacenter's manager membership"
                    ),
                    node_id=self._node_id.short,
                    watched=self._node_id.datacenter,
                    disconnected=disconnected,
                    staleness_seconds=self._manager_membership_view.read(self._clock.monotonic())[1],
                    level=LogLevel.WARN if disconnected else LogLevel.INFO,
                )
            ),
        )
        # Wire the registry's healthy-set-changed signal so every
        # mark_healthy / mark_unhealthy / remove_manager_state path
        # flows through the connection state machine AND drives the
        # event-driven seed-recovery transition detector. Both
        # consumers are idempotent so the wrapper can fire them
        # unconditionally on each signal.
        self._registry._on_healthy_set_changed = (
            self._on_registry_healthy_changed
        )

    def _wire_logger_to_modules(self) -> None:
        """Wire logger to all modules after parent init."""
        self._registry._logger = self._udp_logger
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
        self._stopping = False

        # Setup logging config
        self._lifecycle_manager.setup_logging_config()

        # Start parent server. The base server exposes `start_server`, not
        # `start`; the manager calls it directly on self for the same reason.

        await self._start_event_logger()

        await self.start_server(init_context=self.env.get_swim_init_context())

        # Restore (or create) this node's persisted incarnation so a
        # restarted worker rejoins above its pre-restart value.
        await self.initialize_incarnation_store()

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

        # Set remote manager for cancellation and AD-41 throttling
        self._cancellation_handler_impl.set_remote_manager(remote_manager)
        self._throttle_handler.set_remote_manager(remote_manager)

        # Start remote manager
        await self._lifecycle_manager.start_remote_manager()

        # Run worker pool
        await self._lifecycle_manager.run_worker_pool()

        # Connect to workers
        await self._lifecycle_manager.connect_to_workers(timeout)
        process_exitcodes = self._lifecycle_manager.get_server_pool_process_exitcodes()
        self._known_worker_pool_process_ids = self._live_pool_process_ids(process_exitcodes)

        registration_jitter_max_seconds = (
            self._config.initial_registration_jitter_max_seconds
        )
        if registration_jitter_max_seconds > 0.0:
            await self._clock.sleep(self._random.uniform(0.0, registration_jitter_max_seconds))

        # Set core availability callback
        self._lifecycle_manager.set_on_cores_available(self._on_cores_available)

        # Register with all seed managers
        for manager_addr in self._seed_managers:
            await self._register_with_manager(manager_addr)

        await self._join_known_managers_swim()

        # Start SWIM probe cycle. `start_probe_cycle` is an async loop;
        # submit it to the TaskRunner the same way the manager does
        # (manager/server.py). Calling it sync just creates a coroutine
        # that never runs — workers in that mode never emit SWIM probes.
        self._task_runner.run(self.start_probe_cycle)

        # Start background loops
        await self._start_background_loops()

        # Start the cluster-connection liveness watchdog now that we
        # have at least attempted registration and the SWIM probe
        # cycle is running. Starting earlier would risk the watchdog
        # marking managers stale before their first heartbeat.
        self._cluster_connection.start()
        # AD-52 section 10: the datacenter's manager membership, followed
        # into its soft-state cache; each change becomes the rejoin seeds.
        self._task_runner.run(self._manager_membership_watch.run, alias="manager-membership-watch")

        await self._udp_logger.log(
            ServerInfo(
                message=f"Worker started with {self._total_cores} cores",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _start_event_logger(self) -> None:
        """Open the worker's JSONL event log, when configured, and record the start."""
        if self._config.event_log_dir is None:
            return
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
            self._worker_started_event(),
            name="worker_events",
        )

        self._workflow_executor.set_event_logger(self._event_logger)

    def _worker_started_event(self) -> WorkerStarted:
        """The worker-started event, naming the first seed manager when there is one."""
        return WorkerStarted(
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
        )

    async def stop(
        self, drain_timeout: float = 5, broadcast_leave: bool = True
    ) -> None:
        """Stop the worker server gracefully.

        Active workflows are first marked locally orphaned, then voluntary
        leave is announced before workflow cancellation and local pool teardown.
        Those steps can take seconds for long-running workloads, while manager
        membership must converge immediately so the control plane can stop
        routing work to this worker and reassign any in-flight sub-workflows.

        After the leave is sent, background loops are cancelled before
        teardown that can raise or be externally cancelled. That keeps the
        worker's named background tasks from surviving past shutdown.
        """
        self._stopping = True
        self._worker_state.suppress_active_final_results(
            SERVER_SHUTDOWN_CANCELLATION_REASON
        )

        if broadcast_leave:
            await self._broadcast_leave()

        self._running = False

        if not await self._stop_within_drain_timeout(drain_timeout):
            return

        await super().stop(drain_timeout=0.0, broadcast_leave=False)

        await self._udp_logger.log(
            ServerInfo(
                message="Worker stopped",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _stop_within_drain_timeout(self, drain_timeout: float) -> bool:
        """Stop local resources within ``drain_timeout``; False when it aborted instead."""
        if drain_timeout <= 0:
            await self.abort_and_wait()
            return False

        return await self._drain_after_leave(drain_timeout)

    async def _drain_after_leave(self, drain_timeout: float) -> bool:
        """Run the post-leave teardown under ``drain_timeout``, aborting on expiry; whether it finished."""
        try:
            await self._clock.wait_for(
                self._stop_after_leave(),
                timeout=drain_timeout,
            )
        except asyncio.TimeoutError:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Worker graceful shutdown exceeded drain timeout; "
                        "aborting remaining local resources"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            await self.abort_and_wait()
            return False
        return True

    def _has_cluster_connection(self) -> bool:
        """Whether the cluster-connection state machine was built (absent on a partial init)."""
        return hasattr(self, "_cluster_connection") and self._cluster_connection is not None

    async def _stop_after_leave(self) -> None:
        """Stop worker-local resources after membership leave has been sent."""
        # Tear down the cluster-connection state machine first so any
        # in-flight rejoin task is cancelled before the rest of the
        # shutdown begins to dismantle dependent state.
        if self._has_cluster_connection():
            await self._cluster_connection.stop()
        if hasattr(self, "_manager_membership_watch"):
            self._manager_membership_watch.stop()

        await self._stop_background_loops()
        await self._cancel_cores_notification_task()
        self._stop_modules()
        await self._log_worker_stopping()
        await self._cancel_all_active_workflows()
        await self._shutdown_lifecycle_components()

    def _get_leave_targets(self) -> list[tuple[str, int]]:
        """Return manager UDP targets for worker voluntary leave."""
        return self._registry.get_manager_leave_udp_addrs()

    def _get_additional_leave_targets(self) -> list[tuple[str, int]]:
        """Return known manager UDP addresses that must receive worker leave."""
        return self._registry.get_manager_leave_udp_addrs()

    def _get_registered_node_id_for_addr(self, addr: tuple[str, int]) -> str | None:
        """Return the manager identity currently registered at ``addr``."""
        return self._registry.find_manager_by_udp_addr(addr)

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

    def _cores_notification_task_live(self) -> bool:
        """Whether a cores-available notification task exists and has not finished."""
        return bool(self._cores_notification_task) and not self._cores_notification_task.done()

    async def _cancel_cores_notification_task(self) -> None:
        if not self._cores_notification_task_live():
            return
        self._cores_notification_task.cancel()
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        await self._await_cancelled_cores_notification_task(cancels_requested_before_wait)

    async def _await_cancelled_cores_notification_task(self, cancels_requested_before_wait: int) -> None:
        """Wait out the cancelled notification task, re-raising only a cancel aimed at this task."""
        try:
            await self._cores_notification_task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

    def _stop_modules(self) -> None:
        self._backpressure_manager.stop()
        if self._cancellation_handler_impl:
            self._cancellation_handler_impl.stop()
        if self._background_loops:
            self._background_loops.stop()

    async def _cancel_all_active_workflows(self) -> None:
        for workflow_id in list(self._workflow_tokens.keys()):
            await self._cancel_workflow(
                workflow_id,
                SERVER_SHUTDOWN_CANCELLATION_REASON,
            )

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
        self._stopping = True
        self._running = False
        self._worker_state.suppress_active_final_results(
            SERVER_SHUTDOWN_CANCELLATION_REASON
        )

        # Cancel background tasks synchronously
        self._lifecycle_manager.cancel_background_tasks_sync()

        if self._cores_notification_task_live():
            self._cores_notification_task.cancel()

        # Mark the cluster-connection lifecycle stopped so any
        # in-flight rejoin task self-terminates on its next state
        # check. We cannot ``await stop()`` from sync abort, but
        # setting ``_running = False`` is enough — the rejoin loop
        # checks it on every iteration.
        if self._has_cluster_connection():
            self._cluster_connection._running = False

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

        self._pending_result_retry_task = self._create_background_task(
            self._run_pending_result_retry_loop(
                get_healthy_managers=lambda: self._registry._healthy_manager_ids,
                send_tcp=self.send_tcp,
            ),
            "pending_result_retry",
        )
        self._lifecycle_manager.add_background_task(self._pending_result_retry_task)

        self._dead_manager_reap_task = self._create_background_task(
            self._background_loops.run_dead_manager_reap_loop(
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                task_runner_run=self._task_runner.run,
                is_running=lambda: self._running,
                is_seed_manager=self._manager_id_is_seed,
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
                register_with_manager=self._register_with_manager,
            ),
            "discovery_maintenance",
        )
        self._lifecycle_manager.add_background_task(self._discovery_maintenance_task)

        self._overload_poll_task = self._create_background_task(
            self._backpressure_manager.run_overload_poll_loop(),
            "overload_poll",
        )
        self._lifecycle_manager.add_background_task(self._overload_poll_task)

        # Resource sampling reads the real host through
        # ``asyncio.to_thread`` (``run_in_executor`` — banned on the
        # SimulationLoop) and its readings are inherently
        # non-deterministic; worse, the loop's broad exception handler
        # would swallow the constraint error once per virtual second
        # forever. Like the lifecycle monitors, telemetry is skipped
        # under SIM — scenarios assert on state, not host samples.
        if self._transport_factory is None:
            self._resource_sample_task = self._create_background_task(
                self._run_resource_sample_loop(),
                "resource_sample",
            )
            self._lifecycle_manager.add_background_task(self._resource_sample_task)

        self._worker_pool_health_task = self._create_background_task(
            self._run_worker_pool_health_loop(),
            "worker_pool_health",
        )
        self._lifecycle_manager.add_background_task(self._worker_pool_health_task)

        self._manager_rejoin_watch_task = self._create_background_task(
            self._run_manager_rejoin_watch_loop(),
            "manager_rejoin_watch",
        )
        self._lifecycle_manager.add_background_task(
            self._manager_rejoin_watch_task
        )

        # Phase H4 — autonomous extension trigger. Scans active
        # workflows on a heartbeat-aligned cadence and invokes
        # ``request_extension`` for any workflow approaching its
        # deadline that has shown forward progress since the last
        # request. The actual heartbeat piggyback is set on the
        # WorkerState; the next outbound heartbeat ships it.
        self._extension_trigger_task = self._create_background_task(
            self._extension_trigger.run_loop(
                is_running=lambda: self._running,
                sleep=self._clock.sleep,
            ),
            "extension_trigger",
        )
        self._lifecycle_manager.add_background_task(self._extension_trigger_task)

    async def _run_pending_result_retry_loop(
        self,
        get_healthy_managers: Callable[[], set[str]],
        send_tcp: SendTcp,
    ) -> None:
        while self._running:
            if not await self._pending_result_retry_iteration(get_healthy_managers, send_tcp):
                break

    async def _pending_result_retry_iteration(
        self,
        get_healthy_managers: Callable[[], set[str]],
        send_tcp: SendTcp,
    ) -> bool:
        """One pending-result retry pass and its wait; False once cancelled."""
        try:
            await self._retry_pending_results_once(get_healthy_managers, send_tcp)
            return True
        except asyncio.CancelledError:
            return False
        except Exception as exc:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Pending result retry failed: {type(exc).__name__}: {exc}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            await self._clock.sleep(self._config.result_retry_base_delay_seconds)
            return True

    async def _retry_pending_results_once(
        self,
        get_healthy_managers: Callable[[], set[str]],
        send_tcp: SendTcp,
    ) -> None:
        """Retry due pending results while a manager is reachable, then sleep until the next is due."""
        # Wake when the soonest pending result is due (a refusal's
        # retry-after can be sooner than any backoff step); with no
        # manager reachable, nothing is sent, so the backoff base.
        wait_seconds = self._config.result_retry_base_delay_seconds
        if get_healthy_managers():
            await self._progress_reporter.retry_pending_results(
                send_tcp=send_tcp,
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                task_runner_run=self._task_runner.run,
            )
            wait_seconds = self._progress_reporter.seconds_until_next_result_retry(
                self._clock.monotonic()
            )
        await self._clock.sleep(wait_seconds)

    async def _run_resource_sample_loop(self) -> None:
        while self._running:
            if not await self._resource_sample_iteration():
                break

    async def _resource_sample_iteration(self) -> bool:
        """One resource sample and its one-second wait; False once cancelled."""
        try:
            await self._resource_monitor.sample()
            await self._clock.sleep(1.0)
            return True
        except asyncio.CancelledError:
            return False
        except Exception as exc:
            await self._udp_logger.log(
                f"Resource sampling failed: {exc}",
                level="debug",
            )
            await self._clock.sleep(1.0)
            return True

    async def _run_manager_rejoin_watch_loop(self) -> None:
        """Re-register when a manager RESTARTS under us.

        A manager that restarts faster than the failure detector's
        witness-less death bound (~60s+) is never declared dead: our
        registration state stays CONNECTED while the new generation's
        worker registry is EMPTY — a silent split that starves dispatch
        until the job times out. The restart is detectable anyway: the
        manager's persisted incarnation store makes every boot rejoin
        with a jump of at least the tracker's rejoin bump, which we
        observe passively through normal SWIM traffic. On a jump,
        refresh every manager registration (idempotent — re-
        registration overwrites).
        """
        while self._running:
            if not await self._manager_rejoin_watch_iteration():
                break

    async def _manager_rejoin_watch_iteration(self) -> bool:
        """One manager-restart check and its two-second wait; False once cancelled."""
        try:
            await self._check_manager_rejoins()
            await self._clock.sleep(2.0)
            return True
        except asyncio.CancelledError:
            return False
        except Exception as watch_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Manager rejoin watch iteration failed: "
                        f"{watch_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            await self._clock.sleep(2.0)
            return True

    async def _check_manager_rejoins(self) -> None:
        rejoined_managers = self._collect_rejoined_managers()

        if not rejoined_managers:
            return

        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Manager(s) {rejoined_managers} rejoined with a "
                    "restart-signature incarnation jump — refreshing "
                    "registrations"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        await self.refresh_manager_registrations()

    def _collect_rejoined_managers(self) -> list[tuple[str, int]]:
        """UDP addresses of known managers whose incarnation jumped by a restart's rejoin bump."""
        rejoin_bump = self._incarnation_tracker.minimum_rejoin_incarnation_bump
        rejoined_managers: list[tuple[str, int]] = []

        for manager in self._registry.get_known_manager_values():
            if self._observe_manager_incarnation(manager, rejoin_bump):
                rejoined_managers.append((manager.udp_host, manager.udp_port))
        return rejoined_managers

    def _observe_manager_incarnation(self, manager: ManagerInfo, rejoin_bump: int) -> bool:
        """Record a manager's current incarnation; whether it jumped by at least ``rejoin_bump``."""
        if not manager.udp_host or not manager.udp_port:
            return False
        manager_udp_addr = (manager.udp_host, manager.udp_port)
        current_incarnation = self._incarnation_tracker.get_node_incarnation(
            manager_udp_addr
        )
        previous_incarnation = self._manager_incarnations_seen.get(
            manager_udp_addr
        )
        self._manager_incarnations_seen[manager_udp_addr] = (
            current_incarnation
        )
        return self._is_restart_jump(previous_incarnation, current_incarnation, rejoin_bump)

    @staticmethod
    def _is_restart_jump(previous_incarnation: int | None, current_incarnation: int, rejoin_bump: int) -> bool:
        """Whether an incarnation moved by a restart's rejoin bump since last seen."""
        return (
            previous_incarnation is not None
            and current_incarnation
            >= previous_incarnation + rejoin_bump
        )

    async def _run_worker_pool_health_loop(self) -> None:
        """Fail active workflows when a local runner process exits mid-flight."""
        while self._running:
            if not await self._worker_pool_health_iteration():
                break

    async def _worker_pool_health_iteration(self) -> bool:
        """One worker-pool health check and its wait; False once cancelled."""
        try:
            await self._check_worker_pool_health()
            await self._clock.sleep(0.25)
            return True
        except asyncio.CancelledError:
            return False
        except Exception as exc:
            await self._udp_logger.log(
                f"Worker pool health check failed: {exc}",
                level="debug",
            )
            await self._clock.sleep(1.0)
            return True

    async def _check_worker_pool_health(self) -> None:
        process_exitcodes = self._lifecycle_manager.get_server_pool_process_exitcodes()
        live_process_ids = self._live_pool_process_ids(process_exitcodes)

        if not self._active_workflows or not self._known_worker_pool_process_ids:
            self._known_worker_pool_process_ids = live_process_ids
            return

        await self._handle_exited_pool_processes(process_exitcodes, live_process_ids)

    @staticmethod
    def _live_pool_process_ids(process_exitcodes: dict[int | str, int | None]) -> set[int | str]:
        """The pool processes that have not exited."""
        return {
            process_id
            for process_id, exitcode in process_exitcodes.items()
            if exitcode is None
        }

    def _vanished_pool_process_ids(self, live_process_ids: set[int | str]) -> set[int | str]:
        """Known pool processes no longer alive."""
        return {
            process_id
            for process_id in self._known_worker_pool_process_ids
            if process_id not in live_process_ids
        }

    @staticmethod
    def _exited_pool_process_codes(process_exitcodes: dict[int | str, int | None]) -> set[int | str]:
        """The pool processes reporting an exit code."""
        return {
            process_id
            for process_id, exitcode in process_exitcodes.items()
            if exitcode is not None
        }

    async def _handle_exited_pool_processes(
        self,
        process_exitcodes: dict[int | str, int | None],
        live_process_ids: set[int | str],
    ) -> None:
        """Degrade and fail the active workflows when a known pool process exited."""
        exited_process_ids = self._vanished_pool_process_ids(live_process_ids)
        exited_process_ids.update(
            self._known_worker_pool_process_ids.intersection(
                self._exited_pool_process_codes(process_exitcodes)
            )
        )

        if not exited_process_ids:
            return

        await self._degradation.force_level(DegradationLevel.HEAVY)
        await self._udp_logger.log(
            ServerWarning(
                message=(
                    "Worker pool process exited while workflows were active: "
                    f"{sorted(exited_process_ids)}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        self._known_worker_pool_process_ids = live_process_ids
        await self._fail_active_workflows_for_pool_exit(exited_process_ids)

    async def _fail_active_workflows_for_pool_exit(
        self,
        exited_process_ids: set[int | str],
    ) -> None:
        reason = (
            "Worker subprocess exited during workflow execution "
            f"(pids={sorted(exited_process_ids)})"
        )
        for workflow_id, progress in list(self._active_workflows.items()):
            await self._fail_active_workflow(workflow_id, progress, reason)

    async def _fail_active_workflow(
        self,
        workflow_id: str,
        progress: WorkflowProgress,
        reason: str,
    ) -> None:
        fence_token = await self._worker_state.get_workflow_fence_token(workflow_id)
        job_leader_addr = self._worker_state.get_workflow_job_leader(workflow_id)
        workflow_name = self._workflow_display_name(workflow_id, progress)

        progress.status = WorkflowStatus.FAILED.value
        await self._core_allocator.free(workflow_id)

        final_result = WorkflowFinalResult(
            job_id=progress.job_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            status=WorkflowStatus.FAILED.value,
            results=[],
            context_updates=b"",
            error=reason,
            worker_id=self._node_id.full,
            worker_available_cores=self._core_allocator.available_cores,
            worker_cores_version=self._core_allocator.availability_version,
            fence_token=fence_token,
            job_leader_addr=job_leader_addr,
        )

        await self._progress_reporter.send_final_result(
            final_result=final_result,
            send_tcp=self.send_tcp,
            node_host=self._host,
            node_port=self._tcp_port,
            node_id_short=self._node_id.short,
            task_runner_run=self._task_runner.run,
        )

        workflow_token = self._workflow_tokens.get(workflow_id)
        self._cleanup_workflow_state(workflow_id)
        if workflow_token is not None:
            await self._task_runner.cancel(workflow_token)

    def _workflow_display_name(self, workflow_id: str, progress: WorkflowProgress) -> str:
        """A workflow's name from its progress, else the dispatch record, else its id."""
        return (
            progress.workflow_name
            or self._worker_state._workflow_id_to_name.get(workflow_id)
            or workflow_id
        )

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
        if self._stopping:
            return WorkerStateEnum.DRAINING
        return self._worker_state_for_degradation()

    def _worker_state_for_degradation(self) -> WorkerStateEnum:
        """A running worker's state by degradation level: DRAINING at 3+, DEGRADED at 2, else HEALTHY."""
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

    def _iter_active_workflow_runtimes(self) -> list[WorkflowRuntimeState]:
        """Adapt the worker's ``_active_workflows`` dict to the H4
        ``WorkflowRuntimeState`` shape that ``ExtensionTrigger`` expects.

        Phase H4 needs the multi-dimensional progress counters (cores
        completed, step transitions, action completion) plus the
        workflow's start time. ``WorkflowProgress`` (the wire-level
        message stored in ``_active_workflows``) carries cores and
        action counts directly; the start time comes from
        ``WorkerState._workflow_start_times`` (set when the workflow
        was dispatched). Step transitions default to 0 — the AD-54
        state-machine wiring lands separately and the trigger's
        ``any_advanced`` check works as long as cores or actions
        advance.
        """
        runtimes: list[WorkflowRuntimeState] = []
        for workflow_id, progress in list(self._active_workflows.items()):
            start_time = self._worker_state._workflow_start_times.get(
                workflow_id
            )
            if not start_time:
                continue
            runtimes.append(self._workflow_runtime_state(workflow_id, progress, start_time))
        return runtimes

    def _workflow_runtime_state(
        self,
        workflow_id: str,
        progress: WorkflowProgress,
        start_time: float,
    ) -> WorkflowRuntimeState:
        """One active workflow in the H4 ``WorkflowRuntimeState`` shape."""
        return WorkflowRuntimeState(
            workflow_id=workflow_id,
            job_id=progress.job_id,
            status=progress.status,
            allocated_cores=progress.worker_workflow_assigned_cores or 0,
            fence_token=self._worker_state._workflow_fence_tokens.get(
                workflow_id, -1
            ),
            start_time=start_time,
            cores_completed=progress.cores_completed,
            vus=progress.vus,
            step_transitions=0,
            actions_completed=progress.completed_count,
        )

    def _get_heartbeat(self) -> WorkerHeartbeat:
        """
        Build a WorkerHeartbeat with current state.

        This is the same data that gets embedded in SWIM messages via
        WorkerStateEmbedder, but available for other uses like diagnostics
        or explicit TCP status updates if needed.
        """
        health_overload_state = self._backpressure_manager.get_overload_state_str()
        return WorkerHeartbeat(
            node_id=self._node_id.full,
            state=self._get_worker_state().value,
            # The worker's TCP contact: a manager that does NOT know
            # this worker (it restarted and lost its registry) uses it
            # to send the re-register nudge.
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            available_cores=self._core_allocator.available_cores,
            cores_version=self._core_allocator.availability_version,
            total_cores=self._core_allocator.total_cores,
            queue_depth=len(self._pending_workflows),
            cpu_percent=self._get_cpu_percent(),
            memory_percent=self._get_memory_percent(),
            version=self._state_sync.state_version,
            active_workflows={
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
            health_accepting_work=(
                self._get_worker_state() is not WorkerStateEnum.DRAINING
                and health_overload_state not in {"overloaded", "critical"}
            ),
            health_overload_state=health_overload_state,
            extension_requested=self._worker_state._extension_requested,
            extension_reason=self._worker_state._extension_reason,
            extension_current_progress=self._worker_state._extension_current_progress,
            extension_completed_items=self._worker_state._extension_completed_items,
            extension_total_items=self._worker_state._extension_total_items,
            extension_estimated_completion=self._worker_state._extension_estimated_completion,
            extension_active_workflow_count=len(self._active_workflows),
            # Phase H3 — multi-dimensional progress snapshot piggyback
            extension_step_transitions=self._worker_state._extension_step_transitions,
            extension_actions_completed=self._worker_state._extension_actions_completed,
            extension_snapshot_time=self._worker_state._extension_snapshot_time,
            # AD-19 addendum (Phase D): uniform LHM gossip — workers
            # report their raw LHM score so cross_dc_correlation can
            # see worker-tier stress alongside manager/gate stress.
            lhm_score=self._local_health.score,
        )

    def request_extension(
        self,
        reason: str,
        progress: float = 0.0,
        completed_items: int = 0,
        total_items: int = 0,
        estimated_completion: float = 0.0,
        workflow_id: str = "",
        step_transitions: int = 0,
        actions_completed: int = 0,
        snapshot_time: float = 0.0,
    ) -> None:
        """
        Request a deadline extension via heartbeat piggyback (AD-26).

        This sets the extension request fields in the worker's heartbeat,
        which will be processed by the manager when the next heartbeat is
        received. This is more efficient than a separate TCP call for
        extension requests.

        AD-26 Issue 4: Supports absolute metrics (completed_items, total_items)
        which are preferred over relative progress for robustness.

        Phase H3: also accepts the secondary/tertiary progress
        counters (``step_transitions``, ``actions_completed``) and the
        worker-side capture timestamp. Together with ``completed_items``
        these form the ``WorkflowProgressSnapshot`` the manager
        evaluates against the strict-monotonic-progress witness in
        H5.

        Args:
            reason: Human-readable reason for the extension request.
            progress: Monotonic progress value (not clamped to 0-1). Must strictly
                increase between extension requests for approval. Prefer completed_items.
            completed_items: Absolute count of completed items (preferred metric;
                primary dimension of WorkflowProgressSnapshot).
            total_items: Total items to complete.
            estimated_completion: Estimated seconds until workflow completion.
            workflow_id: Specific workflow this snapshot belongs to. Phase H3.
            step_transitions: AD-54 step state-machine transitions since dispatch.
                Secondary progress dimension.
            actions_completed: Sum of StepStats.completed_count across active steps.
                Tertiary progress dimension.
            snapshot_time: ``self._clock.monotonic()`` on the worker when the
                snapshot was constructed. Used for rate-limiting and the
                throughput-witness time-windowed velocity check.
        """
        self._worker_state._extension_requested = True
        self._worker_state._extension_reason = reason
        self._worker_state._extension_current_progress = max(0.0, progress)
        self._worker_state._extension_completed_items = completed_items
        self._worker_state._extension_total_items = total_items
        self._worker_state._extension_estimated_completion = estimated_completion
        active_workflow_count = len(self._active_workflows)
        self._worker_state._extension_active_workflow_count = active_workflow_count
        # Phase H3 — multi-dimensional progress snapshot piggyback
        self._worker_state._extension_workflow_id = workflow_id
        self._worker_state._extension_step_transitions = step_transitions
        self._worker_state._extension_actions_completed = actions_completed
        self._worker_state._extension_snapshot_time = snapshot_time

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
        # Phase H3 — clear the WorkflowProgressSnapshot piggyback too
        self._worker_state._extension_workflow_id = ""
        self._worker_state._extension_step_transitions = 0
        self._worker_state._extension_actions_completed = 0
        self._worker_state._extension_snapshot_time = 0.0

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

        await self._apply_pending_transfer_if_listed(job_id, workflow_id, pending)

    async def _apply_pending_transfer_if_listed(
        self, job_id: str, workflow_id: str, pending: PendingTransfer
    ) -> None:
        """Apply a pending transfer (Section 8.3) to a workflow it names, then drop it once complete."""
        if workflow_id not in pending.workflow_ids:
            return

        await self._apply_pending_transfer(job_id, workflow_id, pending)
        self._cleanup_pending_transfer_if_complete(job_id, workflow_id, pending)

    def _is_pending_transfer_expired(self, pending: PendingTransfer) -> bool:
        current_time = self._clock.monotonic()
        pending_transfer_ttl = self._config.pending_transfer_ttl_seconds
        return current_time - pending.received_at > pending_transfer_ttl

    async def _apply_pending_transfer(
        self, job_id: str, workflow_id: str, pending: PendingTransfer
    ) -> None:
        job_lock = await self._get_job_transfer_lock(job_id)
        async with job_lock:
            self._workflow_job_leader[workflow_id] = pending.new_manager_addr
            self._job_fence_tokens[job_id] = pending.fence_token

            await self._udp_logger.log(
                ServerInfo(
                    message=f"Applied pending transfer for workflow {workflow_id[:8]}... to job {job_id[:8]}...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    def _cleanup_pending_transfer_if_complete(
        self, job_id: str, workflow_id: str, pending: PendingTransfer
    ) -> None:
        remaining_workflows = self._remaining_transfer_workflows(workflow_id, pending)
        if not remaining_workflows:
            del self._pending_transfers[job_id]

    def _remaining_transfer_workflows(self, workflow_id: str, pending: PendingTransfer) -> list[str]:
        """The transfer's workflows, other than ``workflow_id``, not yet active here."""
        return [
            wf_id
            for wf_id in pending.workflow_ids
            if self._is_transfer_workflow_remaining(wf_id, workflow_id)
        ]

    def _is_transfer_workflow_remaining(self, transfer_workflow_id: str, workflow_id: str) -> bool:
        """Whether a transfer's workflow is neither active here nor ``workflow_id``."""
        return transfer_workflow_id not in self._active_workflows and transfer_workflow_id != workflow_id

    # =========================================================================
    # Registration Methods
    # =========================================================================

    def add_to_probe_scheduler(self, peer_udp_addr: tuple[str, int]) -> None:
        """
        Add a peer to the SWIM probe scheduler.

        Wrapper around _probe_scheduler.add_member for use as callback.

        Args:
            peer_udp_addr: UDP address tuple (host, port) of peer to probe
        """
        self._probe_scheduler.add_member(peer_udp_addr)

    def _on_registry_healthy_changed(self) -> None:
        """Signal handler for ``WorkerRegistry._on_healthy_set_changed``.

        Fires synchronously on every ``mark_manager_healthy`` /
        ``mark_manager_unhealthy`` / ``remove_manager_state``
        invocation. Forwards to the cluster-connection state machine
        (preserves existing behaviour) and to the seed-recovery
        transition detector (new: starts/cancels the seed-retry task
        on N↔0 transitions).
        """
        self._cluster_connection.update()
        self._reconcile_seed_recovery_task()

    def _reconcile_seed_recovery_task(self) -> None:
        """Start or stop the seed-recovery task to match current state.

        Idempotent: every registry healthy-set mutation drives this,
        so the task lifecycle follows the actual state regardless of
        which mutation path triggered the signal.

        * No healthy managers, no configured seeds → nothing to do.
        * No healthy managers, seeds present, task not running → start it.
        * At least one healthy manager, task running → cancel it.
        """
        has_healthy = bool(self._registry._healthy_manager_ids)
        has_seeds = bool(self._seed_managers)
        task_running = self._seed_recovery_task_running()

        if has_healthy:
            self._cancel_running_seed_recovery(task_running)
            return

        if self._should_start_seed_recovery(has_seeds, task_running):
            self._manager_seed_recovery_task = self._create_background_task(
                self._run_manager_seed_recovery(),
                "manager_seed_recovery",
            )
            self._lifecycle_manager.add_background_task(
                self._manager_seed_recovery_task
            )

    def _seed_recovery_task_running(self) -> bool:
        """Whether the seed-recovery task exists and has not finished."""
        return (
            self._manager_seed_recovery_task is not None
            and not self._manager_seed_recovery_task.done()
        )

    def _cancel_running_seed_recovery(self, task_running: bool) -> None:
        """Cancel the seed-recovery task once a manager is healthy again."""
        if task_running:
            self._manager_seed_recovery_task.cancel()

    def _should_start_seed_recovery(self, has_seeds: bool, task_running: bool) -> bool:
        """Whether a running worker with no healthy manager should start retrying its seeds."""
        return has_seeds and not task_running and self._running

    async def _run_manager_seed_recovery(self) -> None:
        """Retry ``refresh_manager_registrations`` until a manager accepts.

        Started by ``_reconcile_seed_recovery_task`` when the worker
        observes zero healthy managers. Each iteration calls
        ``refresh_manager_registrations`` (re-issues TCP
        ``worker_register`` against every configured seed plus every
        known-but-currently-unhealthy manager). On the first
        registration that succeeds, the registry's
        ``mark_manager_healthy`` will fire and the transition
        detector will cancel this task — so the loop ends naturally
        on recovery without polling indefinitely.

        Backoff schedule: 0 s, 1 s, 2 s, 4 s, 8 s, capped at 10 s.
        Tight initial cadence keeps recovery latency low in the
        all-managers-die-then-quorum-returns scenario; the cap
        prevents pathological loops on a genuinely-isolated worker
        from saturating the event loop.
        """
        backoff_seconds = 0.0
        max_backoff_seconds = 10.0
        try:
            await self._retry_seed_registrations(backoff_seconds, max_backoff_seconds)
        except asyncio.CancelledError:
            return

    async def _retry_seed_registrations(self, backoff_seconds: float, max_backoff_seconds: float) -> None:
        """Refresh registrations with capped doubling backoff until a manager accepts or the worker stops."""
        while self._running:
            if (
                backoff_seconds := await self._seed_recovery_round(backoff_seconds, max_backoff_seconds)
            ) is None:
                return

    async def _seed_recovery_round(self, backoff_seconds: float, max_backoff_seconds: float) -> float | None:
        """Wait out the backoff, then refresh; None once a manager accepted, else the next backoff."""
        if backoff_seconds > 0:
            await self._clock.sleep(backoff_seconds)
        if await self._seed_recovery_refresh_succeeded():
            return None
        return self._next_seed_recovery_backoff(backoff_seconds, max_backoff_seconds)

    async def _seed_recovery_refresh_succeeded(self) -> bool:
        """Refresh every manager registration; whether one took and a manager is now healthy."""
        try:
            succeeded = await self.refresh_manager_registrations()
        except Exception as refresh_error:
            succeeded = False
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Seed-recovery refresh raised: "
                        f"{refresh_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        return succeeded and bool(self._registry._healthy_manager_ids)

    @staticmethod
    def _next_seed_recovery_backoff(backoff_seconds: float, max_backoff_seconds: float) -> float:
        """The next seed-recovery backoff: 1 s, then doubling, capped."""
        return min(
            max_backoff_seconds,
            backoff_seconds * 2 if backoff_seconds > 0 else 1.0,
        )

    def _manager_id_is_seed(self, manager_id: str) -> bool:
        """Return True iff ``manager_id`` is bound to a configured seed addr.

        Seeds are the worker's bootstrap path to the cluster; their
        ``_known_managers`` entry must survive arbitrary unhealthy
        durations so the SWIM ``_on_node_join`` recovery callback can
        still match an address to a manager_id when a previously-DEAD
        seed manager rejoins. Reaping a seed strands the worker —
        the registry forgets the manager_id, the recovery callback
        finds nothing on rejoin, and the worker can't re-register
        without external prompting.
        """
        manager_info = self._registry.get_manager(manager_id)
        if manager_info is None:
            return False
        return (manager_info.tcp_host, manager_info.tcp_port) in self._seed_managers

    async def _join_known_managers_swim(self) -> None:
        """Join SWIM with every known manager for healthchecks.

        Workers know these peers are managers (they came from manager
        registration), so the role is passed through and recorded
        authoritatively without waiting for gossip.
        """
        for manager_info in list(self._registry._known_managers.values()):
            manager_udp_addr = (manager_info.udp_host, manager_info.udp_port)
            await self.join_cluster(manager_udp_addr, seed_role="manager")

    async def _join_node(self, target_addr: tuple[str, int]) -> None:
        """Operator join: register with the manager at ``target_addr``.

        The same registration the boot path runs against seed managers.
        An explicit operator join clears any open circuit / cached
        transport from earlier failed attempts, as a lifecycle refresh
        does, and adopts the manager as a seed so reaping and the
        isolation rejoin loop treat it like a configured one.
        """
        self._invalidate_tcp_client_transport(target_addr)
        self._registry.get_or_create_circuit_by_addr(target_addr).reset()

        if not await self._register_with_manager(target_addr):
            raise ClusterJoinError(
                f"manager {target_addr[0]}:{target_addr[1]} did not accept "
                "worker registration (see worker log for the cause)"
            )

        if target_addr not in self._seed_managers:
            self._seed_managers.append(target_addr)
        self._cluster_connection.add_seed_manager(target_addr)

        await self._join_known_managers_swim()
        self._cluster_connection.update()

    async def _send_manager_membership_watch(
        self,
        manager_addr: tuple[str, int],
        payload: bytes,
        timeout: float,
    ) -> bytes | Exception | None:
        """One poll of the datacenter's manager membership watch."""
        response, _clock = await self.send_tcp(manager_addr, "cluster_watch", payload, timeout=timeout)
        return response

    async def _register_with_manager(self, manager_addr: tuple[str, int]) -> bool:
        """Register this worker with a manager."""
        return await self._registration_handler.register_with_manager(
            manager_addr=manager_addr,
            node_info=self.node_info,
            total_cores=self._total_cores,
            available_cores=self._core_allocator.available_cores,
            memory_mb=self._get_memory_mb(),
            available_memory_mb=self._get_available_memory_mb(),
            cluster_id=self._env.CLUSTER_ID,
            environment_id=self._env.ENVIRONMENT_ID,
            send_func=self._send_registration,
            max_retries=self._config.registration_max_retries,
            base_delay=self._config.registration_base_delay_seconds,
        )

    async def refresh_manager_registrations(self) -> bool:
        """Refresh manager registration after a lifecycle-level connectivity change."""
        manager_addr_candidates = dict.fromkeys(self._seed_managers)
        manager_addr_candidates.update(dict.fromkeys(self._known_manager_tcp_addrs()))

        # Issue register attempts concurrently. Serial iteration here
        # serialises a 5 s TCP timeout per dead candidate — with three
        # candidates and one still-dead, the worker can wait ~20 s on
        # the dead address before even trying the live ones, blowing
        # the all-managers-die-quorum-returns recovery budget. The
        # registration handler is per-manager-addressed and the
        # invalidate/circuit-reset side effects are independent per
        # address, so gathering is safe.
        async def attempt(manager_addr: tuple[str, int]) -> bool:
            self._invalidate_tcp_client_transport(manager_addr)
            self._registry.get_or_create_circuit_by_addr(manager_addr).reset()
            return await self._register_with_manager(manager_addr)

        results = await asyncio.gather(
            *(attempt(addr) for addr in manager_addr_candidates),
            return_exceptions=True,
        )
        registered_with_manager = any(
            result is True for result in results
        )

        self._cluster_connection.update()
        return registered_with_manager

    def _known_manager_tcp_addrs(self) -> list[tuple[str, int]]:
        """TCP addresses of every known manager that has one, in registry order."""
        return [
            (manager.tcp_host, manager.tcp_port)
            for manager in filter(self._has_tcp_endpoint, self._registry.get_known_manager_values())
        ]

    @staticmethod
    def _has_tcp_endpoint(manager: ManagerInfo) -> bool:
        """Whether a manager record carries a TCP host and port."""
        return manager.tcp_host and manager.tcp_port

    async def _send_registration(
        self,
        manager_addr: tuple[str, int],
        data: bytes,
        timeout: float = 5.0,
    ) -> bytes | Exception:
        """Send registration data to manager."""
        sent_at = self._clock.monotonic()
        try:
            response, _ = await self.send_tcp(
                manager_addr,
                "worker_register",
                data,
                timeout=timeout,
            )
            # send_tcp reports transport failures (timeouts, refused
            # connections, a peer that cannot decrypt us and never
            # answers) as a returned Exception, not a raised one.
            # Parsing it as a response turned every such failure into
            # "TypeError: a bytes-like object is required".
            return await self._settle_registration_response(manager_addr, response, sent_at)
        except Exception as error:
            self._record_manager_failure(manager_addr)
            return error

    async def _settle_registration_response(
        self,
        manager_addr: tuple[str, int],
        response: bytes | Exception,
        sent_at: float,
    ) -> bytes | Exception:
        """Apply a registration answer: a transport error or refusal counts against the manager (AD-28)."""
        if isinstance(response, Exception):
            self._record_manager_failure(manager_addr)
            return response

        accepted, _ = await self._process_manager_registration_response(
            response
        )
        if not accepted:
            self._record_manager_failure(manager_addr)
            return RuntimeError(
                f"Manager {manager_addr} rejected worker registration"
            )
        self._record_manager_round_trip(
            manager_addr,
            self._clock.monotonic() - sent_at,
        )
        return response

    def _record_manager_round_trip(
        self,
        manager_addr: tuple[str, int],
        round_trip_seconds: float,
    ) -> None:
        """Feed a manager round trip into AD-28's EWMA latency ranking."""
        if manager := self._registry.get_manager_by_addr(manager_addr):
            self._discovery_manager.record_success(
                manager.node_id,
                round_trip_seconds * 1000.0,
            )

    def _record_manager_failure(self, manager_addr: tuple[str, int]) -> None:
        """Count a failed round trip against a known manager (AD-28)."""
        if manager := self._registry.get_manager_by_addr(manager_addr):
            self._discovery_manager.record_failure(manager.node_id)

    async def _process_manager_registration_response(
        self,
        data: bytes,
    ) -> tuple[bool, str | None]:
        """Apply a manager registration response to local worker state."""
        accepted, primary_manager_id = (
            await self._registration_handler.process_registration_response(
                data=data,
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                add_unconfirmed_peer=self.add_unconfirmed_peer,
                add_to_probe_scheduler=self.add_to_probe_scheduler,
                mark_registered=self.register_peer,
            )
        )

        if accepted and primary_manager_id:
            await self._on_registration_accepted(primary_manager_id)

        return accepted, primary_manager_id

    async def _on_registration_accepted(self, primary_manager_id: str) -> None:
        """Count an accepted registration as a heartbeat from every healthy manager, and log it."""
        # A successful registration round-trip is the strongest
        # application-level liveness signal we get from a manager:
        # the manager not only answered our TCP but accepted us
        # into its worker registry. Record this as a heartbeat
        # against every manager we now know is healthy so the
        # cluster-connection watchdog does not immediately mark
        # them stale before their first SWIM heartbeat arrives.
        for manager_id in list(self._registry._healthy_manager_ids):
            self._cluster_connection.record_heartbeat(manager_id)
        await self._udp_logger.log(
            ServerInfo(
                message=(
                    "Registration accepted, primary manager: "
                    f"{primary_manager_id[:8]}..."
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _get_memory_mb(self) -> int:
        """Get total memory in MB (via the machine-telemetry seam —
        constant under SIM, live psutil in REAL mode)."""
        return int(
            _DEFAULT_SYSTEM_RESOURCES.total_memory_bytes() / (1024 * 1024)
        )

    def _get_available_memory_mb(self) -> int:
        """Get available memory in MB (via the machine-telemetry
        seam)."""
        return int(
            _DEFAULT_SYSTEM_RESOURCES.available_memory_bytes()
            / (1024 * 1024)
        )

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
        # Drop any cached TCP client transport to this manager before
        # flipping the registry. ``send_tcp`` only reconnects when the
        # cached transport's ``is_closing()`` is True; without an
        # explicit invalidation here a worker that already has a
        # persistent TCP session to the dying manager will keep
        # sending RPCs over it — fatal once the process restarts at
        # the same address but with a different identity, since the
        # asyncio transport has no way to observe the peer's death
        # short of a write error. Invalidating here ensures the next
        # ``send_tcp`` for any addr that resolved to this manager
        # opens a fresh socket against whoever is listening now.
        manager_info = self._registry.get_manager(manager_id)
        if manager_info is not None:
            self._invalidate_tcp_client_transport(
                (manager_info.tcp_host, manager_info.tcp_port)
            )

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

        self._orphan_workflows_led_by((manager_info.tcp_host, manager_info.tcp_port))

    def _orphan_workflows_led_by(self, manager_addr: tuple[str, int]) -> None:
        """Mark every workflow whose job leader is ``manager_addr`` orphaned."""
        for workflow_id, leader_addr in list(self._workflow_job_leader.items()):
            if leader_addr == manager_addr:
                self._worker_state.mark_workflow_orphaned(workflow_id)

    async def _handle_manager_recovery_async(self, manager_id: str) -> None:
        """Handle manager recovery - mark as healthy and re-register.

        A manager rejoining the cluster after a DEAD detection may have
        come back with empty in-memory state (the ``_workers`` registry
        on the manager is not persisted across process restarts). SWIM
        recovery only tells the manager that we exist as a *peer* —
        the worker pool registry that powers job dispatch is
        TCP-registration-driven. Without re-issuing ``worker_register``
        here, an all-managers-die-then-quorum-returns scenario leaves
        the returning quorum with an empty worker registry: workers
        keep running but no manager knows they're available for work.

        Idempotent on the manager side — re-registering an already-
        known worker just refreshes the entry. Safe under concurrent
        recovery events for multiple managers (each registers
        independently against a single per-addr circuit/transport).
        """
        await self._registry.mark_manager_healthy(manager_id)

        manager_info = self._registry.get_manager(manager_id)
        if manager_info is not None and self._has_tcp_endpoint(manager_info):
            await self._reregister_with_recovered_manager(manager_id, manager_info)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager {manager_id[:8]}... recovered",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _reregister_with_recovered_manager(self, manager_id: str, manager_info: ManagerInfo) -> None:
        """Re-register with a recovered manager over a fresh transport and circuit, logging a failure."""
        manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
        self._invalidate_tcp_client_transport(manager_addr)
        self._registry.get_or_create_circuit_by_addr(manager_addr).reset()
        try:
            await self._register_with_manager(manager_addr)
        except Exception as register_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Re-registration with recovered manager "
                        f"{manager_id[:8]}... failed: {register_error}"
                    ),
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
                # Signal the connection-state machine — this is the only
                # ``_healthy_manager_ids`` mutation outside the registry's
                # own helpers, so we route it through the same signal.
                self._registry._signal_healthy_set_changed()
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
        self, heartbeat: ManagerHeartbeat, source_addr: tuple[str, int]
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

        await self._heartbeat_handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=source_addr,
            confirm_peer=self.confirm_peer,
            node_host=self._host,
            node_port=self._tcp_port,
            node_id_short=self._node_id.short,
            task_runner_run=self._task_runner.run,
        )

        # Record the heartbeat as an application-level liveness
        # signal for the cluster-connection watchdog. SWIM probe
        # success alone is insufficient to prove the manager still
        # knows about us — only an actual heartbeat (which the
        # manager only sends to workers in its registry) proves the
        # cluster-membership relationship is intact.
        self._cluster_connection.record_heartbeat(heartbeat.node_id)
        self._worker_state.record_manager_heartbeat()

    def _on_job_leadership_update(
        self,
        job_leaderships: dict[str, tuple[int, int]],
        manager_addr: tuple[str, int],
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: RunTask,
    ) -> None:
        """Handle job leadership claims from heartbeat."""
        # Check each active workflow to see if this manager leads its job
        for workflow_id, progress in list(self._active_workflows.items()):
            job_id = progress.job_id
            if job_id in job_leaderships:
                # A manager claiming the job leads it: the workflow is not
                # orphaned -- also when the claimant is the leader it had,
                # rejoining after a false death (that orphan mark was never
                # cleared, and the workflow was cancelled at the grace
                # period's end under a live leader).
                self._worker_state.clear_workflow_orphaned(workflow_id)
                current_leader = self._workflow_job_leader.get(workflow_id)
                if current_leader != manager_addr:
                    self._workflow_job_leader[workflow_id] = manager_addr
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

        await self._send_cores_available_heartbeat(manager_addr)

    async def _send_cores_available_heartbeat(self, manager_addr: tuple[str, int]) -> None:
        """Send the primary manager a heartbeat announcing freed cores, logging a failure."""
        try:
            heartbeat = self._get_heartbeat()
            response, _ = await self.send_tcp(
                manager_addr,
                "worker_heartbeat",
                heartbeat.dump(),
                timeout=self._config.heartbeat_send_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response
        except Exception as error:
            await self._udp_logger.log(
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
        self,
        dispatch: WorkflowDispatch,
        addr: tuple[str, int],
        allocation_result: AllocationResult,
    ) -> bytes:
        """Handle the execution phase of a workflow dispatch."""

        async def send_final_result_callback(final_result: WorkflowFinalResult) -> None:
            await self._progress_reporter.send_final_result(
                final_result=final_result,
                send_tcp=self.send_tcp,
                node_host=self._host,
                node_port=self._tcp_port,
                node_id_short=self._node_id.short,
                task_runner_run=self._task_runner.run,
            )

        result = await self._workflow_executor.handle_dispatch_execution(
            dispatch=dispatch,
            dispatching_addr=addr,
            allocated_cores=allocation_result.allocated_cores,
            cores_version=allocation_result.availability_version,
            task_runner_run=self._task_runner.run,
            increment_version=self._increment_version,
            node_id_full=self._node_id.full,
            node_host=self._host,
            node_port=self._tcp_port,
            send_final_result_callback=send_final_result_callback,
        )

        # AD-26 dispatch-time extension. The worker has just accepted
        # a workflow with a declared timeout; tell the manager so the
        # SWIM bracket and the worker-deadline are extended *before*
        # the heavy startup phase of the workflow (subprocess spawn,
        # state hydration, initial heartbeat round) can starve probe
        # acks and trigger false-positive SUSPECT. Without this, the
        # autonomous ExtensionTrigger only fires once a workflow has
        # been running for ``deadline × lookahead_fraction`` seconds
        # (75% by default) — too late to defend the first 5–10 s
        # window where the actual contention lives. The extension is
        # heartbeat-piggybacked; the manager's
        # ``_handle_embedded_worker_heartbeat`` processes it through
        # the same ``_process_extension_request_core`` path the TCP
        # endpoint uses.
        if dispatch.timeout_seconds > 0:
            self.request_extension(
                reason="dispatch-accepted",
                progress=0.0,
                completed_items=0,
                total_items=len(allocation_result.allocated_cores),
                estimated_completion=dispatch.timeout_seconds,
                workflow_id=dispatch.workflow_id,
                step_transitions=0,
                actions_completed=0,
                snapshot_time=self._clock.monotonic(),
            )

        await self._check_pending_transfer_for_job(
            dispatch.job_id, dispatch.workflow_id
        )

        return result

    def _clear_extension_request_for_workflow(self, workflow_id: str) -> None:
        """Termination-callback hook: drop a pending extension request
        that belongs to the just-terminated workflow (leaves requests
        for OTHER workflows untouched)."""
        if self._worker_state._extension_workflow_id == workflow_id:
            self.clear_extension_request()

    def _cleanup_workflow_state(self, workflow_id: str) -> None:
        """Cleanup workflow state on failure."""
        # Phase H4 — drop the trigger's per-workflow tracker so the
        # bookkeeping dict doesn't grow unbounded across the worker's
        # lifetime.
        self._extension_trigger.forget_workflow(workflow_id)
        # Last: its termination callbacks' failures raise.
        self._worker_state.remove_active_workflow(workflow_id)

    # =========================================================================
    # Cancellation
    # =========================================================================

    async def _cancel_workflow(
        self, workflow_id: str, reason: str
    ) -> tuple[bool, list[str]]:
        """Cancel a workflow and clean up resources."""
        if reason == SERVER_SHUTDOWN_CANCELLATION_REASON:
            self._worker_state.suppress_final_result(workflow_id, reason)

        success, errors = await self._cancellation_handler_impl.cancel_workflow(
            workflow_id=workflow_id,
            reason=reason,
            task_runner_cancel=self._task_runner.cancel,
            increment_version=self._increment_version,
        )

        if reason == SERVER_SHUTDOWN_CANCELLATION_REASON:
            return (success, errors)

        self._report_cancellation_complete(workflow_id, success, errors)

        return (success, errors)

    def _report_cancellation_complete(self, workflow_id: str, success: bool, errors: list[str]) -> None:
        """Push a workflow's cancellation completion to its manager, when its job is known."""
        # Push cancellation complete to manager (fire-and-forget via task runner)
        progress = self._active_workflows.get(workflow_id)
        if progress and progress.job_id:
            self._task_runner.run(
                self._progress_reporter.send_cancellation_complete,
                progress.job_id,
                workflow_id,
                success,
                errors,
                self._clock.monotonic(),
                self._node_id.full,
                self.send_tcp,
                self._host,
                self._tcp_port,
                self._node_id.short,
            )

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

        by_job = self._group_progress_updates_by_job(updates)
        return self._select_best_progress_per_job(by_job)

    def _group_progress_updates_by_job(
        self, updates: dict[str, WorkflowProgress]
    ) -> dict[str, list[WorkflowProgress]]:
        by_job: dict[str, list[WorkflowProgress]] = {}
        for progress in updates.values():
            by_job.setdefault(progress.job_id, []).append(progress)
        return by_job

    def _select_best_progress_per_job(
        self, by_job: dict[str, list[WorkflowProgress]]
    ) -> dict[str, WorkflowProgress]:
        aggregated: dict[str, WorkflowProgress] = {}
        for job_updates in by_job.values():
            best_update = max(job_updates, key=lambda p: p.completed_count)
            aggregated[best_update.workflow_id] = best_update
        return aggregated

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
    async def throttle_workflow(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Handle an AD-41 workflow throttle or release request."""
        return await self._throttle_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def cancel_job_workflows(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Handle job-scoped workflow cancellation request.

        Used by the takeover-side cancel path on a new DC leader
        whose ``job.workflows`` map wasn't fully repopulated by
        peer/worker state-sync after failover. The worker iterates
        its ``_active_workflows`` and cancels any workflow for the
        requested ``job_id``, reporting back the set of workflow
        ids it actually cancelled so the new leader can seed its
        cancellation-pending tracker correctly.
        """
        return await self._cancel_job_handler.handle(addr, data, clock_time)

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

    @tcp.receive()
    async def extension_response(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Receive the manager's AD-26 decision for a piggybacked
        extension request and CLEAR the request latch.

        This completes the latch lifecycle the design always intended
        ("the worker clears after the response is processed"): without
        it, ``clear_extension_request`` had zero callers, so the
        dispatch-time latch froze its snapshot forever — every
        heartbeat re-carried the same request (a perpetual ~1/s denial
        stream that outlived the workflow AND the job), and the
        autonomous lookahead ``ExtensionTrigger`` was permanently gated
        dead by ``is_extension_pending``. With the latch cleared, the
        trigger's own progress-advance guard (one re-request only when
        a progress dimension moved) becomes the live pacing mechanism,
        exactly as designed. Delivery is self-healing: if this response
        is lost, the next heartbeat re-carries the request and the
        manager re-responds.
        """
        try:
            response = HealthcheckExtensionResponse.load(data)
        except Exception as error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Discarding unparseable extension_response from "
                        f"{addr}: {type(error).__name__}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

        # A GRANT earned with progress must stretch the workflow's LOCAL
        # deadline too: the stuck-workflow enforcement loop compares
        # elapsed against the dispatch-time timeout, and without this the
        # worker hard-cancels the very workflow the manager just granted
        # more time. A grant with no progress behind it -- the
        # dispatch-time request, protecting this worker's liveness
        # through startup -- stretches neither this deadline nor the
        # job's AD-34 budget (the manager applies the same rule): the
        # job's explicit timeout stands. The latched workflow id names
        # the workflow the request was for; best-effort if it already
        # drained.
        self._extend_granted_workflow_deadline(response)

        self.clear_extension_request()
        await self._log_extension_decision(response)
        return b"ok"

    def _extend_granted_workflow_deadline(self, response: HealthcheckExtensionResponse) -> None:
        """Stretch the latched workflow's local deadline by a progress-backed AD-26 grant."""
        if self._is_progress_backed_grant(response):
            granted_workflow_id = self._worker_state._extension_workflow_id
            if granted_workflow_id:
                self._worker_state.extend_workflow_timeout(
                    granted_workflow_id, response.extension_seconds
                )

    def _is_progress_backed_grant(self, response: HealthcheckExtensionResponse) -> bool:
        """Whether a positive grant answers a request that reported progress (AD-26, AD-34)."""
        return (
            response.granted
            and response.extension_seconds > 0
            and self._has_extension_progress()
        )

    def _has_extension_progress(self) -> bool:
        """Whether the latched extension request reported any advanced progress dimension."""
        worker_state = self._worker_state
        return (
            self._has_extension_progress_counts()
            or worker_state._extension_step_transitions > 0
            or worker_state._extension_actions_completed > 0
        )

    def _has_extension_progress_counts(self) -> bool:
        """Whether the latched request reported progress or completed items."""
        worker_state = self._worker_state
        return (
            worker_state._extension_current_progress > 0.0
            or (worker_state._extension_completed_items or 0) > 0
        )

    async def _log_extension_decision(self, response: HealthcheckExtensionResponse) -> None:
        """Record the manager's extension decision in the event log, when one is open."""
        if self._event_logger is not None:
            await self._event_logger.log(
                self._extension_decision_event(response),
                name="worker_events",
            )

    def _extension_decision_event(self, response: HealthcheckExtensionResponse) -> WorkerExtensionDecision:
        """The event recording a manager's extension decision."""
        return WorkerExtensionDecision(
            message=(
                "Extension "
                + ("granted" if response.granted else "denied")
                + f" ({response.extension_seconds:.1f}s)"
            ),
            node_id=self._node_id.full,
            node_host=self._host,
            node_port=self._tcp_port,
            granted=response.granted,
            extension_seconds=response.extension_seconds,
            denial_reason=response.denial_reason or "",
        )

    @tcp.receive()
    async def eviction_notice(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        """Handle a manager's notice that it deregistered this worker.

        Closes the one-sided-eviction gap: the manager forgot us but
        kept acking our SWIM probes, so without this push we would
        believe the relationship healthy forever and never re-register.
        Mark the evicting manager unhealthy (feeding the existing
        cluster-connection state machine) and schedule a targeted
        re-registration with it; the ack tells the manager its notice
        obligation is discharged.
        """
        notice = WorkerEvictionNotice.load(data)

        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Manager {notice.manager_id[:8]}... deregistered this "
                    f"worker (reason: {notice.reason}) — re-registering"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        await self._registry.mark_manager_unhealthy(notice.manager_id)
        self._cluster_connection.update()
        self._task_runner.run(
            self._reregister_after_eviction,
            (notice.manager_tcp_host, notice.manager_tcp_port),
        )

        return WorkerEvictionNoticeAck(
            worker_id=self._node_id.full,
            will_reregister=True,
        ).dump()

    async def _reregister_after_eviction(
        self, manager_addr: tuple[str, int]
    ) -> None:
        """Targeted re-registration with a manager that evicted us.

        Deliberately direct (not just the RECONNECTING rejoin loop):
        in a multi-manager DC the other managers may still be healthy,
        so the cluster connection never leaves CONNECTED and the rejoin
        loop never runs — but this specific manager still needs a fresh
        registration.
        """
        try:
            await self._register_with_manager(manager_addr)
        except Exception as register_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Post-eviction re-registration with manager at "
                        f"{manager_addr} failed: {register_error} — the "
                        f"manager's notice backstop will re-prompt"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

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
        return await self._registration_handler.process_manager_registration(
            data=data,
            node_id_full=self._node_id.full,
            total_cores=self._total_cores,
            available_cores=self._core_allocator.available_cores,
            add_unconfirmed_peer=self.add_unconfirmed_peer,
            add_to_probe_scheduler=self.add_to_probe_scheduler,
            mark_registered=self.register_peer,
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
        await self._process_manager_registration_response(data)

        return data


__all__ = ["WorkerServer"]
