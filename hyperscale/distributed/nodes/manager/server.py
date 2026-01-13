"""
Manager server composition root.

Thin orchestration layer that wires all manager modules together.
All business logic is delegated to specialized coordinators.
"""

import asyncio
import random
import time
import cloudpickle
from pathlib import Path
from typing import TYPE_CHECKING

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.state.context import Context
from hyperscale.distributed.swim import HealthAwareServer, ManagerStateEmbedder
from hyperscale.distributed.swim.core import (
    ErrorStats,
    CircuitState,
    QuorumTimeoutError,
    QuorumCircuitOpenError,
)
from hyperscale.distributed.swim.detection import HierarchicalConfig
from hyperscale.distributed.swim.health import FederatedHealthMonitor
from hyperscale.distributed.env import Env
from hyperscale.distributed.server import tcp
from hyperscale.distributed.server.protocol.utils import get_peer_certificate_der
from hyperscale.distributed.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    ManagerState as ManagerStateEnum,
    ManagerHeartbeat,
    ManagerStateSnapshot,
    GateInfo,
    GateHeartbeat,
    GateRegistrationRequest,
    GateRegistrationResponse,
    WorkerRegistration,
    WorkerHeartbeat,
    WorkerState,
    WorkerStateSnapshot,
    RegistrationResponse,
    ManagerPeerRegistration,
    ManagerPeerRegistrationResponse,
    JobSubmission,
    JobAck,
    JobStatus,
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowProgress,
    WorkflowProgressAck,
    WorkflowFinalResult,
    WorkflowResultPush,
    WorkflowStatus,
    StateSyncRequest,
    StateSyncResponse,
    JobCancelRequest,
    JobCancelResponse,
    CancelJob,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    WorkflowCancellationComplete,
    WorkflowCancellationQuery,
    WorkflowCancellationResponse,
    WorkflowCancellationStatus,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkflowCancellationPeerNotification,
    CancelledWorkflowInfo,
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
    WorkerDiscoveryBroadcast,
    ContextForward,
    ContextLayerSync,
    ContextLayerSyncAck,
    JobLeadershipAnnouncement,
    JobLeadershipAck,
    JobStateSyncMessage,
    JobStateSyncAck,
    JobLeaderGateTransfer,
    JobLeaderGateTransferAck,
    ProvisionRequest,
    ProvisionConfirm,
    ProvisionCommit,
    JobGlobalTimeout,
    PingRequest,
    ManagerPingResponse,
    WorkerStatus,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    RegisterCallback,
    RegisterCallbackResponse,
    RateLimitResponse,
    TrackingToken,
    restricted_loads,
)
from hyperscale.distributed.models.worker_state import (
    WorkerStateUpdate,
    WorkerListResponse,
    WorkerListRequest,
)
from hyperscale.distributed.reliability import (
    HybridOverloadDetector,
    ServerRateLimiter,
    StatsBuffer,
    StatsBufferConfig,
)
from hyperscale.distributed.resources import ProcessResourceMonitor, ResourceMetrics
from hyperscale.distributed.health import WorkerHealthManager, WorkerHealthManagerConfig
from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    NodeCapabilities,
    ProtocolVersion,
    negotiate_capabilities,
    get_features_for_version,
)
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.discovery.security.role_validator import (
    RoleValidator,
    NodeRole as SecurityNodeRole,
)
from hyperscale.distributed.jobs import (
    JobManager,
    WorkerPool,
    WorkflowDispatcher,
    WindowedStatsCollector,
)
from hyperscale.distributed.ledger.wal import NodeWAL
from hyperscale.logging.lsn import HybridLamportClock
from hyperscale.distributed.jobs.timeout_strategy import (
    TimeoutStrategy,
    LocalAuthorityTimeout,
    GateCoordinatedTimeout,
)
from hyperscale.distributed.workflow import (
    WorkflowStateMachine as WorkflowLifecycleStateMachine,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerWarning,
    ServerError,
    ServerDebug,
)

from .config import ManagerConfig, create_manager_config_from_env
from .state import ManagerState
from .registry import ManagerRegistry
from .dispatch import ManagerDispatchCoordinator
from .cancellation import ManagerCancellationCoordinator
from .leases import ManagerLeaseCoordinator
from .health import ManagerHealthMonitor, HealthcheckExtensionManager
from .sync import ManagerStateSync
from .leadership import ManagerLeadershipCoordinator
from .stats import ManagerStatsCoordinator
from .discovery import ManagerDiscoveryCoordinator
from .load_shedding import ManagerLoadShedder

from .workflow_lifecycle import ManagerWorkflowLifecycle
from .worker_dissemination import WorkerDisseminator
from hyperscale.distributed.swim.gossip.worker_state_gossip_buffer import (
    WorkerStateGossipBuffer,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class ManagerServer(HealthAwareServer):
    """
    Manager node composition root.

    Orchestrates workflow execution within a datacenter by:
    - Receiving jobs from gates (or directly from clients)
    - Dispatching workflows to workers
    - Aggregating status updates from workers
    - Reporting to gates (if present)
    - Participating in leader election among managers
    - Handling quorum-based confirmation for workflow provisioning
    """

    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "default",
        gate_addrs: list[tuple[str, int]] | None = None,
        gate_udp_addrs: list[tuple[str, int]] | None = None,
        seed_managers: list[tuple[str, int]] | None = None,
        manager_peers: list[tuple[str, int]] | None = None,
        manager_udp_peers: list[tuple[str, int]] | None = None,
        quorum_timeout: float = 5.0,
        max_workflow_retries: int = 3,
        workflow_timeout: float = 300.0,
        wal_data_dir: Path | None = None,
    ) -> None:
        """
        Initialize manager server.

        Args:
            host: Host address to bind
            tcp_port: TCP port for data operations
            udp_port: UDP port for SWIM healthchecks
            env: Environment configuration
            dc_id: Datacenter identifier
            gate_addrs: Optional gate TCP addresses for upstream communication
            gate_udp_addrs: Optional gate UDP addresses for SWIM
            seed_managers: Initial manager TCP addresses for peer discovery
            manager_peers: Deprecated alias for seed_managers
            manager_udp_peers: Manager UDP addresses for SWIM cluster
            quorum_timeout: Timeout for quorum operations
            max_workflow_retries: Maximum retry attempts per workflow
            workflow_timeout: Workflow execution timeout in seconds
        """
        # Build configuration from environment
        self._config = create_manager_config_from_env(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            datacenter_id=dc_id,
            seed_gates=gate_addrs,
            gate_udp_addrs=gate_udp_addrs,
            seed_managers=seed_managers or manager_peers,
            manager_udp_peers=manager_udp_peers,
            quorum_timeout=quorum_timeout,
            max_workflow_retries=max_workflow_retries,
            workflow_timeout=workflow_timeout,
            wal_data_dir=wal_data_dir,
        )

        self._node_wal: NodeWAL | None = None

        self._env = env
        self._seed_gates = gate_addrs or []
        self._gate_udp_addrs = gate_udp_addrs or []
        self._seed_managers = seed_managers or manager_peers or []
        self._manager_udp_peers = manager_udp_peers or []
        self._max_workflow_retries = max_workflow_retries
        self._workflow_timeout = workflow_timeout

        # Initialize centralized runtime state
        self._manager_state = ManagerState()

        # Initialize parent HealthAwareServer
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
            node_role="manager",
        )

        # Wire logger to modules
        self._init_modules()

        # Initialize address mappings for SWIM callbacks
        self._init_address_mappings()

        # Register callbacks
        self._register_callbacks()

    def _init_modules(self) -> None:
        """Initialize all modular coordinators."""
        # Registry for workers, gates, peers
        self._registry = ManagerRegistry(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Lease coordinator for fencing tokens and job leadership
        self._leases = ManagerLeaseCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Health monitor for worker health tracking
        self._health_monitor = ManagerHealthMonitor(
            state=self._manager_state,
            config=self._config,
            registry=self._registry,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Extension manager for AD-26 deadline extensions
        self._extension_manager = HealthcheckExtensionManager(
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Dispatch coordinator for workflow dispatch
        self._dispatch = ManagerDispatchCoordinator(
            state=self._manager_state,
            config=self._config,
            registry=self._registry,
            leases=self._leases,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            send_to_worker=self._send_to_worker,
            send_to_peer=self._send_to_peer,
        )

        # Cancellation coordinator for AD-20
        self._cancellation = ManagerCancellationCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            send_to_worker=self._send_to_worker,
            send_to_client=self._send_to_client,
        )

        # State sync coordinator
        self._state_sync = ManagerStateSync(
            state=self._manager_state,
            config=self._config,
            registry=self._registry,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            send_tcp=self._send_to_peer,
        )

        # Leadership coordinator
        self._leadership = ManagerLeadershipCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            is_leader_fn=self.is_leader,
            get_term_fn=lambda: self._leader_election.state.current_term
            if hasattr(self, "_leader_election")
            else 0,
        )

        # Stats coordinator
        self._stats = ManagerStatsCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Discovery coordinator
        self._discovery = ManagerDiscoveryCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            env=self._env,
        )

        # Load shedding (AD-22)
        self._overload_detector = HybridOverloadDetector()
        self._resource_monitor = ProcessResourceMonitor()
        self._last_resource_metrics: "ResourceMetrics | None" = None
        self._manager_health_state: str = "healthy"
        self._previous_manager_health_state: str = "healthy"
        self._load_shedder = ManagerLoadShedder(
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # JobManager for race-safe job/workflow state
        self._job_manager = JobManager(
            datacenter=self._node_id.datacenter,
            manager_id=self._node_id.short,
        )

        self._worker_pool = WorkerPool(
            health_grace_period=30.0,
            get_swim_status=self._get_swim_status_for_worker,
            manager_id=self._node_id.short,
            datacenter=self._node_id.datacenter,
        )

        self._registry.set_worker_pool(self._worker_pool)

        # Workflow lifecycle state machine (AD-33)
        self._workflow_lifecycle = ManagerWorkflowLifecycle(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Rate limiting (AD-24)
        self._rate_limiter = ServerRateLimiter(inactive_cleanup_seconds=300.0)

        # Stats buffer (AD-23)
        self._stats_buffer = StatsBuffer(
            StatsBufferConfig(
                hot_max_entries=self._config.stats_hot_max_entries,
                throttle_threshold=self._config.stats_throttle_threshold,
                batch_threshold=self._config.stats_batch_threshold,
                reject_threshold=self._config.stats_reject_threshold,
            )
        )

        # Windowed stats collector
        self._windowed_stats = WindowedStatsCollector(
            window_size_ms=self._config.stats_window_size_ms,
            drift_tolerance_ms=self._config.stats_drift_tolerance_ms,
            max_window_age_ms=self._config.stats_max_window_age_ms,
        )

        # Worker health manager (AD-26)
        self._worker_health_manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                base_deadline=30.0,
                min_grant=1.0,
                max_extensions=5,
                eviction_threshold=3,
            )
        )

        # WorkflowDispatcher (initialized in start())
        self._workflow_dispatcher: WorkflowDispatcher | None = None

        # WorkflowLifecycleStateMachine (initialized in start())
        self._workflow_lifecycle_states: WorkflowLifecycleStateMachine | None = None

        # WorkerDisseminator (AD-48, initialized in start())
        self._worker_disseminator: "WorkerDisseminator | None" = None

        # Federated health monitor for gate probing
        fed_config = self._env.get_federated_health_config()
        self._gate_health_monitor = FederatedHealthMonitor(
            probe_interval=fed_config["probe_interval"],
            probe_timeout=fed_config["probe_timeout"],
            suspicion_timeout=fed_config["suspicion_timeout"],
            max_consecutive_failures=fed_config["max_consecutive_failures"],
        )

        # Gate circuit breaker
        cb_config = self._env.get_circuit_breaker_config()
        self._gate_circuit = ErrorStats(
            max_errors=cb_config["max_errors"],
            window_seconds=cb_config["window_seconds"],
            half_open_after=cb_config["half_open_after"],
        )

        # Quorum circuit breaker
        self._quorum_circuit = ErrorStats(
            window_seconds=30.0,
            max_errors=3,
            half_open_after=10.0,
        )

        # Recovery semaphore
        self._recovery_semaphore = asyncio.Semaphore(
            self._config.recovery_max_concurrent
        )

        # Role validator for mTLS
        self._role_validator = RoleValidator(
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
            strict_mode=self._config.mtls_strict_mode,
        )

        # Protocol capabilities
        self._node_capabilities = NodeCapabilities.current(node_version="")

        # Background tasks
        self._dead_node_reap_task: asyncio.Task | None = None
        self._orphan_scan_task: asyncio.Task | None = None
        self._discovery_maintenance_task: asyncio.Task | None = None
        self._job_responsiveness_task: asyncio.Task | None = None
        self._stats_push_task: asyncio.Task | None = None
        self._gate_heartbeat_task: asyncio.Task | None = None
        self._rate_limit_cleanup_task: asyncio.Task | None = None
        self._job_cleanup_task: asyncio.Task | None = None
        self._unified_timeout_task: asyncio.Task | None = None
        self._deadline_enforcement_task: asyncio.Task | None = None
        self._peer_job_state_sync_task: asyncio.Task | None = None

    def _init_address_mappings(self) -> None:
        """Initialize UDP to TCP address mappings."""
        # Gate UDP to TCP mapping
        for idx, tcp_addr in enumerate(self._seed_gates):
            if idx < len(self._gate_udp_addrs):
                self._manager_state._gate_udp_to_tcp[self._gate_udp_addrs[idx]] = (
                    tcp_addr
                )

        # Manager UDP to TCP mapping
        for idx, tcp_addr in enumerate(self._seed_managers):
            if idx < len(self._manager_udp_peers):
                self._manager_state._manager_udp_to_tcp[
                    self._manager_udp_peers[idx]
                ] = tcp_addr

    def _register_callbacks(self) -> None:
        """Register SWIM and leadership callbacks."""
        self.register_on_become_leader(self._on_manager_become_leader)
        self.register_on_lose_leadership(self._on_manager_lose_leadership)
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
        self.register_on_peer_confirmed(self._on_peer_confirmed)

        # Initialize hierarchical failure detector (AD-30)
        self.init_hierarchical_detector(
            config=HierarchicalConfig(
                global_min_timeout=10.0,
                global_max_timeout=60.0,
                job_min_timeout=2.0,
                job_max_timeout=15.0,
            ),
            on_global_death=self._on_worker_globally_dead,
            on_job_death=self._on_worker_dead_for_job,
            get_job_n_members=self._get_job_worker_count,
        )

        # Set state embedder
        self.set_state_embedder(self._create_state_embedder())

    def _create_state_embedder(self) -> ManagerStateEmbedder:
        """Create state embedder for SWIM heartbeat embedding."""
        return ManagerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_datacenter=lambda: self._node_id.datacenter,
            is_leader=self.is_leader,
            get_term=lambda: self._leader_election.state.current_term,
            get_state_version=lambda: self._manager_state._state_version,
            get_active_jobs=lambda: self._job_manager.job_count,
            get_active_workflows=self._get_active_workflow_count,
            get_worker_count=lambda: len(self._manager_state._workers),
            get_healthy_worker_count=lambda: len(
                self._registry.get_healthy_worker_ids()
            ),
            get_available_cores=self._get_available_cores_for_healthy_workers,
            get_total_cores=self._get_total_cores,
            on_worker_heartbeat=self._handle_embedded_worker_heartbeat,
            on_manager_heartbeat=self._handle_manager_peer_heartbeat,
            on_gate_heartbeat=self._handle_gate_heartbeat,
            get_manager_state=lambda: self._manager_state._manager_state.value,
            get_tcp_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_udp_host=lambda: self._host,
            get_udp_port=lambda: self._udp_port,
            get_health_accepting_jobs=lambda: self._manager_state._manager_state
            == ManagerStateEnum.ACTIVE,
            get_health_has_quorum=self._has_quorum_available,
            get_health_throughput=self._get_dispatch_throughput,
            get_health_expected_throughput=self._get_expected_dispatch_throughput,
            get_health_overload_state=lambda: self._manager_health_state,
            get_current_gate_leader_id=lambda: self._manager_state._current_gate_leader_id,
            get_current_gate_leader_host=lambda: (
                self._manager_state._current_gate_leader_addr[0]
                if self._manager_state._current_gate_leader_addr
                else None
            ),
            get_current_gate_leader_port=lambda: (
                self._manager_state._current_gate_leader_addr[1]
                if self._manager_state._current_gate_leader_addr
                else None
            ),
            get_known_gates=self._get_known_gates_for_heartbeat,
            get_job_leaderships=self._get_job_leaderships_for_heartbeat,
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def node_info(self) -> NodeInfo:
        """Get this manager's node info."""
        return NodeInfo(
            node_id=self._node_id.full,
            role=NodeRole.MANAGER.value,
            host=self._host,
            port=self._tcp_port,
            datacenter=self._node_id.datacenter,
            version=self._manager_state._state_version,
            udp_port=self._udp_port,
        )

    @property
    def _quorum_size(self) -> int:
        """Calculate required quorum size."""
        total_managers = len(self._manager_state._active_manager_peers) + 1
        return (total_managers // 2) + 1

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    async def start(self, timeout: float | None = None) -> None:
        """Start the manager server."""
        # Initialize locks (requires async context)
        self._manager_state.initialize_locks()

        # Start the underlying server
        await self.start_server(init_context=self._env.get_swim_init_context())

        if self._config.wal_data_dir is not None:
            wal_clock = HybridLamportClock(node_id=hash(self._node_id.full) & 0xFFFF)
            self._node_wal = await NodeWAL.open(
                path=self._config.wal_data_dir / "wal",
                clock=wal_clock,
                logger=self._udp_logger,
            )

        # Update node capabilities with proper version
        self._node_capabilities = NodeCapabilities.current(
            node_version=f"manager-{self._node_id.short}"
        )

        # Initialize workflow lifecycle state machine (AD-33)
        self._workflow_lifecycle_states = WorkflowLifecycleStateMachine()

        self._workflow_dispatcher = WorkflowDispatcher(
            job_manager=self._job_manager,
            worker_pool=self._worker_pool,
            manager_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            send_dispatch=self._send_workflow_dispatch,
            env=self.env,
        )

        self._worker_disseminator = WorkerDisseminator(
            state=self._manager_state,
            config=self._config,
            worker_pool=self._worker_pool,
            logger=self._udp_logger,
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            task_runner=self._task_runner,
            send_tcp=self._send_to_peer,
            gossip_buffer=WorkerStateGossipBuffer(),
        )

        # Mark as started
        self._started = True
        self._manager_state._manager_state = ManagerStateEnum.ACTIVE

        # Register with seed managers
        await self._register_with_peer_managers()

        # Join SWIM clusters
        await self._join_swim_clusters()

        # Request worker lists from peer managers (AD-48)
        if self._worker_disseminator:
            await self._worker_disseminator.request_worker_list_from_peers()

        # Start SWIM probe cycle
        self._task_runner.run(self.start_probe_cycle)

        # Start background tasks
        self._start_background_tasks()

        manager_count = len(self._manager_state._known_manager_peers) + 1
        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager started, {manager_count} managers in cluster",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def stop(
        self,
        drain_timeout: float = 5,
        broadcast_leave: bool = True,
    ) -> None:
        """Stop the manager server."""
        if not self._running and not hasattr(self, "_started"):
            return

        self._running = False
        self._manager_state._manager_state = ManagerStateEnum.DRAINING

        # Cancel background tasks
        await self._cancel_background_tasks()

        if self._node_wal is not None:
            await self._node_wal.close()

        # Graceful shutdown
        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )

    def abort(self) -> None:
        """Abort the manager server immediately."""
        self._running = False
        self._manager_state._manager_state = ManagerStateEnum.OFFLINE

        # Cancel all background tasks synchronously
        for task in self._get_background_tasks():
            if task and not task.done():
                task.cancel()

        super().abort()

    def _get_background_tasks(self) -> list[asyncio.Task | None]:
        """Get list of background tasks."""
        return [
            self._dead_node_reap_task,
            self._orphan_scan_task,
            self._discovery_maintenance_task,
            self._job_responsiveness_task,
            self._stats_push_task,
            self._gate_heartbeat_task,
            self._rate_limit_cleanup_task,
            self._job_cleanup_task,
            self._unified_timeout_task,
            self._deadline_enforcement_task,
            self._peer_job_state_sync_task,
        ]

    def _start_background_tasks(self) -> None:
        self._dead_node_reap_task = self._create_background_task(
            self._dead_node_reap_loop(), "dead_node_reap"
        )
        self._orphan_scan_task = self._create_background_task(
            self._orphan_scan_loop(), "orphan_scan"
        )
        self._discovery_maintenance_task = self._create_background_task(
            self._discovery.maintenance_loop(), "discovery_maintenance"
        )
        self._job_responsiveness_task = self._create_background_task(
            self._job_responsiveness_loop(), "job_responsiveness"
        )
        self._stats_push_task = self._create_background_task(
            self._stats_push_loop(), "stats_push"
        )
        self._gate_heartbeat_task = self._create_background_task(
            self._gate_heartbeat_loop(), "gate_heartbeat"
        )
        self._rate_limit_cleanup_task = self._create_background_task(
            self._rate_limit_cleanup_loop(), "rate_limit_cleanup"
        )
        self._job_cleanup_task = self._create_background_task(
            self._job_cleanup_loop(), "job_cleanup"
        )
        self._unified_timeout_task = self._create_background_task(
            self._unified_timeout_loop(), "unified_timeout"
        )
        self._deadline_enforcement_task = self._create_background_task(
            self._deadline_enforcement_loop(), "deadline_enforcement"
        )
        self._peer_job_state_sync_task = self._create_background_task(
            self._peer_job_state_sync_loop(), "peer_job_state_sync"
        )
        self._resource_sample_task = self._create_background_task(
            self._resource_sample_loop(), "resource_sample"
        )

    async def _cancel_background_tasks(self) -> None:
        """Cancel all background tasks."""
        for task in self._get_background_tasks():
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    # =========================================================================
    # Registration
    # =========================================================================

    async def _register_with_peer_managers(self) -> None:
        """Register with seed peer managers."""
        for seed_addr in self._seed_managers:
            try:
                await self._register_with_manager(seed_addr)
            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to register with peer manager {seed_addr}: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _register_with_manager(self, manager_addr: tuple[str, int]) -> bool:
        """Register with a single peer manager."""
        registration = ManagerPeerRegistration(
            node=self.node_info,
            manager_info=ManagerInfo(
                node_id=self._node_id.full,
                tcp_host=self._host,
                tcp_port=self._tcp_port,
                udp_host=self._host,
                udp_port=self._udp_port,
                datacenter=self._node_id.datacenter,
                is_leader=self.is_leader(),
            ),
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
        )

        try:
            response = await self.send_tcp(
                manager_addr,
                "manager_peer_register",
                registration.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )

            if response and not isinstance(response, Exception):
                parsed = ManagerPeerRegistrationResponse.load(response)
                if parsed.accepted:
                    self._registry.register_manager_peer(parsed.manager_info)
                    return True

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Manager registration error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        return False

    async def _join_swim_clusters(self) -> None:
        """Join SWIM clusters for managers, gates, and workers."""
        # Join manager SWIM cluster
        for udp_addr in self._manager_udp_peers:
            await self.join_cluster(udp_addr)

        # Join gate SWIM cluster if gates configured
        for udp_addr in self._gate_udp_addrs:
            await self.join_cluster(udp_addr)

    # =========================================================================
    # SWIM Callbacks
    # =========================================================================

    def _on_peer_confirmed(self, peer: tuple[str, int]) -> None:
        """Handle peer confirmation via SWIM (AD-29)."""
        # Check if manager peer
        tcp_addr = self._manager_state._manager_udp_to_tcp.get(peer)
        if tcp_addr:
            for peer_id, peer_info in self._manager_state._known_manager_peers.items():
                if (peer_info.udp_host, peer_info.udp_port) == peer:
                    self._manager_state._active_manager_peer_ids.add(peer_id)
                    self._manager_state._active_manager_peers.add(tcp_addr)
                    break

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """Handle node death detected by SWIM."""
        # Check if worker
        worker_id = self._manager_state._worker_addr_to_id.get(node_addr)
        if worker_id:
            if worker_id not in self._manager_state._worker_unhealthy_since:
                self._manager_state._worker_unhealthy_since[worker_id] = (
                    time.monotonic()
                )
            self._task_runner.run(self._handle_worker_failure, worker_id)
            return

        # Check if manager peer
        manager_tcp_addr = self._manager_state._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            self._manager_state._dead_managers.add(manager_tcp_addr)
            self._task_runner.run(
                self._handle_manager_peer_failure, node_addr, manager_tcp_addr
            )
            return

        # Check if gate
        gate_tcp_addr = self._manager_state._gate_udp_to_tcp.get(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(
                self._handle_gate_peer_failure, node_addr, gate_tcp_addr
            )

    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        """Handle node join detected by SWIM."""
        # Check if worker
        worker_id = self._manager_state._worker_addr_to_id.get(node_addr)
        if worker_id:
            self._manager_state._worker_unhealthy_since.pop(worker_id, None)
            return

        # Check if manager peer
        manager_tcp_addr = self._manager_state._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            self._manager_state._dead_managers.discard(manager_tcp_addr)
            self._task_runner.run(
                self._handle_manager_peer_recovery, node_addr, manager_tcp_addr
            )
            return

        # Check if gate
        gate_tcp_addr = self._manager_state._gate_udp_to_tcp.get(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(
                self._handle_gate_peer_recovery, node_addr, gate_tcp_addr
            )

    def _on_manager_become_leader(self) -> None:
        """Handle becoming SWIM cluster leader."""
        self._task_runner.run(self._sync_state_from_workers)
        self._task_runner.run(self._sync_state_from_manager_peers)
        self._task_runner.run(self._scan_for_orphaned_jobs)
        self._task_runner.run(self._resume_timeout_tracking_for_all_jobs)

    def _on_manager_lose_leadership(self) -> None:
        """Handle losing SWIM cluster leadership."""
        pass

    def _on_worker_globally_dead(self, worker_id: str) -> None:
        """Handle worker global death (AD-30)."""
        self._health_monitor.on_global_death(worker_id)
        if self._worker_disseminator:
            self._task_runner.run(
                self._worker_disseminator.broadcast_worker_dead, worker_id, "dead"
            )

    def _on_worker_dead_for_job(self, job_id: str, worker_id: str) -> None:
        """Handle worker death for specific job (AD-30)."""
        # This would trigger workflow reschedule
        pass

    # =========================================================================
    # Failure/Recovery Handlers
    # =========================================================================

    async def _handle_worker_failure(self, worker_id: str) -> None:
        """Handle worker failure."""
        self._health_monitor.handle_worker_failure(worker_id)

        # Trigger workflow retry for workflows on this worker
        # Implementation delegated to workflow lifecycle coordinator

    async def _handle_manager_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle manager peer failure."""
        peer_lock = await self._manager_state.get_peer_state_lock(tcp_addr)
        async with peer_lock:
            self._manager_state._peer_state_epoch[tcp_addr] = (
                self._manager_state._peer_state_epoch.get(tcp_addr, 0) + 1
            )
            self._manager_state._active_manager_peers.discard(tcp_addr)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager peer {tcp_addr} marked DEAD",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Handle job leader failure
        await self._handle_job_leader_failure(tcp_addr)

    async def _handle_manager_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle manager peer recovery."""
        peer_lock = await self._manager_state.get_peer_state_lock(tcp_addr)

        async with peer_lock:
            initial_epoch = self._manager_state._peer_state_epoch.get(tcp_addr, 0)

        async with self._recovery_semaphore:
            jitter = random.uniform(
                self._config.recovery_jitter_min_seconds,
                self._config.recovery_jitter_max_seconds,
            )
            await asyncio.sleep(jitter)

            async with peer_lock:
                current_epoch = self._manager_state._peer_state_epoch.get(tcp_addr, 0)
                if current_epoch != initial_epoch:
                    return

                self._manager_state._active_manager_peers.add(tcp_addr)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager peer {tcp_addr} REJOINED",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _handle_gate_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer failure."""
        # Find gate by address
        gate_node_id = None
        for gate_id, gate_info in self._manager_state._known_gates.items():
            if (gate_info.tcp_host, gate_info.tcp_port) == tcp_addr:
                gate_node_id = gate_id
                break

        if gate_node_id:
            self._registry.mark_gate_unhealthy(gate_node_id)

            if self._manager_state._primary_gate_id == gate_node_id:
                self._manager_state._primary_gate_id = None
                for healthy_id in self._manager_state._healthy_gate_ids:
                    self._manager_state._primary_gate_id = healthy_id
                    break

    async def _handle_gate_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer recovery."""
        for gate_id, gate_info in self._manager_state._known_gates.items():
            if (gate_info.tcp_host, gate_info.tcp_port) == tcp_addr:
                self._registry.mark_gate_healthy(gate_id)
                break

    async def _handle_job_leader_failure(self, failed_addr: tuple[str, int]) -> None:
        """Handle job leader manager failure."""
        if not self.is_leader():
            return

        # Find jobs led by the failed manager and take them over
        jobs_to_takeover = []
        for job_id, leader_addr in self._manager_state._job_leader_addrs.items():
            if leader_addr == failed_addr:
                jobs_to_takeover.append(job_id)

        for job_id in jobs_to_takeover:
            self._leases.claim_job_leadership(job_id, (self._host, self._tcp_port))
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Took over leadership for job {job_id[:8]}...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    # =========================================================================
    # Heartbeat Handlers
    # =========================================================================

    async def _handle_embedded_worker_heartbeat(
        self,
        heartbeat: WorkerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """Handle embedded worker heartbeat from SWIM."""
        self._health_monitor.handle_worker_heartbeat(heartbeat, source_addr)

        # Update worker pool if worker is registered
        worker_id = heartbeat.node_id
        if worker_id in self._manager_state._workers:
            self._worker_pool.update_worker_capacity(
                worker_id=worker_id,
                available_cores=heartbeat.available_cores,
                queue_depth=heartbeat.queue_depth,
            )

    async def _handle_manager_peer_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        peer_id = heartbeat.node_id

        if peer_id not in self._manager_state._known_manager_peers:
            peer_info = ManagerInfo(
                node_id=peer_id,
                tcp_host=heartbeat.tcp_host or source_addr[0],
                tcp_port=heartbeat.tcp_port or source_addr[1] - 1,
                udp_host=source_addr[0],
                udp_port=source_addr[1],
                datacenter=heartbeat.datacenter,
                is_leader=heartbeat.is_leader,
            )
            self._registry.register_manager_peer(peer_info)

        peer_health_state = getattr(heartbeat, "health_overload_state", "healthy")
        previous_peer_state = self._manager_state._peer_manager_health_states.get(
            peer_id
        )
        self._manager_state._peer_manager_health_states[peer_id] = peer_health_state

        if previous_peer_state and previous_peer_state != peer_health_state:
            self._log_peer_manager_health_transition(
                peer_id, previous_peer_state, peer_health_state
            )

        self.confirm_peer(source_addr)

    async def _handle_gate_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """Handle embedded gate heartbeat from SWIM."""
        gate_id = heartbeat.node_id

        # Register gate if not known
        if gate_id not in self._manager_state._known_gates:
            gate_info = GateInfo(
                node_id=gate_id,
                tcp_host=heartbeat.tcp_host or source_addr[0],
                tcp_port=heartbeat.tcp_port or source_addr[1] - 1,
                udp_host=source_addr[0],
                udp_port=source_addr[1],
                datacenter=heartbeat.datacenter,
                is_leader=heartbeat.is_leader,
            )
            self._registry.register_gate(gate_info)

        # Update gate leader tracking
        if heartbeat.is_leader:
            self._manager_state._current_gate_leader_id = gate_id
            gate_info = self._manager_state._known_gates.get(gate_id)
            if gate_info:
                self._manager_state._current_gate_leader_addr = (
                    gate_info.tcp_host,
                    gate_info.tcp_port,
                )

        # Confirm peer
        self.confirm_peer(source_addr)

    # =========================================================================
    # Background Loops
    # =========================================================================

    async def _dead_node_reap_loop(self) -> None:
        """Periodically reap dead nodes."""
        while self._running:
            try:
                await asyncio.sleep(self._config.dead_node_check_interval_seconds)

                now = time.monotonic()

                # Reap dead workers
                worker_reap_threshold = (
                    now - self._config.dead_worker_reap_interval_seconds
                )
                workers_to_reap = [
                    worker_id
                    for worker_id, unhealthy_since in self._manager_state._worker_unhealthy_since.items()
                    if unhealthy_since < worker_reap_threshold
                ]
                for worker_id in workers_to_reap:
                    self._registry.unregister_worker(worker_id)

                # Reap dead peers
                peer_reap_threshold = now - self._config.dead_peer_reap_interval_seconds
                peers_to_reap = [
                    peer_id
                    for peer_id, unhealthy_since in self._manager_state._manager_peer_unhealthy_since.items()
                    if unhealthy_since < peer_reap_threshold
                ]
                for peer_id in peers_to_reap:
                    self._registry.unregister_manager_peer(peer_id)

                # Reap dead gates
                gate_reap_threshold = now - self._config.dead_gate_reap_interval_seconds
                gates_to_reap = [
                    gate_id
                    for gate_id, unhealthy_since in self._manager_state._gate_unhealthy_since.items()
                    if unhealthy_since < gate_reap_threshold
                ]
                for gate_id in gates_to_reap:
                    self._registry.unregister_gate(gate_id)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Dead node reap error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _orphan_scan_loop(self) -> None:
        """
        Periodically scan for orphaned workflows.

        An orphaned workflow is one that:
        1. The manager thinks is running on a worker, but
        2. The worker no longer has it (worker restarted, crashed, etc.)

        This reconciliation ensures no workflows are "lost" due to state
        inconsistencies between manager and workers.
        """
        while self._running:
            try:
                await asyncio.sleep(self._config.orphan_scan_interval_seconds)

                if not self.is_leader():
                    continue

                # Query each worker for their active workflows
                for worker_id, worker in list(self._manager_state._workers.items()):
                    try:
                        worker_addr = (worker.node.host, worker.node.tcp_port)

                        # Request workflow query from worker
                        request = WorkflowQueryRequest(
                            requester_id=self._node_id.full,
                            query_type="active",
                        )

                        response = await self._send_to_worker(
                            worker_addr,
                            "workflow_query",
                            request.dump(),
                            timeout=self._config.orphan_scan_worker_timeout_seconds,
                        )

                        if not response or isinstance(response, Exception):
                            continue

                        # Parse response and compare with our tracking
                        query_response = WorkflowQueryResponse.load(response)
                        worker_workflow_ids = set(query_response.workflow_ids or [])

                        # Find workflows we think are on this worker
                        manager_tracked_ids: set[str] = set()
                        for job in self._job_manager.iter_jobs():
                            for wf_id, wf in job.workflows.items():
                                if (
                                    wf.worker_id == worker_id
                                    and wf.status == WorkflowStatus.RUNNING
                                ):
                                    manager_tracked_ids.add(wf_id)

                        # Workflows we track but worker doesn't have = orphaned
                        orphaned = manager_tracked_ids - worker_workflow_ids

                        for orphaned_id in orphaned:
                            await self._udp_logger.log(
                                ServerWarning(
                                    message=f"Orphaned workflow {orphaned_id[:8]}... detected on worker {worker_id[:8]}..., scheduling retry",
                                    node_host=self._host,
                                    node_port=self._tcp_port,
                                    node_id=self._node_id.short,
                                )
                            )
                            # Re-queue for dispatch
                            if self._workflow_dispatcher:
                                await self._workflow_dispatcher.requeue_workflow(
                                    orphaned_id
                                )

                    except Exception as worker_error:
                        await self._udp_logger.log(
                            ServerDebug(
                                message=f"Orphan scan for worker {worker_id[:8]}... failed: {worker_error}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Orphan scan error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _job_responsiveness_loop(self) -> None:
        """Check job responsiveness (AD-30)."""
        while self._running:
            try:
                await asyncio.sleep(
                    self._config.job_responsiveness_check_interval_seconds
                )

                # Check for expired job suspicions
                expired = self._health_monitor.check_job_suspicion_expiry()

                for job_id, worker_id in expired:
                    # Trigger workflow reschedule for expired suspicions
                    pass

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Job responsiveness check error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _stats_push_loop(self) -> None:
        """Periodically push stats to gates/clients."""
        while self._running:
            try:
                await asyncio.sleep(self._config.batch_push_interval_seconds)

                # Push aggregated stats
                await self._stats.push_batch_stats()

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Stats push error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _gate_heartbeat_loop(self) -> None:
        """
        Periodically send ManagerHeartbeat to gates via TCP.

        This supplements the Serf-style SWIM embedding for reliability.
        Gates use this for datacenter health classification.
        """
        heartbeat_interval = self._config.gate_heartbeat_interval_seconds

        await self._udp_logger.log(
            ServerInfo(
                message="Gate heartbeat loop started",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        while self._running:
            try:
                await asyncio.sleep(heartbeat_interval)

                heartbeat = self._build_manager_heartbeat()

                # Send to all healthy gates (use known gates if available, else seed gates)
                gate_addrs = self._get_healthy_gate_tcp_addrs() or self._seed_gates

                sent_count = 0
                for gate_addr in gate_addrs:
                    try:
                        response = await self.send_tcp(
                            gate_addr,
                            "manager_status_update",
                            heartbeat.dump(),
                            timeout=2.0,
                        )
                        if not isinstance(response, Exception):
                            sent_count += 1
                    except Exception as heartbeat_error:
                        await self._udp_logger.log(
                            ServerWarning(
                                message=f"Failed to send heartbeat to gate: {heartbeat_error}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

                if sent_count > 0:
                    await self._udp_logger.log(
                        ServerDebug(
                            message=f"Sent heartbeat to {sent_count}/{len(gate_addrs)} gates",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Gate heartbeat error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _rate_limit_cleanup_loop(self) -> None:
        """
        Periodically clean up inactive clients from the rate limiter.

        Removes token buckets for clients that haven't made requests
        within the inactive_cleanup_seconds window to prevent memory leaks.
        """
        cleanup_interval = self._config.rate_limit_cleanup_interval_seconds

        while self._running:
            try:
                await asyncio.sleep(cleanup_interval)

                cleaned = self._cleanup_inactive_rate_limit_clients()

                if cleaned > 0:
                    await self._udp_logger.log(
                        ServerDebug(
                            message=f"Rate limiter: cleaned up {cleaned} inactive clients",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Rate limit cleanup error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _job_cleanup_loop(self) -> None:
        """
        Periodically clean up completed/failed jobs and their associated state.

        Runs at JOB_CLEANUP_INTERVAL (default 60s).
        Jobs are eligible for cleanup when:
        - Status is COMPLETED or FAILED
        - More than JOB_RETENTION_SECONDS have elapsed since completion
        """
        cleanup_interval = self._config.job_cleanup_interval_seconds
        retention_seconds = self._config.job_retention_seconds

        while self._running:
            try:
                await asyncio.sleep(cleanup_interval)

                current_time = time.monotonic()
                jobs_cleaned = 0

                for job in list(self._job_manager.iter_jobs()):
                    if job.status in (
                        JobStatus.COMPLETED,
                        JobStatus.FAILED,
                        JobStatus.CANCELLED,
                    ):
                        if (
                            job.completed_at
                            and (current_time - job.completed_at) > retention_seconds
                        ):
                            self._cleanup_job(job.job_id)
                            jobs_cleaned += 1

                if jobs_cleaned > 0:
                    await self._udp_logger.log(
                        ServerInfo(
                            message=f"Cleaned up {jobs_cleaned} completed jobs",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Job cleanup error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _unified_timeout_loop(self) -> None:
        """
        Background task that checks for job timeouts (AD-34 Part 10.4.3).

        Runs at JOB_TIMEOUT_CHECK_INTERVAL (default 30s). Only leader checks timeouts.
        Delegates to strategy.check_timeout() which handles both:
        - Extension-aware timeout (base_timeout + extensions)
        - Stuck detection (no progress for 2+ minutes)
        """
        check_interval = self._config.job_timeout_check_interval_seconds

        while self._running:
            try:
                await asyncio.sleep(check_interval)

                # Only leader checks timeouts
                if not self.is_leader():
                    continue

                for job_id, strategy in list(
                    self._manager_state._job_timeout_strategies.items()
                ):
                    try:
                        timed_out, reason = await strategy.check_timeout(job_id)
                        if timed_out:
                            await self._udp_logger.log(
                                ServerWarning(
                                    message=f"Job {job_id[:8]}... timed out: {reason}",
                                    node_host=self._host,
                                    node_port=self._tcp_port,
                                    node_id=self._node_id.short,
                                )
                            )
                            # Cancel the job due to timeout
                            job = self._job_manager.get_job(job_id)
                            if job and job.status not in (
                                JobStatus.COMPLETED,
                                JobStatus.FAILED,
                                JobStatus.CANCELLED,
                            ):
                                job.status = JobStatus.FAILED
                                await self._manager_state.increment_state_version()
                    except Exception as check_error:
                        await self._udp_logger.log(
                            ServerError(
                                message=f"Timeout check error for job {job_id[:8]}...: {check_error}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Unified timeout loop error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _deadline_enforcement_loop(self) -> None:
        """
        Background loop for worker deadline enforcement (AD-26 Issue 2).

        Checks worker deadlines every 5 seconds and takes action:
        - If deadline expired but within grace period: mark worker as SUSPECTED
        - If deadline expired beyond grace period: evict worker
        """
        check_interval = 5.0

        while self._running:
            try:
                await asyncio.sleep(check_interval)

                current_time = time.monotonic()
                grace_period = self._worker_health_manager._config.base_deadline

                deadlines_snapshot = list(self._manager_state._worker_deadlines.items())

                for worker_id, deadline in deadlines_snapshot:
                    if current_time <= deadline:
                        continue

                    time_since_deadline = current_time - deadline

                    if time_since_deadline <= grace_period:
                        await self._suspect_worker_deadline_expired(worker_id)
                    else:
                        await self._evict_worker_deadline_expired(worker_id)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Deadline enforcement error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _peer_job_state_sync_loop(self) -> None:
        """
        Background loop for periodic job state sync to peer managers.

        Syncs job state (leadership, fencing tokens, context versions)
        to ensure consistency across manager cluster.
        """
        sync_interval = self._config.peer_job_sync_interval_seconds

        while self._running:
            try:
                await asyncio.sleep(sync_interval)

                if not self.is_leader():
                    continue

                led_jobs = self._leases.get_led_job_ids()
                if not led_jobs:
                    continue

                for peer_addr in self._manager_state._active_manager_peers:
                    try:
                        sync_msg = JobStateSyncMessage(
                            source_id=self._node_id.full,
                            job_leaderships={
                                job_id: self._node_id.full for job_id in led_jobs
                            },
                            fence_tokens={
                                job_id: self._manager_state._job_fencing_tokens.get(
                                    job_id, 0
                                )
                                for job_id in led_jobs
                            },
                            state_version=self._manager_state._state_version,
                        )

                        await self._send_to_peer(
                            peer_addr,
                            "job_state_sync",
                            sync_msg.dump(),
                            timeout=2.0,
                        )
                    except Exception as sync_error:
                        await self._udp_logger.log(
                            ServerWarning(
                                message=f"Failed to sync job state to peer: {sync_error}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Peer job state sync error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _resource_sample_loop(self) -> None:
        """
        Background loop for periodic CPU/memory sampling.

        Samples manager's own resource usage and feeds to HybridOverloadDetector
        for overload state classification. Runs at 1s cadence for responsive
        detection while balancing overhead.
        """
        sample_interval = 1.0

        while self._running:
            try:
                await asyncio.sleep(sample_interval)

                metrics = await self._resource_monitor.sample()
                self._last_resource_metrics = metrics

                new_state = self._overload_detector.get_state(
                    metrics.cpu_percent,
                    metrics.memory_percent,
                )
                new_state_str = new_state.value

                if new_state_str != self._manager_health_state:
                    self._previous_manager_health_state = self._manager_health_state
                    self._manager_health_state = new_state_str
                    self._log_manager_health_transition(
                        self._previous_manager_health_state,
                        new_state_str,
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Resource sampling error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    def _log_manager_health_transition(
        self,
        previous_state: str,
        new_state: str,
    ) -> None:
        """Log manager health state transitions."""
        state_severity = {"healthy": 0, "busy": 1, "stressed": 2, "overloaded": 3}
        previous_severity = state_severity.get(previous_state, 0)
        new_severity = state_severity.get(new_state, 0)
        is_degradation = new_severity > previous_severity

        if is_degradation:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Manager health degraded: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
        else:
            self._task_runner.run(
                self._udp_logger.log,
                ServerDebug(
                    message=f"Manager health improved: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    def get_manager_health_state(self) -> str:
        return self._manager_health_state

    def _log_peer_manager_health_transition(
        self,
        peer_id: str,
        previous_state: str,
        new_state: str,
    ) -> None:
        state_severity = {"healthy": 0, "busy": 1, "stressed": 2, "overloaded": 3}
        previous_severity = state_severity.get(previous_state, 0)
        new_severity = state_severity.get(new_state, 0)
        is_degradation = new_severity > previous_severity

        if is_degradation:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Peer manager {peer_id[:8]}... health degraded: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
        else:
            self._task_runner.run(
                self._udp_logger.log,
                ServerDebug(
                    message=f"Peer manager {peer_id[:8]}... health improved: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    def get_peer_manager_health_states(self) -> dict[str, str]:
        return dict(self._manager_state._peer_manager_health_states)

    # =========================================================================
    # State Sync
    # =========================================================================

    async def _sync_state_from_workers(self) -> None:
        """Sync state from all workers."""
        for worker_id, worker in self._manager_state._workers.items():
            try:
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_version=self._manager_state._state_version,
                )

                worker_addr = (worker.node.host, worker.node.tcp_port)
                response = await self.send_tcp(
                    worker_addr,
                    "state_sync_request",
                    request.dump(),
                    timeout=self._config.state_sync_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    # Process worker state
                    pass

            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"State sync from worker {worker_id[:8]}... failed: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _sync_state_from_manager_peers(self) -> None:
        """Sync state from peer managers."""
        for peer_addr in self._manager_state._active_manager_peers:
            try:
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_version=self._manager_state._state_version,
                )

                response = await self.send_tcp(
                    peer_addr,
                    "manager_state_sync_request",
                    request.dump(),
                    timeout=self._config.state_sync_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    # Process peer state
                    pass

            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"State sync from peer {peer_addr} failed: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _scan_for_orphaned_jobs(self) -> None:
        """Scan for orphaned jobs from dead managers."""
        for dead_addr in self._manager_state._dead_managers:
            jobs_to_takeover = [
                job_id
                for job_id, leader_addr in self._manager_state._job_leader_addrs.items()
                if leader_addr == dead_addr
            ]

            for job_id in jobs_to_takeover:
                self._leases.claim_job_leadership(job_id, (self._host, self._tcp_port))

    async def _resume_timeout_tracking_for_all_jobs(self) -> None:
        """Resume timeout tracking for all jobs as new leader."""
        for job_id in self._leases.get_led_job_ids():
            # Re-initialize timeout strategy if needed
            pass

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _get_swim_status_for_worker(self, worker_id: str) -> str:
        """Get SWIM status for a worker."""
        if worker_id in self._manager_state._worker_unhealthy_since:
            return "unhealthy"
        return "healthy"

    def _get_active_workflow_count(self) -> int:
        """Get count of active workflows."""
        return sum(
            len(
                [
                    w
                    for w in job.workflows.values()
                    if w.status == WorkflowStatus.RUNNING
                ]
            )
            for job in self._job_manager.iter_jobs()
        )

    def _get_available_cores_for_healthy_workers(self) -> int:
        """Get total available cores across healthy workers.

        Uses WorkerPool which tracks real-time worker capacity from heartbeats,
        rather than stale WorkerRegistration data from initial registration.
        """
        return self._worker_pool.get_total_available_cores()

    def _get_total_cores(self) -> int:
        """Get total cores across all workers."""
        return sum(w.total_cores for w in self._manager_state._workers.values())

    def _get_job_worker_count(self, job_id: str) -> int:
        """Get number of workers for a job."""
        job = self._job_manager.get_job(job_id)
        if job:
            return len(job.workers)
        return 0

    def _has_quorum_available(self) -> bool:
        """Check if quorum is available."""
        active_count = len(self._manager_state._active_manager_peers) + 1
        return active_count >= self._quorum_size

    def _get_dispatch_throughput(self) -> float:
        """Get current dispatch throughput."""
        current_time = time.monotonic()
        elapsed = current_time - self._manager_state._dispatch_throughput_interval_start

        if elapsed >= self._config.throughput_interval_seconds:
            if elapsed > 0:
                self._manager_state._dispatch_throughput_last_value = (
                    self._manager_state._dispatch_throughput_count / elapsed
                )
            self._manager_state._dispatch_throughput_count = 0
            self._manager_state._dispatch_throughput_interval_start = current_time
            return self._manager_state._dispatch_throughput_last_value

        if elapsed > 0:
            return self._manager_state._dispatch_throughput_count / elapsed
        return self._manager_state._dispatch_throughput_last_value

    def _get_expected_dispatch_throughput(self) -> float:
        """Get expected dispatch throughput."""
        worker_count = len(self._registry.get_healthy_worker_ids())
        if worker_count == 0:
            return 0.0
        # Assume 1 workflow per second per worker as baseline
        return float(worker_count)

    def _get_known_gates_for_heartbeat(self) -> list[GateInfo]:
        """Get known gates for heartbeat embedding."""
        return list(self._manager_state._known_gates.values())

    def _get_job_leaderships_for_heartbeat(self) -> list[str]:
        """Get job leaderships for heartbeat embedding."""
        return self._leases.get_led_job_ids()

    async def _check_rate_limit_for_operation(
        self,
        client_id: str,
        operation: str,
    ) -> tuple[bool, float]:
        """
        Check if a client request is within rate limits for a specific operation.

        Args:
            client_id: Identifier for the client (typically addr as string)
            operation: Type of operation being performed

        Returns:
            Tuple of (allowed, retry_after_seconds). If not allowed,
            retry_after_seconds indicates when client can retry.
        """
        result = await self._rate_limiter.check_rate_limit(client_id, operation)
        return result.allowed, result.retry_after_seconds

    def _get_rate_limit_metrics(self) -> dict:
        """Get rate limiting metrics for monitoring."""
        return self._rate_limiter.get_metrics()

    def _cleanup_inactive_rate_limit_clients(self) -> int:
        """
        Clean up inactive clients from rate limiter.

        Returns:
            Number of clients cleaned up
        """
        return self._rate_limiter.cleanup_inactive_clients()

    def _build_cancel_response(
        self,
        job_id: str,
        success: bool,
        error: str | None = None,
        cancelled_count: int = 0,
        already_cancelled: bool = False,
        already_completed: bool = False,
    ) -> bytes:
        """Build cancel response in AD-20 format."""
        return JobCancelResponse(
            job_id=job_id,
            success=success,
            error=error,
            cancelled_workflow_count=cancelled_count,
            already_cancelled=already_cancelled,
            already_completed=already_completed,
        ).dump()

    def _build_manager_heartbeat(self) -> ManagerHeartbeat:
        health_state_counts = self._health_monitor.get_worker_health_state_counts()
        return ManagerHeartbeat(
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
            state=self._manager_state._manager_state.value,
            worker_count=len(self._manager_state._workers),
            healthy_worker_count=len(self._registry.get_healthy_worker_ids()),
            available_cores=self._get_available_cores_for_healthy_workers(),
            total_cores=self._get_total_cores(),
            active_job_count=self._job_manager.job_count,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            udp_host=self._host,
            udp_port=self._udp_port,
            overloaded_worker_count=health_state_counts.get("overloaded", 0),
            stressed_worker_count=health_state_counts.get("stressed", 0),
            busy_worker_count=health_state_counts.get("busy", 0),
            health_overload_state=self._manager_health_state,
        )

    def _get_healthy_gate_tcp_addrs(self) -> list[tuple[str, int]]:
        """Get TCP addresses of healthy gates."""
        return [
            (gate.tcp_host, gate.tcp_port)
            for gate_id, gate in self._manager_state._known_gates.items()
            if gate_id in self._manager_state._healthy_gate_ids
        ]

    def _get_worker_state_piggyback(self, max_size: int) -> bytes:
        if self._worker_disseminator is None:
            return b""
        return self._worker_disseminator.get_gossip_buffer().encode_piggyback(
            max_count=5,
            max_size=max_size,
        )

    async def _process_worker_state_piggyback(
        self,
        piggyback_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        if self._worker_disseminator is None:
            return

        updates = WorkerStateGossipBuffer.decode_piggyback(piggyback_data)
        for update in updates:
            await self._worker_disseminator.handle_worker_state_update(
                update, source_addr
            )

    async def _push_cancellation_complete_to_origin(
        self,
        job_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """Push cancellation complete notification to origin gate/client."""
        callback_addr = self._manager_state._job_callbacks.get(job_id)
        if not callback_addr:
            callback_addr = self._manager_state._client_callbacks.get(job_id)

        if callback_addr:
            try:
                from hyperscale.distributed.models import JobCancellationComplete

                notification = JobCancellationComplete(
                    job_id=job_id,
                    success=success,
                    errors=errors,
                )
                await self._send_to_client(
                    callback_addr,
                    "job_cancellation_complete",
                    notification.dump(),
                )
            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to push cancellation complete to {callback_addr}: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _notify_timeout_strategies_of_extension(
        self,
        worker_id: str,
        extension_seconds: float,
        worker_progress: float,
    ) -> None:
        """Notify timeout strategies of worker extension (AD-34 Part 10.4.7)."""
        # Find jobs with workflows on this worker
        for job in self._job_manager.iter_jobs():
            if worker_id in job.workers:
                strategy = self._manager_state._job_timeout_strategies.get(job.job_id)
                if strategy and hasattr(strategy, "record_extension"):
                    await strategy.record_extension(
                        job_id=job.job_id,
                        worker_id=worker_id,
                        extension_seconds=extension_seconds,
                    )

    def _select_timeout_strategy(self, submission: JobSubmission) -> TimeoutStrategy:
        """
        Auto-detect timeout strategy based on deployment type (AD-34 Part 10.4.2).

        Single-DC (no gate): LocalAuthorityTimeout - manager has full authority
        Multi-DC (with gate): GateCoordinatedTimeout - gate coordinates globally

        Args:
            submission: Job submission with optional gate_addr

        Returns:
            Appropriate TimeoutStrategy instance
        """
        if submission.gate_addr:
            return GateCoordinatedTimeout(self)
        else:
            return LocalAuthorityTimeout(self)

    async def _suspect_worker_deadline_expired(self, worker_id: str) -> None:
        """
        Mark a worker as suspected when its deadline expires (AD-26 Issue 2).

        Called when a worker's deadline has expired but is still within
        the grace period.

        Args:
            worker_id: The worker node ID that missed its deadline
        """
        worker = self._manager_state._workers.get(worker_id)
        if worker is None:
            self._manager_state._worker_deadlines.pop(worker_id, None)
            return

        hierarchical_detector = self.get_hierarchical_detector()
        if hierarchical_detector is None:
            return

        worker_addr = (worker.node.host, worker.node.udp_port)
        current_status = await hierarchical_detector.get_node_status(worker_addr)

        from hyperscale.distributed.nodes.manager.health import NodeStatus

        if current_status in (NodeStatus.SUSPECTED_GLOBAL, NodeStatus.DEAD_GLOBAL):
            return

        await self.suspect_node_global(
            node=worker_addr,
            incarnation=0,
            from_node=(self._host, self._udp_port),
        )

        await self._udp_logger.log(
            ServerWarning(
                message=f"Worker {worker_id[:8]}... deadline expired, marked as SUSPECTED (within grace period)",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _evict_worker_deadline_expired(self, worker_id: str) -> None:
        """
        Evict a worker when its deadline expires beyond the grace period (AD-26 Issue 2).

        Args:
            worker_id: The worker node ID to evict
        """
        await self._udp_logger.log(
            ServerError(
                message=f"Worker {worker_id[:8]}... deadline expired beyond grace period, evicting",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        await self._handle_worker_failure(worker_id)
        self._manager_state._worker_deadlines.pop(worker_id, None)

        if self._worker_disseminator:
            await self._worker_disseminator.broadcast_worker_dead(worker_id, "evicted")

    def _cleanup_job(self, job_id: str) -> None:
        """
        Clean up all state associated with a job.

        Removes job from tracking dictionaries, cleans up workflow state,
        and notifies relevant systems.
        """
        self._task_runner.run(self._job_manager.complete_job, job_id)
        self._manager_state.clear_job_state(job_id)

        if self._workflow_dispatcher:
            self._task_runner.run(
                self._workflow_dispatcher.cleanup_job,
                job_id,
            )

        workflow_ids_to_remove = [
            wf_id
            for wf_id in self._manager_state._workflow_retries
            if wf_id.startswith(f"{job_id}:")
        ]
        for wf_id in workflow_ids_to_remove:
            self._manager_state._workflow_retries.pop(wf_id, None)

        workflow_ids_to_remove = [
            wf_id
            for wf_id in self._manager_state._workflow_completion_events
            if wf_id.startswith(f"{job_id}:")
        ]
        for wf_id in workflow_ids_to_remove:
            self._manager_state._workflow_completion_events.pop(wf_id, None)

    def _cleanup_reporter_tasks(self, job_id: str) -> None:
        """Clean up reporter background tasks for a job."""
        tasks = self._manager_state._job_reporter_tasks.pop(job_id, {})
        for task in tasks.values():
            if not task.done():
                task.cancel()

    # =========================================================================
    # TCP Send Helpers
    # =========================================================================

    async def _send_to_worker(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | Exception | None:
        """Send TCP message to worker."""
        return await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )

    async def _send_to_peer(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | Exception | None:
        """Send TCP message to peer manager."""
        return await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )

    async def _send_to_client(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | Exception | None:
        """Send TCP message to client."""
        return await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )

    async def _send_workflow_dispatch(
        self,
        worker_addr: tuple[str, int],
        dispatch: WorkflowDispatch,
    ) -> WorkflowDispatchAck | None:
        """Send workflow dispatch to worker."""
        try:
            response = await self.send_tcp(
                worker_addr,
                "workflow_dispatch",
                dispatch.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )

            if response and not isinstance(response, Exception):
                return WorkflowDispatchAck.load(response)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow dispatch error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        return None

    # =========================================================================
    # TCP Handlers
    # =========================================================================

    @tcp.receive()
    async def worker_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle worker registration."""
        try:
            registration = WorkerRegistration.load(data)

            # Register worker
            self._registry.register_worker(registration)

            # Add to worker pool
            self._worker_pool.register_worker(
                worker_id=registration.node.node_id,
                total_cores=registration.total_cores,
                available_cores=registration.available_cores,
                tcp_addr=(registration.node.host, registration.node.tcp_port),
            )

            # Add to SWIM
            worker_udp_addr = (registration.node.host, registration.node.udp_port)
            self._manager_state._worker_addr_to_id[worker_udp_addr] = (
                registration.node.node_id
            )
            self._probe_scheduler.add_member(worker_udp_addr)

            if self._worker_disseminator:
                await self._worker_disseminator.broadcast_worker_registered(
                    registration
                )

            # Build response with known managers
            healthy_managers = [
                self._manager_state._known_manager_peers[peer_id]
                for peer_id in self._manager_state._active_manager_peer_ids
                if peer_id in self._manager_state._known_manager_peers
            ]
            healthy_managers.append(
                ManagerInfo(
                    node_id=self._node_id.full,
                    tcp_host=self._host,
                    tcp_port=self._tcp_port,
                    udp_host=self._host,
                    udp_port=self._udp_port,
                    datacenter=self._node_id.datacenter,
                    is_leader=self.is_leader(),
                )
            )

            response = RegistrationResponse(
                accepted=True,
                manager_id=self._node_id.full,
                healthy_managers=healthy_managers,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            )

            return response.dump()

        except Exception as error:
            return RegistrationResponse(
                accepted=False,
                manager_id=self._node_id.full,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def manager_peer_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle manager peer registration."""
        try:
            registration = ManagerPeerRegistration.load(data)

            # Register peer
            self._registry.register_manager_peer(registration.manager_info)

            # Add to SWIM
            peer_udp_addr = (
                registration.manager_info.udp_host,
                registration.manager_info.udp_port,
            )
            self._manager_state._manager_udp_to_tcp[peer_udp_addr] = (
                registration.manager_info.tcp_host,
                registration.manager_info.tcp_port,
            )
            self._probe_scheduler.add_member(peer_udp_addr)

            response = ManagerPeerRegistrationResponse(
                accepted=True,
                manager_info=ManagerInfo(
                    node_id=self._node_id.full,
                    tcp_host=self._host,
                    tcp_port=self._tcp_port,
                    udp_host=self._host,
                    udp_port=self._udp_port,
                    datacenter=self._node_id.datacenter,
                    is_leader=self.is_leader(),
                ),
            )

            return response.dump()

        except Exception as error:
            return ManagerPeerRegistrationResponse(
                accepted=False,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def workflow_progress(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow progress update from worker."""
        try:
            progress = WorkflowProgress.load(data)

            # Record job progress for AD-30 responsiveness tracking
            worker_id = self._manager_state._worker_addr_to_id.get(addr)
            if worker_id:
                self._health_monitor.record_job_progress(progress.job_id, worker_id)

            # Update job manager
            self._job_manager.update_workflow_progress(
                job_id=progress.job_id,
                workflow_id=progress.workflow_id,
                completed_count=progress.completed_count,
                failed_count=progress.failed_count,
            )

            # Record in windowed stats
            self._windowed_stats.record(progress)

            # Get backpressure signal
            backpressure = self._stats_buffer.get_backpressure_signal()

            from hyperscale.distributed.models import WorkflowProgressAck

            ack = WorkflowProgressAck(
                workflow_id=progress.workflow_id,
                received=True,
                backpressure_level=backpressure.level.value,
                backpressure_delay_ms=backpressure.delay_ms,
            )

            return ack.dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow progress error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            from hyperscale.distributed.models import WorkflowProgressAck

            return WorkflowProgressAck(
                workflow_id="",
                received=False,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def workflow_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            result = WorkflowFinalResult.load(data)

            for stats in result.results:
                if stats and isinstance(stats, dict) and "elapsed" in stats:
                    elapsed_seconds = stats.get("elapsed", 0)
                    if (
                        isinstance(elapsed_seconds, (int, float))
                        and elapsed_seconds > 0
                    ):
                        self._manager_state.record_workflow_latency(
                            elapsed_seconds * 1000.0
                        )

            if result.context_updates:
                await self._job_manager.apply_workflow_context(
                    job_id=result.job_id,
                    workflow_name=result.workflow_name,
                    context_updates_bytes=result.context_updates,
                )

            self._job_manager.complete_workflow(
                job_id=result.job_id,
                workflow_id=result.workflow_id,
                success=result.status == WorkflowStatus.COMPLETED.value,
                results=result.results,
            )

            if (job := self._job_manager.get_job(result.job_id)) and job.is_complete:
                await self._handle_job_completion(result.job_id)

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow result error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def job_cancel(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle job cancellation request (AD-20).

        Robust cancellation flow:
        1. Verify job exists
        2. Remove ALL pending workflows from dispatch queue
        3. Cancel ALL running workflows on workers
        4. Wait for verification that no workflows are still running
        5. Return detailed per-workflow cancellation results

        Accepts both legacy CancelJob and new JobCancelRequest formats at the
        boundary, but normalizes to AD-20 internally.
        """
        try:
            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit_for_operation(
                client_id, "cancel"
            )
            if not allowed:
                return RateLimitResponse(
                    operation="cancel",
                    retry_after_seconds=retry_after,
                ).dump()

            # Parse request - accept both formats at boundary, normalize to AD-20 internally
            try:
                cancel_request = JobCancelRequest.load(data)
                job_id = cancel_request.job_id
                fence_token = cancel_request.fence_token
                requester_id = cancel_request.requester_id
                timestamp = cancel_request.timestamp
                reason = cancel_request.reason
            except Exception:
                # Normalize legacy CancelJob format to AD-20 fields
                cancel = CancelJob.load(data)
                job_id = cancel.job_id
                fence_token = cancel.fence_token
                requester_id = f"{addr[0]}:{addr[1]}"
                timestamp = time.monotonic()
                reason = "Legacy cancel request"

            # Step 1: Verify job exists
            job = self._job_manager.get_job(job_id)
            if not job:
                return self._build_cancel_response(
                    job_id, success=False, error="Job not found"
                )

            # Check fence token if provided (prevents cancelling restarted jobs)
            stored_fence = self._manager_state._job_fencing_tokens.get(job_id, 0)
            if fence_token > 0 and stored_fence != fence_token:
                error_msg = (
                    f"Fence token mismatch: expected {stored_fence}, got {fence_token}"
                )
                return self._build_cancel_response(
                    job_id, success=False, error=error_msg
                )

            # Check if already cancelled (idempotency)
            if job.status == JobStatus.CANCELLED:
                return self._build_cancel_response(
                    job_id, success=True, already_cancelled=True
                )

            # Check if already completed (cannot cancel)
            if job.status == JobStatus.COMPLETED:
                return self._build_cancel_response(
                    job_id,
                    success=False,
                    already_completed=True,
                    error="Job already completed",
                )

            # Track results
            pending_cancelled: list[str] = []
            running_cancelled: list[str] = []
            workflow_errors: dict[str, str] = {}

            # Step 2: Remove ALL pending workflows from dispatch queue FIRST
            if self._workflow_dispatcher:
                removed_pending = (
                    await self._workflow_dispatcher.cancel_pending_workflows(job_id)
                )
                pending_cancelled.extend(removed_pending)

                # Mark pending workflows as cancelled
                for workflow_id in removed_pending:
                    self._manager_state._cancelled_workflows[workflow_id] = (
                        CancelledWorkflowInfo(
                            workflow_id=workflow_id,
                            job_id=job_id,
                            cancelled_at=timestamp,
                            reason=reason,
                        )
                    )

            # Step 3: Cancel ALL running workflows on workers
            for workflow_id, workflow in job.workflows.items():
                if workflow_id in pending_cancelled:
                    continue

                if workflow.status == WorkflowStatus.RUNNING and workflow.worker_id:
                    worker = self._manager_state._workers.get(workflow.worker_id)
                    if not worker:
                        workflow_errors[workflow_id] = (
                            f"Worker {workflow.worker_id} not found"
                        )
                        continue

                    worker_addr = (worker.node.host, worker.node.tcp_port)

                    try:
                        cancel_data = WorkflowCancelRequest(
                            job_id=job_id,
                            workflow_id=workflow_id,
                            requester_id=requester_id,
                            timestamp=timestamp,
                        ).dump()

                        response = await self._send_to_worker(
                            worker_addr,
                            "cancel_workflow",
                            cancel_data,
                            timeout=5.0,
                        )

                        if isinstance(response, bytes):
                            try:
                                wf_response = WorkflowCancelResponse.load(response)
                                if wf_response.success:
                                    running_cancelled.append(workflow_id)
                                    self._manager_state._cancelled_workflows[
                                        workflow_id
                                    ] = CancelledWorkflowInfo(
                                        workflow_id=workflow_id,
                                        job_id=job_id,
                                        cancelled_at=timestamp,
                                        reason=reason,
                                    )
                                else:
                                    error_msg = (
                                        wf_response.error
                                        or "Worker reported cancellation failure"
                                    )
                                    workflow_errors[workflow_id] = error_msg
                            except Exception as parse_error:
                                workflow_errors[workflow_id] = (
                                    f"Failed to parse worker response: {parse_error}"
                                )
                        else:
                            workflow_errors[workflow_id] = "No response from worker"

                    except Exception as send_error:
                        workflow_errors[workflow_id] = (
                            f"Failed to send cancellation to worker: {send_error}"
                        )

            # Stop timeout tracking (AD-34 Part 10.4.9)
            strategy = self._manager_state._job_timeout_strategies.get(job_id)
            if strategy:
                await strategy.stop_tracking(job_id, "cancelled")

            # Update job status
            job.status = JobStatus.CANCELLED
            await self._manager_state.increment_state_version()

            # Build detailed response
            successfully_cancelled = pending_cancelled + running_cancelled
            total_cancelled = len(successfully_cancelled)
            total_errors = len(workflow_errors)

            overall_success = total_errors == 0

            error_str = None
            if workflow_errors:
                error_details = [
                    f"{wf_id[:8]}...: {err}" for wf_id, err in workflow_errors.items()
                ]
                error_str = (
                    f"{total_errors} workflow(s) failed: {'; '.join(error_details)}"
                )

            return self._build_cancel_response(
                job_id,
                success=overall_success,
                cancelled_count=total_cancelled,
                error=error_str,
            )

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job cancel error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return JobCancelResponse(
                job_id="",
                success=False,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def workflow_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle workflow cancellation completion push from worker (AD-20).

        Workers push this notification after successfully (or unsuccessfully)
        cancelling a workflow. The manager:
        1. Tracks completion of all workflows in a job cancellation
        2. Aggregates any errors from failed cancellations
        3. When all workflows report, fires the completion event
        4. Pushes aggregated result to origin gate/client
        """
        try:
            completion = WorkflowCancellationComplete.load(data)
            job_id = completion.job_id
            workflow_id = completion.workflow_id

            await self._udp_logger.log(
                ServerInfo(
                    message=f"Received workflow cancellation complete for {workflow_id[:8]}... "
                    f"(job {job_id[:8]}..., success={completion.success})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Track this workflow as complete
            pending = self._manager_state._cancellation_pending_workflows.get(
                job_id, set()
            )
            if workflow_id in pending:
                pending.discard(workflow_id)

                # Collect any errors
                if not completion.success and completion.errors:
                    for error in completion.errors:
                        self._manager_state._cancellation_errors[job_id].append(
                            f"Workflow {workflow_id[:8]}...: {error}"
                        )

                # Check if all workflows for this job have reported
                if not pending:
                    # All workflows cancelled - fire completion event and push to origin
                    event = self._manager_state._cancellation_completion_events.get(
                        job_id
                    )
                    if event:
                        event.set()

                    errors = self._manager_state._cancellation_errors.get(job_id, [])
                    success = len(errors) == 0

                    # Push completion notification to origin gate/client
                    self._task_runner.run(
                        self._push_cancellation_complete_to_origin,
                        job_id,
                        success,
                        errors,
                    )

                    # Cleanup tracking structures
                    self._manager_state._cancellation_pending_workflows.pop(
                        job_id, None
                    )
                    self._manager_state._cancellation_completion_events.pop(
                        job_id, None
                    )
                    self._manager_state._cancellation_initiated_at.pop(job_id, None)

            # Also delegate to cancellation coordinator for additional handling
            await self._cancellation.handle_workflow_cancelled(completion)

            return b"OK"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Cancellation complete error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"ERROR"

    @tcp.receive()
    async def state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle state sync request."""
        try:
            request = StateSyncRequest.load(data)

            # Build state snapshot
            snapshot = ManagerStateSnapshot(
                node_id=self._node_id.full,
                state_version=self._manager_state._state_version,
                manager_state=self._manager_state._manager_state.value,
                job_count=self._job_manager.job_count,
                worker_count=len(self._manager_state._workers),
            )

            return StateSyncResponse(
                responder_id=self._node_id.full,
                version=self._manager_state._state_version,
                snapshot=snapshot.dump(),
            ).dump()

        except Exception as error:
            return StateSyncResponse(
                responder_id=self._node_id.full,
                version=0,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def extension_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle deadline extension request from worker (AD-26).

        Workers can request deadline extensions when:
        - Executing long-running workflows
        - System is under heavy load but making progress
        - Approaching timeout but not stuck

        Extensions use logarithmic decay and require progress to be granted.
        """
        try:
            request = HealthcheckExtensionRequest.load(data)

            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit_for_operation(
                client_id, "extension"
            )
            if not allowed:
                return HealthcheckExtensionResponse(
                    granted=False,
                    extension_seconds=0.0,
                    new_deadline=0.0,
                    remaining_extensions=0,
                    denial_reason=f"Rate limited, retry after {retry_after:.1f}s",
                ).dump()

            # Check if worker is registered
            worker_id = request.worker_id
            if not worker_id:
                worker_id = self._manager_state._worker_addr_to_id.get(addr)

            if not worker_id:
                return HealthcheckExtensionResponse(
                    granted=False,
                    extension_seconds=0.0,
                    new_deadline=0.0,
                    remaining_extensions=0,
                    denial_reason="Worker not registered",
                ).dump()

            worker = self._manager_state._workers.get(worker_id)
            if not worker:
                return HealthcheckExtensionResponse(
                    granted=False,
                    extension_seconds=0.0,
                    new_deadline=0.0,
                    remaining_extensions=0,
                    denial_reason="Worker not found",
                ).dump()

            # Get current deadline (or set default)
            current_deadline = self._manager_state._worker_deadlines.get(
                worker_id,
                time.monotonic() + 30.0,
            )

            # Handle extension request via worker health manager
            response = self._worker_health_manager.handle_extension_request(
                request=request,
                current_deadline=current_deadline,
            )

            # Update stored deadline if granted
            if response.granted:
                self._manager_state._worker_deadlines[worker_id] = response.new_deadline

                # AD-26 Issue 3: Integrate with SWIM timing wheels (SWIM as authority)
                hierarchical_detector = self.get_hierarchical_detector()
                if hierarchical_detector:
                    worker_addr = (worker.node.host, worker.node.udp_port)
                    (
                        swim_granted,
                        swim_extension,
                        swim_denial,
                        is_warning,
                    ) = await hierarchical_detector.request_extension(
                        node=worker_addr,
                        reason=request.reason,
                        current_progress=request.current_progress,
                    )
                    if not swim_granted:
                        await self._udp_logger.log(
                            ServerWarning(
                                message=f"SWIM denied extension for {worker_id[:8]}... despite WorkerHealthManager grant: {swim_denial}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

                # Notify timeout strategies of extension (AD-34 Part 10.4.7)
                await self._notify_timeout_strategies_of_extension(
                    worker_id=worker_id,
                    extension_seconds=response.extension_seconds,
                    worker_progress=request.current_progress,
                )

                await self._udp_logger.log(
                    ServerInfo(
                        message=f"Granted {response.extension_seconds:.1f}s extension to worker {worker_id[:8]}... (reason: {request.reason})",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            else:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Denied extension to worker {worker_id[:8]}...: {response.denial_reason}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

                # Check if worker should be evicted
                should_evict, eviction_reason = (
                    self._worker_health_manager.should_evict_worker(worker_id)
                )
                if should_evict:
                    await self._udp_logger.log(
                        ServerWarning(
                            message=f"Worker {worker_id[:8]}... should be evicted: {eviction_reason}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            return response.dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Extension request error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return HealthcheckExtensionResponse(
                granted=False,
                extension_seconds=0.0,
                new_deadline=0.0,
                remaining_extensions=0,
                denial_reason=str(error),
            ).dump()

    @tcp.receive()
    async def ping(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle ping request."""
        try:
            request = PingRequest.load(data)

            # Build worker status list
            worker_statuses = [
                WorkerStatus(
                    worker_id=worker_id,
                    state=self._health_monitor.get_worker_health_status(worker_id),
                    available_cores=worker.available_cores,
                    total_cores=worker.total_cores,
                )
                for worker_id, worker in self._manager_state._workers.items()
            ]

            response = ManagerPingResponse(
                manager_id=self._node_id.full,
                is_leader=self.is_leader(),
                state=self._manager_state._manager_state.value,
                state_version=self._manager_state._state_version,
                worker_count=len(self._manager_state._workers),
                healthy_worker_count=self._health_monitor.get_healthy_worker_count(),
                active_job_count=self._job_manager.job_count,
                workers=worker_statuses,
            )

            return response.dump()

        except Exception as error:
            return ManagerPingResponse(
                manager_id=self._node_id.full,
                is_leader=False,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def gate_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle gate registration via TCP."""
        try:
            registration = GateRegistrationRequest.load(data)

            # Cluster isolation validation (AD-28)
            if registration.cluster_id != self._env.CLUSTER_ID:
                return GateRegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    healthy_managers=[],
                    error=f"Cluster isolation violation: gate cluster_id '{registration.cluster_id}' does not match",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            if registration.environment_id != self._env.ENVIRONMENT_ID:
                return GateRegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    healthy_managers=[],
                    error="Environment isolation violation: gate environment_id mismatch",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Protocol version validation (AD-25)
            gate_version = ProtocolVersion(
                registration.protocol_version_major,
                registration.protocol_version_minor,
            )
            gate_caps_set = (
                set(registration.capabilities.split(","))
                if registration.capabilities
                else set()
            )
            gate_caps = NodeCapabilities(
                protocol_version=gate_version,
                capabilities=gate_caps_set,
            )
            local_caps = NodeCapabilities.current()
            negotiated = negotiate_capabilities(local_caps, gate_caps)

            if not negotiated.compatible:
                return GateRegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    healthy_managers=[],
                    error=f"Incompatible protocol version: {gate_version}",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Store gate info
            gate_info = GateInfo(
                node_id=registration.node_id,
                tcp_host=registration.tcp_host,
                tcp_port=registration.tcp_port,
                udp_host=registration.udp_host,
                udp_port=registration.udp_port,
            )

            self._registry.register_gate(gate_info)

            # Track gate addresses
            gate_tcp_addr = (registration.tcp_host, registration.tcp_port)
            gate_udp_addr = (registration.udp_host, registration.udp_port)
            self._manager_state._gate_udp_to_tcp[gate_udp_addr] = gate_tcp_addr

            # Add to SWIM probing
            self.add_unconfirmed_peer(gate_udp_addr)
            self._probe_scheduler.add_member(gate_udp_addr)

            # Store negotiated capabilities
            self._manager_state._gate_negotiated_caps[registration.node_id] = negotiated

            negotiated_caps_str = ",".join(sorted(negotiated.common_features))
            return GateRegistrationResponse(
                accepted=True,
                manager_id=self._node_id.full,
                datacenter=self._node_id.datacenter,
                healthy_managers=self._get_healthy_managers(),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()

        except Exception as error:
            return GateRegistrationResponse(
                accepted=False,
                manager_id=self._node_id.full,
                datacenter=self._node_id.datacenter,
                healthy_managers=[],
                error=str(error),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()

    @tcp.receive()
    async def worker_discovery(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle worker discovery broadcast from peer manager."""
        try:
            broadcast = WorkerDiscoveryBroadcast.load(data)

            worker_id = broadcast.worker_id

            # Skip if already registered
            if worker_id in self._manager_state._workers:
                return b"ok"

            # Schedule direct registration with the worker
            worker_tcp_addr = tuple(broadcast.worker_tcp_addr)
            worker_udp_addr = tuple(broadcast.worker_udp_addr)

            worker_snapshot = WorkerStateSnapshot(
                node_id=worker_id,
                host=worker_tcp_addr[0],
                tcp_port=worker_tcp_addr[1],
                udp_port=worker_udp_addr[1],
                state=WorkerState.HEALTHY.value,
                total_cores=broadcast.available_cores,
                available_cores=broadcast.available_cores,
                version=0,
            )

            self._task_runner.run(
                self._register_with_discovered_worker,
                worker_snapshot,
            )

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Worker discovery error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def receive_worker_status_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle worker status update via TCP."""
        try:
            heartbeat = WorkerHeartbeat.load(data)

            # Process heartbeat via WorkerPool
            await self._worker_pool.process_heartbeat(heartbeat.node_id, heartbeat)

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Worker status update error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def worker_heartbeat(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle worker heartbeat via TCP."""
        try:
            heartbeat = WorkerHeartbeat.load(data)

            # Process heartbeat via WorkerPool
            await self._worker_pool.process_heartbeat(heartbeat.node_id, heartbeat)

            # Trigger dispatch for active jobs
            if self._workflow_dispatcher:
                for job_id, submission in list(
                    self._manager_state._job_submissions.items()
                ):
                    await self._workflow_dispatcher.try_dispatch(job_id, submission)

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Worker heartbeat error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def worker_state_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            update = WorkerStateUpdate.from_bytes(data)
            if update is None:
                return b"invalid"

            if self._worker_disseminator is None:
                return b"not_ready"

            accepted = await self._worker_disseminator.handle_worker_state_update(
                update, addr
            )

            return b"accepted" if accepted else b"rejected"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Worker state update error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def list_workers(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            if self._worker_disseminator is None:
                return WorkerListResponse(
                    manager_id=self._node_id.full, workers=[]
                ).dump()

            response = self._worker_disseminator.build_worker_list_response()
            return response.dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"List workers error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def context_forward(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle context forwarded from non-leader manager."""
        try:
            forward = ContextForward.load(data)

            # Verify we are the job leader
            if not self._is_job_leader(forward.job_id):
                return b"not_leader"

            # Apply context updates
            await self._apply_context_updates(
                forward.job_id,
                forward.workflow_id,
                forward.context_updates,
                forward.context_timestamps,
            )

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Context forward error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def context_layer_sync(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle context layer sync from job leader."""
        try:
            sync = ContextLayerSync.load(data)

            # Check if this is a newer layer version
            current_version = self._manager_state._job_layer_version.get(
                sync.job_id, -1
            )
            if sync.layer_version <= current_version:
                return ContextLayerSyncAck(
                    job_id=sync.job_id,
                    layer_version=sync.layer_version,
                    applied=False,
                    responder_id=self._node_id.full,
                ).dump()

            # Apply context snapshot
            context_dict = cloudpickle.loads(sync.context_snapshot)

            if sync.job_id not in self._manager_state._job_contexts:
                self._manager_state._job_contexts[sync.job_id] = Context()

            context = self._manager_state._job_contexts[sync.job_id]
            for workflow_name, values in context_dict.items():
                await context.from_dict(workflow_name, values)

            # Update layer version
            self._manager_state._job_layer_version[sync.job_id] = sync.layer_version

            # Update job leader if not set
            if sync.job_id not in self._manager_state._job_leaders:
                self._manager_state._job_leaders[sync.job_id] = sync.source_node_id

            return ContextLayerSyncAck(
                job_id=sync.job_id,
                layer_version=sync.layer_version,
                applied=True,
                responder_id=self._node_id.full,
            ).dump()

        except Exception as context_sync_error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Context layer sync failed: {context_sync_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return ContextLayerSyncAck(
                job_id="unknown",
                layer_version=-1,
                applied=False,
                responder_id=self._node_id.full,
            ).dump()

    @tcp.receive()
    async def job_submission(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job submission from gate or client."""
        try:
            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            rate_limit_result = await self._rate_limiter.check_rate_limit(
                client_id, "job_submit"
            )
            if not rate_limit_result.allowed:
                return RateLimitResponse(
                    operation="job_submit",
                    retry_after_seconds=rate_limit_result.retry_after_seconds,
                ).dump()

            # Load shedding check (AD-22)
            if self._load_shedder.should_shed("JobSubmission"):
                overload_state = self._load_shedder.get_current_state()
                return JobAck(
                    job_id="",
                    accepted=False,
                    error=f"System under load ({overload_state.value}), please retry later",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            submission = JobSubmission.load(data)

            # Protocol version negotiation (AD-25)
            client_version = ProtocolVersion(
                major=getattr(submission, "protocol_version_major", 1),
                minor=getattr(submission, "protocol_version_minor", 0),
            )

            if client_version.major != CURRENT_PROTOCOL_VERSION.major:
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Incompatible protocol version: {client_version}",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Negotiate capabilities
            client_caps_str = getattr(submission, "capabilities", "")
            client_features = (
                set(client_caps_str.split(",")) if client_caps_str else set()
            )
            our_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
            negotiated_features = client_features & our_features
            negotiated_caps_str = ",".join(sorted(negotiated_features))

            # Unpickle workflows
            workflows: list[tuple[str, list[str], Workflow]] = restricted_loads(
                submission.workflows
            )

            # Only active managers accept jobs
            if self._manager_state._manager_state != ManagerStateEnum.ACTIVE:
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Manager is {self._manager_state._manager_state.value}, not accepting jobs",
                ).dump()

            # Create job using JobManager
            callback_addr = None
            if submission.callback_addr:
                callback_addr = (
                    tuple(submission.callback_addr)
                    if isinstance(submission.callback_addr, list)
                    else submission.callback_addr
                )

            job_info = await self._job_manager.create_job(
                submission=submission,
                callback_addr=callback_addr,
            )

            job_info.leader_node_id = self._node_id.full
            job_info.leader_addr = (self._host, self._tcp_port)
            job_info.fencing_token = 1

            # Store submission for dispatch
            self._manager_state._job_submissions[submission.job_id] = submission

            # Start timeout tracking (AD-34)
            timeout_strategy = self._select_timeout_strategy(submission)
            await timeout_strategy.start_tracking(
                job_id=submission.job_id,
                timeout_seconds=submission.timeout_seconds,
                gate_addr=tuple(submission.gate_addr) if submission.gate_addr else None,
            )
            self._manager_state._job_timeout_strategies[submission.job_id] = (
                timeout_strategy
            )

            # Set job leadership
            self._manager_state._job_leaders[submission.job_id] = self._node_id.full
            self._manager_state._job_leader_addrs[submission.job_id] = (
                self._host,
                self._tcp_port,
            )
            self._manager_state._job_fencing_tokens[submission.job_id] = 1
            self._manager_state._job_layer_version[submission.job_id] = 0
            self._manager_state._job_contexts[submission.job_id] = Context()

            # Store callbacks
            if submission.callback_addr:
                self._manager_state._job_callbacks[submission.job_id] = (
                    submission.callback_addr
                )
                self._manager_state._progress_callbacks[submission.job_id] = (
                    submission.callback_addr
                )

            if submission.origin_gate_addr:
                self._manager_state._job_origin_gates[submission.job_id] = (
                    submission.origin_gate_addr
                )

            await self._manager_state.increment_state_version()

            # Broadcast job leadership to peers
            workflow_names = [wf.name for _, _, wf in workflows]
            await self._broadcast_job_leadership(
                submission.job_id,
                len(workflows),
                workflow_names,
            )

            # Dispatch workflows
            await self._dispatch_job_workflows(submission, workflows)

            return JobAck(
                job_id=submission.job_id,
                accepted=True,
                queued_position=self._job_manager.job_count,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job submission error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return JobAck(
                job_id="unknown",
                accepted=False,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def job_global_timeout(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle global timeout decision from gate (AD-34)."""
        try:
            timeout_msg = JobGlobalTimeout.load(data)

            strategy = self._manager_state._job_timeout_strategies.get(
                timeout_msg.job_id
            )
            if not strategy:
                return b""

            accepted = await strategy.handle_global_timeout(
                timeout_msg.job_id,
                timeout_msg.reason,
                timeout_msg.fence_token,
            )

            if accepted:
                self._manager_state._job_timeout_strategies.pop(
                    timeout_msg.job_id, None
                )
                await self._udp_logger.log(
                    ServerInfo(
                        message=f"Job {timeout_msg.job_id} globally timed out: {timeout_msg.reason}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            return b""

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job global timeout error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b""

    @tcp.receive()
    async def provision_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle provision request from leader for quorum."""
        try:
            request = ProvisionRequest.load(data)

            # Check if we can confirm
            worker = self._worker_pool.get_worker(request.target_worker)
            can_confirm = (
                worker is not None
                and self._worker_pool.is_worker_healthy(request.target_worker)
                and (worker.available_cores - worker.reserved_cores)
                >= request.cores_required
            )

            return ProvisionConfirm(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                confirming_node=self._node_id.full,
                confirmed=can_confirm,
                version=self._manager_state._state_version,
                error=None if can_confirm else "Worker not available",
            ).dump()

        except Exception as error:
            return ProvisionConfirm(
                job_id="unknown",
                workflow_id="unknown",
                confirming_node=self._node_id.full,
                confirmed=False,
                version=self._manager_state._state_version,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def provision_commit(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle provision commit from leader."""
        try:
            ProvisionCommit.load(data)  # Validate message format
            await self._manager_state.increment_state_version()
            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Provision commit error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def workflow_cancellation_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow cancellation query from worker."""
        try:
            query = WorkflowCancellationQuery.load(data)

            job = self._job_manager.get_job(query.job_id)
            if not job:
                return WorkflowCancellationResponse(
                    job_id=query.job_id,
                    workflow_id=query.workflow_id,
                    workflow_name="",
                    status="UNKNOWN",
                    error="Job not found",
                ).dump()

            # Check job-level cancellation
            if job.status == JobStatus.CANCELLED.value:
                return WorkflowCancellationResponse(
                    job_id=query.job_id,
                    workflow_id=query.workflow_id,
                    workflow_name="",
                    status="CANCELLED",
                ).dump()

            # Check specific workflow status
            for sub_wf in job.sub_workflows.values():
                if str(sub_wf.token) == query.workflow_id:
                    workflow_name = ""
                    status = WorkflowStatus.RUNNING.value
                    if sub_wf.progress is not None:
                        workflow_name = sub_wf.progress.workflow_name
                        status = sub_wf.progress.status
                    return WorkflowCancellationResponse(
                        job_id=query.job_id,
                        workflow_id=query.workflow_id,
                        workflow_name=workflow_name,
                        status=status,
                    ).dump()

            return WorkflowCancellationResponse(
                job_id=query.job_id,
                workflow_id=query.workflow_id,
                workflow_name="",
                status="UNKNOWN",
                error="Workflow not found",
            ).dump()

        except Exception as error:
            return WorkflowCancellationResponse(
                job_id="unknown",
                workflow_id="unknown",
                workflow_name="",
                status="ERROR",
                error=str(error),
            ).dump()

    @tcp.receive()
    async def receive_cancel_single_workflow(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle single workflow cancellation request."""
        try:
            request = SingleWorkflowCancelRequest.load(data)

            # Rate limit check
            client_id = f"{addr[0]}:{addr[1]}"
            rate_limit_result = await self._rate_limiter.check_rate_limit(
                client_id, "cancel_workflow"
            )
            if not rate_limit_result.allowed:
                return RateLimitResponse(
                    operation="cancel_workflow",
                    retry_after_seconds=rate_limit_result.retry_after_seconds,
                ).dump()

            # Check if already cancelled
            if request.workflow_id in self._manager_state._cancelled_workflows:
                existing = self._manager_state._cancelled_workflows[request.workflow_id]
                return SingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=WorkflowCancellationStatus.ALREADY_CANCELLED.value,
                    cancelled_dependents=existing.dependents,
                    datacenter=self._node_id.datacenter,
                ).dump()

            job = self._job_manager.get_job(request.job_id)
            if not job:
                return SingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=WorkflowCancellationStatus.NOT_FOUND.value,
                    errors=["Job not found"],
                    datacenter=self._node_id.datacenter,
                ).dump()

            # Add to cancelled workflows
            self._manager_state._cancelled_workflows[request.workflow_id] = (
                CancelledWorkflowInfo(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    cancelled_at=time.monotonic(),
                    request_id=request.request_id,
                    dependents=[],
                )
            )

            return SingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=WorkflowCancellationStatus.CANCELLED.value,
                datacenter=self._node_id.datacenter,
            ).dump()

        except Exception as error:
            return SingleWorkflowCancelResponse(
                job_id="unknown",
                workflow_id="unknown",
                request_id="unknown",
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=[str(error)],
                datacenter=self._node_id.datacenter,
            ).dump()

    @tcp.receive()
    async def receive_workflow_cancellation_peer_notification(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow cancellation peer notification."""
        try:
            notification = WorkflowCancellationPeerNotification.load(data)

            # Add all cancelled workflows to our bucket
            for wf_id in notification.cancelled_workflows:
                if wf_id not in self._manager_state._cancelled_workflows:
                    self._manager_state._cancelled_workflows[wf_id] = (
                        CancelledWorkflowInfo(
                            job_id=notification.job_id,
                            workflow_id=wf_id,
                            cancelled_at=notification.timestamp or time.monotonic(),
                            request_id=notification.request_id,
                            dependents=[],
                        )
                    )

            return b"OK"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow cancellation peer notification error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"ERROR"

    @tcp.receive()
    async def job_leadership_announcement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job leadership announcement from another manager."""
        try:
            announcement = JobLeadershipAnnouncement.load(data)

            # Don't accept if we're already the leader
            if self._is_job_leader(announcement.job_id):
                return JobLeadershipAck(
                    job_id=announcement.job_id,
                    accepted=False,
                    responder_id=self._node_id.full,
                ).dump()

            # Record job leadership
            self._manager_state._job_leaders[announcement.job_id] = (
                announcement.leader_id
            )
            self._manager_state._job_leader_addrs[announcement.job_id] = (
                announcement.leader_host,
                announcement.leader_tcp_port,
            )

            # Initialize context for this job
            if announcement.job_id not in self._manager_state._job_contexts:
                self._manager_state._job_contexts[announcement.job_id] = Context()

            if announcement.job_id not in self._manager_state._job_layer_version:
                self._manager_state._job_layer_version[announcement.job_id] = 0

            # Track remote job
            await self._job_manager.track_remote_job(
                job_id=announcement.job_id,
                leader_node_id=announcement.leader_id,
                leader_addr=(announcement.leader_host, announcement.leader_tcp_port),
            )

            return JobLeadershipAck(
                job_id=announcement.job_id,
                accepted=True,
                responder_id=self._node_id.full,
            ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job leadership announcement error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def job_state_sync(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job state sync from job leader."""
        try:
            sync_msg = JobStateSyncMessage.load(data)

            # Only accept from actual job leader
            current_leader = self._manager_state._job_leaders.get(sync_msg.job_id)
            if current_leader and current_leader != sync_msg.leader_id:
                return JobStateSyncAck(
                    job_id=sync_msg.job_id,
                    responder_id=self._node_id.full,
                    accepted=False,
                ).dump()

            # Update job state tracking
            job = self._job_manager.get_job(sync_msg.job_id)
            if job:
                job.status = sync_msg.status
                job.workflows_total = sync_msg.workflows_total
                job.workflows_completed = sync_msg.workflows_completed
                job.workflows_failed = sync_msg.workflows_failed
                job.timestamp = time.monotonic()

            # Update fencing token
            current_token = self._manager_state._job_fencing_tokens.get(
                sync_msg.job_id, 0
            )
            if sync_msg.fencing_token > current_token:
                self._manager_state._job_fencing_tokens[sync_msg.job_id] = (
                    sync_msg.fencing_token
                )

            # Update origin gate
            if sync_msg.origin_gate_addr:
                self._manager_state._job_origin_gates[sync_msg.job_id] = (
                    sync_msg.origin_gate_addr
                )

            return JobStateSyncAck(
                job_id=sync_msg.job_id,
                responder_id=self._node_id.full,
                accepted=True,
            ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job state sync error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def job_leader_gate_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job leader gate transfer notification from gate."""
        try:
            transfer = JobLeaderGateTransfer.load(data)

            # Use fence token for consistency
            current_fence = self._manager_state._job_fencing_tokens.get(
                transfer.job_id, 0
            )
            if transfer.fence_token < current_fence:
                return JobLeaderGateTransferAck(
                    job_id=transfer.job_id,
                    manager_id=self._node_id.full,
                    accepted=False,
                ).dump()

            # Update origin gate
            self._manager_state._job_origin_gates[transfer.job_id] = (
                transfer.new_gate_addr
            )

            if transfer.fence_token > current_fence:
                self._manager_state._job_fencing_tokens[transfer.job_id] = (
                    transfer.fence_token
                )

            await self._udp_logger.log(
                ServerInfo(
                    message=f"Job {transfer.job_id} leader gate transferred: {transfer.old_gate_id} -> {transfer.new_gate_id}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            return JobLeaderGateTransferAck(
                job_id=transfer.job_id,
                manager_id=self._node_id.full,
                accepted=True,
            ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job leader gate transfer error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def register_callback(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle client callback registration for job reconnection."""
        try:
            # Rate limit check
            client_id = f"{addr[0]}:{addr[1]}"
            rate_limit_result = await self._rate_limiter.check_rate_limit(
                client_id, "reconnect"
            )
            if not rate_limit_result.allowed:
                return RateLimitResponse(
                    operation="reconnect",
                    retry_after_seconds=rate_limit_result.retry_after_seconds,
                ).dump()

            request = RegisterCallback.load(data)
            job_id = request.job_id

            job = self._job_manager.get_job(job_id)
            if not job:
                return RegisterCallbackResponse(
                    job_id=job_id,
                    success=False,
                    error="Job not found",
                ).dump()

            # Register callback
            self._manager_state._job_callbacks[job_id] = request.callback_addr
            self._manager_state._progress_callbacks[job_id] = request.callback_addr

            # Calculate elapsed time
            elapsed = time.monotonic() - job.timestamp if job.timestamp > 0 else 0.0

            # Count completed/failed
            total_completed = 0
            total_failed = 0
            for wf in job.workflows.values():
                total_completed += wf.completed_count
                total_failed += wf.failed_count

            return RegisterCallbackResponse(
                job_id=job_id,
                success=True,
                status=job.status.value,
                total_completed=total_completed,
                total_failed=total_failed,
                elapsed_seconds=elapsed,
            ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Register callback error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def workflow_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow status query from client."""
        try:
            # Rate limit check
            client_id = f"{addr[0]}:{addr[1]}"
            rate_limit_result = await self._rate_limiter.check_rate_limit(
                client_id, "workflow_query"
            )
            if not rate_limit_result.allowed:
                return RateLimitResponse(
                    operation="workflow_query",
                    retry_after_seconds=rate_limit_result.retry_after_seconds,
                ).dump()

            request = WorkflowQueryRequest.load(data)
            workflows: list[WorkflowStatusInfo] = []

            job = self._job_manager.get_job(request.job_id)
            if job is None:
                return WorkflowQueryResponse(
                    request_id=request.request_id,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    workflows=workflows,
                ).dump()

            # Find matching workflows
            for wf_info in job.workflows.values():
                if wf_info.name in request.workflow_names:
                    workflow_id = wf_info.token.workflow_id or ""
                    status = wf_info.status.value
                    is_enqueued = wf_info.status == WorkflowStatus.PENDING

                    # Aggregate from sub-workflows
                    assigned_workers: list[str] = []
                    provisioned_cores = 0
                    completed_count = 0
                    failed_count = 0
                    rate_per_second = 0.0

                    for sub_token_str in wf_info.sub_workflow_tokens:
                        sub_info = job.sub_workflows.get(sub_token_str)
                        if sub_info:
                            if sub_info.worker_id:
                                assigned_workers.append(sub_info.worker_id)
                            provisioned_cores += sub_info.cores_allocated
                            if sub_info.progress:
                                completed_count += sub_info.progress.completed_count
                                failed_count += sub_info.progress.failed_count
                                rate_per_second += sub_info.progress.rate_per_second

                    workflows.append(
                        WorkflowStatusInfo(
                            workflow_id=workflow_id,
                            workflow_name=wf_info.name,
                            status=status,
                            is_enqueued=is_enqueued,
                            queue_position=0,
                            provisioned_cores=provisioned_cores,
                            completed_count=completed_count,
                            failed_count=failed_count,
                            rate_per_second=rate_per_second,
                            assigned_workers=assigned_workers,
                        )
                    )

            return WorkflowQueryResponse(
                request_id=request.request_id,
                manager_id=self._node_id.full,
                datacenter=self._node_id.datacenter,
                workflows=workflows,
            ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow query error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    # =========================================================================
    # Helper Methods - Job Submission
    # =========================================================================

    def _select_timeout_strategy(self, submission: JobSubmission) -> TimeoutStrategy:
        """Select appropriate timeout strategy based on submission."""
        if submission.gate_addr:
            return GateCoordinatedTimeout(
                send_tcp=self._send_to_peer,
                logger=self._udp_logger,
                node_id=self._node_id.short,
                task_runner=self._task_runner,
            )
        return LocalAuthorityTimeout(
            cancel_job=self._cancellation.cancel_job,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

    async def _broadcast_job_leadership(
        self,
        job_id: str,
        workflow_count: int,
        workflow_names: list[str],
    ) -> None:
        """Broadcast job leadership to peer managers."""
        announcement = JobLeadershipAnnouncement(
            job_id=job_id,
            leader_id=self._node_id.full,
            leader_host=self._host,
            leader_tcp_port=self._tcp_port,
            workflow_count=workflow_count,
            workflow_names=workflow_names,
        )

        for peer_addr in self._manager_state._active_manager_peers:
            try:
                await self.send_tcp(
                    peer_addr,
                    "job_leadership_announcement",
                    announcement.dump(),
                    timeout=2.0,
                )
            except Exception as announcement_error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to send leadership announcement to peer {peer_addr}: {announcement_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _dispatch_job_workflows(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
    ) -> None:
        """Dispatch workflows respecting dependencies."""
        if self._workflow_dispatcher:
            registered = await self._workflow_dispatcher.register_workflows(
                submission,
                workflows,
            )
            if registered:
                await self._workflow_dispatcher.start_job_dispatch(
                    submission.job_id, submission
                )
                await self._workflow_dispatcher.try_dispatch(
                    submission.job_id, submission
                )

        job = self._job_manager.get_job(submission.job_id)
        if job:
            job.status = JobStatus.RUNNING.value
            await self._manager_state.increment_state_version()

    async def _register_with_discovered_worker(
        self,
        worker_snapshot: WorkerStateSnapshot,
    ) -> None:
        """Register with a discovered worker."""
        # Implementation: Contact worker directly to complete registration
        pass

    def _is_job_leader(self, job_id: str) -> bool:
        """Check if this manager is the leader for a job."""
        leader_id = self._manager_state._job_leaders.get(job_id)
        return leader_id == self._node_id.full

    async def _apply_context_updates(
        self,
        job_id: str,
        workflow_id: str,
        updates_bytes: bytes,
        timestamps_bytes: bytes,
    ) -> None:
        """Apply context updates from workflow completion."""
        context = self._manager_state._job_contexts.get(job_id)
        if not context:
            context = Context()
            self._manager_state._job_contexts[job_id] = context

        updates = cloudpickle.loads(updates_bytes)
        timestamps = cloudpickle.loads(timestamps_bytes) if timestamps_bytes else {}

        for key, value in updates.items():
            timestamp = timestamps.get(
                key, await self._manager_state.increment_context_lamport_clock()
            )
            await context.update(
                workflow_id,
                key,
                value,
                timestamp=timestamp,
                source_node=self._node_id.full,
            )

    def _get_healthy_managers(self) -> list[ManagerInfo]:
        """Get list of healthy managers including self."""
        managers = [
            ManagerInfo(
                node_id=self._node_id.full,
                tcp_host=self._host,
                tcp_port=self._tcp_port,
                udp_host=self._host,
                udp_port=self._udp_port,
                datacenter=self._node_id.datacenter,
                is_leader=self.is_leader(),
            )
        ]

        for peer_id in self._manager_state._active_manager_peer_ids:
            peer_info = self._manager_state._known_manager_peers.get(peer_id)
            if peer_info:
                managers.append(peer_info)

        return managers

    # =========================================================================
    # Job Completion
    # =========================================================================

    async def _handle_job_completion(self, job_id: str) -> None:
        """Handle job completion."""
        # Clear job state
        self._leases.clear_job_leases(job_id)
        self._health_monitor.cleanup_job_progress(job_id)
        self._health_monitor.clear_job_suspicions(job_id)
        self._manager_state.clear_job_state(job_id)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Job {job_id[:8]}... completed",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )


__all__ = ["ManagerServer"]
