"""
Worker Node Server.

Workers are the distributed thread/process pool. They:
- Execute workflows assigned by managers
- Report status via TCP to managers
- Participate in UDP healthchecks (SWIM protocol)

Workers are the absolute source of truth for their own state.

Protocols:
- UDP: SWIM healthchecks (inherited from HealthAwareServer)
  - probe/ack for liveness detection
  - indirect probing for network partition handling
  - gossip for membership dissemination
- TCP: Data operations (inherited from MercurySyncBaseServer)
  - Status updates to managers
  - Workflow dispatch from managers
  - State sync requests

Workflow Execution:
- Uses WorkflowRunner from hyperscale.core.jobs.graphs for actual execution
- Reports progress including cores_completed for faster manager reprovisioning
- Supports single-VU (direct execution) and multi-VU (parallel) workflows
"""

import asyncio
import os
import time
from multiprocessing import active_children
from typing import Any

import cloudpickle

# Optional psutil import for system metrics
try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None  # type: ignore
    _PSUTIL_AVAILABLE = False

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.ui import InterfaceUpdatesController
from hyperscale.core.monitoring import CPUMonitor, MemoryMonitor

from hyperscale.distributed_rewrite.server import tcp
from hyperscale.distributed_rewrite.swim import HealthAwareServer, WorkerStateEmbedder
from hyperscale.distributed_rewrite.swim.core import ErrorStats, CircuitState
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    ManagerHeartbeat,
    RegistrationResponse,
    ManagerToWorkerRegistration,
    ManagerToWorkerRegistrationAck,
    WorkflowProgressAck,
    WorkerRegistration,
    WorkerHeartbeat,
    WorkerState,
    WorkerStateSnapshot,
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowProgress,
    WorkflowFinalResult,
    WorkflowStatus,
    StepStats,
    StateSyncRequest,
    StateSyncResponse,
    CancelJob,
    CancelAck,
    WorkflowCancellationQuery,
    WorkflowCancellationResponse,
    # AD-20: Cancellation Propagation
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    restricted_loads,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.jobs import CoreAllocator
from hyperscale.distributed_rewrite.reliability import (
    BackpressureLevel,
    BackpressureSignal,
)
from hyperscale.distributed_rewrite.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    NodeCapabilities,
    ProtocolVersion,
    NegotiatedCapabilities,
    get_features_for_version,
)
from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError, ServerWarning, ServerDebug

# Import WorkflowRunner for actual workflow execution
from hyperscale.core.jobs.models.env import Env as CoreEnv
from hyperscale.core.jobs.runner.local_server_pool import LocalServerPool
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus as CoreWorkflowStatus
from hyperscale.core.jobs.models import Env as LocalEnv


class WorkerServer(HealthAwareServer):
    """
    Worker node in the distributed Hyperscale system.
    
    Workers:
    - Receive workflow dispatches from managers via TCP
    - Execute workflows using available CPU cores via WorkflowRunner
    - Report progress back to managers via TCP (including cores_completed)
    - Participate in SWIM healthchecks via UDP (inherited from HealthAwareServer)
    
    Workers have no knowledge of other workers - they only communicate
    with their local manager cluster.
    
    Healthchecks (UDP - SWIM protocol):
        Workers join the manager cluster's SWIM protocol. Managers probe
        workers via UDP to detect failures. Workers respond to probes
        via the inherited HealthAwareServer.
    
    Status Updates (TCP):
        Workers send status updates to managers via TCP. These contain
        capacity, queue depth, and workflow progress including cores_completed
        for faster provisioning - NOT healthchecks.
    
    Workflow Execution:
        Uses WorkflowRunner from hyperscale.core.jobs.graphs for actual
        workflow execution. Progress updates include cores_completed to
        allow managers to provision new workflows as soon as cores free up,
        without waiting for the entire workflow to complete.
    """
    
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "default",
        seed_managers: list[tuple[str, int]] | None = None,
    ):
        # Core capacity (set before super().__init__ so state embedder can access it)
        self._total_cores = env.WORKER_MAX_CORES or self._get_os_cpus() or 1

        # Core allocator for thread-safe core management
        # Uses composition to encapsulate all core allocation logic
        self._core_allocator = CoreAllocator(self._total_cores)
        
        # Manager discovery
        # Seed managers from config (TCP addresses) - tried in order until one succeeds
        self._seed_managers = seed_managers or []
        # All known managers (populated from registration response and updated from acks)
        self._known_managers: dict[str, ManagerInfo] = {}  # node_id -> ManagerInfo
        # Set of healthy manager node_ids
        self._healthy_manager_ids: set[str] = set()
        # Primary manager for leader operations (set during registration)
        self._primary_manager_id: str | None = None
        # Track when managers were marked unhealthy for reaping
        self._manager_unhealthy_since: dict[str, float] = {}  # manager_id -> time.monotonic() when marked unhealthy
        self._dead_manager_reap_interval: float = env.WORKER_DEAD_MANAGER_REAP_INTERVAL
        self._dead_manager_check_interval: float = env.WORKER_DEAD_MANAGER_CHECK_INTERVAL

        # TCP timeout settings
        self._tcp_timeout_short: float = env.WORKER_TCP_TIMEOUT_SHORT
        self._tcp_timeout_standard: float = env.WORKER_TCP_TIMEOUT_STANDARD

        # Per-manager circuit breakers for communication failures
        # Each manager has its own circuit breaker so failures to one manager
        # don't affect communication with other healthy managers
        self._manager_circuits: dict[str, ErrorStats] = {}  # manager_id -> ErrorStats
        self._manager_addr_circuits: dict[tuple[str, int], ErrorStats] = {}  # (host, port) -> ErrorStats for pre-registration
        
        # Workflow execution state
        self._active_workflows: dict[str, WorkflowProgress] = {}
        self._workflow_tokens: dict[str, str] = {}  # workflow_id -> TaskRunner token
        self._workflow_cancel_events: dict[str, asyncio.Event] = {}
        self._workflow_last_progress: dict[str, float] = {}  # workflow_id -> last update time
        self._workflow_id_to_name: dict[str, str] = {}  # workflow_id -> workflow_name for cancellation

        # Fence token tracking for at-most-once dispatch
        # Tracks highest fence token seen per workflow_id to reject stale/duplicate dispatches
        # Key: workflow_id, Value: highest fence_token seen
        self._workflow_fence_tokens: dict[str, int] = {}
        
        # WorkflowRunner for actual workflow execution
        # Initialized lazily when first workflow is received
        self._core_env: CoreEnv | None = None
        
        # Track cores that have completed within a workflow
        # workflow_id -> set of completed core indices
        self._workflow_cores_completed: dict[str, set[int]] = {}
        
        # Progress update configuration (from Env with sane defaults)
        self._progress_update_interval: float = env.WORKER_PROGRESS_UPDATE_INTERVAL

        # Buffered progress updates - collect updates and send at controlled pace
        self._progress_buffer: dict[str, WorkflowProgress] = {}  # workflow_id -> latest progress
        self._progress_buffer_lock = asyncio.Lock()
        self._progress_flush_interval: float = env.WORKER_PROGRESS_FLUSH_INTERVAL
        self._progress_flush_task: asyncio.Task | None = None

        # Backpressure tracking (AD-23)
        # Track backpressure signals from managers to adjust update frequency
        self._manager_backpressure: dict[str, BackpressureLevel] = {}  # manager_id -> level
        self._backpressure_delay_ms: int = 0  # Current delay suggestion from managers

        # Dead manager reap loop task
        self._dead_manager_reap_task: asyncio.Task | None = None

        # Cancellation polling configuration and task
        self._cancellation_poll_interval: float = env.WORKER_CANCELLATION_POLL_INTERVAL
        self._cancellation_poll_task: asyncio.Task | None = None

        # State versioning (Lamport clock extension)
        self._state_version = 0

        # Extension request state (AD-26)
        # Workers can request deadline extensions via heartbeat piggyback
        # when running long workflows that may exceed the default deadline
        self._extension_requested: bool = False
        self._extension_reason: str = ""
        self._extension_current_progress: float = 0.0  # 0.0-1.0 progress indicator

        # Protocol version negotiation result (AD-25)
        # Set during registration response handling
        self._negotiated_capabilities: NegotiatedCapabilities | None = None
        
        # Queue depth tracking
        self._pending_workflows: list[WorkflowDispatch] = []
        
        # Create state embedder for Serf-style heartbeat embedding in SWIM messages
        state_embedder = WorkerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_worker_state=lambda: self._get_worker_state().value,
            get_available_cores=lambda: self._core_allocator.available_cores,
            get_queue_depth=lambda: len(self._pending_workflows),
            get_cpu_percent=self._get_cpu_percent,
            get_memory_percent=self._get_memory_percent,
            get_state_version=lambda: self._state_version,
            get_active_workflows=lambda: {
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
            on_manager_heartbeat=self._handle_manager_heartbeat,
            get_tcp_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            # Health piggyback fields (AD-19)
            get_health_accepting_work=lambda: self._get_worker_state() in (WorkerState.HEALTHY, WorkerState.DEGRADED),
            get_health_throughput=lambda: 0.0,  # Actual throughput tracking deferred
            get_health_expected_throughput=lambda: 0.0,  # Expected throughput calculation deferred
            get_health_overload_state=lambda: "healthy",  # Workers don't have overload detector yet
            # Extension request fields (AD-26)
            get_extension_requested=lambda: self._extension_requested,
            get_extension_reason=lambda: self._extension_reason,
            get_extension_current_progress=lambda: self._extension_current_progress,
        )
        
        # Initialize parent HealthAwareServer
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
            state_embedder=state_embedder,
        )
        
        # Register callback for manager failure detection via SWIM
        self.register_on_node_dead(self._on_node_dead)

        self._updates = InterfaceUpdatesController()

        self._remote_manger = RemoteGraphManager(
            self._updates,
            self._total_cores,
            status_update_poll_interval=env.STATUS_UPDATE_POLL_INTERVAL,
        )
        self._server_pool = LocalServerPool(self._total_cores)
        self._pool_task: asyncio.Task | None = None
        self._local_udp_port = self._udp_port + (self._total_cores ** 2)
        self._worker_connect_timeout = TimeParser(env.MERCURY_SYNC_CONNECT_SECONDS).time
        self._local_env = LocalEnv(
            MERCURY_SYNC_AUTH_SECRET=env.MERCURY_SYNC_AUTH_SECRET
        )

        self._env = env
        self._cpu_monitor = CPUMonitor(env)
        self._memory_monitor = MemoryMonitor(env)
        self._logging_config: LoggingConfig | None = None
    

    def _bin_and_check_socket_range(self):
        base_worker_port = self._local_udp_port + (self._total_cores ** 2)
        return [
            (
                self._host,
                port,
            )
            for port in range(
                base_worker_port,
                base_worker_port + (self._total_cores**2),
                self._total_cores,
            )
        ]

    def _get_core_env(self) -> CoreEnv:
        """
        Get or create a CoreEnv instance for WorkflowRunner.
        
        Converts from distributed_rewrite Env to core Env with sensible defaults.
        """
        if self._core_env is None:
            self._core_env = CoreEnv(
                MERCURY_SYNC_AUTH_SECRET=self._env.MERCURY_SYNC_AUTH_SECRET,
                MERCURY_SYNC_AUTH_SECRET_PREVIOUS=self._env.MERCURY_SYNC_AUTH_SECRET_PREVIOUS,
                MERCURY_SYNC_LOGS_DIRECTORY=self._env.MERCURY_SYNC_LOGS_DIRECTORY,
                MERCURY_SYNC_LOG_LEVEL=self._env.MERCURY_SYNC_LOG_LEVEL,
                MERCURY_SYNC_MAX_CONCURRENCY=self._env.MERCURY_SYNC_MAX_CONCURRENCY,
                MERCURY_SYNC_TASK_RUNNER_MAX_THREADS=self._total_cores,
                MERCURY_SYNC_MAX_RUNNING_WORKFLOWS=self._total_cores,
                MERCURY_SYNC_MAX_PENDING_WORKFLOWS=100,
            )
        return self._core_env
    
    @property
    def node_info(self) -> NodeInfo:
        """Get this worker's node info."""
        return NodeInfo(
            node_id=self._node_id.full,
            role=NodeRole.WORKER.value,
            host=self._host,
            port=self._tcp_port,
            datacenter=self._node_id.datacenter,
            version=self._state_version,
            udp_port=self._udp_port,
        )
    
    def _increment_version(self) -> int:
        """Increment and return the state version."""
        self._state_version += 1
        return self._state_version
    
    def _get_manager_circuit(self, manager_id: str) -> ErrorStats:
        """
        Get or create a circuit breaker for a specific manager.

        Each manager has its own circuit breaker so that failures to one
        manager don't affect communication with other managers.
        """
        if manager_id not in self._manager_circuits:
            cb_config = self.env.get_circuit_breaker_config()
            self._manager_circuits[manager_id] = ErrorStats(
                max_errors=cb_config['max_errors'],
                window_seconds=cb_config['window_seconds'],
                half_open_after=cb_config['half_open_after'],
            )
        return self._manager_circuits[manager_id]

    def _get_manager_circuit_by_addr(self, addr: tuple[str, int]) -> ErrorStats:
        """
        Get or create a circuit breaker for a manager by address.

        Used during initial registration when we don't yet know the manager's ID.
        """
        if addr not in self._manager_addr_circuits:
            cb_config = self.env.get_circuit_breaker_config()
            self._manager_addr_circuits[addr] = ErrorStats(
                max_errors=cb_config['max_errors'],
                window_seconds=cb_config['window_seconds'],
                half_open_after=cb_config['half_open_after'],
            )
        return self._manager_addr_circuits[addr]

    def _is_manager_circuit_open(self, manager_id: str) -> bool:
        """Check if a specific manager's circuit breaker is open."""
        circuit = self._manager_circuits.get(manager_id)
        if not circuit:
            return False
        return circuit.circuit_state == CircuitState.OPEN

    def _is_manager_circuit_open_by_addr(self, addr: tuple[str, int]) -> bool:
        """Check if a manager's circuit breaker is open by address."""
        circuit = self._manager_addr_circuits.get(addr)
        if not circuit:
            return False
        return circuit.circuit_state == CircuitState.OPEN

    def get_manager_circuit_status(self, manager_id: str | None = None) -> dict:
        """
        Get circuit breaker status for a specific manager or summary of all.

        Args:
            manager_id: Specific manager to get status for, or None for summary

        Returns a dict with circuit breaker state information.
        """
        if manager_id:
            circuit = self._manager_circuits.get(manager_id)
            if not circuit:
                return {"error": f"No circuit breaker for manager {manager_id}"}
            return {
                "manager_id": manager_id,
                "circuit_state": circuit.circuit_state.name,
                "error_count": circuit.error_count,
                "error_rate": circuit.error_rate,
            }

        # Summary of all managers
        return {
            "managers": {
                mid: {
                    "circuit_state": cb.circuit_state.name,
                    "error_count": cb.error_count,
                }
                for mid, cb in self._manager_circuits.items()
            },
            "open_circuits": [
                mid for mid, cb in self._manager_circuits.items()
                if cb.circuit_state == CircuitState.OPEN
            ],
            "healthy_managers": len(self._healthy_manager_ids),
            "primary_manager": self._primary_manager_id,
        }
    
    async def start(self, timeout: float | None = None) -> None:

        if self._logging_config is None:
            self._logging_config = LoggingConfig()
            self._logging_config.update(
                log_directory=self._env.MERCURY_SYNC_LOGS_DIRECTORY,
                log_level=self._env.MERCURY_SYNC_LOG_LEVEL,
            )
        # Start the worker server (TCP/UDP listeners, task runner, etc.)
        # Start the underlying server (TCP/UDP listeners, task runner, etc.)
        # Uses SWIM settings from Env configuration
        await self.start_server(init_context=self.env.get_swim_init_context())

        # Mark as started for stop() guard
        self._started = True

        """Start the worker server and register with managers."""
        if timeout is None:
            timeout = self._worker_connect_timeout
        
        worker_ips = self._bin_and_check_socket_range()

        await self._cpu_monitor.start_background_monitor(
            self._node_id.datacenter,
            self._node_id.full,
        )

        await self._memory_monitor.start_background_monitor(
            self._node_id.datacenter,
            self._node_id.full,
        )

        await self._server_pool.setup()

        await self._remote_manger.start(
            self._host,
            self._local_udp_port,
            self._local_env,
        )

        # Register callback for instant core availability notifications
        # This enables event-driven dispatch when workflows complete
        self._remote_manger.set_on_cores_available(self._on_cores_available)

        # IMPORTANT: leader_address must match where RemoteGraphManager is listening
        # This was previously using self._udp_port which caused workers to connect
        # to the wrong port and hang forever in poll_for_start
        await self._server_pool.run_pool(
            (self._host, self._local_udp_port),  # Must match remote_manger.start() port!
            worker_ips,
            self._local_env,
            enable_server_cleanup=True,
        )

        # Add timeout wrapper since poll_for_start has no internal timeout
        try:
            await asyncio.wait_for(
                self._remote_manger.connect_to_workers(
                    worker_ips,
                    timeout=timeout,
                ),
                timeout=timeout + 10.0,  # Extra buffer for poll_for_start
            )
        except asyncio.TimeoutError:

            await self._udp_logger.log(
                ServerError(
                    message=f"Timeout waiting for {len(worker_ips)} worker processes to start. "
                            f"This may indicate process spawn failures.",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            raise RuntimeError(
                f"Worker process pool failed to start within {timeout + 10.0}s. "
                f"Check logs for process spawn errors."
            )
        
        # Register with ALL seed managers for failover and consistency
        # Each manager needs to know about this worker directly
        successful_registrations = 0
        for seed_addr in self._seed_managers:
            success = await self._register_with_manager(seed_addr)
            if success:
                successful_registrations += 1

        if successful_registrations == 0:
            await self._udp_logger.log(
                ServerError(
                    message=f"Failed to register with any seed manager: {self._seed_managers}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        elif successful_registrations < len(self._seed_managers):
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Registered with {successful_registrations}/{len(self._seed_managers)} seed managers",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        
        # Join SWIM cluster with all known managers for healthchecks
        for manager in list(self._known_managers.values()):
            udp_addr = (manager.udp_host, manager.udp_port)
            await self.join_cluster(udp_addr)
        
        # Start SWIM probe cycle (UDP healthchecks)
        self._task_runner.run(self.start_probe_cycle)

        # Start buffered progress flush loop
        self._progress_flush_task = asyncio.create_task(self._progress_flush_loop())

        # Start dead manager reap loop
        self._dead_manager_reap_task = asyncio.create_task(self._dead_manager_reap_loop())

        # Start cancellation polling loop
        self._cancellation_poll_task = asyncio.create_task(self._cancellation_poll_loop())

        manager_count = len(self._known_managers)
        await self._udp_logger.log(
            ServerInfo(
                message=f"Worker started with {self._total_cores} cores, registered with {manager_count} managers",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """
        Called when a node is marked as DEAD via SWIM.

        Marks the manager as unhealthy in our tracking and records the time
        for eventual reaping after the configured interval.
        """
        # Find which manager this address belongs to
        for manager_id, manager in list(self._known_managers.items()):
            if (manager.udp_host, manager.udp_port) == node_addr:
                self._healthy_manager_ids.discard(manager_id)

                # Track when this manager became unhealthy for reaping
                if manager_id not in self._manager_unhealthy_since:
                    self._manager_unhealthy_since[manager_id] = time.monotonic()

                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Manager {manager_id} marked unhealthy (SWIM DEAD)",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

                # If this was our primary manager, select a new one
                if manager_id == self._primary_manager_id:
                    self._task_runner.run(self._select_new_primary_manager)
                break
    
    def _on_node_alive(self, node_addr: tuple[str, int]) -> None:
        """
        Called when a node is confirmed ALIVE via SWIM.

        Marks the manager as healthy in our tracking and clears the
        unhealthy timestamp so it won't be reaped.
        """
        # Find which manager this address belongs to
        for manager_id, manager in list(self._known_managers.items()):
            if (manager.udp_host, manager.udp_port) == node_addr:
                self._healthy_manager_ids.add(manager_id)
                # Clear unhealthy tracking - manager recovered
                self._manager_unhealthy_since.pop(manager_id, None)
                break
    
    def _handle_manager_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle ManagerHeartbeat received via SWIM message embedding.
        
        This enables workers to track leadership changes in real-time
        without waiting for TCP ack responses. When a manager's leadership
        status changes, workers can immediately update their primary manager.
        """
        # Find or create manager info for this address
        manager_id = heartbeat.node_id
        
        # Check if this is a known manager
        existing_manager = self._known_managers.get(manager_id)
        
        if existing_manager:
            # Update is_leader status if it changed
            old_is_leader = existing_manager.is_leader
            if heartbeat.is_leader != old_is_leader:
                # Update the manager info with new leadership status
                self._known_managers[manager_id] = ManagerInfo(
                    node_id=existing_manager.node_id,
                    tcp_host=existing_manager.tcp_host,
                    tcp_port=existing_manager.tcp_port,
                    udp_host=existing_manager.udp_host,
                    udp_port=existing_manager.udp_port,
                    datacenter=heartbeat.datacenter,
                    is_leader=heartbeat.is_leader,
                )
                
                # If this manager became the leader, switch primary
                if heartbeat.is_leader and self._primary_manager_id != manager_id:
                    old_primary = self._primary_manager_id
                    self._primary_manager_id = manager_id
                    
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"Leadership change via SWIM: {old_primary} -> {manager_id}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
        else:
            # New manager discovered via SWIM - create entry
            # Use TCP address from heartbeat if available, fallback to convention
            tcp_host = heartbeat.tcp_host if heartbeat.tcp_host else source_addr[0]
            tcp_port = heartbeat.tcp_port if heartbeat.tcp_port else source_addr[1] - 1
            new_manager = ManagerInfo(
                node_id=manager_id,
                tcp_host=tcp_host,
                tcp_port=tcp_port,
                udp_host=source_addr[0],
                udp_port=source_addr[1],
                datacenter=heartbeat.datacenter,
                is_leader=heartbeat.is_leader,
            )
            self._known_managers[manager_id] = new_manager
            self._healthy_manager_ids.add(manager_id)

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Discovered new manager via SWIM: {manager_id} (leader={heartbeat.is_leader})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Register with the newly discovered manager for consistency
            # This ensures all managers know about this worker
            self._task_runner.run(
                self._register_with_manager,
                (new_manager.tcp_host, new_manager.tcp_port),
            )

            # If this is a leader and we don't have one, use it
            if heartbeat.is_leader and not self._primary_manager_id:
                self._primary_manager_id = manager_id
    
    async def _select_new_primary_manager(self) -> None:
        """Select a new primary manager from healthy managers."""
        # Prefer the leader if we know one
        for manager_id in self._healthy_manager_ids:
            manager = self._known_managers.get(manager_id)
            if manager and manager.is_leader:
                self._primary_manager_id = manager_id
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Selected new primary manager (leader): {manager_id}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return
        
        # Otherwise pick any healthy manager
        if self._healthy_manager_ids:
            self._primary_manager_id = next(iter(self._healthy_manager_ids))
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Selected new primary manager: {self._primary_manager_id}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        else:
            self._primary_manager_id = None
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message="No healthy managers available!",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message="No available managers for failover - worker is orphaned",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
    async def _report_active_workflows_to_managers(self) -> None:
        """Report all active workflows to all healthy managers."""
        if not self._healthy_manager_ids:
            return
        
        for workflow_id, progress in list(self._active_workflows.items()):
            try:
                await self._send_progress_to_all_managers(progress)
            except Exception:
                pass
    
    def _get_healthy_manager_tcp_addrs(self) -> list[tuple[str, int]]:
        """Get TCP addresses of all healthy managers."""
        addrs = []
        for manager_id in self._healthy_manager_ids:
            manager = self._known_managers.get(manager_id)
            if manager:
                addrs.append((manager.tcp_host, manager.tcp_port))
        return addrs
    
    def _get_primary_manager_tcp_addr(self) -> tuple[str, int] | None:
        """Get TCP address of the primary manager."""
        if not self._primary_manager_id:
            return None
        manager = self._known_managers.get(self._primary_manager_id)
        if manager:
            return (manager.tcp_host, manager.tcp_port)
        return None
    
    async def stop(
        self,
        drain_timeout: float = 5,
        broadcast_leave: bool = True
    ) -> None:
        """Stop the worker server."""
        # Guard against stopping a server that was never started
        # _running is False by default and only set to True in start()
        if not self._running and not hasattr(self, '_started'):
            return

        # Set _running to False early to stop all background loops
        # This ensures progress monitors and flush loop exit their while loops
        self._running = False

        # Skip all progress monitoring tasks to prevent new status updates
        progress_task_names = [
            name for name in self._task_runner.tasks.keys()
            if name.startswith("progress:")
        ]
        if progress_task_names:
            self._task_runner.skip_tasks(progress_task_names)

        # Cancel progress flush loop
        if self._progress_flush_task and not self._progress_flush_task.done():
            self._progress_flush_task.cancel()
            try:
                await self._progress_flush_task
            except asyncio.CancelledError:
                pass

        # Cancel dead manager reap loop
        if self._dead_manager_reap_task and not self._dead_manager_reap_task.done():
            self._dead_manager_reap_task.cancel()
            try:
                await self._dead_manager_reap_task
            except asyncio.CancelledError:
                pass

        # Cancel cancellation poll loop
        if self._cancellation_poll_task and not self._cancellation_poll_task.done():
            self._cancellation_poll_task.cancel()
            try:
                await self._cancellation_poll_task
            except asyncio.CancelledError:
                pass

        # Cancel all active workflows via TaskRunner
        for workflow_id in list(self._workflow_tokens.keys()):
            await self._cancel_workflow(workflow_id, "server_shutdown")

        # Graceful shutdown (broadcasts leave via SWIM)

        await self._cpu_monitor.stop_background_monitor(
            self._node_id.datacenter,
            self._node_id.full,
        )
        await self._memory_monitor.stop_background_monitor(
            self._node_id.datacenter,
            self._node_id.full,
        )

        await self._remote_manger.shutdown_workers()
        await self._remote_manger.close()

        # Kill any remaining child processes
        try:
            loop = asyncio.get_running_loop()
            children = await loop.run_in_executor(None, active_children)
            if children:
                await asyncio.gather(
                    *[loop.run_in_executor(None, child.kill) for child in children]
                )
        except RuntimeError:
            # No running loop - kill children synchronously
            for child in active_children():
                try:
                    child.kill()
                except Exception:
                    pass

        await self._server_pool.shutdown()

        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )


    def abort(self):
        # Set _running to False early to stop all background loops
        self._running = False

        # Cancel progress flush loop
        if self._progress_flush_task and not self._progress_flush_task.done():
            try:
                self._progress_flush_task.cancel()
            except Exception:
                pass

        # Cancel dead manager reap loop
        if self._dead_manager_reap_task and not self._dead_manager_reap_task.done():
            try:
                self._dead_manager_reap_task.cancel()
            except Exception:
                pass

        # Cancel cancellation poll loop
        if self._cancellation_poll_task and not self._cancellation_poll_task.done():
            try:
                self._cancellation_poll_task.cancel()
            except Exception:
                pass

        try:
            self._cpu_monitor.abort_all_background_monitors()

        except Exception:
            pass

        try:
            self._memory_monitor.abort_all_background_monitors()

        except Exception:
            pass

        try:
            self._remote_manger.abort()
        except (Exception, asyncio.CancelledError):
            pass

        try:
            self._server_pool.abort()
        except (Exception, asyncio.CancelledError):
            pass

        return super().abort()
    
    async def _register_with_manager(
        self,
        manager_addr: tuple[str, int],
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> bool:
        """
        Register this worker with a manager.

        Uses exponential backoff for retries:
        - Attempt 1: immediate
        - Attempt 2: 0.5s delay
        - Attempt 3: 1.0s delay
        - Attempt 4: 2.0s delay

        Each manager has its own circuit breaker - failures to one manager
        don't affect registration with other managers.

        Args:
            manager_addr: (host, port) tuple of manager
            max_retries: Maximum number of retry attempts (default 3)
            base_delay: Base delay in seconds for exponential backoff (default 0.5)

        Returns:
            True if registration succeeded, False otherwise
        """
        # Get per-manager circuit breaker (by address since we don't know ID yet)
        circuit = self._get_manager_circuit_by_addr(manager_addr)

        # Check circuit breaker first
        if circuit.circuit_state == CircuitState.OPEN:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Cannot register with {manager_addr}: circuit breaker is OPEN",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

        # Build capabilities string from current protocol version (AD-25)
        current_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
        capabilities_str = ",".join(sorted(current_features))

        registration = WorkerRegistration(
            node=self.node_info,
            total_cores=self._total_cores,
            available_cores=self._core_allocator.available_cores,
            memory_mb=self._get_memory_mb(),
            available_memory_mb=self._get_available_memory_mb(),
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            capabilities=capabilities_str,
        )

        for attempt in range(max_retries + 1):
            try:
                # Use decorated send method - handle() will capture manager's address
                result = await self.send_worker_register(
                    manager_addr,
                    registration.dump(),
                    timeout=5.0,
                )

                if not isinstance(result, Exception):
                    circuit.record_success()
                    if attempt > 0:
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerInfo(
                                message=f"Registered with manager {manager_addr} after {attempt + 1} attempts",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                    return True

            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Registration attempt {attempt + 1}/{max_retries + 1} failed for {manager_addr}: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            # Exponential backoff before retry (except after last attempt)
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)

        # All retries exhausted - record error on this manager's circuit breaker
        circuit.record_error()
        self._task_runner.run(
            self._udp_logger.log,
            ServerError(
                message=f"Failed to register with manager {manager_addr} after {max_retries + 1} attempts",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return False
    
    def _get_worker_state(self) -> WorkerState:
        """Determine current worker state."""
        if not self._running:
            return WorkerState.OFFLINE
        
        if self._degradation.current_level.value >= 3:
            return WorkerState.DRAINING
        elif self._degradation.current_level.value >= 2:
            return WorkerState.DEGRADED
        
        return WorkerState.HEALTHY
    
    def _get_os_cpus(self) -> int:
        if not _PSUTIL_AVAILABLE:
            return os.cpu_count()
        
        return psutil.cpu_count(logical=False)
    
    def _get_memory_mb(self) -> int:
        """Get total memory in MB."""
        if not _PSUTIL_AVAILABLE:
            return 0
        return psutil.virtual_memory().total // (1024 * 1024)
    
    def _get_available_memory_mb(self) -> int:
        """Get available memory in MB."""
        if not _PSUTIL_AVAILABLE:
            return 0
        return psutil.virtual_memory().available // (1024 * 1024)
    
    def _get_cpu_percent(self) -> float:
        """Get CPU utilization percentage."""
        if not _PSUTIL_AVAILABLE:
            return 0.0
        return psutil.cpu_percent()
    
    def _get_memory_percent(self) -> float:
        """Get memory utilization percentage."""
        if not _PSUTIL_AVAILABLE:
            return 0.0
        return psutil.virtual_memory().percent
    
    def _get_state_snapshot(self) -> WorkerStateSnapshot:
        """Get a complete state snapshot."""
        return WorkerStateSnapshot(
            node_id=self._node_id.full,
            state=self._get_worker_state().value,
            total_cores=self._total_cores,
            available_cores=self._core_allocator.available_cores,
            version=self._state_version,
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
            version=self._state_version,
            active_workflows={
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
            # Extension request fields (AD-26)
            extension_requested=self._extension_requested,
            extension_reason=self._extension_reason,
            extension_current_progress=self._extension_current_progress,
        )

    def request_extension(self, reason: str, progress: float = 0.0) -> None:
        """
        Request a deadline extension via heartbeat piggyback (AD-26).

        This sets the extension request fields in the worker's heartbeat,
        which will be processed by the manager when the next heartbeat is
        received. This is more efficient than a separate TCP call for
        extension requests.

        Args:
            reason: Human-readable reason for the extension request.
            progress: Current progress (0.0-1.0) to help manager make decisions.
        """
        self._extension_requested = True
        self._extension_reason = reason
        self._extension_current_progress = max(0.0, min(1.0, progress))

    def clear_extension_request(self) -> None:
        """
        Clear the extension request after it's been processed.

        Called when the worker completes its task or the manager has
        processed the extension request.
        """
        self._extension_requested = False
        self._extension_reason = ""
        self._extension_current_progress = 0.0
    
    # =========================================================================
    # Core Allocation (delegates to CoreAllocator)
    # =========================================================================

    async def get_core_assignments(self) -> dict[int, str | None]:
        """Get a copy of the current core assignments."""
        return await self._core_allocator.get_core_assignments()

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


        for wf_id in workflows:
            if await self._cancel_workflow(wf_id, reason):
                stopped.append(wf_id)

        return stopped
    
    async def _cancel_workflow(self, workflow_id: str, reason: str) -> bool:
        """Cancel a running workflow."""
        token = self._workflow_tokens.get(workflow_id)
        if not token:
            return False

        cancel_event = self._workflow_cancel_events.get(workflow_id)
        if cancel_event:
            cancel_event.set()

        await self._task_runner.cancel(token)

        if workflow_id in self._active_workflows:
            self._active_workflows[workflow_id].status = WorkflowStatus.CANCELLED.value

        # Cancel in RemoteGraphManager if we have the workflow name
        workflow_name = self._workflow_id_to_name.get(workflow_id)
        if workflow_name:
            run_id = hash(workflow_id) % (2**31)
            try:
                await self._remote_manger.cancel_workflow(run_id, workflow_name)
            except Exception:
                # Best effort - don't fail the cancellation if remote manager fails
                pass

        self._increment_version()
        return True
    
    # =========================================================================
    # TCP Handlers - Registration
    # =========================================================================
    
    @tcp.send('worker_register')
    async def send_worker_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send worker registration to manager."""
        return (addr, data, timeout)
    
    @tcp.handle('worker_register')
    async def handle_worker_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle registration response from manager - populate known managers."""
        try:
            response = RegistrationResponse.load(data)

            if response.accepted:
                # Populate known managers from response
                self._update_known_managers(response.healthy_managers)

                # Set primary manager (prefer leader)
                for manager in response.healthy_managers:
                    if manager.is_leader:
                        self._primary_manager_id = manager.node_id
                        break
                else:
                    # No leader indicated, use responding manager
                    self._primary_manager_id = response.manager_id

                # Store negotiated capabilities (AD-25)
                manager_version = ProtocolVersion(
                    response.protocol_version_major,
                    response.protocol_version_minor,
                )
                negotiated_features = (
                    set(response.capabilities.split(","))
                    if response.capabilities
                    else set()
                )
                # Remove empty string if present (from split of empty string)
                negotiated_features.discard("")

                # Store negotiated capabilities for this manager connection
                self._negotiated_capabilities = NegotiatedCapabilities(
                    local_version=CURRENT_PROTOCOL_VERSION,
                    remote_version=manager_version,
                    common_features=negotiated_features,
                    compatible=True,  # If we got here with accepted=True, we're compatible
                )

                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=(
                            f"Registered with {len(response.healthy_managers)} managers, primary: {self._primary_manager_id} "
                            f"(protocol: {manager_version}, features: {len(negotiated_features)})"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            else:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Registration rejected: {response.error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
        except Exception as e:
            # Fallback for simple b'ok' responses (backwards compatibility)
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Registration ack from {addr} (legacy format)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        return data
    
    def _update_known_managers(self, managers: list[ManagerInfo]) -> None:
        """Update known managers from a list (e.g., from registration or ack)."""
        for manager in managers:
            self._known_managers[manager.node_id] = manager
            # Mark as healthy since we just received this info
            self._healthy_manager_ids.add(manager.node_id)

    @tcp.handle('manager_register')
    async def handle_manager_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle registration request from a manager.

        This enables bidirectional registration: managers can proactively
        register with workers they discover via state sync from peer managers.
        This speeds up cluster formation.
        """
        try:
            registration = ManagerToWorkerRegistration.load(data)

            # Add this manager to our known managers
            self._known_managers[registration.manager.node_id] = registration.manager
            self._healthy_manager_ids.add(registration.manager.node_id)

            # Also add any other managers included in the registration
            if registration.known_managers:
                self._update_known_managers(registration.known_managers)

            # Update primary manager if this one is the leader
            if registration.is_leader:
                self._primary_manager_id = registration.manager.node_id

            # Add manager's UDP address to SWIM for probing
            manager_udp_addr = (registration.manager.udp_host, registration.manager.udp_port)
            if manager_udp_addr[0] and manager_udp_addr[1]:
                self._probe_scheduler.add_member(manager_udp_addr)

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Manager {registration.manager.node_id[:8]}... registered with us (leader={registration.is_leader})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Return acknowledgment with our info
            ack = ManagerToWorkerRegistrationAck(
                accepted=True,
                worker_id=self._node_id.full,
                total_cores=self._total_cores,
                available_cores=self._core_allocator.available_cores,
            )
            return ack.dump()

        except Exception as e:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Failed to process manager registration: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            ack = ManagerToWorkerRegistrationAck(
                accepted=False,
                worker_id=self._node_id.full,
                error=str(e),
            )
            return ack.dump()

    # =========================================================================
    # TCP Handlers - Manager -> Worker
    # =========================================================================
    
    @tcp.send('workflow_dispatch_response')
    async def send_workflow_dispatch_response(
        self,
        address: tuple[str, int],
        ack: WorkflowDispatchAck,
    ) -> tuple[tuple[str, int], bytes]:
        """Send workflow dispatch acknowledgment."""
        return (address, ack.dump())
    
    @tcp.receive()
    async def workflow_dispatch(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Receive a workflow dispatch from a manager.

        This is the main entry point for work arriving at the worker.
        Uses atomic core allocation via CoreAllocator to prevent races.
        """
        dispatch: WorkflowDispatch | None = None
        allocation_succeeded = False

        try:
            dispatch = WorkflowDispatch.load(data)

            # VUs are the virtual users, cores are the CPU cores to allocate
            vus_for_workflow = dispatch.vus
            cores_to_allocate = dispatch.cores

            # Check backpressure first (fast path rejection)
            if self._get_worker_state() == WorkerState.DRAINING:
                ack = WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error="Worker is draining, not accepting new work",
                )
                return ack.dump()

            # Validate fence token for at-most-once dispatch
            # Reject if we've seen this workflow_id with a higher or equal fence token
            current_fence_token = self._workflow_fence_tokens.get(dispatch.workflow_id, -1)
            if dispatch.fence_token <= current_fence_token:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Rejecting stale dispatch for {dispatch.workflow_id}: "
                                f"fence_token={dispatch.fence_token} <= current={current_fence_token}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                ack = WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error=f"Stale fence token: {dispatch.fence_token} <= {current_fence_token}",
                )
                return ack.dump()

            # Update fence token tracking
            self._workflow_fence_tokens[dispatch.workflow_id] = dispatch.fence_token

            # Atomic core allocation - no TOCTOU race
            # CoreAllocator checks availability and allocates in one atomic operation
            allocation_result = await self._core_allocator.allocate(
                dispatch.workflow_id,
                cores_to_allocate,
            )

            if not allocation_result.success:
                ack = WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error=allocation_result.error or f"Failed to allocate {cores_to_allocate} cores",
                )
                return ack.dump()

            allocation_succeeded = True
            allocated_cores = allocation_result.allocated_cores
            self._increment_version()

            # Create progress tracker with assigned cores
            progress = WorkflowProgress(
                job_id=dispatch.job_id,
                workflow_id=dispatch.workflow_id,
                workflow_name="",
                status=WorkflowStatus.RUNNING.value,
                completed_count=0,
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
                collected_at=time.time(),  # Unix timestamp for cross-node alignment
                assigned_cores=allocated_cores,
                worker_available_cores=self._core_allocator.available_cores,
                worker_workflow_completed_cores=0,
                worker_workflow_assigned_cores=cores_to_allocate,
            )
            self._active_workflows[dispatch.workflow_id] = progress

            # Create cancellation event
            cancel_event = asyncio.Event()
            self._workflow_cancel_events[dispatch.workflow_id] = cancel_event

            # Start execution task via TaskRunner
            # vus_for_workflow = VUs (virtual users, can be 50k+)
            # len(allocated_cores) = CPU cores (from priority, e.g., 4)
            run = self._task_runner.run(
                self._execute_workflow,
                dispatch,
                progress,
                cancel_event,
                vus_for_workflow,  # VUs for the workflow
                len(allocated_cores),  # CPU cores allocated
                alias=f"workflow:{dispatch.workflow_id}",
            )
            # Store the token string (not the Run object) for later cancellation
            self._workflow_tokens[dispatch.workflow_id] = run.token

            # Task started successfully - cores are now managed by _execute_workflow's finally block
            allocation_succeeded = False  # Clear so exception handler won't free them

            # Return acknowledgment
            ack = WorkflowDispatchAck(
                workflow_id=dispatch.workflow_id,
                accepted=True,
                cores_assigned=cores_to_allocate,
            )
            return ack.dump()

        except Exception as e:
            # Free any allocated cores if task didn't start successfully
            if dispatch and allocation_succeeded:
                await self._core_allocator.free(dispatch.workflow_id)
                self._workflow_cancel_events.pop(dispatch.workflow_id, None)
                self._active_workflows.pop(dispatch.workflow_id, None)
                self._workflow_fence_tokens.pop(dispatch.workflow_id, None)

            workflow_id = dispatch.workflow_id if dispatch else "unknown"
            ack = WorkflowDispatchAck(
                workflow_id=workflow_id,
                accepted=False,
                error=str(e),
            )
            return ack.dump()
    
    async def _execute_workflow(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        cancel_event: asyncio.Event,
        allocated_vus: int,
        allocated_cores: int,
    ):
        """Execute a workflow using WorkflowRunner."""
        start_time = time.monotonic()
        run_id = hash(dispatch.workflow_id) % (2**31)
        error: Exception | None = None
        workflow_error: str | None = None
        workflow_results: dict = {}
        context_updates: bytes = b''
        progress_token = None

        try:
            # Phase 1: Setup - unpickle workflow and context
            workflow = dispatch.load_workflow()
            context_dict = dispatch.load_context()

            progress.workflow_name = workflow.name
            self._increment_version()
            self._workflow_id_to_name[dispatch.workflow_id] = workflow.name
            self._workflow_cores_completed[dispatch.workflow_id] = set()

            # Transition to RUNNING - sends immediate update (lifecycle event)
            await self._transition_workflow_status(progress, WorkflowStatus.RUNNING, start_time)

            # Start progress monitor
            progress_token = self._task_runner.run(
                self._monitor_workflow_progress,
                dispatch,
                progress,
                run_id,
                cancel_event,
                alias=f"progress:{dispatch.workflow_id}",
            )

            # Phase 2: Execute the workflow
            (
                _,
                workflow_results,
                context,
                error,
                status,
            ) = await self._remote_manger.execute_workflow(
                run_id,
                workflow,
                context_dict,
                allocated_vus,
                max(allocated_cores, 1),
            )

            progress.cores_completed = len(progress.assigned_cores)

            # Phase 3: Determine final status and transition
            if status != CoreWorkflowStatus.COMPLETED:
                workflow_error = str(error) if error else "Unknown error"
                await self._transition_workflow_status(progress, WorkflowStatus.FAILED, start_time)
            else:
                await self._transition_workflow_status(progress, WorkflowStatus.COMPLETED, start_time)

            context_updates = cloudpickle.dumps(context.dict() if context else {})

        except asyncio.CancelledError:
            workflow_error = "Cancelled"
            await self._transition_workflow_status(progress, WorkflowStatus.CANCELLED, start_time)
        except Exception as e:
            workflow_error = str(e) if e else "Unknown error"
            error = e
            await self._transition_workflow_status(progress, WorkflowStatus.FAILED, start_time)
        finally:
            # Stop progress monitor
            if progress_token:
                await self._task_runner.cancel(progress_token.token)

            # Free cores
            await self._core_allocator.free(dispatch.workflow_id)

            # Send final result to manager
            await self._send_workflow_final_result(
                dispatch, progress, workflow_results, context_updates, workflow_error
            )

            # Cleanup state
            self._increment_version()
            self._workflow_tokens.pop(dispatch.workflow_id, None)
            self._workflow_cancel_events.pop(dispatch.workflow_id, None)
            self._active_workflows.pop(dispatch.workflow_id, None)
            self._workflow_last_progress.pop(dispatch.workflow_id, None)
            self._workflow_cores_completed.pop(dispatch.workflow_id, None)
            self._workflow_fence_tokens.pop(dispatch.workflow_id, None)
            self._workflow_id_to_name.pop(dispatch.workflow_id, None)
            self._remote_manger.start_server_cleanup()

        return (
            progress,
            error,
        )
    
    async def _monitor_workflow_progress(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        run_id: int,
        cancel_event: asyncio.Event,
    ) -> None:
        """Monitor workflow progress and send updates to manager."""
        start_time = time.monotonic()
        workflow_name = progress.workflow_name

        
        while not cancel_event.is_set():
            try:
                await asyncio.sleep(self._progress_update_interval)

                # Drain all pending stats from WorkflowRunner, get most recent
                # This prevents backlog when updates are produced faster than consumed
                workflow_status_update = await self._remote_manger.drain_workflow_updates(run_id, workflow_name)
                if workflow_status_update is None:
                    # No update available yet, keep waiting
                    continue

                status = CoreWorkflowStatus(workflow_status_update.status)
                
                # Get system stats
                avg_cpu, avg_mem = (
                    self._cpu_monitor.get_moving_avg(
                        run_id,
                        progress.workflow_name,
                    ),
                    self._memory_monitor.get_moving_avg(
                        run_id,
                        progress.workflow_name,
                    ),
                )
                
                # Update progress
                progress.completed_count = workflow_status_update.completed_count
                progress.failed_count = workflow_status_update.failed_count
                progress.elapsed_seconds = time.monotonic() - start_time
                progress.rate_per_second = (
                    workflow_status_update.completed_count / progress.elapsed_seconds
                    if progress.elapsed_seconds > 0 else 0.0
                )
                progress.timestamp = time.monotonic()
                progress.collected_at = time.time()  # Unix timestamp for cross-node alignment
                progress.avg_cpu_percent = avg_cpu
                progress.avg_memory_mb = avg_mem

                availability = await self._remote_manger.get_availability()
                (
                    workflow_assigned_cores,
                    workflow_completed_cores,
                    worker_available_cores,  # Live count of free cores from RemoteGraphManager
                ) = availability

                if worker_available_cores > 0:
                    await self._core_allocator.free_subset(progress.workflow_id, worker_available_cores)

                progress.worker_workflow_assigned_cores = workflow_assigned_cores
                progress.worker_workflow_completed_cores = workflow_completed_cores
                # Live available cores from CoreAllocator - this is the real-time
                # count of cores that have finished their work and are available
                progress.worker_available_cores = self._core_allocator.available_cores
                
                # Convert step stats
                progress.step_stats = [
                    StepStats(
                        step_name=step_name,
                        completed_count=stats.get("ok", 0),
                        failed_count=stats.get("err", 0),
                        total_count=stats.get("total", 0),
                    )
                    for step_name, stats in workflow_status_update.step_stats.items()
                ]
                
                # Estimate cores_completed based on work completed
                total_cores = len(progress.assigned_cores)
                if total_cores > 0:
                    # Use VUs as the total work units for estimation
                    total_work = max(dispatch.vus * 100, 1)  # VUs * iterations estimate
                    estimated_complete = min(
                        total_cores,
                        int(total_cores * (workflow_status_update.completed_count / total_work))
                    )
                    progress.cores_completed = estimated_complete
                
                # Map status
                if status == CoreWorkflowStatus.RUNNING:
                    progress.status = WorkflowStatus.RUNNING.value
                elif status == CoreWorkflowStatus.COMPLETED:
                    progress.status = WorkflowStatus.COMPLETED.value
                    progress.cores_completed = total_cores
                elif status == CoreWorkflowStatus.FAILED:
                    progress.status = WorkflowStatus.FAILED.value
                elif status == CoreWorkflowStatus.PENDING:
                    progress.status = WorkflowStatus.ASSIGNED.value
                
                # Send update directly (not buffered) for real-time streaming
                # The manager's windowed stats collector handles time-correlation
                if self._healthy_manager_ids:
                    await self._send_progress_update_direct(progress)
                    self._workflow_last_progress[dispatch.workflow_id] = time.monotonic()
                
            except asyncio.CancelledError:
                break
            except Exception as err:
                await self._udp_logger.log(
                    ServerError(
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=self._node_id.full,
                        message=f'Encountered Update Error: {str(err)} for workflow: {progress.workflow_name} workflow id: {progress.workflow_id}'
                    )
                )
    
    async def _transition_workflow_status(
        self,
        progress: WorkflowProgress,
        new_status: WorkflowStatus,
        start_time: float | None = None,
    ) -> None:
        """
        Transition workflow to a new status and send an immediate progress update.

        This is the ONLY method that should change workflow status. By funneling
        all status changes through here, we guarantee:
        1. Every status transition triggers a progress update
        2. Updates are sent immediately (not buffered) for lifecycle events
        3. Timestamps are consistently set
        4. Consistent behavior regardless of workflow duration

        Args:
            progress: The workflow progress to update
            new_status: The new status to transition to
            start_time: Optional start time for elapsed_seconds calculation
        """
        progress.status = new_status.value
        progress.timestamp = time.monotonic()
        progress.collected_at = time.time()

        if start_time is not None:
            progress.elapsed_seconds = time.monotonic() - start_time

        # Always send lifecycle transitions immediately (not buffered)
        # This ensures short-running workflows still get all state updates
        if self._healthy_manager_ids:
            await self._send_progress_update_direct(progress)

    async def _send_progress_update(
        self,
        progress: WorkflowProgress,
    ) -> None:
        """
        Buffer a progress update for batched sending to manager.

        Instead of sending immediately, updates are collected in a buffer
        and flushed periodically by _progress_flush_loop. This reduces
        network traffic and noisy status updates.

        NOTE: For status transitions, use _transition_workflow_status instead
        to ensure immediate delivery.

        Args:
            progress: Workflow progress to buffer
        """
        async with self._progress_buffer_lock:
            # Always keep the latest progress for each workflow
            self._progress_buffer[progress.workflow_id] = progress

    async def _progress_flush_loop(self) -> None:
        """
        Background loop that flushes buffered progress updates to manager.

        Runs continuously while the worker is active, flushing all buffered
        progress updates at a controlled interval. Respects backpressure signals
        from managers to adjust update frequency (AD-23).
        """
        while self._running:
            try:
                # Calculate effective flush interval based on backpressure
                effective_interval = self._get_effective_flush_interval()
                await asyncio.sleep(effective_interval)

                # Skip if under heavy backpressure (BATCH or REJECT level)
                max_backpressure = self._get_max_backpressure_level()
                if max_backpressure >= BackpressureLevel.REJECT:
                    # Drop non-critical updates under heavy backpressure
                    async with self._progress_buffer_lock:
                        self._progress_buffer.clear()
                    continue

                # Get and clear the buffer atomically
                async with self._progress_buffer_lock:
                    if not self._progress_buffer:
                        continue
                    updates_to_send = dict(self._progress_buffer)
                    self._progress_buffer.clear()

                # Send buffered updates
                if self._healthy_manager_ids:
                    for workflow_id, progress in updates_to_send.items():
                        await self._send_progress_update_direct(progress)

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def _get_effective_flush_interval(self) -> float:
        """
        Get effective flush interval based on backpressure signals.

        Increases interval when managers signal backpressure.
        """
        base_interval = self._progress_flush_interval

        # Add backpressure delay if signaled
        if self._backpressure_delay_ms > 0:
            delay_seconds = self._backpressure_delay_ms / 1000.0
            return base_interval + delay_seconds

        return base_interval

    def _get_max_backpressure_level(self) -> BackpressureLevel:
        """Get the maximum backpressure level across all managers."""
        if not self._manager_backpressure:
            return BackpressureLevel.NONE
        return max(self._manager_backpressure.values())

    def _handle_backpressure_signal(
        self,
        manager_id: str,
        signal: BackpressureSignal,
    ) -> None:
        """
        Handle backpressure signal from a manager.

        Updates tracking state to adjust future update behavior.

        Args:
            manager_id: ID of manager that sent the signal
            signal: BackpressureSignal from the manager
        """
        self._manager_backpressure[manager_id] = signal.level
        self._backpressure_delay_ms = max(
            self._backpressure_delay_ms,
            signal.suggested_delay_ms,
        )

    def _on_cores_available(self, available_cores: int) -> None:
        """
        Callback invoked by RemoteGraphManager when cores become available.

        Immediately notifies the Manager so it can dispatch waiting workflows.
        This enables event-driven dispatch instead of polling-based.

        Args:
            available_cores: Number of cores now available
        """
        if not self._running or available_cores <= 0:
            return

        # Update the core allocator first
        # Note: free_subset is async but we're in a sync callback,
        # so we schedule it on the event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule the async notification
                loop.create_task(self._notify_manager_cores_available(available_cores))
        except RuntimeError:
            pass  # Event loop not available, skip notification

    async def _notify_manager_cores_available(self, available_cores: int) -> None:
        """
        Send immediate core availability notification to Manager.

        Creates a lightweight heartbeat with current core status and sends
        it directly to trigger workflow dispatch.
        """
        if not self._healthy_manager_ids:
            return

        try:
            # Create heartbeat with current state
            heartbeat = self._get_heartbeat()

            # Send to primary manager via TCP
            manager_addr = self._get_primary_manager_tcp_addr()
            if manager_addr:
                await self.send_tcp(
                    manager_addr,
                    "worker_heartbeat",
                    heartbeat.dump(),
                    timeout=1.0,
                )
        except Exception:
            # Best effort - don't fail if notification fails
            pass

    async def _dead_manager_reap_loop(self) -> None:
        """
        Background loop that reaps dead managers after the configured interval.

        Managers that have been unhealthy for longer than WORKER_DEAD_MANAGER_REAP_INTERVAL
        are removed from _known_managers along with their circuit breakers.
        """
        while self._running:
            try:
                await asyncio.sleep(self._dead_manager_check_interval)

                now = time.monotonic()
                managers_to_reap: list[str] = []

                for manager_id, unhealthy_since in list(self._manager_unhealthy_since.items()):
                    if now - unhealthy_since >= self._dead_manager_reap_interval:
                        managers_to_reap.append(manager_id)

                for manager_id in managers_to_reap:
                    manager_info = self._known_managers.get(manager_id)
                    manager_addr = None
                    if manager_info:
                        manager_addr = (manager_info.tcp_host, manager_info.tcp_port)

                    # Remove from all tracking structures
                    self._known_managers.pop(manager_id, None)
                    self._healthy_manager_ids.discard(manager_id)
                    self._manager_unhealthy_since.pop(manager_id, None)
                    self._manager_circuits.pop(manager_id, None)

                    # Also clean up address-based circuit breaker if we know the address
                    if manager_addr:
                        self._manager_addr_circuits.pop(manager_addr, None)

                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"Reaped dead manager {manager_id} after {self._dead_manager_reap_interval}s",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _cancellation_poll_loop(self) -> None:
        """
        Background loop that polls managers for cancellation status of running workflows.

        This provides a robust fallback for cancellation when push notifications fail
        (e.g., due to network issues or manager failover).
        """
        while self._running:
            try:
                await asyncio.sleep(self._cancellation_poll_interval)

                # Skip if no active workflows
                if not self._active_workflows:
                    continue

                # Get primary manager address
                manager_addr = self._get_primary_manager_tcp_addr()
                if not manager_addr:
                    continue

                # Check circuit breaker
                if self._primary_manager_id:
                    circuit = self._manager_circuits.get(self._primary_manager_id)
                    if circuit and circuit.state == CircuitState.OPEN:
                        continue

                # Poll for each active workflow
                workflows_to_cancel: list[str] = []
                for workflow_id, progress in list(self._active_workflows.items()):
                    query = WorkflowCancellationQuery(
                        job_id=progress.job_id,
                        workflow_id=workflow_id,
                    )

                    try:
                        response_data = await self.send_tcp(
                            manager_addr,
                            "workflow_cancellation_query",
                            query.dump(),
                            timeout=2.0,
                        )

                        if response_data:
                            response = WorkflowCancellationResponse.load(response_data)
                            if response.status == "CANCELLED":
                                workflows_to_cancel.append(workflow_id)

                    except Exception:
                        # Network errors are expected sometimes - don't log each one
                        pass

                # Cancel any workflows that the manager says are cancelled
                for workflow_id in workflows_to_cancel:
                    cancel_event = self._workflow_cancel_events.get(workflow_id)
                    if cancel_event and not cancel_event.is_set():
                        cancel_event.set()

                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerInfo(
                                message=f"Cancelling workflow {workflow_id} via poll (manager confirmed cancellation)",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _send_progress_update_direct(
        self,
        progress: WorkflowProgress,
        max_retries: int = 2,
        base_delay: float = 0.2,
    ) -> None:
        """
        Send a progress update directly to the primary manager and process ack.

        Uses limited retries with exponential backoff:
        - Progress updates happen frequently, so we keep retries short
        - Attempt 1: immediate
        - Attempt 2: 0.2s delay
        - Attempt 3: 0.4s delay

        Circuit breaker prevents attempts when managers are unreachable.

        Args:
            progress: Workflow progress to send
            max_retries: Maximum retry attempts (default 2)
            base_delay: Base delay for exponential backoff (default 0.2s)
        """
        manager_addr = self._get_primary_manager_tcp_addr()
        if not manager_addr:
            return

        # Get per-manager circuit breaker
        primary_id = self._primary_manager_id
        if primary_id and self._is_manager_circuit_open(primary_id):
            return  # Fail fast - don't attempt communication

        circuit = self._get_manager_circuit_by_addr(manager_addr) if not primary_id else self._get_manager_circuit(primary_id)

        for attempt in range(max_retries + 1):
            try:
                response, _ = await self.send_tcp(
                    manager_addr,
                    "workflow_progress",
                    progress.dump(),
                    timeout=1.0,
                )

                # Process ack to update manager topology
                if response and isinstance(response, bytes) and response != b'error':
                    self._process_workflow_progress_ack(response)
                    circuit.record_success()
                    return  # Success

            except Exception:
                pass

            # Exponential backoff before retry (except after last attempt)
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)

        # All retries exhausted
        circuit.record_error()
    
    async def _send_progress_to_all_managers(self, progress: WorkflowProgress) -> None:
        """Send a progress update to ALL healthy managers and process acks."""
        for manager_id in list(self._healthy_manager_ids):
            manager_info = self._known_managers.get(manager_id)
            if not manager_info:
                continue

            manager_addr = (manager_info.tcp_host, manager_info.tcp_port)

            # Check per-manager circuit breaker
            if self._is_manager_circuit_open(manager_id):
                continue  # Skip this manager, try others

            circuit = self._get_manager_circuit(manager_id)

            try:
                response, _ = await self.send_tcp(
                    manager_addr,
                    "workflow_progress",
                    progress.dump(),
                    timeout=1.0,
                )

                # Process ack to update manager topology
                if response and isinstance(response, bytes) and response != b'error':
                    self._process_workflow_progress_ack(response)
                    circuit.record_success()
                else:
                    circuit.record_error()

            except Exception:
                circuit.record_error()
    
    async def _send_workflow_final_result(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        workflow_results: dict,
        context_updates: bytes,
        workflow_error: str | None,
    ) -> None:
        """
        Build and send final result to manager.

        Encapsulates the final result creation and sending logic.
        Logs but does not propagate errors from sending.
        """
        final_result = WorkflowFinalResult(
            job_id=dispatch.job_id,
            workflow_id=dispatch.workflow_id,
            workflow_name=progress.workflow_name,
            status=progress.status,
            results=workflow_results if workflow_results else b'',
            context_updates=context_updates if context_updates else b'',
            error=workflow_error,
            worker_id=self._node_id.full,
            worker_available_cores=self._core_allocator.available_cores,
        )

        try:
            await self._send_final_result(final_result)
        except Exception as send_err:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Failed to send final result for {dispatch.workflow_id}: {send_err}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _send_final_result(
        self,
        final_result: WorkflowFinalResult,
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> None:
        """
        Send workflow final result to the primary manager.

        Final results are critical - they contain:
        - Workflow results/stats
        - Context updates for dependent workflows
        - Error information for failed workflows

        Uses retries with exponential backoff since this is a critical path.
        If the primary manager's circuit breaker is open, tries other healthy managers.

        Args:
            final_result: The final result to send
            max_retries: Maximum retry attempts (default 3)
            base_delay: Base delay for exponential backoff (default 0.5s)
        """
        # Try primary manager first, then fall back to other healthy managers
        target_managers: list[str] = []

        if self._primary_manager_id:
            target_managers.append(self._primary_manager_id)

        # Add other healthy managers as fallbacks
        for manager_id in self._healthy_manager_ids:
            if manager_id not in target_managers:
                target_managers.append(manager_id)

        if not target_managers:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Cannot send final result for {final_result.workflow_id}: no healthy managers",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        # Try each manager until one succeeds
        for manager_id in target_managers:
            # Check per-manager circuit breaker
            if self._is_manager_circuit_open(manager_id):
                continue  # Skip this manager, try next

            manager_info = self._known_managers.get(manager_id)
            if manager_info is None:
                continue

            manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
            circuit = self._get_manager_circuit(manager_id)

            for attempt in range(max_retries + 1):
                try:
                    response, _ = await self.send_tcp(
                        manager_addr,
                        "workflow_final_result",
                        final_result.dump(),
                        timeout=5.0,  # Longer timeout for final results
                    )

                    if response and isinstance(response, bytes) and response != b'error':
                        circuit.record_success()
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerDebug(
                                message=f"Sent final result for {final_result.workflow_id} status={final_result.status}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                        return  # Success

                except Exception as e:
                    await self._udp_logger.log(
                        ServerError(
                            message=f"Failed to send final result for {final_result.workflow_id} attempt {attempt+1}: {e}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                # Exponential backoff before retry (except after last attempt)
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)

            # All retries exhausted for this manager
            circuit.record_error()

        # All managers failed
        await self._udp_logger.log(
            ServerError(
                message=f"Failed to send final result for {final_result.workflow_id} to any manager after {max_retries + 1} attempts each",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    def _process_workflow_progress_ack(self, data: bytes) -> None:
        """
        Process WorkflowProgressAck to update manager topology.
        
        This enables continuous manager list refresh - every ack includes
        the current list of healthy managers and leadership status.
        """
        try:
            ack = WorkflowProgressAck.load(data)
            
            # Update known managers from ack
            self._update_known_managers(ack.healthy_managers)
            
            # Update primary manager if leadership changed
            if ack.is_leader and self._primary_manager_id != ack.manager_id:
                old_primary = self._primary_manager_id
                self._primary_manager_id = ack.manager_id
                
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Leadership change detected: {old_primary} -> {ack.manager_id}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                
        except Exception:
            # Backwards compatibility: ignore parse errors for old b'ok' responses
            pass
    
    # =========================================================================
    # TCP Handlers - State Sync
    # =========================================================================
    
    @tcp.receive()
    async def state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle state sync request from a new manager leader."""
        try:
            request = StateSyncRequest.load(data)
            
            response = StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=self._state_version,
                worker_state=self._get_state_snapshot(),
            )
            return response.dump()
            
        except Exception:
            return b''
    
    # =========================================================================
    # TCP Handlers - Cancellation
    # =========================================================================
    
    @tcp.receive()
    async def cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job cancellation request from manager."""
        try:
            cancel_request = CancelJob.load(data)

            # Find and cancel all workflows for this job
            cancelled_count = 0
            for workflow_id, progress in list(self._active_workflows.items()):
                if progress.job_id == cancel_request.job_id:
                    if await self._cancel_workflow(workflow_id, cancel_request.reason):
                        cancelled_count += 1

            ack = CancelAck(
                job_id=cancel_request.job_id,
                cancelled=True,
                workflows_cancelled=cancelled_count,
            )
            return ack.dump()

        except Exception as e:
            ack = CancelAck(
                job_id="unknown",
                cancelled=False,
                error=str(e),
            )
            return ack.dump()

    @tcp.receive()
    async def cancel_workflow(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle workflow cancellation request from manager (AD-20).

        Cancels a specific workflow rather than all workflows for a job.
        This is the preferred method for targeted cancellation.
        """
        try:
            request = WorkflowCancelRequest.load(data)

            # Check if workflow exists
            progress = self._active_workflows.get(request.workflow_id)
            if not progress:
                # Workflow not found - check if it was already completed/cancelled
                # Return success with already_completed=True if we have no record
                response = WorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    success=True,
                    was_running=False,
                    already_completed=True,
                )
                return response.dump()

            # Check if workflow is for the specified job (safety check)
            if progress.job_id != request.job_id:
                response = WorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    success=False,
                    error=f"Workflow {request.workflow_id} belongs to job {progress.job_id}, not {request.job_id}",
                )
                return response.dump()

            # Check if already cancelled
            if progress.status == WorkflowStatus.CANCELLED.value:
                response = WorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    success=True,
                    was_running=False,
                    already_completed=True,
                )
                return response.dump()

            # Check if already completed or failed
            if progress.status in (WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value):
                response = WorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    success=True,
                    was_running=False,
                    already_completed=True,
                )
                return response.dump()

            # Cancel the workflow
            was_running = progress.status == WorkflowStatus.RUNNING.value
            cancelled = await self._cancel_workflow(request.workflow_id, "manager_cancel_request")

            if cancelled:
                await self._udp_logger.log(
                    ServerInfo(
                        message=f"Cancelled workflow {request.workflow_id} for job {request.job_id}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            response = WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=cancelled,
                was_running=was_running,
                already_completed=False,
            )
            return response.dump()

        except Exception as e:
            await self._udp_logger.log(
                ServerError(
                    message=f"Failed to cancel workflow: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            response = WorkflowCancelResponse(
                job_id="unknown",
                workflow_id="unknown",
                success=False,
                error=str(e),
            )
            return response.dump()

    @tcp.receive()
    async def workflow_status_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle workflow status query from manager.

        Used by the manager's orphan scanner to verify which workflows
        are actually running on this worker.

        Returns comma-separated list of active workflow IDs.
        """
        try:
            # Return list of all active workflow IDs
            active_ids = list(self._active_workflows.keys())
            return ",".join(active_ids).encode('utf-8')

        except Exception:
            return b'error'
