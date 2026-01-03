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
from typing import Any

# Optional psutil import for system metrics
try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None  # type: ignore
    _PSUTIL_AVAILABLE = False

from hyperscale.distributed_rewrite.server import tcp
from hyperscale.distributed_rewrite.swim import HealthAwareServer, WorkerStateEmbedder
from hyperscale.distributed_rewrite.swim.core import ErrorStats, CircuitState
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    ManagerHeartbeat,
    RegistrationResponse,
    WorkflowProgressAck,
    WorkerRegistration,
    WorkerHeartbeat,
    WorkerState,
    WorkerStateSnapshot,
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowProgress,
    WorkflowStatus,
    StepStats,
    StateSyncRequest,
    StateSyncResponse,
    CancelJob,
    CancelAck,
    restricted_loads,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError

# Import WorkflowRunner for actual workflow execution
from hyperscale.core.jobs.graphs import WorkflowRunner
from hyperscale.core.jobs.models.env import Env as CoreEnv
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus as CoreWorkflowStatus


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
        total_cores: int | None = None,
        seed_managers: list[tuple[str, int]] | None = None,
    ):
        # Core capacity (set before super().__init__ so state embedder can access it)
        self._total_cores = total_cores or os.cpu_count() or 1
        self._available_cores = self._total_cores
        
        # Per-core workflow assignment tracking
        # Maps core_index -> workflow_id (None if core is free)
        self._core_assignments: dict[int, str | None] = {
            i: None for i in range(self._total_cores)
        }
        # Reverse mapping: workflow_id -> list of assigned core indices
        self._workflow_cores: dict[str, list[int]] = {}
        
        # Manager discovery
        # Seed managers from config (TCP addresses) - tried in order until one succeeds
        self._seed_managers = seed_managers or []
        # All known managers (populated from registration response and updated from acks)
        self._known_managers: dict[str, ManagerInfo] = {}  # node_id -> ManagerInfo
        # Set of healthy manager node_ids
        self._healthy_manager_ids: set[str] = set()
        # Primary manager for leader operations (set during registration)
        self._primary_manager_id: str | None = None
        
        # Circuit breaker for manager communication
        # Tracks failures and implements fail-fast when managers are unreachable
        cb_config = env.get_circuit_breaker_config()
        self._manager_circuit = ErrorStats(
            max_errors=cb_config['max_errors'],
            window_seconds=cb_config['window_seconds'],
            half_open_after=cb_config['half_open_after'],
        )
        
        # Workflow execution state
        self._active_workflows: dict[str, WorkflowProgress] = {}
        self._workflow_tokens: dict[str, str] = {}  # workflow_id -> TaskRunner token
        self._workflow_cancel_events: dict[str, asyncio.Event] = {}
        self._workflow_last_progress: dict[str, float] = {}  # workflow_id -> last update time
        
        # WorkflowRunner for actual workflow execution
        # Initialized lazily when first workflow is received
        self._workflow_runner: WorkflowRunner | None = None
        self._core_env: CoreEnv | None = None
        
        # Track cores that have completed within a workflow
        # workflow_id -> set of completed core indices
        self._workflow_cores_completed: dict[str, set[int]] = {}
        
        # Progress update configuration
        self._progress_update_interval: float = 1.0  # Update every 1 second
        
        # State versioning (Lamport clock extension)
        self._state_version = 0
        
        # Queue depth tracking
        self._pending_workflows: list[WorkflowDispatch] = []
        
        # Create state embedder for Serf-style heartbeat embedding in SWIM messages
        state_embedder = WorkerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_worker_state=lambda: self._get_worker_state().value,
            get_available_cores=lambda: self._available_cores,
            get_queue_depth=lambda: len(self._pending_workflows),
            get_cpu_percent=self._get_cpu_percent,
            get_memory_percent=self._get_memory_percent,
            get_state_version=lambda: self._state_version,
            get_active_workflows=lambda: {
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
            on_manager_heartbeat=self._handle_manager_heartbeat,
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
    
    def _get_workflow_runner(self) -> WorkflowRunner:
        """
        Get or create the WorkflowRunner for executing workflows.
        """
        if self._workflow_runner is None:
            core_env = self._get_core_env()
            # Use node_id instance as worker_id and node_id
            node_id_int = hash(self._node_id.full) % (2**31)
            self._workflow_runner = WorkflowRunner(
                env=core_env,
                worker_id=node_id_int,
                node_id=node_id_int,
            )
            self._workflow_runner.setup()
        return self._workflow_runner
    
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
        )
    
    def _increment_version(self) -> int:
        """Increment and return the state version."""
        self._state_version += 1
        return self._state_version
    
    def _is_manager_circuit_open(self) -> bool:
        """Check if manager circuit breaker is open (fail-fast mode)."""
        return self._manager_circuit.circuit_state == CircuitState.OPEN
    
    def get_manager_circuit_status(self) -> dict:
        """
        Get current manager circuit breaker status.
        
        Returns a dict with:
        - circuit_state: Current state (CLOSED, OPEN, HALF_OPEN)
        - error_count: Recent error count
        - error_rate: Error rate over window
        - healthy_managers: Count of healthy managers
        - primary_manager: Current primary manager ID
        """
        return {
            "circuit_state": self._manager_circuit.circuit_state.name,
            "error_count": self._manager_circuit.error_count,
            "error_rate": self._manager_circuit.error_rate,
            "healthy_managers": len(self._healthy_manager_ids),
            "primary_manager": self._primary_manager_id,
        }
    
    async def start(self) -> None:
        """Start the worker server."""
        # Start the underlying server (TCP/UDP listeners, task runner, etc.)
        # Uses SWIM settings from Env configuration
        await self.start_server(init_context=self.env.get_swim_init_context())
        
        # Try seed managers in order until one succeeds
        # Registration response includes list of all healthy managers
        registered = False
        for seed_addr in self._seed_managers:
            success = await self._register_with_manager(seed_addr)
            if success:
                registered = True
                break
        
        if not registered:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Failed to register with any seed manager: {self._seed_managers}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        
        # Join SWIM cluster with all known managers for healthchecks
        for manager in self._known_managers.values():
            udp_addr = (manager.udp_host, manager.udp_port)
            await self.join_cluster(udp_addr)
        
        # Start SWIM probe cycle (UDP healthchecks)
        self._task_runner.run(self.start_probe_cycle)
        
        manager_count = len(self._known_managers)
        self._task_runner.run(
            self._udp_logger.log,
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
        
        Marks the manager as unhealthy in our tracking.
        """
        # Find which manager this address belongs to
        for manager_id, manager in self._known_managers.items():
            if (manager.udp_host, manager.udp_port) == node_addr:
                self._healthy_manager_ids.discard(manager_id)
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
        
        Marks the manager as healthy in our tracking.
        """
        # Find which manager this address belongs to
        for manager_id, manager in self._known_managers.items():
            if (manager.udp_host, manager.udp_port) == node_addr:
                self._healthy_manager_ids.add(manager_id)
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
            # We need TCP/UDP host:port from source_addr
            self._known_managers[manager_id] = ManagerInfo(
                node_id=manager_id,
                tcp_host=source_addr[0],
                tcp_port=source_addr[1] - 1,  # Convention: TCP = UDP - 1
                udp_host=source_addr[0],
                udp_port=source_addr[1],
                datacenter=heartbeat.datacenter,
                is_leader=heartbeat.is_leader,
            )
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
        
        for workflow_id, progress in self._active_workflows.items():
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
    
    async def stop(self) -> None:
        """Stop the worker server."""
        # Cancel all active workflows via TaskRunner
        for workflow_id in list(self._workflow_tokens.keys()):
            await self._cancel_workflow(workflow_id, "server_shutdown")
        
        # Graceful shutdown (broadcasts leave via SWIM)
        await self.graceful_shutdown()
    
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
        
        Also respects the circuit breaker - if open, fails fast.
        
        Args:
            manager_addr: (host, port) tuple of manager
            max_retries: Maximum number of retry attempts (default 3)
            base_delay: Base delay in seconds for exponential backoff (default 0.5)
            
        Returns:
            True if registration succeeded, False otherwise
        """
        # Check circuit breaker first
        if self._is_manager_circuit_open():
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
        
        registration = WorkerRegistration(
            node=self.node_info,
            total_cores=self._total_cores,
            available_cores=self._available_cores,
            memory_mb=self._get_memory_mb(),
            available_memory_mb=self._get_available_memory_mb(),
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
                    self._manager_circuit.record_success()
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
        
        # All retries exhausted
        self._manager_circuit.record_error()
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
            available_cores=self._available_cores,
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
            available_cores=self._available_cores,
            queue_depth=len(self._pending_workflows),
            cpu_percent=self._get_cpu_percent(),
            memory_percent=self._get_memory_percent(),
            version=self._state_version,
            active_workflows={
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
        )
    
    # =========================================================================
    # Per-Core Assignment Tracking
    # =========================================================================
    
    def _allocate_cores(self, workflow_id: str, num_cores: int) -> list[int]:
        """Allocate a number of cores to a workflow."""
        free_cores = [i for i, wf_id in self._core_assignments.items() if wf_id is None]
        
        if len(free_cores) < num_cores:
            return []
        
        allocated = free_cores[:num_cores]
        for core_idx in allocated:
            self._core_assignments[core_idx] = workflow_id
        
        self._workflow_cores[workflow_id] = allocated
        self._available_cores = len(
            [i for i, wf_id in self._core_assignments.items() if wf_id is None]
        )
        
        return allocated
    
    def _free_cores(self, workflow_id: str) -> list[int]:
        """Free all cores allocated to a workflow."""
        allocated = self._workflow_cores.pop(workflow_id, [])
        
        for core_idx in allocated:
            if self._core_assignments.get(core_idx) == workflow_id:
                self._core_assignments[core_idx] = None
        
        self._available_cores = len(
            [i for i, wf_id in self._core_assignments.items() if wf_id is None]
        )
        
        return allocated
    
    def _get_workflow_cores(self, workflow_id: str) -> list[int]:
        """Get the core indices assigned to a workflow."""
        return self._workflow_cores.get(workflow_id, [])
    
    def get_core_assignments(self) -> dict[int, str | None]:
        """Get a copy of the current core assignments."""
        return dict(self._core_assignments)
    
    def get_workflows_on_cores(self, core_indices: list[int]) -> set[str]:
        """Get workflows running on specific cores."""
        workflows = set()
        for core_idx in core_indices:
            wf_id = self._core_assignments.get(core_idx)
            if wf_id:
                workflows.add(wf_id)
        return workflows
    
    async def stop_workflows_on_cores(
        self,
        core_indices: list[int],
        reason: str = "core_stop",
    ) -> list[str]:
        """Stop all workflows running on specific cores (hierarchical stop)."""
        workflows = self.get_workflows_on_cores(core_indices)
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
                
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Registered with {len(response.healthy_managers)} managers, primary: {self._primary_manager_id}",
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
        """
        try:
            dispatch = WorkflowDispatch.load(data)
            
            # Check if we can accept this workflow
            if self._available_cores < dispatch.vus:
                ack = WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error=f"Insufficient cores: need {dispatch.vus}, have {self._available_cores}",
                )
                return ack.dump()
            
            # Check backpressure
            if self._get_worker_state() == WorkerState.DRAINING:
                ack = WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error="Worker is draining, not accepting new work",
                )
                return ack.dump()
            
            # Allocate cores to this workflow
            allocated_cores = self._allocate_cores(dispatch.workflow_id, dispatch.vus)
            if not allocated_cores:
                ack = WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error=f"Failed to allocate {dispatch.vus} cores",
                )
                return ack.dump()
            
            self._increment_version()
            
            # Create progress tracker with assigned cores
            progress = WorkflowProgress(
                job_id=dispatch.job_id,
                workflow_id=dispatch.workflow_id,
                workflow_name="",
                status=WorkflowStatus.ASSIGNED.value,
                completed_count=0,
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
                assigned_cores=allocated_cores,
            )
            self._active_workflows[dispatch.workflow_id] = progress
            
            # Create cancellation event
            cancel_event = asyncio.Event()
            self._workflow_cancel_events[dispatch.workflow_id] = cancel_event
            
            # Start execution task via TaskRunner
            token = self._task_runner.run(
                self._execute_workflow,
                dispatch,
                progress,
                cancel_event,
                alias=f"workflow:{dispatch.workflow_id}",
            )
            self._workflow_tokens[dispatch.workflow_id] = token
            
            # Return acknowledgment
            ack = WorkflowDispatchAck(
                workflow_id=dispatch.workflow_id,
                accepted=True,
                cores_assigned=dispatch.vus,
            )
            return ack.dump()
            
        except Exception as e:
            ack = WorkflowDispatchAck(
                workflow_id="unknown",
                accepted=False,
                error=str(e),
            )
            return ack.dump()
    
    async def _execute_workflow(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        cancel_event: asyncio.Event,
    ) -> None:
        """Execute a workflow using WorkflowRunner."""
        start_time = time.monotonic()
        run_id = hash(dispatch.workflow_id) % (2**31)
        
        try:
            # Unpickle workflow and context
            workflow = restricted_loads(dispatch.workflow)
            context_dict = restricted_loads(dispatch.context)
            
            progress.workflow_name = getattr(workflow, 'name', workflow.__class__.__name__)
            progress.status = WorkflowStatus.RUNNING.value
            self._increment_version()
            
            # Initialize cores_completed tracking
            self._workflow_cores_completed[dispatch.workflow_id] = set()
            
            # Get or create WorkflowRunner
            runner = self._get_workflow_runner()
            
            # Start progress monitor
            progress_token = self._task_runner.run(
                self._monitor_workflow_progress,
                dispatch,
                progress,
                run_id,
                cancel_event,
                alias=f"progress:{dispatch.workflow_id}",
            )
            
            try:
                # Execute the workflow
                (
                    returned_run_id,
                    results,
                    result_context,
                    error,
                    status,
                ) = await runner.run(
                    run_id,
                    workflow,
                    context_dict if isinstance(context_dict, dict) else {},
                    dispatch.vus,
                )
                
                # Map status
                if status == CoreWorkflowStatus.COMPLETED:
                    progress.status = WorkflowStatus.COMPLETED.value
                    progress.cores_completed = len(progress.assigned_cores)
                elif status == CoreWorkflowStatus.FAILED:
                    progress.status = WorkflowStatus.FAILED.value
                elif status == CoreWorkflowStatus.REJECTED:
                    progress.status = WorkflowStatus.FAILED.value
                else:
                    progress.status = WorkflowStatus.COMPLETED.value
                    progress.cores_completed = len(progress.assigned_cores)
                    
            except asyncio.CancelledError:
                progress.status = WorkflowStatus.CANCELLED.value
                raise
            except Exception as e:
                progress.status = WorkflowStatus.FAILED.value
            finally:
                await self._task_runner.cancel(progress_token)
            
            # Final progress update
            progress.elapsed_seconds = time.monotonic() - start_time
            progress.timestamp = time.monotonic()
            if self._healthy_manager_ids:
                await self._send_progress_update(progress)
                
        except asyncio.CancelledError:
            progress.status = WorkflowStatus.CANCELLED.value
        except Exception as e:
            progress.status = WorkflowStatus.FAILED.value
        finally:
            self._free_cores(dispatch.workflow_id)
            self._increment_version()
            
            self._workflow_tokens.pop(dispatch.workflow_id, None)
            self._workflow_cancel_events.pop(dispatch.workflow_id, None)
            self._active_workflows.pop(dispatch.workflow_id, None)
            self._workflow_last_progress.pop(dispatch.workflow_id, None)
            self._workflow_cores_completed.pop(dispatch.workflow_id, None)
    
    async def _monitor_workflow_progress(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        run_id: int,
        cancel_event: asyncio.Event,
    ) -> None:
        """Monitor workflow progress and send updates to manager."""
        start_time = time.monotonic()
        runner = self._get_workflow_runner()
        workflow_name = progress.workflow_name
        
        while not cancel_event.is_set():
            try:
                await asyncio.sleep(self._progress_update_interval)
                
                # Get stats from WorkflowRunner
                (
                    status,
                    completed_count,
                    failed_count,
                    step_stats_dict,
                ) = runner.get_running_workflow_stats(run_id, workflow_name)
                
                # Get system stats
                avg_cpu, avg_mem = runner.get_system_stats(run_id, workflow_name)
                
                # Update progress
                progress.completed_count = completed_count
                progress.failed_count = failed_count
                progress.elapsed_seconds = time.monotonic() - start_time
                progress.rate_per_second = (
                    completed_count / progress.elapsed_seconds
                    if progress.elapsed_seconds > 0 else 0.0
                )
                progress.timestamp = time.monotonic()
                progress.avg_cpu_percent = avg_cpu
                progress.avg_memory_mb = avg_mem
                
                # Convert step stats
                progress.step_stats = [
                    StepStats(
                        step_name=step_name,
                        completed_count=stats.get("ok", 0),
                        failed_count=stats.get("err", 0),
                        total_count=stats.get("total", 0),
                    )
                    for step_name, stats in step_stats_dict.items()
                ]
                
                # Estimate cores_completed
                total_cores = len(progress.assigned_cores)
                if total_cores > 0:
                    estimated_complete = min(
                        total_cores,
                        int(total_cores * (completed_count / max(dispatch.vus * 100, 1)))
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
                
                # Send update
                if self._healthy_manager_ids:
                    await self._send_progress_update(progress)
                    self._workflow_last_progress[dispatch.workflow_id] = time.monotonic()
                
            except asyncio.CancelledError:
                break
            except Exception:
                pass
    
    async def _send_progress_update(
        self,
        progress: WorkflowProgress,
        max_retries: int = 2,
        base_delay: float = 0.2,
    ) -> None:
        """
        Send a progress update to the primary manager and process ack.
        
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
        # Check circuit breaker first
        if self._is_manager_circuit_open():
            return  # Fail fast - don't attempt communication
        
        manager_addr = self._get_primary_manager_tcp_addr()
        if not manager_addr:
            return
        
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
                    self._manager_circuit.record_success()
                    return  # Success
                    
            except Exception:
                pass
            
            # Exponential backoff before retry (except after last attempt)
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
        
        # All retries exhausted
        self._manager_circuit.record_error()
    
    async def _send_progress_to_all_managers(self, progress: WorkflowProgress) -> None:
        """Send a progress update to ALL healthy managers and process acks."""
        # Check circuit breaker first
        if self._is_manager_circuit_open():
            return  # Fail fast
        
        success_count = 0
        for manager_addr in self._get_healthy_manager_tcp_addrs():
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
                    success_count += 1
                    
            except Exception:
                pass
        
        # Record circuit breaker result based on overall success
        if success_count > 0:
            self._manager_circuit.record_success()
        elif len(self._healthy_manager_ids) > 0:
            self._manager_circuit.record_error()
    
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
    async def receive_state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> tuple[bytes, bytes]:
        """Handle state sync request from a new manager leader."""
        try:
            request = StateSyncRequest.load(data)
            
            response = StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=self._state_version,
                worker_state=self._get_state_snapshot(),
            )
            return (b'state_sync_response', response.dump())
            
        except Exception:
            return (b'state_sync_response', b'')
    
    # =========================================================================
    # TCP Handlers - Cancellation
    # =========================================================================
    
    @tcp.receive()
    async def receive_cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> tuple[bytes, bytes]:
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
            return (b'cancel_ack', ack.dump())
            
        except Exception as e:
            ack = CancelAck(
                job_id="unknown",
                cancelled=False,
                error=str(e),
            )
            return (b'cancel_ack', ack.dump())
