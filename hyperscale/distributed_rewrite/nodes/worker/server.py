"""
Worker server composition root (Phase 15.2.7).

Thin orchestration layer that wires all worker modules together.
All business logic is delegated to specialized modules.
"""

import asyncio
import time
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.swim import HealthAwareServer, WorkerStateEmbedder
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    WorkerState,
    WorkerStateSnapshot,
    WorkflowProgress,
)
from hyperscale.distributed_rewrite.jobs import CoreAllocator
from hyperscale.distributed_rewrite.protocol.version import (
    NodeCapabilities,
    NegotiatedCapabilities,
)
from hyperscale.distributed_rewrite.server import tcp

from .config import WorkerConfig
from .registry import WorkerRegistry
from .execution import WorkerExecutor
from .sync import WorkerStateSync
from .health import WorkerHealthIntegration
from .backpressure import WorkerBackpressureManager
from .discovery import WorkerDiscoveryManager
from .handlers import (
    WorkflowDispatchHandler,
    WorkflowCancelHandler,
    JobLeaderTransferHandler,
    WorkflowProgressHandler,
    StateSyncHandler,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger


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

        # Initialize modules (will be fully wired after super().__init__)
        self._registry = WorkerRegistry(
            logger=None,  # Set after parent init
            recovery_jitter_min=env.RECOVERY_JITTER_MIN,
            recovery_jitter_max=env.RECOVERY_JITTER_MAX,
            recovery_semaphore_size=env.RECOVERY_SEMAPHORE_SIZE,
        )

        self._backpressure_manager = WorkerBackpressureManager(
            logger=None,
            registry=self._registry,
        )

        self._executor = WorkerExecutor(
            core_allocator=self._core_allocator,
            logger=None,
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

        self._discovery_manager = WorkerDiscoveryManager(
            env=env,
            seed_managers=self._seed_managers,
            logger=None,
        )

        # Runtime state
        self._active_workflows: dict[str, WorkflowProgress] = {}
        self._workflow_tokens: dict[str, str] = {}
        self._workflow_cancel_events: dict[str, asyncio.Event] = {}
        self._workflow_job_leader: dict[str, tuple[str, int]] = {}
        self._workflow_fence_tokens: dict[str, int] = {}
        self._pending_workflows: list = []
        self._orphaned_workflows: dict[str, float] = {}

        # Section 8: Job leadership transfer
        self._job_leader_transfer_locks: dict[str, asyncio.Lock] = {}
        self._job_fence_tokens: dict[str, int] = {}
        self._pending_transfers: dict = {}

        # Transfer metrics (8.6)
        self._transfer_metrics_received: int = 0
        self._transfer_metrics_accepted: int = 0
        self._transfer_metrics_rejected_stale_token: int = 0
        self._transfer_metrics_rejected_unknown_manager: int = 0
        self._transfer_metrics_rejected_other: int = 0

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
            get_health_accepting_work=lambda: self._get_worker_state() in (
                WorkerState.HEALTHY, WorkerState.DEGRADED
            ),
            get_health_throughput=self._executor.get_throughput,
            get_health_expected_throughput=self._executor.get_expected_throughput,
            get_health_overload_state=self._backpressure_manager.get_overload_state_str,
            get_extension_requested=lambda: False,
            get_extension_reason=lambda: "",
            get_extension_current_progress=lambda: 0.0,
            get_extension_completed_items=lambda: 0,
            get_extension_total_items=lambda: 0,
            get_extension_estimated_completion=lambda: 0.0,
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

        # Wire logger to modules after parent init
        self._wire_logger_to_modules()

        # Register SWIM callbacks
        self.register_on_node_dead(self._health_integration.on_node_dead)
        self.register_on_node_join(self._health_integration.on_node_join)
        self._health_integration.set_failure_callback(self._on_manager_failure)
        self._health_integration.set_recovery_callback(self._on_manager_recovery)

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

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    async def start(self, timeout: float | None = None) -> None:
        """Start the worker server."""
        # Delegate to worker_impl for full implementation
        from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer as ImplServer
        await ImplServer.start(self, timeout)

    async def stop(self, drain_timeout: float = 5, broadcast_leave: bool = True) -> None:
        """Stop the worker server."""
        from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer as ImplServer
        await ImplServer.stop(self, drain_timeout, broadcast_leave)

    def abort(self):
        """Abort the worker server."""
        from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer as ImplServer
        return ImplServer.abort(self)

    # =========================================================================
    # State Methods
    # =========================================================================

    def _get_worker_state(self) -> WorkerState:
        """Determine current worker state."""
        if not self._running:
            return WorkerState.OFFLINE
        if self._degradation.current_level.value >= 3:
            return WorkerState.DRAINING
        if self._degradation.current_level.value >= 2:
            return WorkerState.DEGRADED
        return WorkerState.HEALTHY

    def _increment_version(self) -> int:
        """Increment and return the state version."""
        return self._state_sync.increment_version()

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

    # =========================================================================
    # Lock Helpers (Section 8)
    # =========================================================================

    def _get_job_transfer_lock(self, job_id: str) -> asyncio.Lock:
        """Get or create a lock for job leadership transfers."""
        if job_id not in self._job_leader_transfer_locks:
            self._job_leader_transfer_locks[job_id] = asyncio.Lock()
        return self._job_leader_transfer_locks[job_id]

    def _validate_transfer_fence_token(
        self, job_id: str, new_fence_token: int
    ) -> tuple[bool, str]:
        """Validate a transfer's fence token."""
        current_token = self._job_fence_tokens.get(job_id, -1)
        if new_fence_token <= current_token:
            return (False, f"Stale fence token: received {new_fence_token}, current {current_token}")
        return (True, "")

    def _validate_transfer_manager(self, new_manager_id: str) -> tuple[bool, str]:
        """Validate that the new manager is known."""
        if new_manager_id not in self._registry._known_managers:
            return (False, f"Unknown manager: {new_manager_id} not in known managers")
        return (True, "")

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
        """Async handler for manager failure."""
        from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer as ImplServer
        await ImplServer._handle_manager_failure(self, manager_id)

    async def _handle_manager_recovery_async(self, manager_id: str) -> None:
        """Async handler for manager recovery."""
        from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer as ImplServer
        await ImplServer._handle_manager_recovery(self, manager_id)

    def _handle_manager_heartbeat(self, heartbeat, source_addr: tuple[str, int]) -> None:
        """Handle manager heartbeat from SWIM."""
        from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer as ImplServer
        ImplServer._handle_manager_heartbeat(self, heartbeat, source_addr)

    # =========================================================================
    # Dispatch Execution Delegation (for tcp_dispatch.py)
    # =========================================================================

    async def _handle_dispatch_execution(
        self, dispatch, addr: tuple[str, int], allocation_result
    ) -> bytes:
        """Delegate dispatch execution to worker_impl."""
        from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer as ImplServer
        return await ImplServer._handle_dispatch_execution(self, dispatch, addr, allocation_result)

    def _cleanup_workflow_state(self, workflow_id: str) -> None:
        """Cleanup workflow state on failure."""
        # Clear from tracking dicts
        self._active_workflows.pop(workflow_id, None)
        self._workflow_tokens.pop(workflow_id, None)
        self._workflow_cancel_events.pop(workflow_id, None)
        self._workflow_job_leader.pop(workflow_id, None)
        self._workflow_fence_tokens.pop(workflow_id, None)
        self._orphaned_workflows.pop(workflow_id, None)

    # =========================================================================
    # Cancellation Delegation (for tcp_cancel.py - AD-20)
    # =========================================================================

    async def _cancel_workflow(
        self, workflow_id: str, reason: str
    ) -> tuple[bool, str | None]:
        """Delegate workflow cancellation to worker_impl."""
        from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer as ImplServer
        return await ImplServer._cancel_workflow(self, workflow_id, reason)

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
        """Get CPU utilization percentage."""
        try:
            import psutil
            return psutil.cpu_percent()
        except ImportError:
            return 0.0

    def _get_memory_percent(self) -> float:
        """Get memory utilization percentage."""
        try:
            import psutil
            return psutil.virtual_memory().percent
        except ImportError:
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


__all__ = ["WorkerServer"]
