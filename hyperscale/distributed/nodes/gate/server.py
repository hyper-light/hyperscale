"""
Gate Server composition root.

This module provides the GateServer class that wires together all modular
gate components following the one-class-per-file pattern.

The GateServer extends the base implementation and adds modular coordinators
and handlers for clean, testable business logic separation.

Module Structure:
- Coordinators: Business logic (leadership, dispatch, stats, cancellation, peer, health)
- Handlers: TCP message processing (job, manager, cancellation, state sync, ping)
- State: GateRuntimeState for mutable runtime state
- Config: GateConfig for immutable configuration
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.nodes.gate_impl import GateServer as GateServerImpl
from hyperscale.distributed.reliability import BackpressureLevel, BackpressureSignal

from .stats_coordinator import GateStatsCoordinator
from .cancellation_coordinator import GateCancellationCoordinator
from .dispatch_coordinator import GateDispatchCoordinator
from .leadership_coordinator import GateLeadershipCoordinator
from .peer_coordinator import GatePeerCoordinator
from .health_coordinator import GateHealthCoordinator

from .config import GateConfig, create_gate_config
from .state import GateRuntimeState

from .handlers import (
    GatePingHandler,
    GateJobHandler,
    GateManagerHandler,
    GateCancellationHandler,
    GateStateSyncHandler,
)

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env


class GateServer(GateServerImpl):
    """
    Gate node in the distributed Hyperscale system.

    This is the composition root that wires together all gate modules:
    - Configuration (GateConfig)
    - Runtime state (GateRuntimeState)
    - Coordinators (leadership, dispatch, stats, cancellation, peer, health)
    - Handlers (TCP/UDP message handlers)

    The class extends GateServerImpl for backward compatibility while
    progressively delegating to modular components.

    Gates:
    - Form a gossip cluster for leader election (UDP SWIM)
    - Accept job submissions from clients (TCP)
    - Dispatch jobs to managers in target datacenters (TCP)
    - Aggregate global job status across DCs (TCP)
    - Manage leases for at-most-once semantics
    """

    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: "Env",
        dc_id: str = "global",
        datacenter_managers: dict[str, list[tuple[str, int]]] | None = None,
        datacenter_manager_udp: dict[str, list[tuple[str, int]]] | None = None,
        gate_peers: list[tuple[str, int]] | None = None,
        gate_udp_peers: list[tuple[str, int]] | None = None,
        lease_timeout: float = 30.0,
    ):
        """
        Initialize the Gate server.

        Args:
            host: Host address to bind
            tcp_port: TCP port for data operations
            udp_port: UDP port for SWIM protocol
            env: Environment configuration
            dc_id: Datacenter identifier (default "global" for gates)
            datacenter_managers: DC -> manager TCP addresses mapping
            datacenter_manager_udp: DC -> manager UDP addresses mapping
            gate_peers: Peer gate TCP addresses
            gate_udp_peers: Peer gate UDP addresses
            lease_timeout: Lease timeout in seconds
        """
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
            datacenter_managers=datacenter_managers,
            datacenter_manager_udp=datacenter_manager_udp,
            gate_peers=gate_peers,
            gate_udp_peers=gate_udp_peers,
            lease_timeout=lease_timeout,
        )

        # Create modular runtime state
        self._modular_state = GateRuntimeState()

        # Coordinators (initialized in _init_coordinators)
        self._stats_coordinator: GateStatsCoordinator | None = None
        self._cancellation_coordinator: GateCancellationCoordinator | None = None
        self._dispatch_coordinator: GateDispatchCoordinator | None = None
        self._leadership_coordinator: GateLeadershipCoordinator | None = None
        self._peer_coordinator: GatePeerCoordinator | None = None
        self._health_coordinator: GateHealthCoordinator | None = None

        # Handlers (initialized in _init_handlers)
        self._ping_handler: GatePingHandler | None = None
        self._job_handler: GateJobHandler | None = None
        self._manager_handler: GateManagerHandler | None = None
        self._cancellation_handler: GateCancellationHandler | None = None
        self._state_sync_handler: GateStateSyncHandler | None = None

    async def start(self) -> None:
        """
        Start the gate server.

        Initializes coordinators, wires handlers, and starts background tasks.
        """
        await super().start()

        self._init_coordinators()
        self._init_handlers()

    def _init_coordinators(self) -> None:
        """Initialize coordinator instances with dependencies."""
        # Stats coordinator
        self._stats_coordinator = GateStatsCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            windowed_stats=self._windowed_stats,
            get_job_callback=self._job_manager.get_callback,
            get_job_status=self._job_manager.get_job,
            send_tcp=self._send_tcp,
            stats_push_interval_ms=self._stats_push_interval_ms,
        )

        # Cancellation coordinator
        self._cancellation_coordinator = GateCancellationCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            get_job_target_dcs=self._job_manager.get_target_dcs,
            get_dc_manager_addr=lambda job_id, dc_id: self._job_dc_managers.get(job_id, {}).get(dc_id),
            send_tcp=self._send_tcp,
            is_job_leader=self._job_leadership_tracker.is_leader,
        )

        # Leadership coordinator
        self._leadership_coordinator = GateLeadershipCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            leadership_tracker=self._job_leadership_tracker,
            get_node_id=lambda: self._node_id,
            get_node_addr=lambda: (self._host, self._tcp_port),
            send_tcp=self._send_tcp,
            get_active_peers=lambda: list(self._active_gate_peers),
        )

        # Dispatch coordinator
        self._dispatch_coordinator = GateDispatchCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_router=self._job_router,
            check_rate_limit=self._check_rate_limit_for_operation,
            should_shed_request=self._should_shed_request,
            has_quorum_available=self._has_quorum_available,
            quorum_size=self._quorum_size,
            quorum_circuit=self._quorum_circuit,
            select_datacenters=self._select_datacenters_with_fallback,
            assume_leadership=self._job_leadership_tracker.assume_leadership,
            broadcast_leadership=self._broadcast_job_leadership,
            dispatch_to_dcs=self._dispatch_job_to_datacenters,
        )

        # Peer coordinator
        self._peer_coordinator = GatePeerCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            peer_discovery=self._peer_discovery,
            job_hash_ring=self._job_hash_ring,
            job_forwarding_tracker=self._job_forwarding_tracker,
            job_leadership_tracker=self._job_leadership_tracker,
            versioned_clock=self._versioned_clock,
            gate_health_config=vars(self._gate_health_config),
            recovery_semaphore=self._recovery_semaphore,
            recovery_jitter_min=0.0,
            recovery_jitter_max=getattr(self.env, 'GATE_RECOVERY_JITTER_MAX', 1.0),
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_udp_port=lambda: self._udp_port,
            confirm_peer=self._confirm_peer,
            handle_job_leader_failure=self._handle_job_leader_failure,
        )

        # Health coordinator
        self._health_coordinator = GateHealthCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            dc_health_manager=self._dc_health_manager,
            dc_health_monitor=self._dc_health_monitor,
            cross_dc_correlation=self._cross_dc_correlation,
            dc_manager_discovery=self._dc_manager_discovery,
            versioned_clock=self._versioned_clock,
            manager_dispatcher=self._manager_dispatcher,
            manager_health_config=vars(self._manager_health_config),
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            confirm_manager_for_dc=self._confirm_manager_for_dc,
        )

    def _init_handlers(self) -> None:
        """Initialize handler instances with dependencies."""
        # Ping handler
        self._ping_handler = GatePingHandler(
            state=self._modular_state,
            logger=self._udp_logger,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            is_leader=self.is_leader,
            get_current_term=lambda: self._leader_election.state.current_term,
            classify_dc_health=self._classify_datacenter_health,
            count_active_dcs=self._count_active_datacenters,
            get_all_job_ids=self._job_manager.get_all_job_ids,
            get_datacenter_managers=lambda: self._datacenter_managers,
        )

        # Job handler
        self._job_handler = GateJobHandler(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_router=self._job_router,
            job_leadership_tracker=self._job_leadership_tracker,
            quorum_circuit=self._quorum_circuit,
            load_shedder=self._load_shedder,
            job_lease_manager=self._job_lease_manager,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            is_leader=self.is_leader,
            check_rate_limit=self._check_rate_limit_for_operation,
            should_shed_request=self._should_shed_request,
            has_quorum_available=self._has_quorum_available,
            quorum_size=self._quorum_size,
            select_datacenters_with_fallback=self._select_datacenters_with_fallback,
            get_healthy_gates=self._get_healthy_gates,
            broadcast_job_leadership=self._broadcast_job_leadership,
            dispatch_job_to_datacenters=self._dispatch_job_to_datacenters,
            forward_job_progress_to_peers=self._forward_job_progress_to_peers,
            record_request_latency=self._record_request_latency,
            record_dc_job_stats=self._record_dc_job_stats,
            handle_update_by_tier=self._handle_update_by_tier,
        )

        # Manager handler
        self._manager_handler = GateManagerHandler(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            env=self.env,
            datacenter_managers=self._datacenter_managers,
            role_validator=self._role_validator,
            node_capabilities=self._node_capabilities,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_healthy_gates=self._get_healthy_gates,
            record_manager_heartbeat=self._record_manager_heartbeat,
            handle_manager_backpressure_signal=self._handle_manager_backpressure_signal,
            update_dc_backpressure=self._update_dc_backpressure,
            broadcast_manager_discovery=self._broadcast_manager_discovery,
        )

        # Cancellation handler
        self._cancellation_handler = GateCancellationHandler(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            datacenter_managers=self._datacenter_managers,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            check_rate_limit=self._check_rate_limit_for_operation,
            send_tcp=self._send_tcp,
            get_available_datacenters=self._get_available_datacenters,
        )

        # State sync handler
        self._state_sync_handler = GateStateSyncHandler(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_leadership_tracker=self._job_leadership_tracker,
            versioned_clock=self._versioned_clock,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            is_leader=self.is_leader,
            get_term=lambda: self._leader_election.state.current_term,
            get_state_snapshot=self._get_state_snapshot,
            apply_state_snapshot=self._apply_gate_state_snapshot,
        )

    # Coordinator accessors
    @property
    def stats_coordinator(self) -> GateStatsCoordinator | None:
        """Get the stats coordinator."""
        return self._stats_coordinator

    @property
    def cancellation_coordinator(self) -> GateCancellationCoordinator | None:
        """Get the cancellation coordinator."""
        return self._cancellation_coordinator

    @property
    def dispatch_coordinator(self) -> GateDispatchCoordinator | None:
        """Get the dispatch coordinator."""
        return self._dispatch_coordinator

    @property
    def leadership_coordinator(self) -> GateLeadershipCoordinator | None:
        """Get the leadership coordinator."""
        return self._leadership_coordinator

    @property
    def peer_coordinator(self) -> GatePeerCoordinator | None:
        """Get the peer coordinator."""
        return self._peer_coordinator

    @property
    def health_coordinator(self) -> GateHealthCoordinator | None:
        """Get the health coordinator."""
        return self._health_coordinator


__all__ = [
    "GateServer",
    "GateConfig",
    "create_gate_config",
    "GateRuntimeState",
    "GateStatsCoordinator",
    "GateCancellationCoordinator",
    "GateDispatchCoordinator",
    "GateLeadershipCoordinator",
    "GatePeerCoordinator",
    "GateHealthCoordinator",
    "GatePingHandler",
    "GateJobHandler",
    "GateManagerHandler",
    "GateCancellationHandler",
    "GateStateSyncHandler",
]
