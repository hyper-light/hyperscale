"""
Gate Server composition root.

This module provides the GateServer class as a thin orchestration layer
that wires together all gate modules following the REFACTOR.md pattern.

Note: During the transition period, this delegates to the monolithic
gate.py implementation. Full extraction is tracked in TODO.md 15.3.7.
"""

from typing import TYPE_CHECKING

# Import the existing monolithic implementation for delegation
from hyperscale.distributed.nodes.gate_impl import GateServer as GateServerImpl

# Import coordinators (new modular implementations)
from hyperscale.distributed.nodes.gate.stats_coordinator import GateStatsCoordinator
from hyperscale.distributed.nodes.gate.cancellation_coordinator import GateCancellationCoordinator
from hyperscale.distributed.nodes.gate.dispatch_coordinator import GateDispatchCoordinator
from hyperscale.distributed.nodes.gate.leadership_coordinator import GateLeadershipCoordinator

# Import configuration and state
from hyperscale.distributed.nodes.gate.config import GateConfig, create_gate_config
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

# Import handlers
from hyperscale.distributed.nodes.gate.handlers.tcp_ping import GatePingHandler

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env


class GateServer(GateServerImpl):
    """
    Gate node in the distributed Hyperscale system.

    This is the composition root that wires together all gate modules:
    - Configuration (GateConfig)
    - Runtime state (GateRuntimeState)
    - Coordinators (stats, cancellation, dispatch, leadership)
    - Handlers (TCP/UDP message handlers)

    During the transition period, this inherits from the monolithic
    GateServerImpl to preserve behavior. Full extraction is tracked
    in TODO.md Phase 15.3.7.

    Gates:
    - Form a gossip cluster for leader election (UDP SWIM)
    - Accept job submissions from clients (TCP)
    - Dispatch jobs to managers in target datacenters (TCP)
    - Probe managers via UDP to detect DC failures (SWIM)
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
        # Initialize the base implementation
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

        # Create modular runtime state (mirrors base state for now)
        self._modular_state = GateRuntimeState()

        # Initialize coordinators (these can be used in parallel with base methods)
        self._stats_coordinator: GateStatsCoordinator | None = None
        self._cancellation_coordinator: GateCancellationCoordinator | None = None
        self._dispatch_coordinator: GateDispatchCoordinator | None = None
        self._leadership_coordinator: GateLeadershipCoordinator | None = None

        # Handler instances (wired during start())
        self._ping_handler: GatePingHandler | None = None

    async def start(self) -> None:
        """
        Start the gate server.

        Initializes coordinators, wires handlers, and starts background tasks.
        """
        # Call base start first
        await super().start()

        # Initialize coordinators with dependencies from base implementation
        self._init_coordinators()

        # Initialize handlers
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

    # Coordinator accessors for external use
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


__all__ = [
    "GateServer",
    "GateConfig",
    "create_gate_config",
    "GateRuntimeState",
    "GateStatsCoordinator",
    "GateCancellationCoordinator",
    "GateDispatchCoordinator",
    "GateLeadershipCoordinator",
    "GatePingHandler",
]
