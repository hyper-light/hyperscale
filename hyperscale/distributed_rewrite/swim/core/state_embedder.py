"""
State Embedder Protocol and Implementations.

This module provides a composition-based approach for embedding application
state (heartbeats) in SWIM UDP messages, enabling Serf-style passive state
dissemination.

The StateEmbedder protocol is injected into HealthAwareServer, allowing different
node types (Worker, Manager, Gate) to provide their own state without
requiring inheritance-based overrides.

Phase 6.1 Enhancement: StateEmbedders now also provide HealthPiggyback objects
for the HealthGossipBuffer, enabling O(log n) health state dissemination
alongside membership gossip.
"""

from dataclasses import dataclass
from typing import Protocol, Callable, Any
import time

from hyperscale.distributed_rewrite.models import (
    WorkerHeartbeat,
    ManagerHeartbeat,
    GateHeartbeat,
)
from hyperscale.distributed_rewrite.health.tracker import HealthPiggyback


class StateEmbedder(Protocol):
    """
    Protocol for embedding and processing state in SWIM messages.

    Implementations provide:
    - get_state(): Returns serialized state to embed in outgoing messages
    - process_state(): Handles state received from other nodes
    - get_health_piggyback(): Returns HealthPiggyback for gossip buffer (Phase 6.1)
    """

    def get_state(self) -> bytes | None:
        """
        Get serialized state to embed in SWIM probe responses.

        Returns:
            Serialized state bytes, or None if no state to embed.
        """
        ...

    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Process embedded state received from another node.

        Args:
            state_data: Serialized state bytes from the remote node.
            source_addr: The (host, port) of the node that sent the state.
        """
        ...

    def get_health_piggyback(self) -> HealthPiggyback | None:
        """
        Get HealthPiggyback for the HealthGossipBuffer (Phase 6.1).

        This returns a compact health representation for O(log n) gossip
        dissemination. Unlike get_state() which embeds full heartbeats in
        ACK messages, this provides minimal health info for gossip on all
        SWIM messages.

        Returns:
            HealthPiggyback with current health state, or None if unavailable.
        """
        ...


class NullStateEmbedder:
    """
    Default no-op state embedder.

    Used when no state embedding is needed (base HealthAwareServer behavior).
    """

    def get_state(self) -> bytes | None:
        """No state to embed."""
        return None

    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Ignore received state."""
        pass

    def get_health_piggyback(self) -> HealthPiggyback | None:
        """No health piggyback available."""
        return None


@dataclass(slots=True)
class WorkerStateEmbedder:
    """
    State embedder for Worker nodes.

    Embeds WorkerHeartbeat data in SWIM messages so managers can
    passively learn worker capacity and status.

    Also processes ManagerHeartbeat from managers to track leadership
    changes without requiring TCP acks.

    Attributes:
        get_node_id: Callable returning the node's full ID.
        get_worker_state: Callable returning current WorkerState.
        get_available_cores: Callable returning available core count.
        get_queue_depth: Callable returning pending workflow count.
        get_cpu_percent: Callable returning CPU utilization.
        get_memory_percent: Callable returning memory utilization.
        get_state_version: Callable returning state version.
        get_active_workflows: Callable returning workflow ID -> status dict.
        get_tcp_host: Callable returning TCP host address.
        get_tcp_port: Callable returning TCP port.
        on_manager_heartbeat: Optional callback for received ManagerHeartbeat.
        get_health_accepting_work: Callable returning whether worker accepts work.
        get_health_throughput: Callable returning current throughput.
        get_health_expected_throughput: Callable returning expected throughput.
        get_health_overload_state: Callable returning overload state.
    """
    get_node_id: Callable[[], str]
    get_worker_state: Callable[[], str]
    get_available_cores: Callable[[], int]
    get_queue_depth: Callable[[], int]
    get_cpu_percent: Callable[[], float]
    get_memory_percent: Callable[[], float]
    get_state_version: Callable[[], int]
    get_active_workflows: Callable[[], dict[str, str]]
    on_manager_heartbeat: Callable[[Any, tuple[str, int]], None] | None = None
    get_tcp_host: Callable[[], str] | None = None
    get_tcp_port: Callable[[], int] | None = None
    # Health piggyback fields (AD-19)
    get_health_accepting_work: Callable[[], bool] | None = None
    get_health_throughput: Callable[[], float] | None = None
    get_health_expected_throughput: Callable[[], float] | None = None
    get_health_overload_state: Callable[[], str] | None = None
    # Extension request fields (AD-26)
    get_extension_requested: Callable[[], bool] | None = None
    get_extension_reason: Callable[[], str] | None = None
    get_extension_current_progress: Callable[[], float] | None = None

    def get_state(self) -> bytes | None:
        """Get WorkerHeartbeat to embed in SWIM messages."""
        heartbeat = WorkerHeartbeat(
            node_id=self.get_node_id(),
            state=self.get_worker_state(),
            available_cores=self.get_available_cores(),
            queue_depth=self.get_queue_depth(),
            cpu_percent=self.get_cpu_percent(),
            memory_percent=self.get_memory_percent(),
            version=self.get_state_version(),
            active_workflows=self.get_active_workflows(),
            tcp_host=self.get_tcp_host() if self.get_tcp_host else "",
            tcp_port=self.get_tcp_port() if self.get_tcp_port else 0,
            # Health piggyback fields
            health_accepting_work=self.get_health_accepting_work() if self.get_health_accepting_work else True,
            health_throughput=self.get_health_throughput() if self.get_health_throughput else 0.0,
            health_expected_throughput=self.get_health_expected_throughput() if self.get_health_expected_throughput else 0.0,
            health_overload_state=self.get_health_overload_state() if self.get_health_overload_state else "healthy",
            # Extension request fields (AD-26)
            extension_requested=self.get_extension_requested() if self.get_extension_requested else False,
            extension_reason=self.get_extension_reason() if self.get_extension_reason else "",
            extension_current_progress=self.get_extension_current_progress() if self.get_extension_current_progress else 0.0,
        )
        return heartbeat.dump()

    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process ManagerHeartbeat from managers to track leadership."""
        if self.on_manager_heartbeat:
            try:
                obj = ManagerHeartbeat.load(state_data)  # Base unpickle
                # Only process if actually a ManagerHeartbeat
                if isinstance(obj, ManagerHeartbeat):
                    self.on_manager_heartbeat(obj, source_addr)
            except Exception:
                # Invalid data - ignore
                pass

    def get_health_piggyback(self) -> HealthPiggyback | None:
        """
        Get HealthPiggyback for gossip dissemination (Phase 6.1).

        Returns compact health state for O(log n) propagation on all SWIM
        messages, not just ACKs.
        """
        return HealthPiggyback(
            node_id=self.get_node_id(),
            node_type="worker",
            is_alive=True,
            accepting_work=self.get_health_accepting_work() if self.get_health_accepting_work else True,
            capacity=self.get_available_cores(),
            throughput=self.get_health_throughput() if self.get_health_throughput else 0.0,
            expected_throughput=self.get_health_expected_throughput() if self.get_health_expected_throughput else 0.0,
            overload_state=self.get_health_overload_state() if self.get_health_overload_state else "healthy",
            timestamp=time.monotonic(),
        )


@dataclass(slots=True)
class ManagerStateEmbedder:
    """
    State embedder for Manager nodes.

    Embeds ManagerHeartbeat data and processes:
    - WorkerHeartbeat from workers
    - ManagerHeartbeat from peer managers
    - GateHeartbeat from gates

    Attributes:
        get_node_id: Callable returning the node's full ID.
        get_datacenter: Callable returning datacenter ID.
        is_leader: Callable returning leadership status.
        get_term: Callable returning current leadership term.
        get_state_version: Callable returning state version.
        get_active_jobs: Callable returning active job count.
        get_active_workflows: Callable returning active workflow count.
        get_worker_count: Callable returning registered worker count.
        get_available_cores: Callable returning total available cores.
        get_manager_state: Callable returning ManagerState value (syncing/active).
        get_tcp_host: Callable returning TCP host address.
        get_tcp_port: Callable returning TCP port.
        get_udp_host: Callable returning UDP host address.
        get_udp_port: Callable returning UDP port.
        on_worker_heartbeat: Callable to handle received WorkerHeartbeat.
        on_manager_heartbeat: Callable to handle received ManagerHeartbeat from peers.
        on_gate_heartbeat: Callable to handle received GateHeartbeat from gates.
        get_health_accepting_jobs: Callable returning whether manager accepts jobs.
        get_health_has_quorum: Callable returning whether manager has quorum.
        get_health_throughput: Callable returning current throughput.
        get_health_expected_throughput: Callable returning expected throughput.
        get_health_overload_state: Callable returning overload state.
    """
    get_node_id: Callable[[], str]
    get_datacenter: Callable[[], str]
    is_leader: Callable[[], bool]
    get_term: Callable[[], int]
    get_state_version: Callable[[], int]
    get_active_jobs: Callable[[], int]
    get_active_workflows: Callable[[], int]
    get_worker_count: Callable[[], int]
    get_healthy_worker_count: Callable[[], int]
    get_available_cores: Callable[[], int]
    get_total_cores: Callable[[], int]
    on_worker_heartbeat: Callable[[Any, tuple[str, int]], None]
    on_manager_heartbeat: Callable[[Any, tuple[str, int]], None] | None = None
    on_gate_heartbeat: Callable[[Any, tuple[str, int]], None] | None = None
    get_manager_state: Callable[[], str] | None = None
    get_tcp_host: Callable[[], str] | None = None
    get_tcp_port: Callable[[], int] | None = None
    get_udp_host: Callable[[], str] | None = None
    get_udp_port: Callable[[], int] | None = None
    # Health piggyback fields (AD-19)
    get_health_accepting_jobs: Callable[[], bool] | None = None
    get_health_has_quorum: Callable[[], bool] | None = None
    get_health_throughput: Callable[[], float] | None = None
    get_health_expected_throughput: Callable[[], float] | None = None
    get_health_overload_state: Callable[[], str] | None = None

    def get_state(self) -> bytes | None:
        """Get ManagerHeartbeat to embed in SWIM messages."""
        heartbeat = ManagerHeartbeat(
            node_id=self.get_node_id(),
            datacenter=self.get_datacenter(),
            is_leader=self.is_leader(),
            term=self.get_term(),
            version=self.get_state_version(),
            active_jobs=self.get_active_jobs(),
            active_workflows=self.get_active_workflows(),
            worker_count=self.get_worker_count(),
            healthy_worker_count=self.get_healthy_worker_count(),
            available_cores=self.get_available_cores(),
            total_cores=self.get_total_cores(),
            state=self.get_manager_state() if self.get_manager_state else "active",
            tcp_host=self.get_tcp_host() if self.get_tcp_host else "",
            tcp_port=self.get_tcp_port() if self.get_tcp_port else 0,
            udp_host=self.get_udp_host() if self.get_udp_host else "",
            udp_port=self.get_udp_port() if self.get_udp_port else 0,
            # Health piggyback fields
            health_accepting_jobs=self.get_health_accepting_jobs() if self.get_health_accepting_jobs else True,
            health_has_quorum=self.get_health_has_quorum() if self.get_health_has_quorum else True,
            health_throughput=self.get_health_throughput() if self.get_health_throughput else 0.0,
            health_expected_throughput=self.get_health_expected_throughput() if self.get_health_expected_throughput else 0.0,
            health_overload_state=self.get_health_overload_state() if self.get_health_overload_state else "healthy",
        )
        return heartbeat.dump()
    
    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process embedded state from workers, peer managers, or gates."""
        # Unpickle once and dispatch based on actual type
        # This is necessary because load() doesn't validate type - it returns
        # whatever was pickled regardless of which class's load() was called
        try:
            obj = WorkerHeartbeat.load(state_data)  # Base unpickle
        except Exception:
            return  # Invalid data

        # Dispatch based on actual type
        if isinstance(obj, WorkerHeartbeat):
            self.on_worker_heartbeat(obj, source_addr)
        elif isinstance(obj, ManagerHeartbeat) and self.on_manager_heartbeat:
            # Don't process our own heartbeat
            if obj.node_id != self.get_node_id():
                self.on_manager_heartbeat(obj, source_addr)
        elif isinstance(obj, GateHeartbeat) and self.on_gate_heartbeat:
            self.on_gate_heartbeat(obj, source_addr)

    def get_health_piggyback(self) -> HealthPiggyback | None:
        """
        Get HealthPiggyback for gossip dissemination (Phase 6.1).

        Returns compact health state for O(log n) propagation on all SWIM
        messages, not just ACKs.
        """
        return HealthPiggyback(
            node_id=self.get_node_id(),
            node_type="manager",
            is_alive=True,
            accepting_work=self.get_health_accepting_jobs() if self.get_health_accepting_jobs else True,
            capacity=self.get_available_cores(),
            throughput=self.get_health_throughput() if self.get_health_throughput else 0.0,
            expected_throughput=self.get_health_expected_throughput() if self.get_health_expected_throughput else 0.0,
            overload_state=self.get_health_overload_state() if self.get_health_overload_state else "healthy",
            timestamp=time.monotonic(),
        )


@dataclass(slots=True)
class GateStateEmbedder:
    """
    State embedder for Gate nodes.

    Embeds GateHeartbeat data and processes:
    - ManagerHeartbeat from datacenter managers
    - GateHeartbeat from peer gates

    Attributes:
        get_node_id: Callable returning the node's full ID.
        get_datacenter: Callable returning datacenter ID.
        is_leader: Callable returning leadership status.
        get_term: Callable returning current leadership term.
        get_state_version: Callable returning state version.
        get_gate_state: Callable returning GateState value.
        get_active_jobs: Callable returning active job count.
        get_active_datacenters: Callable returning active datacenter count.
        get_manager_count: Callable returning registered manager count.
        get_tcp_host: Callable returning TCP host for routing.
        get_tcp_port: Callable returning TCP port for routing.
        on_manager_heartbeat: Callable to handle received ManagerHeartbeat.
        on_gate_heartbeat: Callable to handle received GateHeartbeat from peers.
        get_known_managers: Callable returning piggybacked manager info.
        get_known_gates: Callable returning piggybacked gate info.
        get_job_leaderships: Callable returning job leadership info (like managers).
        get_job_dc_managers: Callable returning per-DC manager leaders for each job.
        get_health_has_dc_connectivity: Callable returning DC connectivity status.
        get_health_connected_dc_count: Callable returning connected DC count.
        get_health_throughput: Callable returning current throughput.
        get_health_expected_throughput: Callable returning expected throughput.
        get_health_overload_state: Callable returning overload state.
    """
    # Required fields (no defaults) - must come first
    get_node_id: Callable[[], str]
    get_datacenter: Callable[[], str]
    is_leader: Callable[[], bool]
    get_term: Callable[[], int]
    get_state_version: Callable[[], int]
    get_gate_state: Callable[[], str]
    get_active_jobs: Callable[[], int]
    get_active_datacenters: Callable[[], int]
    get_manager_count: Callable[[], int]
    on_manager_heartbeat: Callable[[Any, tuple[str, int]], None]
    # Optional fields (with defaults)
    get_tcp_host: Callable[[], str] | None = None
    get_tcp_port: Callable[[], int] | None = None
    on_gate_heartbeat: Callable[[Any, tuple[str, int]], None] | None = None
    # Piggybacking callbacks for discovery
    get_known_managers: Callable[[], dict[str, tuple[str, int, str, int, str]]] | None = None
    get_known_gates: Callable[[], dict[str, tuple[str, int, str, int]]] | None = None
    # Job leadership piggybacking (like managers - Serf-style consistency)
    get_job_leaderships: Callable[[], dict[str, tuple[int, int]]] | None = None
    get_job_dc_managers: Callable[[], dict[str, dict[str, tuple[str, int]]]] | None = None
    # Health piggyback fields (AD-19)
    get_health_has_dc_connectivity: Callable[[], bool] | None = None
    get_health_connected_dc_count: Callable[[], int] | None = None
    get_health_throughput: Callable[[], float] | None = None
    get_health_expected_throughput: Callable[[], float] | None = None
    get_health_overload_state: Callable[[], str] | None = None

    def get_state(self) -> bytes | None:
        """Get GateHeartbeat to embed in SWIM messages."""
        # Build piggybacked discovery info
        known_managers: dict[str, tuple[str, int, str, int, str]] = {}
        if self.get_known_managers:
            known_managers = self.get_known_managers()

        known_gates: dict[str, tuple[str, int, str, int]] = {}
        if self.get_known_gates:
            known_gates = self.get_known_gates()

        # Build job leadership piggybacking (Serf-style like managers)
        job_leaderships: dict[str, tuple[int, int]] = {}
        if self.get_job_leaderships:
            job_leaderships = self.get_job_leaderships()

        job_dc_managers: dict[str, dict[str, tuple[str, int]]] = {}
        if self.get_job_dc_managers:
            job_dc_managers = self.get_job_dc_managers()

        heartbeat = GateHeartbeat(
            node_id=self.get_node_id(),
            datacenter=self.get_datacenter(),
            is_leader=self.is_leader(),
            term=self.get_term(),
            version=self.get_state_version(),
            state=self.get_gate_state(),
            active_jobs=self.get_active_jobs(),
            active_datacenters=self.get_active_datacenters(),
            manager_count=self.get_manager_count(),
            tcp_host=self.get_tcp_host() if self.get_tcp_host else "",
            tcp_port=self.get_tcp_port() if self.get_tcp_port else 0,
            known_managers=known_managers,
            known_gates=known_gates,
            # Job leadership piggybacking (Serf-style like managers)
            job_leaderships=job_leaderships,
            job_dc_managers=job_dc_managers,
            # Health piggyback fields
            health_has_dc_connectivity=self.get_health_has_dc_connectivity() if self.get_health_has_dc_connectivity else True,
            health_connected_dc_count=self.get_health_connected_dc_count() if self.get_health_connected_dc_count else 0,
            health_throughput=self.get_health_throughput() if self.get_health_throughput else 0.0,
            health_expected_throughput=self.get_health_expected_throughput() if self.get_health_expected_throughput else 0.0,
            health_overload_state=self.get_health_overload_state() if self.get_health_overload_state else "healthy",
        )
        return heartbeat.dump()
    
    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process embedded state from managers or peer gates."""
        # Unpickle once and dispatch based on actual type
        try:
            obj = ManagerHeartbeat.load(state_data)  # Base unpickle
        except Exception:
            return  # Invalid data

        # Dispatch based on actual type
        if isinstance(obj, ManagerHeartbeat):
            self.on_manager_heartbeat(obj, source_addr)
        elif isinstance(obj, GateHeartbeat) and self.on_gate_heartbeat:
            # Don't process our own heartbeat
            if obj.node_id != self.get_node_id():
                self.on_gate_heartbeat(obj, source_addr)

    def get_health_piggyback(self) -> HealthPiggyback | None:
        """
        Get HealthPiggyback for gossip dissemination (Phase 6.1).

        Returns compact health state for O(log n) propagation on all SWIM
        messages, not just ACKs.
        """
        # Gates use connected DC count as capacity metric
        connected_dcs = self.get_health_connected_dc_count() if self.get_health_connected_dc_count else 0

        return HealthPiggyback(
            node_id=self.get_node_id(),
            node_type="gate",
            is_alive=True,
            accepting_work=self.get_health_has_dc_connectivity() if self.get_health_has_dc_connectivity else True,
            capacity=connected_dcs,
            throughput=self.get_health_throughput() if self.get_health_throughput else 0.0,
            expected_throughput=self.get_health_expected_throughput() if self.get_health_expected_throughput else 0.0,
            overload_state=self.get_health_overload_state() if self.get_health_overload_state else "healthy",
            timestamp=time.monotonic(),
        )
