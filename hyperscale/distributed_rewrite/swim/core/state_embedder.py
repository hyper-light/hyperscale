"""
State Embedder Protocol and Implementations.

This module provides a composition-based approach for embedding application
state (heartbeats) in SWIM UDP messages, enabling Serf-style passive state
dissemination.

The StateEmbedder protocol is injected into HealthAwareServer, allowing different
node types (Worker, Manager, Gate) to provide their own state without
requiring inheritance-based overrides.
"""

from dataclasses import dataclass
from typing import Protocol, Callable, Any
import time

from hyperscale.distributed_rewrite.models import (
    WorkerHeartbeat,
    ManagerHeartbeat,
    GateHeartbeat,
)


class StateEmbedder(Protocol):
    """
    Protocol for embedding and processing state in SWIM messages.
    
    Implementations provide:
    - get_state(): Returns serialized state to embed in outgoing messages
    - process_state(): Handles state received from other nodes
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
        on_manager_heartbeat: Optional callback for received ManagerHeartbeat.
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
                heartbeat = ManagerHeartbeat.load(state_data)
                self.on_manager_heartbeat(heartbeat, source_addr)
            except Exception:
                # Not a ManagerHeartbeat or invalid data - ignore
                pass


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
        on_worker_heartbeat: Callable to handle received WorkerHeartbeat.
        on_manager_heartbeat: Callable to handle received ManagerHeartbeat from peers.
        on_gate_heartbeat: Callable to handle received GateHeartbeat from gates.
    """
    get_node_id: Callable[[], str]
    get_datacenter: Callable[[], str]
    is_leader: Callable[[], bool]
    get_term: Callable[[], int]
    get_state_version: Callable[[], int]
    get_active_jobs: Callable[[], int]
    get_active_workflows: Callable[[], int]
    get_worker_count: Callable[[], int]
    get_available_cores: Callable[[], int]
    on_worker_heartbeat: Callable[[Any, tuple[str, int]], None]
    on_manager_heartbeat: Callable[[Any, tuple[str, int]], None] | None = None
    on_gate_heartbeat: Callable[[Any, tuple[str, int]], None] | None = None
    get_manager_state: Callable[[], str] | None = None
    
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
            available_cores=self.get_available_cores(),
            state=self.get_manager_state() if self.get_manager_state else "active",
        )
        return heartbeat.dump()
    
    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process embedded state from workers, peer managers, or gates."""
        # Try parsing as WorkerHeartbeat first (most common)
        try:
            heartbeat = WorkerHeartbeat.load(state_data)
            self.on_worker_heartbeat(heartbeat, source_addr)
            return
        except Exception:
            pass
        
        # Try parsing as ManagerHeartbeat from peer managers
        if self.on_manager_heartbeat:
            try:
                heartbeat = ManagerHeartbeat.load(state_data)
                # Don't process our own heartbeat
                if heartbeat.node_id != self.get_node_id():
                    self.on_manager_heartbeat(heartbeat, source_addr)
                    return
            except Exception:
                pass
        
        # Try parsing as GateHeartbeat from gates
        if self.on_gate_heartbeat:
            try:
                heartbeat = GateHeartbeat.load(state_data)
                self.on_gate_heartbeat(heartbeat, source_addr)
            except Exception:
                pass


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
        on_manager_heartbeat: Callable to handle received ManagerHeartbeat.
        on_gate_heartbeat: Callable to handle received GateHeartbeat from peers.
    """
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
    on_gate_heartbeat: Callable[[Any, tuple[str, int]], None] | None = None
    
    def get_state(self) -> bytes | None:
        """Get GateHeartbeat to embed in SWIM messages."""
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
        )
        return heartbeat.dump()
    
    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process embedded state from managers or peer gates."""
        # Try parsing as ManagerHeartbeat first (most common)
        try:
            heartbeat = ManagerHeartbeat.load(state_data)
            self.on_manager_heartbeat(heartbeat, source_addr)
            return
        except Exception:
            pass
        
        # Try parsing as GateHeartbeat from peer gates
        if self.on_gate_heartbeat:
            try:
                heartbeat = GateHeartbeat.load(state_data)
                # Don't process our own heartbeat
                if heartbeat.node_id != self.get_node_id():
                    self.on_gate_heartbeat(heartbeat, source_addr)
            except Exception:
                pass

