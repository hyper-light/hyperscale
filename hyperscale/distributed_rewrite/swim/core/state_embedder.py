"""
State Embedder Protocol and Implementations.

This module provides a composition-based approach for embedding application
state (heartbeats) in SWIM UDP messages, enabling Serf-style passive state
dissemination.

The StateEmbedder protocol is injected into UDPServer, allowing different
node types (Worker, Manager, Gate) to provide their own state without
requiring inheritance-based overrides.
"""

from dataclasses import dataclass
from typing import Protocol, Callable, Any
import time


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
    
    Used when no state embedding is needed (base UDPServer behavior).
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
    
    Attributes:
        get_node_id: Callable returning the node's full ID.
        get_worker_state: Callable returning current WorkerState.
        get_available_cores: Callable returning available core count.
        get_queue_depth: Callable returning pending workflow count.
        get_cpu_percent: Callable returning CPU utilization.
        get_memory_percent: Callable returning memory utilization.
        get_state_version: Callable returning state version.
        get_active_workflows: Callable returning workflow ID -> status dict.
    """
    get_node_id: Callable[[], str]
    get_worker_state: Callable[[], str]
    get_available_cores: Callable[[], int]
    get_queue_depth: Callable[[], int]
    get_cpu_percent: Callable[[], float]
    get_memory_percent: Callable[[], float]
    get_state_version: Callable[[], int]
    get_active_workflows: Callable[[], dict[str, str]]
    
    def get_state(self) -> bytes | None:
        """Get WorkerHeartbeat to embed in SWIM messages."""
        # Import here to avoid circular imports
        from hyperscale.distributed_rewrite.models import WorkerHeartbeat
        
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
        """Workers don't track other nodes' state."""
        pass


@dataclass(slots=True)
class ManagerStateEmbedder:
    """
    State embedder for Manager nodes.
    
    Embeds ManagerHeartbeat data and processes WorkerHeartbeat from workers.
    
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
        on_worker_heartbeat: Callable to handle received WorkerHeartbeat.
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
    
    def get_state(self) -> bytes | None:
        """Get ManagerHeartbeat to embed in SWIM messages."""
        from hyperscale.distributed_rewrite.models import ManagerHeartbeat
        
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
        )
        return heartbeat.dump()
    
    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process embedded state from workers."""
        from hyperscale.distributed_rewrite.models import WorkerHeartbeat
        
        try:
            heartbeat = WorkerHeartbeat.load(state_data)
            self.on_worker_heartbeat(heartbeat, source_addr)
        except Exception:
            # Not a WorkerHeartbeat or invalid data - ignore
            pass


@dataclass(slots=True)
class GateStateEmbedder:
    """
    State embedder for Gate nodes.
    
    Gates don't embed much state (they're coordinators), but they
    process ManagerHeartbeat from datacenter managers.
    
    Attributes:
        get_node_id: Callable returning the node's full ID.
        get_datacenter: Callable returning datacenter ID.
        is_leader: Callable returning leadership status.
        get_term: Callable returning current leadership term.
        get_state_version: Callable returning state version.
        get_active_jobs: Callable returning active job count.
        on_manager_heartbeat: Callable to handle received ManagerHeartbeat.
    """
    get_node_id: Callable[[], str]
    get_datacenter: Callable[[], str]
    is_leader: Callable[[], bool]
    get_term: Callable[[], int]
    get_state_version: Callable[[], int]
    get_active_jobs: Callable[[], int]
    on_manager_heartbeat: Callable[[Any, tuple[str, int]], None]
    
    def get_state(self) -> bytes | None:
        """
        Gates embed minimal state - they're primarily coordinators.
        
        Could embed a GateHeartbeat in the future if peer gates
        need to know about each other's status.
        """
        # For now, gates don't embed state
        # Could add GateHeartbeat if needed for gate-to-gate awareness
        return None
    
    def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process embedded state from managers."""
        from hyperscale.distributed_rewrite.models import ManagerHeartbeat
        
        try:
            heartbeat = ManagerHeartbeat.load(state_data)
            self.on_manager_heartbeat(heartbeat, source_addr)
        except Exception:
            # Not a ManagerHeartbeat or invalid data - ignore
            pass

