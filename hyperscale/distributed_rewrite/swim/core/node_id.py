"""
Structured Node Identifier for multi-datacenter SWIM clusters.

Encodes datacenter, priority, creation time, and random component
for human-readable, sortable, unique node identification.
"""

from dataclasses import dataclass, field
import time
import uuid


@dataclass(frozen=True)
class NodeId:
    """
    Structured node identifier for multi-datacenter SWIM clusters.
    
    Format: {datacenter}-{priority:02d}-{timestamp_ms:013x}-{random:12}
    Example: DC-EAST-01-0018a3b2c4d5e-f3a2b1c9d8e7
    
    Components:
    - datacenter: Datacenter identifier (e.g., "DC-EAST", "US-WEST-2")
    - priority: Node priority for leadership (00-99, lower = higher priority)
    - created_ms: Unix timestamp in milliseconds when node started
    - random: 12 hex chars for uniqueness within same ms
    
    The ID is:
    - Globally unique across all datacenters
    - Lexicographically sortable (by DC, then priority, then time)
    - Human-readable for debugging
    - Stable across the node's lifetime
    """
    
    datacenter: str
    priority: int
    created_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    random: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    
    def __post_init__(self):
        """Validate node ID components."""
        if not self.datacenter:
            raise ValueError("datacenter cannot be empty")
        if not 0 <= self.priority <= 99:
            raise ValueError("priority must be between 0 and 99")
        if len(self.random) != 12:
            raise ValueError("random component must be 12 hex characters")
    
    def __str__(self) -> str:
        """Full string representation of the node ID."""
        return f"{self.datacenter}-{self.priority:02d}-{self.created_ms:013x}-{self.random}"
    
    def __repr__(self) -> str:
        return f"NodeId({self!s})"
    
    def __hash__(self) -> int:
        return hash((self.datacenter, self.priority, self.created_ms, self.random))
    
    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeId):
            return (
                self.datacenter == other.datacenter
                and self.priority == other.priority
                and self.created_ms == other.created_ms
                and self.random == other.random
            )
        if isinstance(other, str):
            return str(self) == other
        return False
    
    def __lt__(self, other: 'NodeId') -> bool:
        """Compare node IDs lexicographically (for sorting/leadership)."""
        return str(self) < str(other)
    
    @property
    def short(self) -> str:
        """Short form for logging: DC-EAST-01-f3a2"""
        return f"{self.datacenter}-{self.priority:02d}-{self.random[:4]}"
    
    @property
    def full(self) -> str:
        """Full string representation of the node ID (alias for str())."""
        return str(self)
    
    @property
    def age_seconds(self) -> float:
        """How old this node ID is in seconds."""
        return (time.time() * 1000 - self.created_ms) / 1000
    
    @classmethod
    def parse(cls, s: str) -> 'NodeId':
        """
        Parse a node ID string back into a NodeId object.
        
        Args:
            s: String in format "DC-PRIORITY-TIMESTAMP-RANDOM"
        
        Returns:
            NodeId instance
        
        Raises:
            ValueError: If string format is invalid
        """
        try:
            # Split from the right to handle datacenter names with dashes
            parts = s.rsplit('-', 3)
            if len(parts) != 4:
                raise ValueError(f"Expected 4 parts, got {len(parts)}")
            
            dc, priority_str, ts_str, rand = parts
            priority = int(priority_str)
            created_ms = int(ts_str, 16)
            
            return cls(
                datacenter=dc,
                priority=priority,
                created_ms=created_ms,
                random=rand,
            )
        except Exception as e:
            raise ValueError(f"Invalid node ID format '{s}': {e}") from e
    
    @classmethod
    def generate(
        cls,
        datacenter: str,
        priority: int = 50,
    ) -> 'NodeId':
        """
        Generate a new node ID for a node in the given datacenter.
        
        Args:
            datacenter: Datacenter identifier
            priority: Leadership priority (0-99, lower = higher priority)
        
        Returns:
            New NodeId instance
        """
        return cls(datacenter=datacenter, priority=priority)
    
    def to_bytes(self) -> bytes:
        """Encode the node ID as bytes for network transmission."""
        return str(self).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'NodeId':
        """Decode a node ID from bytes."""
        return cls.parse(data.decode('utf-8'))
    
    def same_datacenter(self, other: 'NodeId') -> bool:
        """Check if another node is in the same datacenter."""
        return self.datacenter == other.datacenter
    
    def has_higher_priority(self, other: 'NodeId') -> bool:
        """Check if this node has higher priority (lower number) than another."""
        return self.priority < other.priority


@dataclass(slots=True)
class NodeAddress:
    """
    Combines a NodeId with network address information.
    
    This allows tracking both the logical node identity and its
    current network location, supporting node restarts on different
    ports or IP address changes.
    """
    
    node_id: NodeId
    host: str
    port: int
    
    def __str__(self) -> str:
        return f"{self.node_id.short}@{self.host}:{self.port}"
    
    def __repr__(self) -> str:
        return f"NodeAddress({self.node_id!s}, {self.host}:{self.port})"
    
    def __hash__(self) -> int:
        return hash(self.node_id)
    
    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeAddress):
            return self.node_id == other.node_id
        return False
    
    @property
    def addr_tuple(self) -> tuple[str, int]:
        """Get the (host, port) tuple for socket operations."""
        return (self.host, self.port)
    
    @property
    def addr_str(self) -> str:
        """Get 'host:port' string."""
        return f"{self.host}:{self.port}"
    
    def to_bytes(self) -> bytes:
        """Encode for network transmission: node_id|host:port"""
        return f"{self.node_id}|{self.host}:{self.port}".encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'NodeAddress':
        """Decode from network transmission."""
        s = data.decode('utf-8')
        node_id_str, addr = s.split('|', 1)
        host, port_str = addr.rsplit(':', 1)
        return cls(
            node_id=NodeId.parse(node_id_str),
            host=host,
            port=int(port_str),
        )

