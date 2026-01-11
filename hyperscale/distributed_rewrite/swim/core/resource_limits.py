"""
Resource limits and bounded collections for SWIM protocol.

Provides bounded data structures that prevent unbounded memory growth
in high-churn distributed environments.
"""

import time
from dataclasses import dataclass, field
from typing import TypeVar, Generic, Callable, Any
from collections import OrderedDict

K = TypeVar('K')
V = TypeVar('V')


from .protocols import LoggerProtocol


@dataclass(slots=True)
class BoundedDict(Generic[K, V]):
    """
    A dictionary with bounded size and automatic eviction.
    
    Eviction policies:
    - LRU: Least Recently Used (default) - uses OrderedDict position
    - LRA: Least Recently Added - uses _add_times dict
    - OLDEST: Same as LRA
    
    Memory optimization:
    - LRU uses OrderedDict's built-in ordering, no extra dict needed
    - LRA/OLDEST only store _add_times when needed
    
    Example:
        nodes = BoundedDict[tuple[str, int], NodeState](
            max_size=10000,
            eviction_policy='LRU',
        )
    """
    
    max_size: int = 10000
    """Maximum number of entries before eviction."""
    
    eviction_policy: str = 'LRU'
    """How to choose entries for eviction: 'LRU', 'LRA', 'OLDEST'."""
    
    eviction_batch: int = 100
    """Number of entries to evict at once when limit reached."""
    
    on_evict: Callable[[K, V], None] | None = None
    """Optional callback when an entry is evicted."""
    
    name: str = ""
    """Name for this dict (used in error logging)."""
    
    # Internal storage
    # - OrderedDict tracks access order for LRU (most recent at end)
    # - _add_times only populated for LRA/OLDEST policies
    _data: OrderedDict[K, V] = field(default_factory=OrderedDict)
    _add_times: dict[K, float] = field(default_factory=dict)
    
    # Error tracking
    _evict_callback_errors: int = 0
    
    def __post_init__(self):
        self._data = OrderedDict()
        self._evict_callback_errors = 0
        # Only allocate _add_times if needed for the eviction policy
        if self.eviction_policy in ('LRA', 'OLDEST'):
            self._add_times = {}
        else:
            self._add_times = {}  # Empty, won't be used but needed for type safety
    
    def __len__(self) -> int:
        return len(self._data)
    
    def __contains__(self, key: K) -> bool:
        return key in self._data
    
    def __getitem__(self, key: K) -> V:
        value = self._data[key]
        # Move to end for LRU tracking (OrderedDict handles access order)
        self._data.move_to_end(key)
        return value
    
    def __setitem__(self, key: K, value: V) -> None:
        if key in self._data:
            self._data[key] = value
            self._data.move_to_end(key)
        else:
            # Check if we need to evict
            if len(self._data) >= self.max_size:
                self._evict()
            
            self._data[key] = value
            # Only track add time for LRA/OLDEST policies
            if self.eviction_policy in ('LRA', 'OLDEST'):
                self._add_times[key] = time.monotonic()
    
    def __delitem__(self, key: K) -> None:
        if key in self._data:
            del self._data[key]
            self._add_times.pop(key, None)
    
    def get(self, key: K, default: V | None = None) -> V | None:
        if key in self._data:
            return self[key]
        return default
    
    def pop(self, key: K, default: V | None = None) -> V | None:
        if key in self._data:
            value = self._data.pop(key)
            self._add_times.pop(key, None)
            return value
        return default
    
    def keys(self):
        return self._data.keys()
    
    def values(self):
        return self._data.values()
    
    def items(self):
        return self._data.items()
    
    def clear(self) -> None:
        self._data.clear()
        self._add_times.clear()
    
    def _evict(self) -> None:
        """Evict entries according to the eviction policy."""
        to_evict: list[K] = []
        
        if self.eviction_policy == 'LRU':
            # Evict least recently used (front of OrderedDict)
            # OrderedDict maintains insertion/access order, oldest at front
            for key in list(self._data.keys())[:self.eviction_batch]:
                to_evict.append(key)
        
        elif self.eviction_policy in ('LRA', 'OLDEST'):
            # Evict least recently added - use _add_times
            if self._add_times:
                sorted_by_add = sorted(
                    self._add_times.items(),
                    key=lambda x: x[1],
                )
                to_evict = [k for k, _ in sorted_by_add[:self.eviction_batch]]
            else:
                # Fallback to front of OrderedDict if no add times
                for key in list(self._data.keys())[:self.eviction_batch]:
                    to_evict.append(key)
        
        for key in to_evict:
            value = self._data.pop(key, None)
            self._add_times.pop(key, None)
            
            if value is not None and self.on_evict:
                try:
                    self.on_evict(key, value)
                except Exception:
                    # Count error but don't fail eviction - would cause memory issues
                    # Cannot log async from sync __setitem__ - error count is tracked
                    self._evict_callback_errors += 1
    
    def cleanup_by_predicate(
        self,
        predicate: Callable[[K, V], bool],
    ) -> int:
        """
        Remove entries matching a predicate.
        
        Args:
            predicate: Function(key, value) -> bool. True means remove.
        
        Returns:
            Number of entries removed.
        """
        to_remove = [
            key for key, value in self._data.items()
            if predicate(key, value)
        ]
        
        for key in to_remove:
            del self[key]
        
        return len(to_remove)
    
    def cleanup_older_than(self, max_age_seconds: float) -> int:
        """
        Remove entries older than max_age_seconds.
        
        Returns:
            Number of entries removed.
        """
        cutoff = time.monotonic() - max_age_seconds
        return self.cleanup_by_predicate(
            lambda k, _: self._add_times.get(k, 0) < cutoff
        )


@dataclass(slots=True)
class CleanupConfig:
    """
    Configuration for periodic cleanup of SWIM state.
    
    Used to configure garbage collection of dead nodes,
    expired suspicions, etc.
    """
    
    # Node state cleanup
    max_node_states: int = 10000
    """Maximum tracked nodes before eviction."""
    
    dead_node_retention_seconds: float = 3600.0
    """How long to remember dead nodes (for proper refutation)."""
    
    # Suspicion cleanup
    max_suspicions: int = 1000
    """Maximum concurrent suspicions."""
    
    orphaned_suspicion_timeout: float = 300.0
    """Timeout for suspicions with no timer (orphaned)."""
    
    # Gossip buffer cleanup
    max_gossip_updates: int = 1000
    """Maximum pending gossip updates."""
    
    stale_gossip_age_seconds: float = 60.0
    """Remove gossip updates older than this."""
    
    # Indirect probe cleanup  
    max_pending_probes: int = 100
    """Maximum concurrent indirect probes."""
    
    probe_retention_seconds: float = 30.0
    """How long to keep completed probe records."""
    
    # Cleanup frequency
    cleanup_interval_seconds: float = 30.0
    """How often to run cleanup."""


def create_cleanup_config_from_context(context: dict[str, Any]) -> CleanupConfig:
    """Create CleanupConfig from server context with sensible defaults."""
    return CleanupConfig(
        max_node_states=context.get('max_node_states', 10000),
        dead_node_retention_seconds=context.get('dead_node_retention', 3600.0),
        max_suspicions=context.get('max_suspicions', 1000),
        max_gossip_updates=context.get('max_gossip_updates', 1000),
        max_pending_probes=context.get('max_pending_probes', 100),
        cleanup_interval_seconds=context.get('cleanup_interval', 30.0),
    )

