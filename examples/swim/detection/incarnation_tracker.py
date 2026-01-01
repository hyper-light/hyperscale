"""
Incarnation number tracking for SWIM protocol.
"""

import time
from dataclasses import dataclass, field
from typing import Callable, Any
from ..core.types import Status
from ..core.node_state import NodeState

from hyperscale.logging.hyperscale_logging_models import ServerDebug


from ..core.protocols import LoggerProtocol

# Maximum valid incarnation number (2^31 - 1 for wide compatibility)
MAX_INCARNATION = 2**31 - 1

# Maximum allowed incarnation jump in a single message
# Larger jumps may indicate attack or corruption
MAX_INCARNATION_JUMP = 1000


@dataclass 
class IncarnationTracker:
    """
    Tracks incarnation numbers for SWIM protocol.
    
    Each node maintains:
    - Its own incarnation number (incremented on refutation)
    - Known incarnation numbers for all other nodes
    
    Incarnation numbers are used to:
    - Order messages about the same node
    - Allow refutation of false suspicions
    - Prevent old messages from overriding newer state
    
    Resource limits:
    - max_nodes: Maximum tracked nodes (default 10000)
    - dead_node_retention: How long to keep dead nodes (default 1 hour)
    - Automatic cleanup of stale entries
    """
    self_incarnation: int = 0
    node_states: dict[tuple[str, int], NodeState] = field(default_factory=dict)
    
    # Resource limits
    max_nodes: int = 10000
    """Maximum number of nodes to track before eviction."""
    
    dead_node_retention_seconds: float = 3600.0
    """How long to retain dead node state for proper refutation."""
    
    # Callbacks for eviction events
    _on_node_evicted: Callable[[tuple[str, int], NodeState], None] | None = None
    
    # Stats for monitoring
    _eviction_count: int = 0
    _cleanup_count: int = 0
    
    # Logger for structured logging (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0
    
    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
        node_id: int,
    ) -> None:
        """Set logger for structured logging."""
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id
    
    async def _log_debug(self, message: str) -> None:
        """Log a debug message."""
        if self._logger:
            try:
                await self._logger.log(ServerDebug(
                    message=f"[IncarnationTracker] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                ))
            except Exception:
                pass  # Don't let logging errors propagate
    
    def get_self_incarnation(self) -> int:
        """Get current incarnation number for this node."""
        return self.self_incarnation
    
    def increment_self_incarnation(self) -> int:
        """
        Increment own incarnation number.
        Called when refuting a suspicion about ourselves.
        Returns the new incarnation number.
        
        Raises:
            OverflowError: If incarnation would exceed MAX_INCARNATION.
        """
        if self.self_incarnation >= MAX_INCARNATION:
            raise OverflowError(
                f"Incarnation number exhausted (at {MAX_INCARNATION}). "
                "Node must restart to continue participating in cluster."
            )
        self.self_incarnation += 1
        return self.self_incarnation
    
    def is_valid_incarnation(self, incarnation: int) -> bool:
        """
        Check if an incarnation number is valid.
        
        Returns False for:
        - Negative numbers
        - Numbers exceeding MAX_INCARNATION
        """
        return 0 <= incarnation <= MAX_INCARNATION
    
    def is_suspicious_jump(
        self,
        node: tuple[str, int],
        new_incarnation: int,
    ) -> bool:
        """
        Check if an incarnation jump is suspiciously large.
        
        Large jumps may indicate:
        - Attack (trying to fast-forward incarnation)
        - Data corruption
        - Node restart with persisted high incarnation
        
        Returns True if jump exceeds MAX_INCARNATION_JUMP.
        """
        current = self.get_node_incarnation(node)
        jump = new_incarnation - current
        return jump > MAX_INCARNATION_JUMP
    
    def get_node_state(self, node: tuple[str, int]) -> NodeState | None:
        """Get the current state for a known node."""
        return self.node_states.get(node)
    
    def get_node_incarnation(self, node: tuple[str, int]) -> int:
        """Get the incarnation number for a node, or 0 if unknown."""
        state = self.node_states.get(node)
        return state.incarnation if state else 0
    
    def update_node(
        self, 
        node: tuple[str, int], 
        status: Status, 
        incarnation: int,
        timestamp: float,
        validate: bool = True,
    ) -> bool:
        """
        Update the state of a node.
        
        Args:
            node: Node address tuple (host, port).
            status: Node status (OK, SUSPECT, DEAD, JOIN).
            incarnation: Node's incarnation number.
            timestamp: Time of this update.
            validate: Whether to validate incarnation number.
        
        Returns:
            True if the state was updated, False if message was rejected.
            
        Note:
            If validate=True, invalid or suspicious incarnation numbers
            are rejected and the method returns False.
        """
        if validate:
            if not self.is_valid_incarnation(incarnation):
                return False
            if self.is_suspicious_jump(node, incarnation):
                # Log suspicious activity but still reject
                return False
        
        if node not in self.node_states:
            self.node_states[node] = NodeState(
                status=status,
                incarnation=incarnation,
                last_update_time=timestamp,
            )
            return True
        return self.node_states[node].update(status, incarnation, timestamp)
    
    def remove_node(self, node: tuple[str, int]) -> bool:
        """Remove a node from tracking. Returns True if it existed."""
        if node in self.node_states:
            del self.node_states[node]
            return True
        return False
    
    def get_all_nodes(self) -> list[tuple[tuple[str, int], NodeState]]:
        """Get all known nodes and their states."""
        return list(self.node_states.items())
    
    def is_message_fresh(
        self, 
        node: tuple[str, int], 
        incarnation: int, 
        status: Status,
        validate: bool = True,
    ) -> bool:
        """
        Check if a message about a node is fresh (should be processed).
        
        A message is fresh if:
        - Incarnation number is valid (if validate=True)
        - Jump is not suspiciously large (if validate=True)
        - We don't know about the node yet
        - It has a higher incarnation number
        - Same incarnation but higher priority status
        
        Args:
            node: Node address tuple.
            incarnation: Incarnation number from message.
            status: Status from message.
            validate: Whether to validate incarnation number.
        
        Returns:
            True if message should be processed, False otherwise.
        """
        if validate:
            if not self.is_valid_incarnation(incarnation):
                return False
            if self.is_suspicious_jump(node, incarnation):
                return False
        
        state = self.node_states.get(node)
        if state is None:
            return True
        if incarnation > state.incarnation:
            return True
        if incarnation == state.incarnation:
            status_priority = {b'OK': 0, b'JOIN': 0, b'SUSPECT': 1, b'DEAD': 2}
            return status_priority.get(status, 0) > status_priority.get(state.status, 0)
        return False
    
    def set_eviction_callback(
        self,
        callback: Callable[[tuple[str, int], NodeState], None],
    ) -> None:
        """Set callback for when nodes are evicted."""
        self._on_node_evicted = callback
    
    async def cleanup_dead_nodes(self) -> int:
        """
        Remove dead nodes that have exceeded retention period.
        
        Returns:
            Number of nodes removed.
        """
        now = time.monotonic()
        cutoff = now - self.dead_node_retention_seconds
        
        to_remove = []
        for node, state in self.node_states.items():
            if state.status == b'DEAD' and state.last_update_time < cutoff:
                to_remove.append(node)
        
        for node in to_remove:
            state = self.node_states.pop(node)
            self._cleanup_count += 1
            if self._on_node_evicted:
                try:
                    self._on_node_evicted(node, state)
                except Exception as e:
                    await self._log_debug(
                        f"Eviction callback error for node {node}: "
                        f"{type(e).__name__}: {e}"
                    )
        
        return len(to_remove)
    
    async def evict_if_needed(self) -> int:
        """
        Evict oldest nodes if we exceed max_nodes limit.
        
        Eviction priority:
        1. Dead nodes (oldest first)
        2. Suspect nodes (oldest first)
        3. OK nodes (oldest first)
        
        Returns:
            Number of nodes evicted.
        """
        if len(self.node_states) <= self.max_nodes:
            return 0
        
        to_evict_count = len(self.node_states) - self.max_nodes + 100  # Evict batch
        
        # Sort by (status_priority, last_update_time)
        status_priority = {b'DEAD': 0, b'SUSPECT': 1, b'OK': 2, b'JOIN': 2}
        
        sorted_nodes = sorted(
            self.node_states.items(),
            key=lambda x: (
                status_priority.get(x[1].status, 2),
                x[1].last_update_time,
            ),
        )
        
        evicted = 0
        for node, state in sorted_nodes[:to_evict_count]:
            del self.node_states[node]
            self._eviction_count += 1
            evicted += 1
            if self._on_node_evicted:
                try:
                    self._on_node_evicted(node, state)
                except Exception as e:
                    await self._log_debug(
                        f"Eviction callback error for node {node}: "
                        f"{type(e).__name__}: {e}"
                    )
        
        return evicted
    
    async def cleanup(self) -> dict[str, int]:
        """
        Run all cleanup operations.
        
        Returns:
            Dict with cleanup stats.
        """
        dead_removed = await self.cleanup_dead_nodes()
        evicted = await self.evict_if_needed()
        
        return {
            'dead_removed': dead_removed,
            'evicted': evicted,
            'total_nodes': len(self.node_states),
        }
    
    def get_stats(self) -> dict[str, int]:
        """Get tracker statistics for monitoring."""
        status_counts = {b'OK': 0, b'SUSPECT': 0, b'DEAD': 0, b'JOIN': 0}
        for state in self.node_states.values():
            status_counts[state.status] = status_counts.get(state.status, 0) + 1
        
        return {
            'total_nodes': len(self.node_states),
            'ok_nodes': status_counts.get(b'OK', 0),
            'suspect_nodes': status_counts.get(b'SUSPECT', 0),
            'dead_nodes': status_counts.get(b'DEAD', 0),
            'total_evictions': self._eviction_count,
            'total_cleanups': self._cleanup_count,
        }

