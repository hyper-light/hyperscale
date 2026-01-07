"""
Gossip buffer for SWIM membership update dissemination.
"""

import heapq
import math
import time
from dataclasses import dataclass, field
from typing import Any

from ..core.types import UpdateType
from .piggyback_update import PiggybackUpdate


# UDP MTU considerations:
# - Ethernet MTU: 1500 bytes
# - IP header: 20 bytes (min)
# - UDP header: 8 bytes
# - Safe payload: ~1472 bytes
# We leave headroom for the base message (probe, ack, etc.)
MAX_PIGGYBACK_SIZE = 1200  # Safe size for piggybacked data
MAX_UDP_PAYLOAD = 1400  # Maximum total UDP payload


@dataclass(slots=True)
class GossipBuffer:
    """
    Buffer for membership updates to be piggybacked on messages.
    
    Maintains a priority queue of updates ordered by broadcast count,
    so that less-disseminated updates are sent first. Updates are
    removed after being broadcast lambda * log(n) times.
    
    Resource limits:
    - max_updates: Maximum pending updates before eviction
    - stale_age: Remove updates older than this
    """
    updates: dict[tuple[str, int], PiggybackUpdate] = field(default_factory=dict)
    # Multiplier for max broadcasts (lambda in the paper)
    broadcast_multiplier: int = 3
    
    # Resource limits
    max_updates: int = 1000
    """Maximum pending updates before eviction."""
    
    stale_age_seconds: float = 60.0
    """Remove updates older than this (seconds)."""
    
    # Message size limits
    max_piggyback_size: int = MAX_PIGGYBACK_SIZE
    """Maximum bytes for piggybacked data."""
    
    # Stats
    _evicted_count: int = 0
    _stale_removed_count: int = 0
    _size_limited_count: int = 0  # Times we hit size limit
    _oversized_updates_count: int = 0  # Individual updates that were too large
    _overflow_count: int = 0  # Times we had to evict due to capacity
    
    # Callbacks
    _on_overflow: Any = None  # Callable[[int, int], None] - (evicted, capacity)
    
    def set_overflow_callback(self, callback: Any) -> None:
        """Set callback to be called when buffer overflows and eviction occurs."""
        self._on_overflow = callback

    def add_update(
        self,
        update_type: UpdateType,
        node: tuple[str, int],
        incarnation: int,
        n_members: int = 1,
    ) -> bool:
        """
        Add or update a membership update in the buffer.
        
        If an update for the same node exists with lower incarnation,
        it is replaced. Updates with equal or higher incarnation are
        only replaced if the new status has higher priority.
        
        Returns:
            True if update was added, False if rejected due to limits.
        """
        # Check limits for new updates
        if node not in self.updates and len(self.updates) >= self.max_updates:
            # Try cleanup first
            self.cleanup_stale()
            self.cleanup_broadcast_complete()
            
            # Still at limit? Evict oldest
            if len(self.updates) >= self.max_updates:
                self._evict_oldest()
        
        # Calculate max broadcasts: lambda * log(n+1)
        max_broadcasts = max(1, int(
            self.broadcast_multiplier * math.log(n_members + 1)
        ))
        
        existing = self.updates.get(node)
        
        if existing is None:
            # New update
            self.updates[node] = PiggybackUpdate(
                update_type=update_type,
                node=node,
                incarnation=incarnation,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
            return True
        elif incarnation > existing.incarnation:
            # Higher incarnation replaces
            self.updates[node] = PiggybackUpdate(
                update_type=update_type,
                node=node,
                incarnation=incarnation,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
            return True
        elif incarnation == existing.incarnation:
            # Same incarnation - check status priority
            priority = {'alive': 0, 'join': 0, 'suspect': 1, 'dead': 2, 'leave': 2}
            if priority.get(update_type, 0) > priority.get(existing.update_type, 0):
                self.updates[node] = PiggybackUpdate(
                    update_type=update_type,
                    node=node,
                    incarnation=incarnation,
                    timestamp=time.monotonic(),
                    max_broadcasts=max_broadcasts,
                )
                return True
        
        return False
    
    def get_updates_to_piggyback(self, max_count: int = 5) -> list[PiggybackUpdate]:
        """
        Get updates to piggyback on the next message.
        
        Args:
            max_count: Max updates to return (bounded to 1-100).
        
        Returns up to max_count updates, prioritizing those with
        the lowest broadcast count (least disseminated).
        
        Uses heapq.nsmallest() for O(n log k) complexity instead of
        O(n log n) from full sorting.
        """
        # Bound max_count to prevent excessive iteration
        max_count = max(1, min(max_count, 100))
        
        # Filter to broadcastable updates
        candidates = (u for u in self.updates.values() if u.should_broadcast())
        
        # Use nsmallest for efficient top-k selection: O(n log k) vs O(n log n)
        return heapq.nsmallest(max_count, candidates, key=lambda u: u.broadcast_count)
    
    def mark_broadcasts(self, updates: list[PiggybackUpdate]) -> None:
        """Mark updates as having been broadcast and remove if done."""
        for update in updates:
            if update.node in self.updates:
                self.updates[update.node].mark_broadcast()
                if not self.updates[update.node].should_broadcast():
                    del self.updates[update.node]
    
    # Maximum allowed max_count to prevent excessive iteration
    MAX_ENCODE_COUNT = 100
    
    def encode_piggyback(
        self, 
        max_count: int = 5, 
        max_size: int | None = None,
    ) -> bytes:
        """
        Get piggybacked updates as bytes to append to a message.
        Format: |update1|update2|update3
        
        Args:
            max_count: Maximum number of updates to include (1-100).
            max_size: Maximum total size in bytes (defaults to max_piggyback_size).
        
        Returns:
            Encoded piggyback data respecting size limits.
        """
        # Validate and bound max_count
        max_count = max(1, min(max_count, self.MAX_ENCODE_COUNT))
        
        if max_size is None:
            max_size = self.max_piggyback_size
        
        updates = self.get_updates_to_piggyback(max_count)
        if not updates:
            return b''
        
        # Build result respecting size limit
        result_parts: list[bytes] = []
        total_size = 0  # Not counting leading '|' yet
        included_updates: list[PiggybackUpdate] = []
        
        for update in updates:
            encoded = update.to_bytes()
            update_size = len(encoded) + 1  # +1 for separator '|'
            
            # Check if individual update is too large
            if update_size > max_size:
                self._oversized_updates_count += 1
                continue
            
            # Check if adding this update would exceed limit
            if total_size + update_size > max_size:
                self._size_limited_count += 1
                break
            
            result_parts.append(encoded)
            total_size += update_size
            included_updates.append(update)
        
        if not result_parts:
            return b''
        
        self.mark_broadcasts(included_updates)
        return b'|' + b'|'.join(result_parts)
    
    def encode_piggyback_with_base(
        self,
        base_message: bytes,
        max_count: int = 5,
    ) -> bytes:
        """
        Encode piggyback data considering the base message size.
        
        Ensures the total message (base + piggyback) doesn't exceed MTU.
        
        Args:
            base_message: The core message (probe, ack, etc.)
            max_count: Maximum number of updates to include.
        
        Returns:
            Encoded piggyback data that fits within UDP limits.
        """
        remaining = MAX_UDP_PAYLOAD - len(base_message)
        if remaining <= 0:
            return b''
        
        return self.encode_piggyback(max_count, max_size=remaining)
    
    # Maximum updates to decode from a single piggyback message
    MAX_DECODE_UPDATES = 100
    
    @staticmethod
    def decode_piggyback(data: bytes, max_updates: int = 100) -> list[PiggybackUpdate]:
        """
        Decode piggybacked updates from message suffix.
        
        Args:
            data: Raw piggyback data starting with '|'.
            max_updates: Maximum updates to decode (default 100).
                        Prevents malicious messages with thousands of updates.
        
        Returns:
            List of decoded updates (bounded by max_updates).
        """
        if not data or data[0:1] != b'|':
            return []
        
        # Bound max_updates to prevent abuse
        bounded_max = min(max_updates, GossipBuffer.MAX_DECODE_UPDATES)
        
        updates = []
        parts = data[1:].split(b'|')
        for part in parts:
            if len(updates) >= bounded_max:
                # Stop decoding - we've hit the limit
                break
            if part:
                update = PiggybackUpdate.from_bytes(part)
                if update:
                    updates.append(update)
        return updates
    
    def clear(self) -> None:
        """Clear all pending updates."""
        self.updates.clear()
    
    def remove_node(self, node: tuple[str, int]) -> bool:
        """
        Remove all pending updates for a specific node.
        
        Used when a node rejoins to clear stale state.
        
        Returns:
            True if any updates were removed.
        """
        if node in self.updates:
            del self.updates[node]
            return True
        return False
    
    def _evict_oldest(self, count: int = 10) -> int:
        """
        Evict the oldest updates.
        
        Uses heapq.nsmallest() for O(n log k) complexity.
        
        Returns:
            Number of updates evicted.
        """
        if not self.updates:
            return 0
        
        # Use nsmallest for efficient bottom-k selection: O(n log k) vs O(n log n)
        oldest = heapq.nsmallest(
            count,
            self.updates.items(),
            key=lambda x: x[1].timestamp,
        )
        
        evicted = 0
        for node, _ in oldest:
            del self.updates[node]
            self._evicted_count += 1
            evicted += 1
        
        if evicted > 0:
            self._overflow_count += 1
            if self._on_overflow:
                try:
                    self._on_overflow(evicted, self.max_updates)
                except Exception:
                    pass  # Don't let callback errors affect buffer operations
        
        return evicted
    
    def cleanup_stale(self) -> int:
        """
        Remove updates older than stale_age_seconds.
        
        Returns:
            Number of stale updates removed.
        """
        now = time.monotonic()
        cutoff = now - self.stale_age_seconds
        
        to_remove = []
        for node, update in self.updates.items():
            if update.timestamp < cutoff:
                to_remove.append(node)
        
        for node in to_remove:
            del self.updates[node]
            self._stale_removed_count += 1
        
        return len(to_remove)
    
    def cleanup_broadcast_complete(self) -> int:
        """
        Remove updates that have been broadcast enough times.
        
        Returns:
            Number of completed updates removed.
        """
        to_remove = []
        for node, update in self.updates.items():
            if not update.should_broadcast():
                to_remove.append(node)
        
        for node in to_remove:
            del self.updates[node]
        
        return len(to_remove)
    
    def cleanup(self) -> dict[str, int]:
        """
        Run all cleanup operations.
        
        Returns:
            Dict with cleanup stats.
        """
        stale = self.cleanup_stale()
        complete = self.cleanup_broadcast_complete()
        
        return {
            'stale_removed': stale,
            'complete_removed': complete,
            'pending_updates': len(self.updates),
        }
    
    def get_stats(self) -> dict[str, Any]:
        """Get buffer statistics for monitoring."""
        return {
            'pending_updates': len(self.updates),
            'total_evicted': self._evicted_count,
            'total_stale_removed': self._stale_removed_count,
            'size_limited_count': self._size_limited_count,
            'oversized_updates': self._oversized_updates_count,
            'overflow_events': self._overflow_count,
            'max_piggyback_size': self.max_piggyback_size,
            'max_updates': self.max_updates,
        }

