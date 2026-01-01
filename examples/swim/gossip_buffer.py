"""
Gossip buffer for SWIM membership update dissemination.
"""

import math
import time
from dataclasses import dataclass, field

from .types import UpdateType
from .piggyback_update import PiggybackUpdate


@dataclass
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
    
    # Stats
    _evicted_count: int = 0
    _stale_removed_count: int = 0
    
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
        
        Returns up to max_count updates, prioritizing those with
        the lowest broadcast count (least disseminated).
        """
        # Sort by broadcast count (ascending) to prioritize new updates
        candidates = sorted(
            [u for u in self.updates.values() if u.should_broadcast()],
            key=lambda u: u.broadcast_count,
        )
        
        return candidates[:max_count]
    
    def mark_broadcasts(self, updates: list[PiggybackUpdate]) -> None:
        """Mark updates as having been broadcast and remove if done."""
        for update in updates:
            if update.node in self.updates:
                self.updates[update.node].mark_broadcast()
                if not self.updates[update.node].should_broadcast():
                    del self.updates[update.node]
    
    def encode_piggyback(self, max_count: int = 5) -> bytes:
        """
        Get piggybacked updates as bytes to append to a message.
        Format: |update1|update2|update3
        """
        updates = self.get_updates_to_piggyback(max_count)
        if not updates:
            return b''
        
        self.mark_broadcasts(updates)
        return b'|' + b'|'.join(u.to_bytes() for u in updates)
    
    @staticmethod
    def decode_piggyback(data: bytes) -> list[PiggybackUpdate]:
        """
        Decode piggybacked updates from message suffix.
        """
        if not data or data[0:1] != b'|':
            return []
        
        updates = []
        parts = data[1:].split(b'|')
        for part in parts:
            if part:
                update = PiggybackUpdate.from_bytes(part)
                if update:
                    updates.append(update)
        return updates
    
    def clear(self) -> None:
        """Clear all pending updates."""
        self.updates.clear()
    
    def _evict_oldest(self, count: int = 10) -> int:
        """
        Evict the oldest updates.
        
        Returns:
            Number of updates evicted.
        """
        if not self.updates:
            return 0
        
        # Sort by timestamp (oldest first)
        sorted_updates = sorted(
            self.updates.items(),
            key=lambda x: x[1].timestamp,
        )
        
        evicted = 0
        for node, _ in sorted_updates[:count]:
            del self.updates[node]
            self._evicted_count += 1
            evicted += 1
        
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
    
    def get_stats(self) -> dict[str, int]:
        """Get buffer statistics for monitoring."""
        return {
            'pending_updates': len(self.updates),
            'total_evicted': self._evicted_count,
            'total_stale_removed': self._stale_removed_count,
        }

