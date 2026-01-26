"""
Health gossip buffer for SWIM health state dissemination (Phase 6.1).

Provides O(log n) dissemination of health state alongside membership updates.
This enables faster propagation of overload signals, capacity changes, and
health degradation compared to heartbeat-only propagation.

Key differences from membership gossip:
- Updates are keyed by node_id (string) not (host, port) tuple
- Updates have TTL based on staleness, not broadcast count
- Updates are prioritized by overload_state severity
- Size is more aggressively bounded since health is "best effort"

This integrates with the Lifeguard LHM (Local Health Multiplier) by:
- Receiving health updates from peers to inform probe timeout calculations
- Propagating local health state so peers can adjust their behavior
"""

import heapq
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Callable

from hyperscale.distributed.health.tracker import HealthPiggyback


class OverloadSeverity(IntEnum):
    """
    Severity ordering for health state prioritization.

    Higher severity = propagate faster (lower broadcast count threshold).
    This ensures overloaded nodes are known quickly across the cluster.
    """
    HEALTHY = 0
    BUSY = 1
    STRESSED = 2
    OVERLOADED = 3
    UNKNOWN = 0  # Treat unknown as healthy (don't prioritize)


# Pre-encode common strings for fast serialization
_OVERLOAD_STATE_TO_SEVERITY: dict[str, OverloadSeverity] = {
    "healthy": OverloadSeverity.HEALTHY,
    "busy": OverloadSeverity.BUSY,
    "stressed": OverloadSeverity.STRESSED,
    "overloaded": OverloadSeverity.OVERLOADED,
}

# Maximum size for health piggyback section (leaves room for membership gossip)
MAX_HEALTH_PIGGYBACK_SIZE = 600  # bytes


@dataclass(slots=True)
class HealthGossipEntry:
    """
    A health update entry in the gossip buffer.

    Uses __slots__ for memory efficiency since many instances may exist.
    """
    health: HealthPiggyback
    timestamp: float
    broadcast_count: int = 0
    max_broadcasts: int = 5  # Fewer than membership (health is less critical)

    @property
    def severity(self) -> OverloadSeverity:
        """Get severity for prioritization."""
        return _OVERLOAD_STATE_TO_SEVERITY.get(
            self.health.overload_state,
            OverloadSeverity.UNKNOWN,
        )

    def should_broadcast(self) -> bool:
        """Check if this entry should still be broadcast."""
        return self.broadcast_count < self.max_broadcasts

    def mark_broadcast(self) -> None:
        """Mark that this entry was broadcast."""
        self.broadcast_count += 1

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Check if this entry is stale based on its own timestamp."""
        return self.health.is_stale(max_age_seconds)

    def to_bytes(self) -> bytes:
        """
        Serialize entry for transmission.

        Format: node_id|node_type|overload_state|accepting_work|capacity|throughput|expected|timestamp

        Uses compact format to maximize entries per message.
        Field separator: '|' (pipe)
        """
        health = self.health
        # Convert overload_state enum to its string name
        overload_state_str = (
            health.overload_state.name
            if hasattr(health.overload_state, 'name')
            else str(health.overload_state)
        )
        parts = [
            health.node_id,
            health.node_type,
            overload_state_str,
            "1" if health.accepting_work else "0",
            str(health.capacity),
            f"{health.throughput:.2f}",
            f"{health.expected_throughput:.2f}",
            f"{health.timestamp:.2f}",
        ]
        return "|".join(parts).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "HealthGossipEntry | None":
        """
        Deserialize entry from bytes.

        Returns None if data is invalid or malformed.
        """
        try:
            text = data.decode()
            parts = text.split("|", maxsplit=7)
            if len(parts) < 8:
                return None

            node_id = parts[0]
            node_type = parts[1]
            overload_state = parts[2]
            accepting_work = parts[3] == "1"
            capacity = int(parts[4])
            throughput = float(parts[5])
            expected_throughput = float(parts[6])
            timestamp = float(parts[7])

            health = HealthPiggyback(
                node_id=node_id,
                node_type=node_type,
                overload_state=overload_state,
                accepting_work=accepting_work,
                capacity=capacity,
                throughput=throughput,
                expected_throughput=expected_throughput,
                timestamp=timestamp,
            )

            return cls(
                health=health,
                timestamp=time.monotonic(),
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None


@dataclass(slots=True)
class HealthGossipBufferConfig:
    """Configuration for HealthGossipBuffer."""

    # Maximum entries in the buffer
    max_entries: int = 500

    # Staleness threshold - entries older than this are removed
    stale_age_seconds: float = 30.0

    # Maximum bytes for health piggyback data
    max_piggyback_size: int = MAX_HEALTH_PIGGYBACK_SIZE

    # Broadcast multiplier (lower than membership since health is best-effort)
    broadcast_multiplier: int = 2

    # Minimum broadcasts for healthy nodes (they're less urgent)
    min_broadcasts_healthy: int = 3

    # Minimum broadcasts for overloaded nodes (propagate faster)
    min_broadcasts_overloaded: int = 8


@dataclass(slots=True)
class HealthGossipBuffer:
    """
    Buffer for health state updates to be piggybacked on SWIM messages.

    Maintains a collection of health updates keyed by node_id, with
    prioritization based on overload severity. More severe states
    (overloaded, stressed) are propagated faster than healthy states.

    This complements heartbeat-based health propagation by:
    1. Propagating health on ALL SWIM messages, not just ACKs
    2. Using O(log n) gossip dissemination
    3. Prioritizing critical states for faster propagation

    Resource limits:
    - max_entries: Maximum health entries before eviction
    - stale_age: Remove entries older than this
    - max_piggyback_size: Maximum bytes per message
    """
    config: HealthGossipBufferConfig = field(default_factory=HealthGossipBufferConfig)

    # Entries keyed by node_id
    _entries: dict[str, HealthGossipEntry] = field(default_factory=dict)

    # Statistics
    _total_updates: int = 0
    _evicted_count: int = 0
    _stale_removed_count: int = 0
    _size_limited_count: int = 0
    _malformed_count: int = 0

    # Callback for when we receive health updates
    _on_health_update: Callable[[HealthPiggyback], None] | None = None

    def set_health_update_callback(
        self,
        callback: Callable[[HealthPiggyback], None],
    ) -> None:
        """
        Set callback to be invoked when health updates are received.

        This allows integration with:
        - NodeHealthTracker for routing decisions
        - LocalHealthMultiplier for timeout adjustments
        - Load shedding for traffic reduction
        """
        self._on_health_update = callback

    def update_local_health(self, health: HealthPiggyback) -> None:
        """
        Update local node's health state for propagation.

        This should be called periodically (e.g., every probe cycle)
        to ensure our health state is propagated to peers.

        Args:
            health: Current health state of this node
        """
        self._add_or_update_entry(health)

    def process_received_health(self, health: HealthPiggyback) -> bool:
        """
        Process health state received from another node.

        Returns True if the update was newer and accepted.

        Args:
            health: Health state from remote node
        """
        self._total_updates += 1

        # Check if we have an existing entry
        existing = self._entries.get(health.node_id)

        # Only accept if newer
        if existing and existing.health.timestamp >= health.timestamp:
            return False

        # Add/update entry
        self._add_or_update_entry(health)

        # Invoke callback if set
        if self._on_health_update:
            try:
                self._on_health_update(health)
            except Exception:
                pass  # Don't let callback errors affect gossip

        return True

    def _add_or_update_entry(self, health: HealthPiggyback) -> None:
        """Add or update a health entry."""
        # Enforce capacity limit
        if health.node_id not in self._entries:
            if len(self._entries) >= self.config.max_entries:
                # Only evict enough to make room (evict 10% or at least 1)
                evict_count = max(1, self.config.max_entries // 10)
                self._evict_least_important(count=evict_count)

        # Calculate max broadcasts based on severity
        severity = _OVERLOAD_STATE_TO_SEVERITY.get(
            health.overload_state,
            OverloadSeverity.HEALTHY,
        )

        if severity >= OverloadSeverity.STRESSED:
            max_broadcasts = self.config.min_broadcasts_overloaded
        else:
            max_broadcasts = self.config.min_broadcasts_healthy

        # Preserve broadcast count if updating existing entry
        existing = self._entries.get(health.node_id)
        broadcast_count = 0
        if existing:
            # If state changed significantly, reset broadcast count
            if existing.health.overload_state != health.overload_state:
                broadcast_count = 0
            else:
                broadcast_count = existing.broadcast_count

        self._entries[health.node_id] = HealthGossipEntry(
            health=health,
            timestamp=time.monotonic(),
            broadcast_count=broadcast_count,
            max_broadcasts=max_broadcasts,
        )

    def get_entries_to_piggyback(self, max_count: int = 10) -> list[HealthGossipEntry]:
        """
        Get entries to piggyback on the next message.

        Prioritizes:
        1. Entries with higher severity (overloaded > stressed > busy > healthy)
        2. Entries with lower broadcast count (less disseminated)

        Args:
            max_count: Maximum entries to return (bounded to 1-50)

        Returns:
            List of entries to piggyback, prioritized by importance
        """
        max_count = max(1, min(max_count, 50))

        # Filter to broadcastable entries
        candidates = [e for e in self._entries.values() if e.should_broadcast()]

        if not candidates:
            return []

        # Sort by: severity (descending), then broadcast_count (ascending)
        # This ensures overloaded nodes are broadcast first and most often
        def priority_key(entry: HealthGossipEntry) -> tuple[int, int]:
            return (-entry.severity, entry.broadcast_count)

        # Use nsmallest with inverted severity for proper ordering
        return heapq.nsmallest(max_count, candidates, key=priority_key)

    def mark_broadcasts(self, entries: list[HealthGossipEntry]) -> None:
        """Mark entries as having been broadcast."""
        for entry in entries:
            if entry.health.node_id in self._entries:
                self._entries[entry.health.node_id].mark_broadcast()

    # Health piggyback marker - consistent with #|s (state) and #|m (membership)
    HEALTH_SEPARATOR = b"#|h"
    # Entry separator within health piggyback (safe since we strip #|h block first)
    ENTRY_SEPARATOR = b";"

    def encode_piggyback(
        self,
        max_count: int = 10,
        max_size: int | None = None,
    ) -> bytes:
        """
        Get piggybacked health updates as bytes.

        Format: #|hentry1;entry2;entry3
        - Starts with '#|h' marker (consistent with #|s state, #|m membership)
        - Entries separated by ';'

        Args:
            max_count: Maximum entries to include
            max_size: Maximum bytes (defaults to config value)

        Returns:
            Encoded health piggyback data
        """
        if max_size is None:
            max_size = self.config.max_piggyback_size

        entries = self.get_entries_to_piggyback(max_count)
        if not entries:
            return b""

        # Build result respecting size limit
        result_parts: list[bytes] = []
        total_size = 3  # '#|h' prefix
        included_entries: list[HealthGossipEntry] = []

        for entry in entries:
            encoded = entry.to_bytes()
            entry_size = len(encoded) + 1  # +1 for ';' separator

            if total_size + entry_size > max_size:
                self._size_limited_count += 1
                break

            result_parts.append(encoded)
            total_size += entry_size
            included_entries.append(entry)

        if not result_parts:
            return b""

        self.mark_broadcasts(included_entries)
        return self.HEALTH_SEPARATOR + self.ENTRY_SEPARATOR.join(result_parts)

    @classmethod
    def is_health_piggyback(cls, data: bytes) -> bool:
        """Check if data contains health piggyback."""
        return data.startswith(cls.HEALTH_SEPARATOR)

    def decode_and_process_piggyback(self, data: bytes) -> int:
        """
        Decode and process health piggyback data.

        Args:
            data: Raw piggyback data starting with '#|h'

        Returns:
            Number of health updates processed
        """
        if not self.is_health_piggyback(data):
            return 0

        # Remove '#|h' prefix
        content = data[3:]
        if not content:
            return 0

        processed = 0
        parts = content.split(self.ENTRY_SEPARATOR)

        for part in parts:
            if not part:
                continue

            entry = HealthGossipEntry.from_bytes(part)
            if entry:
                if self.process_received_health(entry.health):
                    processed += 1
            else:
                self._malformed_count += 1

        return processed

    def get_health(self, node_id: str) -> HealthPiggyback | None:
        """Get current health state for a node."""
        entry = self._entries.get(node_id)
        if entry:
            return entry.health
        return None

    def get_overloaded_nodes(self) -> list[str]:
        """Get list of nodes currently in overloaded state."""
        return [
            node_id
            for node_id, entry in self._entries.items()
            if entry.health.overload_state == "overloaded"
        ]

    def get_stressed_nodes(self) -> list[str]:
        """Get list of nodes currently in stressed or overloaded state."""
        return [
            node_id
            for node_id, entry in self._entries.items()
            if entry.health.overload_state in ("stressed", "overloaded")
        ]

    def get_nodes_not_accepting_work(self) -> list[str]:
        """Get list of nodes not accepting work."""
        return [
            node_id
            for node_id, entry in self._entries.items()
            if not entry.health.accepting_work
        ]

    def _evict_least_important(self, count: int = 10) -> int:
        """
        Evict least important entries.

        Priority for eviction (evict first):
        1. Healthy nodes (keep overloaded info longer)
        2. Older entries
        3. Higher broadcast count (already disseminated)

        Returns:
            Number of entries evicted
        """
        if not self._entries:
            return 0

        # Sort by eviction priority: healthy first, then oldest, then most broadcast
        def eviction_key(item: tuple[str, HealthGossipEntry]) -> tuple[int, float, int]:
            _, entry = item
            return (
                entry.severity,  # Lower severity = evict first
                entry.timestamp,  # Older = evict first
                -entry.broadcast_count,  # More broadcasts = evict first
            )

        to_evict = heapq.nsmallest(count, self._entries.items(), key=eviction_key)

        evicted = 0
        for node_id, _ in to_evict:
            del self._entries[node_id]
            self._evicted_count += 1
            evicted += 1

        return evicted

    def cleanup_stale(self) -> int:
        """
        Remove entries that are stale.

        Returns:
            Number of stale entries removed
        """
        stale_nodes = [
            node_id
            for node_id, entry in self._entries.items()
            if entry.is_stale(self.config.stale_age_seconds)
        ]

        for node_id in stale_nodes:
            del self._entries[node_id]
            self._stale_removed_count += 1

        return len(stale_nodes)

    def cleanup_broadcast_complete(self) -> int:
        """
        Remove entries that have been broadcast enough times.

        Returns:
            Number of completed entries removed
        """
        complete_nodes = [
            node_id
            for node_id, entry in self._entries.items()
            if not entry.should_broadcast()
        ]

        for node_id in complete_nodes:
            del self._entries[node_id]

        return len(complete_nodes)

    def cleanup(self) -> dict[str, int]:
        """
        Run all cleanup operations.

        Returns:
            Dict with cleanup statistics
        """
        stale = self.cleanup_stale()
        complete = self.cleanup_broadcast_complete()

        return {
            "stale_removed": stale,
            "complete_removed": complete,
            "pending_entries": len(self._entries),
        }

    def clear(self) -> None:
        """Clear all entries."""
        self._entries.clear()

    def remove_node(self, node_id: str) -> bool:
        """
        Remove health entry for a specific node.

        Returns:
            True if entry was removed
        """
        if node_id in self._entries:
            del self._entries[node_id]
            return True
        return False

    def get_stats(self) -> dict[str, int | float]:
        """Get buffer statistics for monitoring."""
        overloaded_count = len(self.get_overloaded_nodes())
        stressed_count = len(self.get_stressed_nodes())

        return {
            "pending_entries": len(self._entries),
            "total_updates": self._total_updates,
            "evicted_count": self._evicted_count,
            "stale_removed_count": self._stale_removed_count,
            "size_limited_count": self._size_limited_count,
            "malformed_count": self._malformed_count,
            "overloaded_nodes": overloaded_count,
            "stressed_nodes": stressed_count,
            "max_entries": self.config.max_entries,
            "max_piggyback_size": self.config.max_piggyback_size,
        }
