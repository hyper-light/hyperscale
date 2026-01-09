"""
Peer Health Awareness for SWIM Protocol (Phase 6.2).

Tracks peer health state received via health gossip and provides recommendations
for adapting SWIM behavior based on peer load. This enables the cluster to
"go easy" on overloaded nodes.

Key behaviors when a peer is overloaded:
1. Extend probe timeout (similar to LHM but based on peer state)
2. Prefer other peers for indirect probes
3. Reduce gossip piggyback load to that peer
4. Skip low-priority state updates to that peer

This integrates with:
- HealthGossipBuffer: Receives peer health updates via callback
- LocalHealthMultiplier: Combines local and peer health for timeouts
- IndirectProbeManager: Avoids overloaded peers as proxies
- ProbeScheduler: May reorder probing to prefer healthy peers
"""

import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Callable

from hyperscale.distributed_rewrite.health.tracker import HealthPiggyback


class PeerLoadLevel(IntEnum):
    """
    Peer load level classification for behavior adaptation.

    Higher values indicate more load - more accommodation needed.
    """
    UNKNOWN = 0   # No health info yet (treat as healthy)
    HEALTHY = 1   # Normal operation
    BUSY = 2      # Slightly elevated load
    STRESSED = 3  # Significant load - reduce traffic
    OVERLOADED = 4  # Critically loaded - minimal traffic only


# Map overload_state string to PeerLoadLevel
_OVERLOAD_STATE_TO_LEVEL: dict[str, PeerLoadLevel] = {
    "healthy": PeerLoadLevel.HEALTHY,
    "busy": PeerLoadLevel.BUSY,
    "stressed": PeerLoadLevel.STRESSED,
    "overloaded": PeerLoadLevel.OVERLOADED,
}


@dataclass(slots=True)
class PeerHealthInfo:
    """
    Cached health information for a single peer.

    Used to make adaptation decisions without requiring
    full HealthPiggyback lookups.
    """
    node_id: str
    load_level: PeerLoadLevel
    accepting_work: bool
    capacity: int
    throughput: float
    expected_throughput: float
    last_update: float

    @property
    def is_overloaded(self) -> bool:
        """Check if peer is in overloaded state."""
        return self.load_level >= PeerLoadLevel.OVERLOADED

    @property
    def is_stressed(self) -> bool:
        """Check if peer is stressed or worse."""
        return self.load_level >= PeerLoadLevel.STRESSED

    @property
    def is_healthy(self) -> bool:
        """Check if peer is healthy."""
        return self.load_level <= PeerLoadLevel.HEALTHY

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Check if this info is stale."""
        return (time.monotonic() - self.last_update) > max_age_seconds

    @classmethod
    def from_piggyback(cls, piggyback: HealthPiggyback) -> "PeerHealthInfo":
        """Create PeerHealthInfo from HealthPiggyback."""
        load_level = _OVERLOAD_STATE_TO_LEVEL.get(
            piggyback.overload_state,
            PeerLoadLevel.UNKNOWN,
        )

        return cls(
            node_id=piggyback.node_id,
            load_level=load_level,
            accepting_work=piggyback.accepting_work,
            capacity=piggyback.capacity,
            throughput=piggyback.throughput,
            expected_throughput=piggyback.expected_throughput,
            last_update=time.monotonic(),
        )


@dataclass(slots=True)
class PeerHealthAwarenessConfig:
    """Configuration for peer health awareness."""

    # Timeout multipliers based on peer load
    # Applied on top of base probe timeout
    timeout_multiplier_busy: float = 1.25  # 25% longer for busy peers
    timeout_multiplier_stressed: float = 1.75  # 75% longer for stressed peers
    timeout_multiplier_overloaded: float = 2.5  # 150% longer for overloaded peers

    # Staleness threshold for peer health info
    stale_threshold_seconds: float = 30.0

    # Maximum peers to track (prevent memory growth)
    max_tracked_peers: int = 1000

    # Enable behavior adaptations
    enable_timeout_adaptation: bool = True
    enable_proxy_avoidance: bool = True
    enable_gossip_reduction: bool = True


@dataclass(slots=True)
class PeerHealthAwareness:
    """
    Tracks peer health state and provides SWIM behavior recommendations.

    This class is the central point for peer-load-aware behavior adaptation.
    It receives health updates from HealthGossipBuffer and provides methods
    for other SWIM components to query peer status.

    Usage:
        awareness = PeerHealthAwareness()

        # Connect to health gossip
        health_gossip_buffer.set_health_update_callback(awareness.on_health_update)

        # Query for behavior adaptation
        timeout = awareness.get_probe_timeout("peer-1", base_timeout=1.0)
        should_use = awareness.should_use_as_proxy("peer-1")
    """
    config: PeerHealthAwarenessConfig = field(default_factory=PeerHealthAwarenessConfig)

    # Tracked peer health info
    _peers: dict[str, PeerHealthInfo] = field(default_factory=dict)

    # Statistics
    _total_updates: int = 0
    _overloaded_updates: int = 0
    _stale_removals: int = 0

    # Callbacks for significant state changes
    _on_peer_overloaded: Callable[[str], None] | None = None
    _on_peer_recovered: Callable[[str], None] | None = None

    def set_overload_callback(
        self,
        on_overloaded: Callable[[str], None] | None = None,
        on_recovered: Callable[[str], None] | None = None,
    ) -> None:
        """
        Set callbacks for peer overload state changes.

        Args:
            on_overloaded: Called when a peer enters overloaded state
            on_recovered: Called when a peer exits overloaded/stressed state
        """
        self._on_peer_overloaded = on_overloaded
        self._on_peer_recovered = on_recovered

    def on_health_update(self, health: HealthPiggyback) -> None:
        """
        Process health update from HealthGossipBuffer.

        This should be connected as the callback for HealthGossipBuffer.

        Args:
            health: Health piggyback from peer
        """
        self._total_updates += 1

        # Get previous state for change detection
        previous = self._peers.get(health.node_id)
        previous_overloaded = previous.is_stressed if previous else False

        # Create new peer info
        peer_info = PeerHealthInfo.from_piggyback(health)

        # Enforce capacity limit
        if health.node_id not in self._peers and len(self._peers) >= self.config.max_tracked_peers:
            self._evict_oldest_peer()

        # Store update
        self._peers[health.node_id] = peer_info

        # Track overloaded updates
        if peer_info.is_stressed:
            self._overloaded_updates += 1

        # Invoke callbacks for state transitions
        if peer_info.is_stressed and not previous_overloaded:
            if self._on_peer_overloaded:
                try:
                    self._on_peer_overloaded(health.node_id)
                except Exception:
                    pass  # Don't let callback errors affect processing
        elif not peer_info.is_stressed and previous_overloaded:
            if self._on_peer_recovered:
                try:
                    self._on_peer_recovered(health.node_id)
                except Exception:
                    pass

    def get_peer_info(self, node_id: str) -> PeerHealthInfo | None:
        """
        Get cached health info for a peer.

        Returns None if peer is not tracked or info is stale.
        """
        peer_info = self._peers.get(node_id)
        if peer_info and peer_info.is_stale(self.config.stale_threshold_seconds):
            # Remove stale info
            del self._peers[node_id]
            self._stale_removals += 1
            return None
        return peer_info

    def get_load_level(self, node_id: str) -> PeerLoadLevel:
        """
        Get load level for a peer.

        Returns UNKNOWN if peer is not tracked.
        """
        peer_info = self.get_peer_info(node_id)
        if peer_info:
            return peer_info.load_level
        return PeerLoadLevel.UNKNOWN

    def get_probe_timeout(self, node_id: str, base_timeout: float) -> float:
        """
        Get adapted probe timeout for a peer based on their load.

        When peers are overloaded, we give them more time to respond
        to avoid false failure detection.

        Args:
            node_id: Peer node ID
            base_timeout: Base probe timeout in seconds

        Returns:
            Adapted timeout (>= base_timeout)
        """
        if not self.config.enable_timeout_adaptation:
            return base_timeout

        peer_info = self.get_peer_info(node_id)
        if not peer_info:
            return base_timeout

        # Apply multiplier based on load level
        if peer_info.load_level == PeerLoadLevel.OVERLOADED:
            return base_timeout * self.config.timeout_multiplier_overloaded
        elif peer_info.load_level == PeerLoadLevel.STRESSED:
            return base_timeout * self.config.timeout_multiplier_stressed
        elif peer_info.load_level == PeerLoadLevel.BUSY:
            return base_timeout * self.config.timeout_multiplier_busy

        return base_timeout

    def should_use_as_proxy(self, node_id: str) -> bool:
        """
        Check if a peer should be used as an indirect probe proxy.

        We avoid using stressed/overloaded peers as proxies because:
        1. They may be slow to respond, causing indirect probe timeouts
        2. We want to reduce load on already-stressed nodes

        Args:
            node_id: Peer node ID to check

        Returns:
            True if peer can be used as proxy
        """
        if not self.config.enable_proxy_avoidance:
            return True

        peer_info = self.get_peer_info(node_id)
        if not peer_info:
            return True  # Unknown peers are OK to use

        # Don't use stressed or overloaded peers as proxies
        return not peer_info.is_stressed

    def get_gossip_reduction_factor(self, node_id: str) -> float:
        """
        Get gossip reduction factor for a peer.

        When peers are overloaded, we reduce the amount of gossip
        we piggyback on messages to them.

        Args:
            node_id: Peer node ID

        Returns:
            Factor from 0.0 (no gossip) to 1.0 (full gossip)
        """
        if not self.config.enable_gossip_reduction:
            return 1.0

        peer_info = self.get_peer_info(node_id)
        if not peer_info:
            return 1.0

        # Reduce gossip based on load
        if peer_info.load_level == PeerLoadLevel.OVERLOADED:
            return 0.25  # Only 25% of normal gossip
        elif peer_info.load_level == PeerLoadLevel.STRESSED:
            return 0.50  # Only 50% of normal gossip
        elif peer_info.load_level == PeerLoadLevel.BUSY:
            return 0.75  # 75% of normal gossip

        return 1.0

    def get_healthy_peers(self) -> list[str]:
        """Get list of peers in healthy state."""
        return [
            node_id
            for node_id, peer_info in self._peers.items()
            if peer_info.is_healthy and not peer_info.is_stale(self.config.stale_threshold_seconds)
        ]

    def get_stressed_peers(self) -> list[str]:
        """Get list of peers in stressed or overloaded state."""
        return [
            node_id
            for node_id, peer_info in self._peers.items()
            if peer_info.is_stressed and not peer_info.is_stale(self.config.stale_threshold_seconds)
        ]

    def get_overloaded_peers(self) -> list[str]:
        """Get list of peers in overloaded state."""
        return [
            node_id
            for node_id, peer_info in self._peers.items()
            if peer_info.is_overloaded and not peer_info.is_stale(self.config.stale_threshold_seconds)
        ]

    def get_peers_not_accepting_work(self) -> list[str]:
        """Get list of peers not accepting work."""
        return [
            node_id
            for node_id, peer_info in self._peers.items()
            if not peer_info.accepting_work and not peer_info.is_stale(self.config.stale_threshold_seconds)
        ]

    def filter_proxy_candidates(self, candidates: list[str]) -> list[str]:
        """
        Filter a list of potential proxies to exclude overloaded ones.

        Args:
            candidates: List of node IDs to filter

        Returns:
            Filtered list excluding stressed/overloaded peers
        """
        if not self.config.enable_proxy_avoidance:
            return candidates

        return [
            node_id
            for node_id in candidates
            if self.should_use_as_proxy(node_id)
        ]

    def rank_by_health(self, node_ids: list[str]) -> list[str]:
        """
        Rank nodes by health (healthiest first).

        Useful for preferring healthy nodes in proxy selection
        or probe ordering.

        Args:
            node_ids: List of node IDs to rank

        Returns:
            Sorted list with healthiest first
        """
        def health_sort_key(node_id: str) -> int:
            peer_info = self.get_peer_info(node_id)
            if not peer_info:
                return 0  # Unknown comes first (same as healthy)
            return peer_info.load_level

        return sorted(node_ids, key=health_sort_key)

    def remove_peer(self, node_id: str) -> bool:
        """
        Remove a peer from tracking.

        Called when a peer is declared dead and removed from membership.

        Returns:
            True if peer was tracked
        """
        if node_id in self._peers:
            del self._peers[node_id]
            return True
        return False

    def cleanup_stale(self) -> int:
        """
        Remove stale peer entries.

        Returns:
            Number of entries removed
        """
        stale_nodes = [
            node_id
            for node_id, peer_info in self._peers.items()
            if peer_info.is_stale(self.config.stale_threshold_seconds)
        ]

        for node_id in stale_nodes:
            del self._peers[node_id]
            self._stale_removals += 1

        return len(stale_nodes)

    def clear(self) -> None:
        """Clear all tracked peers."""
        self._peers.clear()

    def _evict_oldest_peer(self) -> None:
        """Evict oldest peer to make room for new one."""
        if not self._peers:
            return

        # Find peer with oldest update
        oldest_node_id = min(
            self._peers.keys(),
            key=lambda node_id: self._peers[node_id].last_update,
        )
        del self._peers[oldest_node_id]

    def get_stats(self) -> dict[str, int | float]:
        """Get statistics for monitoring."""
        overloaded_count = len(self.get_overloaded_peers())
        stressed_count = len(self.get_stressed_peers())

        return {
            "tracked_peers": len(self._peers),
            "total_updates": self._total_updates,
            "overloaded_updates": self._overloaded_updates,
            "stale_removals": self._stale_removals,
            "current_overloaded": overloaded_count,
            "current_stressed": stressed_count,
            "max_tracked_peers": self.config.max_tracked_peers,
        }
