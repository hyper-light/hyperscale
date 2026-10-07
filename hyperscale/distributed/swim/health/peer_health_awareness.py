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

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from enum import IntEnum
from itertools import compress
from operator import attrgetter, methodcaller
from types import MappingProxyType
from typing import Callable
from hyperscale.distributed.health.tracker import HealthPiggyback
from hyperscale.distributed.runtime import Clock, RealClock

from .peer_health_info import _DEFAULT_CLOCK
from .peer_load_level import _OVERLOAD_STATE_TO_LEVEL
from .peer_health_awareness_config import PeerHealthAwarenessConfig
from .peer_health_info import PeerHealthInfo
from .peer_load_level import PeerLoadLevel


def _unadapted_timeout_multiplier(config: PeerHealthAwarenessConfig) -> float:
    """The multiplier for a HEALTHY or UNKNOWN peer: no timeout adaptation."""
    return 1.0


def _not_accepting_work(peer_info: PeerHealthInfo) -> bool:
    """Whether the peer reported it is not accepting work."""
    return not peer_info.accepting_work


# Load levels that stretch timeouts, each read from its configured multiplier.
_LOAD_LEVEL_TIMEOUT_MULTIPLIER: MappingProxyType[PeerLoadLevel, Callable[[PeerHealthAwarenessConfig], float]] = (
    MappingProxyType(
        {
            PeerLoadLevel.OVERLOADED: attrgetter("timeout_multiplier_overloaded"),
            PeerLoadLevel.STRESSED: attrgetter("timeout_multiplier_stressed"),
            PeerLoadLevel.BUSY: attrgetter("timeout_multiplier_busy"),
        }
    )
)

# Share of normal gossip piggybacked to a peer at each reduced load level.
_LOAD_LEVEL_GOSSIP_REDUCTION: MappingProxyType[PeerLoadLevel, float] = MappingProxyType(
    {
        PeerLoadLevel.OVERLOADED: 0.25,  # Only 25% of normal gossip
        PeerLoadLevel.STRESSED: 0.50,  # Only 50% of normal gossip
        PeerLoadLevel.BUSY: 0.75,  # 75% of normal gossip
    }
)


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

        # Invoke callbacks for state transitions. The update is stored: a
        # failing callback raises to the caller rather than vanishing.
        if peer_info.is_stressed and not previous_overloaded:
            if self._on_peer_overloaded:
                self._on_peer_overloaded(health.node_id)
        elif not peer_info.is_stressed and previous_overloaded:
            if self._on_peer_recovered:
                self._on_peer_recovered(health.node_id)

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

    def get_load_multiplier(self, node_id: str) -> float:
        """
        Return the timeout multiplier driven by a peer's reported load.

        This is the building block that ``get_probe_timeout`` (probe
        path) and the suspicion-timer composition in
        ``HierarchicalFailureDetector.suspect_global`` (Phase C) both
        consume. Returning a multiplier instead of a fully-applied
        timeout lets callers compose it multiplicatively with other
        adjustment factors (self-LHM, Vivaldi quality) per AD-35:186
        without coupling to any single base-timeout value.

        Returns 1.0 when:
        * timeout adaptation is disabled
        * the peer is unknown to PeerHealthAwareness (no gossip yet, or
          info has gone stale and was evicted)
        * the peer is reported HEALTHY or UNKNOWN

        Otherwise returns the load-level multiplier configured in
        ``PeerHealthAwarenessConfig`` (BUSY 1.25×, STRESSED 1.75×,
        OVERLOADED 2.5× by default).
        """
        if not self.config.enable_timeout_adaptation:
            return 1.0

        peer_info = self.get_peer_info(node_id)
        if not peer_info:
            return 1.0

        return _LOAD_LEVEL_TIMEOUT_MULTIPLIER.get(
            peer_info.load_level, _unadapted_timeout_multiplier
        )(self.config)

    def get_probe_timeout(self, node_id: str, base_timeout: float) -> float:
        """
        Get adapted probe timeout for a peer based on their load.

        Thin wrapper over :meth:`get_load_multiplier` so existing
        probe-path callers preserve their current API.

        Args:
            node_id: Peer node ID
            base_timeout: Base probe timeout in seconds

        Returns:
            Adapted timeout (>= base_timeout)
        """
        return base_timeout * self.get_load_multiplier(node_id)

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
        return _LOAD_LEVEL_GOSSIP_REDUCTION.get(peer_info.load_level, 1.0)

    def get_healthy_peers(self) -> list[str]:
        """Get list of peers in healthy state."""
        return self._fresh_peers_matching(attrgetter("is_healthy"))

    def get_stressed_peers(self) -> list[str]:
        """Get list of peers in stressed or overloaded state."""
        return self._fresh_peers_matching(attrgetter("is_stressed"))

    def get_overloaded_peers(self) -> list[str]:
        """Get list of peers in overloaded state."""
        return self._fresh_peers_matching(attrgetter("is_overloaded"))

    def get_peers_not_accepting_work(self) -> list[str]:
        """Get list of peers not accepting work."""
        return self._fresh_peers_matching(_not_accepting_work)

    def _fresh_peers_matching(self, predicate: Callable[[PeerHealthInfo], bool]) -> list[str]:
        """Node ids of non-stale peers whose info satisfies ``predicate``."""
        return [
            node_id
            for node_id, peer_info in self._peers.items()
            if self._is_fresh_match(peer_info, predicate)
        ]

    def _is_fresh_match(self, peer_info: PeerHealthInfo, predicate: Callable[[PeerHealthInfo], bool]) -> bool:
        """Whether ``peer_info`` satisfies ``predicate`` and is not stale (staleness checked only on a match)."""
        return predicate(peer_info) and not peer_info.is_stale(self.config.stale_threshold_seconds)

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

        return list(filter(self.should_use_as_proxy, candidates))

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
        # Keys and values iterate in the same order, so compress selects the stale peers.
        stale_nodes = list(
            compress(
                self._peers.keys(),
                map(methodcaller("is_stale", self.config.stale_threshold_seconds), self._peers.values()),
            )
        )

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

_REHOMED = (
    PeerLoadLevel,
    PeerHealthInfo,
    PeerHealthAwarenessConfig,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
