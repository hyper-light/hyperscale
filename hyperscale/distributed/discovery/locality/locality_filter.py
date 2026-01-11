"""
Locality-aware peer filtering.

Filters and sorts peers based on network topology proximity,
preferring same-DC, then same-region, then global peers.
"""

from dataclasses import dataclass, field
from typing import TypeVar, Callable

from hyperscale.distributed.discovery.models.locality_info import (
    LocalityInfo,
    LocalityTier,
)
from hyperscale.distributed.discovery.models.peer_info import PeerInfo


T = TypeVar("T")


@dataclass
class LocalityFilter:
    """
    Filter and sort peers by locality preference.

    Implements locality-aware peer selection as specified in AD-28:
    - SAME_DC (tier 0): Lowest latency, highest preference
    - SAME_REGION (tier 1): Medium latency, medium preference
    - GLOBAL (tier 2): Highest latency, fallback only

    Usage:
        filter = LocalityFilter(local_locality=my_locality)
        sorted_peers = filter.sort_by_locality(all_peers)
        same_dc_peers = filter.filter_same_dc(all_peers)
    """

    local_locality: LocalityInfo
    """The locality information for the local node."""

    prefer_same_dc: bool = True
    """If True, prefer same-DC peers over same-region."""

    global_fallback_enabled: bool = True
    """If True, allow global peers when no local peers available."""

    min_local_peers: int = 0
    """Minimum local peers before considering remote (0 = always consider remote)."""

    _tier_cache: dict[str, LocalityTier] = field(default_factory=dict, repr=False)
    """Cache of peer_id -> locality tier."""

    def get_tier(self, peer: PeerInfo) -> LocalityTier:
        """
        Get the locality tier for a peer.

        Uses caching to avoid repeated calculations.

        Args:
            peer: The peer to evaluate

        Returns:
            LocalityTier indicating preference level
        """
        # Check cache first
        cached = self._tier_cache.get(peer.peer_id)
        if cached is not None:
            return cached

        # Calculate tier using LocalityInfo's method
        tier = self.local_locality.get_tier_for_peer(
            peer_dc=peer.datacenter_id,
            peer_region=peer.region_id,
        )

        # Cache the result
        self._tier_cache[peer.peer_id] = tier
        return tier

    def sort_by_locality(self, peers: list[PeerInfo]) -> list[PeerInfo]:
        """
        Sort peers by locality preference (same-DC first, then region, then global).

        Args:
            peers: List of peers to sort

        Returns:
            New list sorted by locality tier (ascending = more preferred first)
        """
        return sorted(peers, key=lambda peer: self.get_tier(peer))

    def filter_same_dc(self, peers: list[PeerInfo]) -> list[PeerInfo]:
        """
        Filter to only same-datacenter peers.

        Args:
            peers: List of peers to filter

        Returns:
            Peers in the same datacenter
        """
        return [
            peer
            for peer in peers
            if self.get_tier(peer) == LocalityTier.SAME_DC
        ]

    def filter_same_region(self, peers: list[PeerInfo]) -> list[PeerInfo]:
        """
        Filter to same-region peers (including same-DC).

        Args:
            peers: List of peers to filter

        Returns:
            Peers in the same region (SAME_DC or SAME_REGION tier)
        """
        return [
            peer
            for peer in peers
            if self.get_tier(peer) in (LocalityTier.SAME_DC, LocalityTier.SAME_REGION)
        ]

    def filter_by_max_tier(
        self,
        peers: list[PeerInfo],
        max_tier: LocalityTier,
    ) -> list[PeerInfo]:
        """
        Filter peers up to a maximum locality tier.

        Args:
            peers: List of peers to filter
            max_tier: Maximum tier to include (inclusive)

        Returns:
            Peers with tier <= max_tier
        """
        return [peer for peer in peers if self.get_tier(peer) <= max_tier]

    def group_by_tier(
        self,
        peers: list[PeerInfo],
    ) -> dict[LocalityTier, list[PeerInfo]]:
        """
        Group peers by their locality tier.

        Args:
            peers: List of peers to group

        Returns:
            Dict mapping tier to list of peers in that tier
        """
        groups: dict[LocalityTier, list[PeerInfo]] = {
            LocalityTier.SAME_DC: [],
            LocalityTier.SAME_REGION: [],
            LocalityTier.GLOBAL: [],
        }

        for peer in peers:
            tier = self.get_tier(peer)
            groups[tier].append(peer)

        return groups

    def select_with_fallback(
        self,
        peers: list[PeerInfo],
        selector: Callable[[list[PeerInfo]], T | None],
    ) -> tuple[T | None, LocalityTier | None]:
        """
        Select from peers with locality-aware fallback.

        Tries same-DC first, then same-region, then global (if enabled).
        Returns the result and the tier it was selected from.

        Args:
            peers: List of peers to select from
            selector: Function to select from a list of peers (returns None if none suitable)

        Returns:
            Tuple of (selected result, tier) or (None, None) if no peer selected
        """
        groups = self.group_by_tier(peers)

        # Try same-DC first
        if self.prefer_same_dc and groups[LocalityTier.SAME_DC]:
            result = selector(groups[LocalityTier.SAME_DC])
            if result is not None:
                return (result, LocalityTier.SAME_DC)

        # Check minimum local peers threshold
        local_count = len(groups[LocalityTier.SAME_DC])
        if self.min_local_peers > 0 and local_count >= self.min_local_peers:
            # Have enough local peers, don't fall back
            return (None, None)

        # Try same-region
        if groups[LocalityTier.SAME_REGION]:
            result = selector(groups[LocalityTier.SAME_REGION])
            if result is not None:
                return (result, LocalityTier.SAME_REGION)

        # Try global (if enabled)
        if self.global_fallback_enabled and groups[LocalityTier.GLOBAL]:
            result = selector(groups[LocalityTier.GLOBAL])
            if result is not None:
                return (result, LocalityTier.GLOBAL)

        return (None, None)

    def invalidate_cache(self, peer_id: str | None = None) -> int:
        """
        Invalidate cached tier calculations.

        Args:
            peer_id: Specific peer to invalidate, or None to clear all

        Returns:
            Number of entries invalidated
        """
        if peer_id is not None:
            if peer_id in self._tier_cache:
                del self._tier_cache[peer_id]
                return 1
            return 0
        else:
            count = len(self._tier_cache)
            self._tier_cache.clear()
            return count

    def update_local_locality(self, new_locality: LocalityInfo) -> None:
        """
        Update the local locality and clear the tier cache.

        Call this if the local node's locality changes (rare).

        Args:
            new_locality: The new locality information
        """
        self.local_locality = new_locality
        self._tier_cache.clear()

    @property
    def cache_size(self) -> int:
        """Return the number of cached tier calculations."""
        return len(self._tier_cache)
