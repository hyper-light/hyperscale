"""
Discovery Service facade for node integration.

Provides a unified interface for nodes to use discovery, peer selection,
and health tracking without directly managing individual components.

This facade combines:
- DNS resolution with caching
- Locality-aware peer filtering
- Adaptive peer selection (Power of Two Choices with EWMA)
- Peer health tracking
- Discovery metrics

Usage:
    from hyperscale.distributed_rewrite.discovery import (
        DiscoveryConfig,
        DiscoveryService,
    )

    # Create service with config
    config = DiscoveryConfig(
        cluster_id="hyperscale-prod",
        environment_id="prod",
        dns_names=["managers.hyperscale.local"],
        datacenter_id="us-east-1",
    )
    service = DiscoveryService(config)

    # Discover peers from DNS
    await service.discover_peers()

    # Select best peer for a key
    selection = service.select_peer("workflow-123")

    # Record feedback
    service.record_success(selection.peer_id, latency_ms=15.0)
"""

import time
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed_rewrite.discovery.dns.resolver import (
    AsyncDNSResolver,
    DNSError,
)
from hyperscale.distributed_rewrite.discovery.selection.adaptive_selector import (
    AdaptiveEWMASelector,
    PowerOfTwoConfig,
    SelectionResult,
)
from hyperscale.distributed_rewrite.discovery.selection.ewma_tracker import EWMAConfig
from hyperscale.distributed_rewrite.discovery.locality.locality_filter import (
    LocalityFilter,
)
from hyperscale.distributed_rewrite.discovery.models.discovery_config import (
    DiscoveryConfig,
)
from hyperscale.distributed_rewrite.discovery.models.peer_info import (
    PeerInfo,
    PeerHealth,
)
from hyperscale.distributed_rewrite.discovery.models.locality_info import (
    LocalityInfo,
    LocalityTier,
)
from hyperscale.distributed_rewrite.discovery.metrics.discovery_metrics import (
    DiscoveryMetrics,
)


@dataclass
class DiscoveryService:
    """
    Unified discovery service for node integration.

    Combines DNS resolution, locality filtering, adaptive peer selection,
    and health tracking into a single cohesive interface.

    The service maintains:
    - A set of known peers from DNS discovery and static seeds
    - Health/latency tracking for each peer
    - Locality-aware selection preferences
    - Metrics for observability

    Thread Safety:
        This class is NOT thread-safe. Use appropriate locking if accessed
        from multiple coroutines concurrently.
    """

    config: DiscoveryConfig
    """Discovery configuration."""

    _resolver: AsyncDNSResolver = field(init=False)
    """DNS resolver with caching."""

    _selector: AdaptiveEWMASelector = field(init=False)
    """Adaptive peer selector."""

    _locality_filter: LocalityFilter | None = field(init=False, default=None)
    """Locality-aware peer filter (None if no locality configured)."""

    _local_locality: LocalityInfo | None = field(init=False, default=None)
    """Local node's locality info."""

    _metrics: DiscoveryMetrics = field(init=False)
    """Discovery metrics."""

    _peers: dict[str, PeerInfo] = field(default_factory=dict)
    """Known peers by peer_id."""

    _last_discovery: float = field(default=0.0)
    """Timestamp of last successful discovery."""

    _discovery_in_progress: bool = field(default=False)
    """Whether a discovery operation is in progress."""

    _on_peer_added: Callable[[PeerInfo], None] | None = field(default=None)
    """Callback when a new peer is added."""

    _on_peer_removed: Callable[[str], None] | None = field(default=None)
    """Callback when a peer is removed."""

    def __post_init__(self) -> None:
        """Initialize internal components."""
        # DNS resolver
        self._resolver = AsyncDNSResolver(
            default_ttl_seconds=self.config.dns_cache_ttl,
            resolution_timeout_seconds=self.config.dns_timeout,
            max_concurrent_resolutions=self.config.max_concurrent_probes,
        )

        # Adaptive selector with power of two choices
        power_of_two_config = PowerOfTwoConfig(
            candidate_count=min(self.config.candidate_set_size, 4),
            use_rendezvous_ranking=True,
            latency_threshold_ms=self.config.baseline_latency_ms * 2,
        )
        ewma_config = EWMAConfig(
            alpha=self.config.ewma_alpha,
            initial_latency_ms=self.config.baseline_latency_ms,
            failure_penalty_ms=self.config.baseline_latency_ms * self.config.latency_multiplier_threshold,
        )
        self._selector = AdaptiveEWMASelector(
            power_of_two_config=power_of_two_config,
            ewma_config=ewma_config,
        )

        # Locality filter (only if locality is configured)
        if self.config.datacenter_id or self.config.region_id:
            self._local_locality = LocalityInfo(
                datacenter_id=self.config.datacenter_id,
                region_id=self.config.region_id,
            )
            self._locality_filter = LocalityFilter(
                local_locality=self._local_locality,
                prefer_same_dc=self.config.prefer_same_dc,
                global_fallback_enabled=True,
                min_local_peers=self.config.min_peers_per_tier,
            )

        # Metrics tracking
        self._metrics = DiscoveryMetrics()

        # Add static seeds as initial peers
        for seed in self.config.static_seeds:
            self._add_static_seed(seed)

    def _add_static_seed(self, seed: str) -> None:
        """
        Add a static seed address as a peer.

        Args:
            seed: Address in format "host:port" or "host"
        """
        if ":" in seed:
            host, port_str = seed.rsplit(":", 1)
            port = int(port_str)
        else:
            host = seed
            port = self.config.default_port

        peer_id = f"seed-{host}-{port}"
        peer = PeerInfo(
            peer_id=peer_id,
            host=host,
            port=port,
            role=self.config.node_role,
            cluster_id=self.config.cluster_id,
            environment_id=self.config.environment_id,
        )
        self._peers[peer_id] = peer
        self._selector.add_peer(peer_id, weight=1.0)

    async def discover_peers(self, force_refresh: bool = False) -> list[PeerInfo]:
        """
        Discover peers via DNS resolution.

        Resolves configured DNS names and adds discovered addresses as peers.
        Uses caching unless force_refresh is True.

        Args:
            force_refresh: If True, bypass cache and force fresh DNS lookup

        Returns:
            List of newly discovered peers
        """
        if self._discovery_in_progress:
            return []

        self._discovery_in_progress = True
        discovered: list[PeerInfo] = []

        try:
            for dns_name in self.config.dns_names:
                try:
                    result = await self._resolver.resolve(
                        dns_name,
                        port=self.config.default_port,
                        force_refresh=force_refresh,
                    )
                    self._metrics.record_dns_success()

                    for addr in result.addresses:
                        port = result.port or self.config.default_port
                        peer_id = f"dns-{addr}-{port}"

                        if peer_id not in self._peers:
                            peer = PeerInfo(
                                peer_id=peer_id,
                                host=addr,
                                port=port,
                                role="manager",  # Discovered peers are typically managers
                                cluster_id=self.config.cluster_id,
                                environment_id=self.config.environment_id,
                            )
                            self._peers[peer_id] = peer
                            self._selector.add_peer(peer_id, weight=1.0)
                            discovered.append(peer)

                            if self._on_peer_added is not None:
                                self._on_peer_added(peer)

                except DNSError:
                    self._metrics.record_dns_failure()
                    # Continue with other DNS names

            self._last_discovery = time.monotonic()

        finally:
            self._discovery_in_progress = False

        return discovered

    def add_peer(
        self,
        peer_id: str,
        host: str,
        port: int,
        role: str = "manager",
        datacenter_id: str = "",
        region_id: str = "",
        weight: float = 1.0,
    ) -> PeerInfo:
        """
        Manually add a peer (e.g., from registration response).

        Args:
            peer_id: Unique peer identifier (node_id)
            host: Peer's IP address or hostname
            port: Peer's TCP port
            role: Peer's role (default: "manager")
            datacenter_id: Peer's datacenter
            region_id: Peer's region
            weight: Selection weight

        Returns:
            The added or updated peer
        """
        peer = PeerInfo(
            peer_id=peer_id,
            host=host,
            port=port,
            role=role,
            cluster_id=self.config.cluster_id,
            environment_id=self.config.environment_id,
            datacenter_id=datacenter_id,
            region_id=region_id,
        )

        is_new = peer_id not in self._peers
        self._peers[peer_id] = peer

        if is_new:
            self._selector.add_peer(peer_id, weight=weight)
            if self._on_peer_added is not None:
                self._on_peer_added(peer)
        else:
            self._selector.update_weight(peer_id, weight)

        return peer

    def add_peer_from_info(self, peer: PeerInfo) -> PeerInfo:
        """
        Add a peer from an existing PeerInfo object.

        Args:
            peer: PeerInfo to add

        Returns:
            The added or updated peer
        """
        is_new = peer.peer_id not in self._peers
        self._peers[peer.peer_id] = peer

        if is_new:
            self._selector.add_peer(peer.peer_id, weight=peer.health_weight)
            if self._on_peer_added is not None:
                self._on_peer_added(peer)
        else:
            self._selector.update_weight(peer.peer_id, peer.health_weight)

        return peer

    def remove_peer(self, peer_id: str) -> bool:
        """
        Remove a peer from the discovery service.

        Args:
            peer_id: The peer to remove

        Returns:
            True if the peer was removed
        """
        if peer_id not in self._peers:
            return False

        del self._peers[peer_id]
        self._selector.remove_peer(peer_id)

        # Invalidate locality cache for this peer
        if self._locality_filter is not None:
            self._locality_filter.invalidate_cache(peer_id)

        if self._on_peer_removed is not None:
            self._on_peer_removed(peer_id)

        return True

    def select_peer(self, key: str) -> SelectionResult | None:
        """
        Select the best peer for a key.

        Uses Power of Two Choices with EWMA for load-aware selection.
        Considers locality preferences if configured.

        Args:
            key: The key to select for (e.g., workflow_id)

        Returns:
            SelectionResult or None if no peers available
        """
        # If locality filter is configured, use locality-aware selection
        if self._locality_filter is not None and len(self._peers) > 0:
            peers_list = list(self._peers.values())
            result_peer, tier = self._locality_filter.select_with_fallback(
                peers_list,
                selector=lambda ps: ps[0] if ps else None,  # Get first matching
            )

            if result_peer is not None and tier is not None:
                # Use selector with filter for locality-preferred peers
                preferred_tier = tier

                def locality_filter_fn(peer_id: str) -> bool:
                    return self._get_peer_tier(peer_id) == preferred_tier

                selection = self._selector.select_with_filter(key, locality_filter_fn)
                if selection is not None:
                    self._metrics.record_selection(
                        was_load_balanced=selection.was_load_balanced
                    )
                    return selection

        # Fall back to standard selection
        result = self._selector.select(key)
        if result is not None:
            self._metrics.record_selection(
                was_load_balanced=result.was_load_balanced
            )
        return result

    def _get_peer_tier(self, peer_id: str) -> LocalityTier:
        """Get locality tier for a peer."""
        if self._locality_filter is None or self._local_locality is None:
            return LocalityTier.GLOBAL

        peer = self._peers.get(peer_id)
        if peer is None:
            return LocalityTier.GLOBAL

        return self._locality_filter.get_tier(peer)

    def select_peer_with_filter(
        self,
        key: str,
        filter_fn: Callable[[str], bool],
    ) -> SelectionResult | None:
        """
        Select best peer with a custom filter.

        Args:
            key: The key to select for
            filter_fn: Function that returns True for acceptable peers

        Returns:
            SelectionResult or None if no acceptable peers
        """
        result = self._selector.select_with_filter(key, filter_fn)
        if result is not None:
            self._metrics.record_selection(
                was_load_balanced=result.was_load_balanced
            )
        return result

    def record_success(self, peer_id: str, latency_ms: float) -> None:
        """
        Record a successful request to a peer.

        Args:
            peer_id: The peer that handled the request
            latency_ms: Request latency in milliseconds
        """
        self._selector.record_success(peer_id, latency_ms)
        self._metrics.record_request_success(latency_ms)

        # Also update PeerInfo
        peer = self._peers.get(peer_id)
        if peer is not None:
            peer.record_success(latency_ms, ewma_alpha=self.config.ewma_alpha)

    def record_failure(self, peer_id: str) -> None:
        """
        Record a failed request to a peer.

        Args:
            peer_id: The peer that failed
        """
        self._selector.record_failure(peer_id)
        self._metrics.record_request_failure()

        # Also update PeerInfo
        peer = self._peers.get(peer_id)
        if peer is not None:
            peer.record_failure()
            # Update selector weight based on health
            self._selector.update_weight(peer_id, peer.health_weight)

    def get_peer(self, peer_id: str) -> PeerInfo | None:
        """
        Get a peer by ID.

        Args:
            peer_id: The peer to look up

        Returns:
            PeerInfo or None if not found
        """
        return self._peers.get(peer_id)

    def get_peer_address(self, peer_id: str) -> tuple[str, int] | None:
        """
        Get a peer's address by ID.

        Args:
            peer_id: The peer to look up

        Returns:
            Tuple of (host, port) or None if not found
        """
        peer = self._peers.get(peer_id)
        if peer is None:
            return None
        return peer.address

    def get_all_peers(self) -> list[PeerInfo]:
        """Get all known peers."""
        return list(self._peers.values())

    def get_healthy_peers(self) -> list[PeerInfo]:
        """
        Get peers with healthy status.

        Returns:
            List of healthy peers
        """
        return [
            peer for peer in self._peers.values()
            if peer.health in (PeerHealth.HEALTHY, PeerHealth.UNKNOWN)
        ]

    def get_peers_by_health(self, health: PeerHealth) -> list[PeerInfo]:
        """
        Get peers with a specific health status.

        Args:
            health: The health status to filter by

        Returns:
            List of peers with the specified health
        """
        return [peer for peer in self._peers.values() if peer.health == health]

    def get_effective_latency(self, peer_id: str) -> float:
        """
        Get the effective latency for a peer.

        Args:
            peer_id: The peer to look up

        Returns:
            Effective latency in milliseconds
        """
        return self._selector.get_effective_latency(peer_id)

    def update_peer_locality(
        self,
        peer_id: str,
        datacenter_id: str,
        region_id: str,
    ) -> bool:
        """
        Update a peer's locality information.

        Args:
            peer_id: The peer to update
            datacenter_id: New datacenter ID
            region_id: New region ID

        Returns:
            True if updated
        """
        peer = self._peers.get(peer_id)
        if peer is None:
            return False

        peer.datacenter_id = datacenter_id
        peer.region_id = region_id

        # Invalidate locality cache if filter exists
        if self._locality_filter is not None:
            self._locality_filter.invalidate_cache(peer_id)

        return True

    def decay_failures(self) -> int:
        """
        Decay failure counts for all peers.

        Call periodically to allow failed peers to recover.

        Returns:
            Number of peers with decayed counts
        """
        return self._selector.decay_failures()

    def cleanup_expired_dns(self) -> tuple[int, int]:
        """
        Clean up expired DNS cache entries.

        Returns:
            Tuple of (positive entries removed, negative entries removed)
        """
        return self._resolver.cleanup_expired()

    def set_callbacks(
        self,
        on_peer_added: Callable[[PeerInfo], None] | None = None,
        on_peer_removed: Callable[[str], None] | None = None,
    ) -> None:
        """
        Set callbacks for peer lifecycle events.

        Args:
            on_peer_added: Called when a new peer is added
            on_peer_removed: Called when a peer is removed
        """
        self._on_peer_added = on_peer_added
        self._on_peer_removed = on_peer_removed

    def get_metrics_snapshot(self) -> dict:
        """
        Get a snapshot of discovery metrics.

        Returns:
            Dict with metric values
        """
        health_counts = {h.value: 0 for h in PeerHealth}
        for peer in self._peers.values():
            health_counts[peer.health.value] += 1

        return {
            "peer_count": len(self._peers),
            "healthy_peer_count": len(self.get_healthy_peers()),
            "health_distribution": health_counts,
            "dns_cache_stats": self._resolver.cache_stats,
            "last_discovery_seconds_ago": time.monotonic() - self._last_discovery if self._last_discovery > 0 else -1,
            "selector_peer_count": self._selector.peer_count,
        }

    @property
    def peer_count(self) -> int:
        """Return the number of known peers."""
        return len(self._peers)

    @property
    def has_peers(self) -> bool:
        """Check if any peers are known."""
        return len(self._peers) > 0

    @property
    def local_locality(self) -> LocalityInfo | None:
        """Get this node's locality info."""
        return self._local_locality

    def contains(self, peer_id: str) -> bool:
        """Check if a peer is known."""
        return peer_id in self._peers

    def clear(self) -> None:
        """Clear all peers and reset state."""
        self._peers.clear()
        self._selector.clear()
        if self._locality_filter is not None:
            self._locality_filter.invalidate_cache()
        self._last_discovery = 0.0
