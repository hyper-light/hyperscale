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

from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

from hyperscale.distributed.discovery.dns.resolver import (
    AsyncDNSResolver,
    DNSError,
    DNSResult,
    SRVRecord,
)
from hyperscale.distributed.discovery.dns.security import (
    DNSSecurityValidator,
)
from hyperscale.distributed.discovery.selection.adaptive_selector import (
    AdaptiveEWMASelector,
    PowerOfTwoConfig,
    SelectionResult,
)
from hyperscale.distributed.discovery.selection.ewma_tracker import EWMAConfig
from hyperscale.distributed.discovery.locality.locality_filter import (
    LocalityFilter,
)
from hyperscale.distributed.discovery.models.discovery_config import (
    DiscoveryConfig,
)
from hyperscale.distributed.discovery.models.peer_info import (
    PeerInfo,
    PeerHealth,
)
from hyperscale.distributed.discovery.models.locality_info import (
    LocalityInfo,
    LocalityTier,
)
from hyperscale.distributed.discovery.metrics.discovery_metrics import (
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

    _dns_peer_ids: dict[str, set[str]] = field(default_factory=dict)
    """The peers each configured DNS name answered last, by peer_id."""

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
        # DNS security validator (if any security settings are configured)
        security_validator: DNSSecurityValidator | None = self._build_security_validator()

        # DNS resolver
        self._resolver = AsyncDNSResolver(
            default_ttl_seconds=self.config.dns_cache_ttl,
            resolution_timeout_seconds=self.config.dns_timeout,
            max_concurrent_resolutions=self.config.max_concurrent_dns_resolutions,
            security_validator=security_validator,
            reject_on_security_violation=self.config.dns_reject_on_security_violation,
        )

        # Adaptive selector with power of two choices
        power_of_two_config = PowerOfTwoConfig(
            candidate_count=min(self.config.candidate_set_size, 4),
            use_rendezvous_ranking=True,
            latency_threshold_ms=self.config.baseline_latency_ms * 2,
        )
        ewma_config = EWMAConfig(
            alpha=self.config.ewma_alpha,
            initial_estimate_ms=self.config.baseline_latency_ms,
            failure_penalty_ms=self.config.baseline_latency_ms
            * self.config.latency_multiplier_threshold,
        )
        self._selector = AdaptiveEWMASelector(
            power_of_two_config=power_of_two_config,
            ewma_config=ewma_config,
        )

        # Locality filter (only if locality is configured)
        self._configure_locality()

        # Metrics tracking
        self._metrics = DiscoveryMetrics()

        # Add static seeds as initial peers
        for seed in self.config.static_seeds:
            self._add_static_seed(seed)

    def _dns_security_configured(self) -> bool:
        """Whether any DNS security setting is configured."""
        return bool(
            self.config.dns_allowed_cidrs
            or self.config.dns_block_private_for_public
            or self.config.dns_detect_ip_changes
        )

    def _build_security_validator(self) -> DNSSecurityValidator | None:
        """The DNS security validator the configured settings call for, if any."""
        if not self._dns_security_configured():
            return None
        return DNSSecurityValidator(
            allowed_cidrs=self.config.dns_allowed_cidrs,
            block_private_for_public=self.config.dns_block_private_for_public,
            detect_ip_changes=self.config.dns_detect_ip_changes,
            max_ip_changes_per_window=self.config.dns_max_ip_changes_per_window,
            ip_change_window_seconds=self.config.dns_ip_change_window_seconds,
        )

    def _configure_locality(self) -> None:
        """Build the local locality and its filter when a datacenter or region is configured."""
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
        A peer no name answers anymore is retired; a failed lookup changes
        nothing (a DNS outage is not a departure). Uses caching unless
        force_refresh is True.

        Supports both A/AAAA records (hostname -> IPs) and SRV records
        (_service._proto.domain -> priority, weight, port, target).
        For SRV records, each target's individual port is used.

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
                await self._discover_from_dns_name(dns_name, force_refresh, discovered)

            self._last_discovery = _DEFAULT_CLOCK.monotonic()

        finally:
            self._discovery_in_progress = False

        return discovered

    async def _discover_from_dns_name(
        self,
        dns_name: str,
        force_refresh: bool,
        discovered: list[PeerInfo],
    ) -> None:
        """Resolve one configured DNS name, adding the peers it answers to
        ``discovered``; a failed lookup is counted and changes nothing else."""
        try:
            result = await self._resolver.resolve(
                dns_name,
                port=self.config.default_port,
                force_refresh=force_refresh,
            )
            # Note: We don't have cache info from resolver, record as uncached query
            self._metrics.record_dns_query(cached=False)

            answered_peer_ids, added = self._add_peers_from_result(result)
            discovered.extend(added)
            self._retire_unanswered_dns_peers(dns_name, answered_peer_ids)

        except DNSError:
            self._metrics.record_dns_failure()
            # Continue with other DNS names

    def _add_peers_from_result(self, result: DNSResult) -> tuple[set[str], list[PeerInfo]]:
        """Add the peers one DNS answer names: the answered ids, and the new peers."""
        # Handle SRV records specially - each target may have a different port
        if result.srv_records:
            return self._add_peers_from_srv_records(result)

        # Standard A/AAAA record handling
        return self._add_peers_from_addresses(
            result.addresses,
            result.port or self.config.default_port,
        )

    def _retire_unanswered_dns_peers(
        self,
        dns_name: str,
        answered_peer_ids: set[str],
    ) -> None:
        """Record what ``dns_name`` answered, and retire the peers it
        answered before that no configured name answers now."""
        previously_answered = self._dns_peer_ids.get(dns_name, set())
        self._dns_peer_ids[dns_name] = answered_peer_ids
        still_answered = set().union(*self._dns_peer_ids.values())
        for peer_id in previously_answered - still_answered:
            self.remove_peer(peer_id)

    def get_dns_peer_addresses(self) -> list[tuple[str, int]]:
        """The addresses the configured DNS names answer now."""
        answered_peer_ids = set().union(*self._dns_peer_ids.values())
        return [
            (peer.host, peer.port)
            for peer_id in sorted(answered_peer_ids)
            if (peer := self._peers.get(peer_id)) is not None
        ]

    def _add_peers_from_addresses(
        self,
        addresses: list[str],
        port: int,
    ) -> tuple[set[str], list[PeerInfo]]:
        """
        Add peers from resolved IP addresses (A/AAAA records).

        Args:
            addresses: List of resolved IP addresses
            port: Port to use for all addresses

        Returns:
            The answered peers' ids, and the newly added peers
        """
        answered_peer_ids: set[str] = set()
        added: list[PeerInfo] = []

        for addr in addresses:
            peer_id = f"dns-{addr}-{port}"
            answered_peer_ids.add(peer_id)

            if peer_id not in self._peers:
                peer = PeerInfo(
                    peer_id=peer_id,
                    host=addr,
                    port=port,
                    role="manager",  # Discovered peers are typically managers
                    cluster_id=self.config.cluster_id,
                    environment_id=self.config.environment_id,
                )
                self._admit_new_peer(peer, 1.0, added)

        return answered_peer_ids, added

    def _admit_new_peer(self, peer: PeerInfo, selector_weight: float, added: list[PeerInfo]) -> None:
        """Track a newly discovered peer, add it to ``added`` and announce it."""
        self._peers[peer.peer_id] = peer
        self._selector.add_peer(peer.peer_id, weight=selector_weight)
        added.append(peer)

        if self._on_peer_added is not None:
            self._on_peer_added(peer)

    def _add_peers_from_srv_records(
        self,
        result: DNSResult,
    ) -> tuple[set[str], list[PeerInfo]]:
        """
        Add peers from SRV record resolution.

        Each SRV record specifies a target hostname and port. The target
        hostnames have already been resolved to IP addresses by the resolver.
        This method maps IPs back to their SRV record ports.

        For SRV records, we create peers with:
        - Priority-based ordering (lower priority = preferred)
        - Per-target port from the SRV record
        - Weight information stored for potential load balancing

        Args:
            result: DNS result containing srv_records and resolved addresses

        Returns:
            The answered peers' ids, and the newly added peers
        """
        answered_peer_ids: set[str] = set()
        added: list[PeerInfo] = []

        # Build a mapping of target hostname to SRV record for port lookup
        # Note: The resolver resolves each SRV target and collects all IPs
        # We need to use the port from the corresponding SRV record
        target_to_srv: dict[str, SRVRecord] = {}
        for srv_record in result.srv_records:
            target_to_srv[srv_record.target] = srv_record

        # If we have SRV records, use each record's port and target
        # The addresses in result are the resolved IPs of all targets
        # Since _do_resolve_srv resolves each target separately, we iterate
        # through srv_records to get the proper port for each target
        for srv_record in result.srv_records:
            self._admit_srv_record(srv_record, answered_peer_ids, added)

        return answered_peer_ids, added

    def _admit_srv_record(
        self,
        srv_record: SRVRecord,
        answered_peer_ids: set[str],
        added: list[PeerInfo],
    ) -> None:
        """Mark one SRV target answered, adding it as a peer (weighted by
        priority and weight) when new."""
        # The port comes from the SRV record
        port = srv_record.port
        target = srv_record.target

        # Create peer using the target hostname (it will be resolved on connect)
        # or we can use the already-resolved IPs if available
        # For now, use the target hostname to preserve the SRV semantics
        peer_id = f"srv-{target}-{port}"
        answered_peer_ids.add(peer_id)

        if peer_id not in self._peers:
            # Calculate weight factor from SRV priority and weight
            # Lower priority is better, higher weight is better
            # Normalize to 0.1 - 1.0 range for selector weight
            priority_factor = 1.0 / (1.0 + srv_record.priority)
            weight_factor = (srv_record.weight + 1) / 100.0  # Normalize weight
            selector_weight = max(0.1, min(1.0, priority_factor * weight_factor))

            peer = PeerInfo(
                peer_id=peer_id,
                host=target,
                port=port,
                role="manager",  # Discovered peers are typically managers
                cluster_id=self.config.cluster_id,
                environment_id=self.config.environment_id,
            )
            self._admit_new_peer(peer, selector_weight, added)

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

        self._after_peer_removed(peer_id)

        return True

    def _after_peer_removed(self, peer_id: str) -> None:
        """Drop a removed peer's locality cache entry and announce its removal."""
        # Invalidate locality cache for this peer
        if self._locality_filter is not None:
            self._locality_filter.invalidate_cache(peer_id)

        if self._on_peer_removed is not None:
            self._on_peer_removed(peer_id)

    def select_peer(self, key: str) -> SelectionResult | None:
        """
        Select the best peer for a key: locality-aware selection if
        configured, else Power of Two Choices with EWMA.

        Args:
            key: The key to select for (e.g., workflow_id)

        Returns:
            SelectionResult or None if no peers available
        """
        return self._select_peer_internal(key)

    def _select_peer_internal(self, key: str) -> SelectionResult | None:
        """
        Internal peer selection.

        Uses locality-aware selection if configured, then falls back
        to Power of Two Choices with EWMA.

        Args:
            key: The key to select for

        Returns:
            SelectionResult or None if no peers available
        """
        # If locality filter is configured, use locality-aware selection
        if self._locality_selection_enabled() and (selection := self._select_locality_preferred(key)) is not None:
            return selection

        # Fall back to standard selection
        return self._select_globally(key)

    def _locality_selection_enabled(self) -> bool:
        """Whether a locality filter is configured and there are peers to filter."""
        return self._locality_filter is not None and len(self._peers) > 0

    def _select_locality_preferred(self, key: str) -> SelectionResult | None:
        """Select among the peers of the best locality tier that has any."""
        peers_list = list(self._peers.values())
        result_peer, tier = self._locality_filter.select_with_fallback(
            peers_list,
            selector=lambda ps: ps[0] if ps else None,  # Get first matching
        )

        if result_peer is None or tier is None:
            return None

        return self._select_in_tier(key, tier)

    def _select_in_tier(self, key: str, preferred_tier: LocalityTier) -> SelectionResult | None:
        """Select among ``preferred_tier``'s peers, recording the selection."""
        # Use selector with filter for locality-preferred peers
        def locality_filter_fn(peer_id: str) -> bool:
            return self._get_peer_tier(peer_id) == preferred_tier

        selection = self._selector.select_with_filter(key, locality_filter_fn)
        if selection is not None:
            self._metrics.record_selection(
                tier=preferred_tier,
                load_balanced=selection.was_load_balanced,
            )
        return selection

    def _select_globally(self, key: str) -> SelectionResult | None:
        """Power of Two Choices with EWMA over every peer, recording the selection."""
        result = self._selector.select(key)
        if result is not None:
            self._metrics.record_selection(
                tier=LocalityTier.GLOBAL,
                load_balanced=result.was_load_balanced,
            )
        return result

    def _get_peer_tier(self, peer_id: str) -> LocalityTier:
        """Get locality tier for a peer."""
        if self._locality_filter is None or self._local_locality is None:
            return LocalityTier.GLOBAL

        return self._known_peer_tier(peer_id)

    def _known_peer_tier(self, peer_id: str) -> LocalityTier:
        """The locality filter's tier for a known peer; GLOBAL for an unknown one."""
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
                tier=self._get_peer_tier(result.peer_id),
                load_balanced=result.was_load_balanced,
            )
        return result

    def select_peers(
        self,
        key: str,
        count: int = 3,
    ) -> list[SelectionResult]:
        """
        Select multiple peers for a key with primary/backup ordering.

        Returns a list of peers ordered by preference:
        - First peer is the primary (lowest latency, healthy)
        - Subsequent peers are backups in order of preference

        Backups are selected from remaining healthy peers sorted by latency.

        Args:
            key: The key to select for (e.g., workflow_id)
            count: Maximum number of peers to return (default: 3)

        Returns:
            List of SelectionResults, ordered primary-first. May be empty if no peers.
        """
        if not self._peers:
            return []

        results: list[SelectionResult] = []
        used_peer_ids: set[str] = set()

        self._append_primary_selection(key, results, used_peer_ids)

        # Get backup peers from remaining healthy peers
        if len(results) < count:
            self._append_backup_peers(results, used_peer_ids, count)

        return results

    def _append_primary_selection(
        self,
        key: str,
        results: list[SelectionResult],
        used_peer_ids: set[str],
    ) -> None:
        """Append the primary selection for ``key``, if any, marking it used."""
        primary = self.select_peer(key)
        if primary is not None:
            results.append(primary)
            used_peer_ids.add(primary.peer_id)

    def _append_backup_peers(
        self,
        results: list[SelectionResult],
        used_peer_ids: set[str],
        count: int,
    ) -> None:
        """Fill ``results`` up to ``count`` with unused healthy peers, lowest latency first."""
        healthy_peers = self.get_healthy_peers()

        # Sort by latency for backup ordering
        peer_latencies = self._unused_peer_latencies(healthy_peers, used_peer_ids)

        # Sort by latency (ascending)
        peer_latencies.sort(key=lambda pair: pair[1])

        # Add backup peers
        for peer_id, latency in peer_latencies:
            if len(results) >= count:
                break

            results.append(
                SelectionResult(
                    peer_id=peer_id,
                    effective_latency_ms=latency,
                    was_load_balanced=False,
                    candidates_considered=len(peer_latencies),
                )
            )
            used_peer_ids.add(peer_id)

    def _unused_peer_latencies(
        self,
        healthy_peers: list[PeerInfo],
        used_peer_ids: set[str],
    ) -> list[tuple[str, float]]:
        """``(peer_id, effective latency)`` of each healthy peer not yet used, in peer order."""
        peer_latencies: list[tuple[str, float]] = []
        for peer in healthy_peers:
            if peer.peer_id not in used_peer_ids:
                effective_latency = self._selector.get_effective_latency(
                    peer.peer_id
                )
                peer_latencies.append((peer.peer_id, effective_latency))
        return peer_latencies

    def record_success(self, peer_id: str, latency_ms: float) -> None:
        """
        Record a successful request to a peer.

        Updates selector EWMA tracking and peer health metrics.

        Args:
            peer_id: The peer that handled the request
            latency_ms: Request latency in milliseconds
        """
        self._selector.record_success(peer_id, latency_ms)
        self._metrics.record_peer_latency(latency_ms)

        # Update PeerInfo
        peer = self._peers.get(peer_id)
        if peer is not None:
            peer.record_success(latency_ms, ewma_alpha=self.config.ewma_alpha)

    def record_failure(self, peer_id: str) -> None:
        """
        Record a failed request to a peer.

        Updates selector penalty tracking and peer health metrics.

        Args:
            peer_id: The peer that failed
        """
        self._selector.record_failure(peer_id)
        self._metrics.record_connection_failed()

        # Update PeerInfo
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
            peer
            for peer in self._peers.values()
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

    def _health_distribution(self) -> dict[str, int]:
        """Peer count per health status (every status present, zero or more)."""
        health_counts = {h.value: 0 for h in PeerHealth}
        for peer in self._peers.values():
            health_counts[peer.health.value] += 1
        return health_counts

    def get_metrics_snapshot(self) -> dict:
        """
        Get a snapshot of discovery metrics.

        Returns:
            Dict with metric values
        """
        health_counts = self._health_distribution()

        return {
            "peer_count": len(self._peers),
            "healthy_peer_count": len(self.get_healthy_peers()),
            "health_distribution": health_counts,
            "dns_cache_stats": self._resolver.cache_stats,
            "last_discovery_seconds_ago": _DEFAULT_CLOCK.monotonic() - self._last_discovery
            if self._last_discovery > 0
            else -1,
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
        self._dns_peer_ids.clear()
        self._selector.clear()
        if self._locality_filter is not None:
            self._locality_filter.invalidate_cache()
        self._last_discovery = 0.0
