"""
Discovery system metrics collection and reporting.

Provides comprehensive observability for peer discovery operations.
"""

import time
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed_rewrite.discovery.models.locality_info import LocalityTier


@dataclass(slots=True)
class MetricsSnapshot:
    """Point-in-time snapshot of discovery metrics."""

    timestamp: float
    """When this snapshot was taken (monotonic)."""

    # DNS metrics
    dns_queries_total: int = 0
    """Total DNS queries performed."""

    dns_cache_hits: int = 0
    """DNS queries served from cache."""

    dns_cache_misses: int = 0
    """DNS queries that required resolution."""

    dns_negative_cache_hits: int = 0
    """Queries blocked by negative cache."""

    dns_failures: int = 0
    """DNS resolution failures."""

    dns_avg_latency_ms: float = 0.0
    """Average DNS resolution latency."""

    # Selection metrics
    selections_total: int = 0
    """Total peer selections performed."""

    selections_load_balanced: int = 0
    """Selections where load balancing changed the choice."""

    selections_by_tier: dict[LocalityTier, int] = field(default_factory=dict)
    """Selection count broken down by locality tier."""

    # Connection pool metrics
    connections_active: int = 0
    """Currently active connections."""

    connections_idle: int = 0
    """Currently idle connections."""

    connections_created: int = 0
    """Total connections created."""

    connections_closed: int = 0
    """Total connections closed."""

    connections_failed: int = 0
    """Connection failures."""

    # Sticky binding metrics
    sticky_bindings_total: int = 0
    """Current number of sticky bindings."""

    sticky_bindings_healthy: int = 0
    """Sticky bindings to healthy peers."""

    sticky_evictions: int = 0
    """Sticky bindings evicted due to health."""

    # Peer health metrics
    peers_total: int = 0
    """Total known peers."""

    peers_healthy: int = 0
    """Peers in healthy state."""

    peers_degraded: int = 0
    """Peers in degraded state."""

    peers_unhealthy: int = 0
    """Peers in unhealthy state."""

    # Latency tracking
    peer_avg_latency_ms: float = 0.0
    """Average latency across all peers."""

    peer_p50_latency_ms: float = 0.0
    """P50 peer latency."""

    peer_p99_latency_ms: float = 0.0
    """P99 peer latency."""


@dataclass
class DiscoveryMetrics:
    """
    Metrics collector for the discovery system.

    Tracks DNS, selection, connection, and health metrics
    for observability and debugging.

    Usage:
        metrics = DiscoveryMetrics()

        # Record events
        metrics.record_dns_query(cached=True)
        metrics.record_selection(tier=LocalityTier.SAME_DC, load_balanced=False)
        metrics.record_connection_created()

        # Get snapshot for reporting
        snapshot = metrics.get_snapshot()
        print(f"DNS cache hit rate: {snapshot.dns_cache_hits / snapshot.dns_queries_total}")
    """

    _dns_queries_total: int = field(default=0, repr=False)
    _dns_cache_hits: int = field(default=0, repr=False)
    _dns_cache_misses: int = field(default=0, repr=False)
    _dns_negative_cache_hits: int = field(default=0, repr=False)
    _dns_failures: int = field(default=0, repr=False)
    _dns_latency_sum_ms: float = field(default=0.0, repr=False)
    _dns_latency_count: int = field(default=0, repr=False)

    _selections_total: int = field(default=0, repr=False)
    _selections_load_balanced: int = field(default=0, repr=False)
    _selections_by_tier: dict[LocalityTier, int] = field(default_factory=dict, repr=False)

    _connections_created: int = field(default=0, repr=False)
    _connections_closed: int = field(default=0, repr=False)
    _connections_failed: int = field(default=0, repr=False)
    _connections_active: int = field(default=0, repr=False)

    _sticky_evictions: int = field(default=0, repr=False)

    _peer_latencies_ms: list[float] = field(default_factory=list, repr=False)
    _max_latency_samples: int = field(default=1000, repr=False)

    _on_snapshot: Callable[[MetricsSnapshot], None] | None = field(
        default=None, repr=False
    )
    """Optional callback when snapshot is generated."""

    # External state providers (set by DiscoveryService)
    _get_connection_stats: Callable[[], dict[str, int]] | None = field(
        default=None, repr=False
    )
    _get_sticky_stats: Callable[[], dict[str, int]] | None = field(
        default=None, repr=False
    )
    _get_peer_stats: Callable[[], dict[str, int]] | None = field(
        default=None, repr=False
    )

    # --- DNS Metrics ---

    def record_dns_query(
        self,
        cached: bool = False,
        negative_cached: bool = False,
        latency_ms: float | None = None,
    ) -> None:
        """
        Record a DNS query.

        Args:
            cached: True if served from positive cache
            negative_cached: True if blocked by negative cache
            latency_ms: Resolution latency (if not cached)
        """
        self._dns_queries_total += 1

        if cached:
            self._dns_cache_hits += 1
        elif negative_cached:
            self._dns_negative_cache_hits += 1
        else:
            self._dns_cache_misses += 1
            if latency_ms is not None:
                self._dns_latency_sum_ms += latency_ms
                self._dns_latency_count += 1

    def record_dns_failure(self) -> None:
        """Record a DNS resolution failure."""
        self._dns_failures += 1

    # --- Selection Metrics ---

    def record_selection(
        self,
        tier: LocalityTier,
        load_balanced: bool = False,
    ) -> None:
        """
        Record a peer selection.

        Args:
            tier: Locality tier of the selected peer
            load_balanced: True if load balancing changed the choice
        """
        self._selections_total += 1

        if load_balanced:
            self._selections_load_balanced += 1

        if tier not in self._selections_by_tier:
            self._selections_by_tier[tier] = 0
        self._selections_by_tier[tier] += 1

    # --- Connection Metrics ---

    def record_connection_created(self) -> None:
        """Record a new connection being created."""
        self._connections_created += 1
        self._connections_active += 1

    def record_connection_closed(self) -> None:
        """Record a connection being closed."""
        self._connections_closed += 1
        self._connections_active = max(0, self._connections_active - 1)

    def record_connection_failed(self) -> None:
        """Record a connection failure."""
        self._connections_failed += 1

    # --- Sticky Binding Metrics ---

    def record_sticky_eviction(self, count: int = 1) -> None:
        """
        Record sticky binding eviction(s).

        Args:
            count: Number of bindings evicted
        """
        self._sticky_evictions += count

    # --- Latency Tracking ---

    def record_peer_latency(self, latency_ms: float) -> None:
        """
        Record a peer request latency.

        Args:
            latency_ms: Request latency in milliseconds
        """
        self._peer_latencies_ms.append(latency_ms)

        # Keep bounded
        if len(self._peer_latencies_ms) > self._max_latency_samples:
            self._peer_latencies_ms = self._peer_latencies_ms[-self._max_latency_samples:]

    # --- Snapshot Generation ---

    def get_snapshot(self) -> MetricsSnapshot:
        """
        Generate a point-in-time metrics snapshot.

        Returns:
            MetricsSnapshot with current metrics
        """
        snapshot = MetricsSnapshot(timestamp=time.monotonic())

        # DNS metrics
        snapshot.dns_queries_total = self._dns_queries_total
        snapshot.dns_cache_hits = self._dns_cache_hits
        snapshot.dns_cache_misses = self._dns_cache_misses
        snapshot.dns_negative_cache_hits = self._dns_negative_cache_hits
        snapshot.dns_failures = self._dns_failures

        if self._dns_latency_count > 0:
            snapshot.dns_avg_latency_ms = (
                self._dns_latency_sum_ms / self._dns_latency_count
            )

        # Selection metrics
        snapshot.selections_total = self._selections_total
        snapshot.selections_load_balanced = self._selections_load_balanced
        snapshot.selections_by_tier = dict(self._selections_by_tier)

        # Connection metrics (from pool if available)
        snapshot.connections_created = self._connections_created
        snapshot.connections_closed = self._connections_closed
        snapshot.connections_failed = self._connections_failed

        if self._get_connection_stats is not None:
            pool_stats = self._get_connection_stats()
            snapshot.connections_active = pool_stats.get("in_use", 0)
            snapshot.connections_idle = pool_stats.get("idle", 0)
        else:
            snapshot.connections_active = self._connections_active

        # Sticky binding metrics (from manager if available)
        if self._get_sticky_stats is not None:
            sticky_stats = self._get_sticky_stats()
            snapshot.sticky_bindings_total = sticky_stats.get("total_bindings", 0)
            snapshot.sticky_bindings_healthy = sticky_stats.get("healthy_bindings", 0)
        snapshot.sticky_evictions = self._sticky_evictions

        # Peer health metrics (from selector if available)
        if self._get_peer_stats is not None:
            peer_stats = self._get_peer_stats()
            snapshot.peers_total = peer_stats.get("total", 0)
            snapshot.peers_healthy = peer_stats.get("healthy", 0)
            snapshot.peers_degraded = peer_stats.get("degraded", 0)
            snapshot.peers_unhealthy = peer_stats.get("unhealthy", 0)

        # Latency percentiles
        if self._peer_latencies_ms:
            sorted_latencies = sorted(self._peer_latencies_ms)
            count = len(sorted_latencies)

            snapshot.peer_avg_latency_ms = sum(sorted_latencies) / count
            snapshot.peer_p50_latency_ms = sorted_latencies[int(count * 0.5)]
            snapshot.peer_p99_latency_ms = sorted_latencies[int(count * 0.99)]

        # Notify callback if set
        if self._on_snapshot is not None:
            self._on_snapshot(snapshot)

        return snapshot

    def reset(self) -> None:
        """Reset all metrics to zero."""
        self._dns_queries_total = 0
        self._dns_cache_hits = 0
        self._dns_cache_misses = 0
        self._dns_negative_cache_hits = 0
        self._dns_failures = 0
        self._dns_latency_sum_ms = 0.0
        self._dns_latency_count = 0

        self._selections_total = 0
        self._selections_load_balanced = 0
        self._selections_by_tier.clear()

        self._connections_created = 0
        self._connections_closed = 0
        self._connections_failed = 0
        self._connections_active = 0

        self._sticky_evictions = 0

        self._peer_latencies_ms.clear()

    def set_state_providers(
        self,
        connection_stats: Callable[[], dict[str, int]] | None = None,
        sticky_stats: Callable[[], dict[str, int]] | None = None,
        peer_stats: Callable[[], dict[str, int]] | None = None,
    ) -> None:
        """
        Set external state providers for richer snapshots.

        Args:
            connection_stats: Function returning connection pool stats
            sticky_stats: Function returning sticky binding stats
            peer_stats: Function returning peer health stats
        """
        self._get_connection_stats = connection_stats
        self._get_sticky_stats = sticky_stats
        self._get_peer_stats = peer_stats

    def set_snapshot_callback(
        self,
        callback: Callable[[MetricsSnapshot], None] | None,
    ) -> None:
        """
        Set callback for when snapshots are generated.

        Args:
            callback: Function to call with each snapshot
        """
        self._on_snapshot = callback

    # --- Convenience Properties ---

    @property
    def dns_cache_hit_rate(self) -> float:
        """Calculate DNS cache hit rate."""
        if self._dns_queries_total == 0:
            return 0.0
        return self._dns_cache_hits / self._dns_queries_total

    @property
    def load_balance_rate(self) -> float:
        """Calculate rate of selections that were load balanced."""
        if self._selections_total == 0:
            return 0.0
        return self._selections_load_balanced / self._selections_total

    @property
    def connection_failure_rate(self) -> float:
        """Calculate connection failure rate."""
        total = self._connections_created + self._connections_failed
        if total == 0:
            return 0.0
        return self._connections_failed / total
