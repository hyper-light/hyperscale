"""``MetricsSnapshot`` -- pickled under the namespace
``hyperscale.distributed.discovery.metrics.discovery_metrics`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.distributed.discovery.models.locality_info import LocalityTier


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

    # Connection metrics
    connections_active: int = 0
    """Currently active connections."""

    connections_created: int = 0
    """Total connections created."""

    connections_closed: int = 0
    """Total connections closed."""

    connections_failed: int = 0
    """Connection failures."""

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
