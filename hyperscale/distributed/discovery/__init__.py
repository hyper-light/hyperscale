"""
Enhanced DNS Discovery with Peer Selection (AD-28).

Provides robust, locality-aware peer discovery and selection for the
Hyperscale distributed system.

Features:
- DNS resolution with positive and negative caching
- Cluster ID and environment ID enforcement
- Role-based mTLS certificate validation
- Locality-aware discovery (prefer same-DC peers)
- Weighted Rendezvous Hash for deterministic selection
- Power of Two Choices for load balancing
- EWMA latency tracking for adaptive selection
- Sticky connections with health-based eviction
- Comprehensive metrics for observability

Usage:
    from hyperscale.distributed_rewrite.discovery import (
        DiscoveryConfig,
        AsyncDNSResolver,
        AdaptiveEWMASelector,
        LocalityFilter,
    )

    # Create resolver with caching
    resolver = AsyncDNSResolver()
    result = await resolver.resolve("managers.hyperscale.local")

    # Create adaptive selector with power of two choices
    selector = AdaptiveEWMASelector()
    selector.add_peer("peer1", weight=1.0)
    selection = selector.select("job-123")
"""

# Models
from hyperscale.distributed.discovery.models.discovery_config import (
    DiscoveryConfig as DiscoveryConfig,
)
from hyperscale.distributed.discovery.models.peer_info import (
    PeerInfo as PeerInfo,
    PeerHealth as PeerHealth,
)
from hyperscale.distributed.discovery.models.locality_info import (
    LocalityInfo as LocalityInfo,
    LocalityTier as LocalityTier,
)
from hyperscale.distributed.discovery.models.connection_state import (
    ConnectionState as ConnectionState,
)

# DNS
from hyperscale.distributed.discovery.dns.resolver import (
    AsyncDNSResolver as AsyncDNSResolver,
    DNSResult as DNSResult,
    DNSError as DNSError,
)
from hyperscale.distributed.discovery.dns.negative_cache import (
    NegativeCache as NegativeCache,
    NegativeEntry as NegativeEntry,
)

# Locality
from hyperscale.distributed.discovery.locality.locality_filter import (
    LocalityFilter as LocalityFilter,
)

# Selection
from hyperscale.distributed.discovery.selection.rendezvous_hash import (
    WeightedRendezvousHash as WeightedRendezvousHash,
)
from hyperscale.distributed.discovery.selection.ewma_tracker import (
    EWMATracker as EWMATracker,
    EWMAConfig as EWMAConfig,
    PeerLatencyStats as PeerLatencyStats,
)
from hyperscale.distributed.discovery.selection.adaptive_selector import (
    AdaptiveEWMASelector as AdaptiveEWMASelector,
    PowerOfTwoConfig as PowerOfTwoConfig,
    SelectionResult as SelectionResult,
)

# Pool
from hyperscale.distributed.discovery.pool.connection_pool import (
    ConnectionPool as ConnectionPool,
    ConnectionPoolConfig as ConnectionPoolConfig,
    PooledConnection as PooledConnection,
)
from hyperscale.distributed.discovery.pool.sticky_connection import (
    StickyConnectionManager as StickyConnectionManager,
    StickyConfig as StickyConfig,
    StickyBinding as StickyBinding,
)

# Security
from hyperscale.distributed.discovery.security.role_validator import (
    RoleValidator as RoleValidator,
    CertificateClaims as CertificateClaims,
    ValidationResult as ValidationResult,
    RoleValidationError as RoleValidationError,
    NodeRole as NodeRole,
)

# Metrics
from hyperscale.distributed.discovery.metrics.discovery_metrics import (
    DiscoveryMetrics as DiscoveryMetrics,
    MetricsSnapshot as MetricsSnapshot,
)

# Service facade
from hyperscale.distributed.discovery.discovery_service import (
    DiscoveryService as DiscoveryService,
)
