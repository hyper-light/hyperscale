"""
Discovery configuration for the enhanced DNS discovery system (AD-28).
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class DiscoveryConfig:
    """
    Configuration for enhanced peer discovery.

    This configuration controls all aspects of peer discovery including
    DNS resolution, security validation, locality preferences, peer
    selection algorithms, and connection pool management.
    """

    # ===== Security (Required) =====
    cluster_id: str
    """Unique cluster identifier (e.g., 'hyperscale-prod').

    Prevents accidental cross-cluster joins. All nodes in a cluster
    must have the same cluster_id.
    """

    environment_id: str
    """Environment identifier (e.g., 'production', 'staging', 'dev').

    Prevents accidental cross-environment joins. Nodes will reject
    connections from peers with different environment_id.
    """

    # ===== DNS Configuration =====
    dns_names: list[str] = field(default_factory=list)
    """DNS names to resolve for peer discovery (SRV or A/AAAA records).

    Supports two resolution modes:

    1. **A/AAAA Records** (standard hostnames):
       - Format: 'hostname.domain.tld'
       - Example: 'managers.hyperscale.svc.cluster.local'
       - Returns IP addresses; uses default_port for connections

    2. **SRV Records** (service discovery):
       - Format: '_service._proto.domain' (must start with '_' and contain '._tcp.' or '._udp.')
       - Example: '_hyperscale-manager._tcp.cluster.local'
       - Returns (priority, weight, port, target) tuples
       - Targets are resolved to IPs with ports from SRV records
       - Results sorted by priority (ascending) then weight (descending)

    SRV records are the standard DNS mechanism for service discovery, used by
    Kubernetes, Consul, and other orchestration systems. They allow:
    - Multiple service instances with different ports
    - Priority-based failover (lower priority = preferred)
    - Weight-based load balancing (higher weight = more traffic)

    Example SRV record:
        _hyperscale-manager._tcp.cluster.local. 30 IN SRV 0 10 8080 manager1.cluster.local.
        _hyperscale-manager._tcp.cluster.local. 30 IN SRV 0 10 8080 manager2.cluster.local.
        _hyperscale-manager._tcp.cluster.local. 30 IN SRV 1 5  8080 manager3.cluster.local.  # backup
    """

    static_seeds: list[str] = field(default_factory=list)
    """Static seed addresses as fallback when DNS fails.

    Format: ['host:port', 'host:port']
    Example: ['10.0.1.5:9000', '10.0.1.6:9000']
    """

    default_port: int = 9000
    """Default port when not specified in address."""

    dns_timeout: float = 2.0
    """Timeout for DNS resolution in seconds."""

    dns_cache_ttl: float = 30.0
    """Cache TTL for successful DNS lookups (overrides DNS TTL if set)."""

    negative_cache_ttl: float = 30.0
    """Cache TTL for failed DNS lookups (prevents hammering failed names)."""

    # ===== DNS Security (AD-28 Phase 2) =====
    # Protections against: Cache Poisoning, DNS Hijacking, DNS Spoofing, Rebinding
    dns_allowed_cidrs: list[str] = field(default_factory=list)
    """CIDR ranges that resolved IPs must be within.

    Empty list disables IP range validation.
    Example: ['10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16']

    For internal services, restrict to your network ranges to prevent
    DNS cache poisoning attacks from redirecting to external IPs.
    """

    dns_block_private_for_public: bool = False
    """Block private IPs for public hostnames (DNS rebinding protection).

    When True, if a hostname doesn't end with internal TLDs
    (.local, .internal, .svc, etc.), private IPs will be rejected.
    """

    dns_detect_ip_changes: bool = True
    """Enable anomaly detection for IP changes.

    Tracks historical IPs per hostname and alerts on:
    - Rapid IP rotation (possible fast-flux attack)
    - Unexpected IP changes (possible hijacking)
    """

    dns_max_ip_changes_per_window: int = 5
    """Maximum IP changes allowed before triggering rapid rotation alert."""

    dns_ip_change_window_seconds: float = 300.0
    """Time window for tracking IP changes (5 minutes default)."""

    dns_reject_on_security_violation: bool = True
    """Reject IPs that fail security validation.

    When True (recommended), IPs outside allowed CIDRs are filtered.
    When False, violations are logged but IPs are still usable.
    """

    # ===== Locality =====
    datacenter_id: str = ""
    """This node's datacenter identifier (e.g., 'us-east-1').

    Used for locality-aware peer selection.
    """

    region_id: str = ""
    """This node's region identifier (e.g., 'us-east').

    A region contains multiple datacenters. Used for fallback
    when same-DC peers are unavailable.
    """

    prefer_same_dc: bool = True
    """Prefer peers in the same datacenter."""

    prefer_same_region: bool = True
    """Prefer peers in the same region when same-DC unavailable."""

    min_peers_per_tier: int = 3
    """Minimum peers required before falling back to next locality tier."""

    # ===== Peer Selection =====
    candidate_set_size: int = 8
    """Number of candidate peers to consider (K for rendezvous hash).

    Larger values provide more redundancy but increase state tracking.
    """

    primary_connections: int = 3
    """Number of active primary connections to maintain."""

    backup_connections: int = 2
    """Number of warm standby connections ready for promotion."""

    ewma_alpha: float = 0.2
    """EWMA smoothing factor for latency tracking (0-1).

    Lower values = more smoothing (slower response to changes).
    Higher values = less smoothing (faster response to changes).
    """

    # ===== Health Thresholds =====
    error_rate_threshold: float = 0.05
    """Error rate threshold for marking peer as degraded (5% = 0.05)."""

    consecutive_failure_limit: int = 3
    """Number of consecutive failures before evicting a peer."""

    latency_multiplier_threshold: float = 3.0
    """Latency threshold as multiplier of baseline (3x baseline = evict)."""

    baseline_latency_ms: float = 10.0
    """Expected baseline latency in milliseconds."""

    # ===== Timing =====
    probe_timeout: float = 0.5
    """Timeout for probing a peer in seconds (500ms)."""

    max_concurrent_probes: int = 10
    """Maximum number of concurrent probe operations."""

    initial_backoff: float = 0.5
    """Initial backoff delay in seconds when all probes fail."""

    max_backoff: float = 15.0
    """Maximum backoff delay in seconds."""

    backoff_multiplier: float = 2.0
    """Multiplier for exponential backoff."""

    jitter_factor: float = 0.25
    """Jitter factor for backoff randomization (0-1)."""

    refresh_interval: float = 60.0
    """Interval in seconds for re-evaluating candidate set."""

    promotion_jitter_min: float = 0.1
    """Minimum jitter for backup promotion (100ms)."""

    promotion_jitter_max: float = 0.5
    """Maximum jitter for backup promotion (500ms)."""

    connection_max_age: float = 3600.0
    """Maximum age of a connection before considering refresh (1 hour)."""

    # ===== Role Configuration =====
    node_role: str = "manager"
    """This node's role ('client', 'gate', 'manager', 'worker')."""

    allow_dynamic_registration: bool = False
    """Allow discovery without initial seeds (peers register dynamically).

    When True, the requirement for dns_names or static_seeds is relaxed.
    Use this for manager->worker discovery where workers register themselves
    rather than being discovered from seeds.
    """

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.cluster_id:
            raise ValueError("cluster_id is required")
        if not self.environment_id:
            raise ValueError("environment_id is required")
        if not self.allow_dynamic_registration and not self.dns_names and not self.static_seeds:
            raise ValueError("At least one of dns_names or static_seeds is required")
        if self.candidate_set_size < 1:
            raise ValueError("candidate_set_size must be at least 1")
        if self.primary_connections < 1:
            raise ValueError("primary_connections must be at least 1")
        if not 0.0 < self.ewma_alpha <= 1.0:
            raise ValueError("ewma_alpha must be in (0, 1]")
        if self.node_role not in ("client", "gate", "manager", "worker"):
            raise ValueError(f"Invalid node_role: {self.node_role}")
