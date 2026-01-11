"""
Peer information models for the discovery system.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from functools import total_ordering


@total_ordering
class PeerHealth(Enum):
    """
    Health status of a peer.

    Ordering: HEALTHY > UNKNOWN > DEGRADED > UNHEALTHY > EVICTED
    Higher values indicate better health.
    """
    EVICTED = ("evicted", 0)       # Removed from pool
    UNHEALTHY = ("unhealthy", 1)   # Failed consecutive probes
    DEGRADED = ("degraded", 2)     # High error rate or latency
    UNKNOWN = ("unknown", 3)       # Not yet probed
    HEALTHY = ("healthy", 4)       # Responding normally

    def __init__(self, label: str, order: int) -> None:
        self._label = label
        self._order = order

    @property
    def value(self) -> str:
        """Return the string value for serialization."""
        return self._label

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, PeerHealth):
            return NotImplemented
        return self._order < other._order

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PeerHealth):
            return NotImplemented
        return self._order == other._order

    def __hash__(self) -> int:
        return hash(self._label)


@dataclass(slots=True)
class PeerInfo:
    """
    Information about a discovered peer.

    Tracks connection details, health metrics, and locality information
    for peer selection and connection management.
    """

    # ===== Identity =====
    peer_id: str
    """Unique identifier for this peer (typically node_id)."""

    host: str
    """Hostname or IP address."""

    port: int
    """Port number."""

    role: str
    """Node role ('client', 'gate', 'manager', 'worker')."""

    # ===== Cluster/Environment =====
    cluster_id: str = ""
    """Cluster this peer belongs to."""

    environment_id: str = ""
    """Environment this peer belongs to."""

    # ===== Locality =====
    datacenter_id: str = ""
    """Peer's datacenter identifier."""

    region_id: str = ""
    """Peer's region identifier."""

    # ===== Health Metrics =====
    health: PeerHealth = PeerHealth.UNKNOWN
    """Current health status."""

    ewma_latency_ms: float = 0.0
    """Exponentially weighted moving average latency in milliseconds."""

    error_rate: float = 0.0
    """Recent error rate (0.0 - 1.0)."""

    consecutive_failures: int = 0
    """Number of consecutive failures."""

    total_requests: int = 0
    """Total requests sent to this peer."""

    total_errors: int = 0
    """Total errors from this peer."""

    # ===== Timing =====
    discovered_at: float = field(default_factory=time.monotonic)
    """Timestamp when peer was discovered."""

    last_seen_at: float = 0.0
    """Timestamp of last successful interaction."""

    last_failure_at: float = 0.0
    """Timestamp of last failure."""

    # ===== Selection Score =====
    rendezvous_score: float = 0.0
    """Cached rendezvous hash score for this peer."""

    health_weight: float = 1.0
    """Weight multiplier based on health (0.1 - 1.0)."""

    @property
    def address(self) -> tuple[str, int]:
        """Return (host, port) tuple."""
        return (self.host, self.port)

    @property
    def address_string(self) -> str:
        """Return 'host:port' string."""
        return f"{self.host}:{self.port}"

    def record_success(self, latency_ms: float, ewma_alpha: float = 0.2) -> None:
        """
        Record a successful interaction.

        Args:
            latency_ms: Observed latency in milliseconds
            ewma_alpha: Smoothing factor for EWMA update
        """
        self.total_requests += 1
        self.consecutive_failures = 0
        self.last_seen_at = time.monotonic()

        # Update EWMA latency
        if self.ewma_latency_ms == 0.0:
            self.ewma_latency_ms = latency_ms
        else:
            self.ewma_latency_ms = (
                ewma_alpha * latency_ms +
                (1 - ewma_alpha) * self.ewma_latency_ms
            )

        # Update error rate (decaying)
        self.error_rate = max(0.0, self.error_rate * 0.95)

        # Update health
        self._update_health()

    def record_failure(self) -> None:
        """Record a failed interaction."""
        self.total_requests += 1
        self.total_errors += 1
        self.consecutive_failures += 1
        self.last_failure_at = time.monotonic()

        # Update error rate
        error_increment = 1.0 / max(1, self.total_requests)
        self.error_rate = min(1.0, self.error_rate + error_increment)

        # Update health
        self._update_health()

    def _update_health(self) -> None:
        """Update health status based on metrics."""
        if self.consecutive_failures >= 3:
            self.health = PeerHealth.UNHEALTHY
            self.health_weight = 0.1
        elif self.error_rate > 0.10:
            self.health = PeerHealth.DEGRADED
            self.health_weight = 0.5
        elif self.error_rate > 0.05:
            self.health = PeerHealth.DEGRADED
            self.health_weight = 0.7
        else:
            self.health = PeerHealth.HEALTHY
            self.health_weight = 1.0

    def should_evict(
        self,
        error_rate_threshold: float,
        consecutive_failure_limit: int,
        latency_threshold_ms: float,
    ) -> bool:
        """
        Check if this peer should be evicted from the connection pool.

        Args:
            error_rate_threshold: Max acceptable error rate
            consecutive_failure_limit: Max consecutive failures
            latency_threshold_ms: Max acceptable latency

        Returns:
            True if peer should be evicted
        """
        if self.consecutive_failures >= consecutive_failure_limit:
            return True
        if self.error_rate > error_rate_threshold:
            return True
        if self.ewma_latency_ms > latency_threshold_ms:
            return True
        return False

    def matches_locality(self, datacenter_id: str, region_id: str) -> tuple[bool, bool]:
        """
        Check locality match with given datacenter and region.

        Returns:
            Tuple of (same_datacenter, same_region)
        """
        same_dc = self.datacenter_id == datacenter_id if datacenter_id else False
        same_region = self.region_id == region_id if region_id else False
        return (same_dc, same_region)

    def __hash__(self) -> int:
        return hash((self.peer_id, self.host, self.port))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PeerInfo):
            return False
        return (
            self.peer_id == other.peer_id and
            self.host == other.host and
            self.port == other.port
        )
