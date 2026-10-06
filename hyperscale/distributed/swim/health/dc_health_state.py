"""``DCHealthState`` -- pickled under the namespace
``hyperscale.distributed.swim.health.federated_health_monitor`` (see that module)."""

from dataclasses import dataclass

from .cross_cluster_ack import CrossClusterAck
from .dc_reachability import DCReachability


@dataclass(slots=True)
class DCHealthState:
    """
    Gate's view of a datacenter's health.

    Combines probe reachability with self-reported health.
    """

    datacenter: str
    leader_udp_addr: tuple[str, int] | None = None
    leader_tcp_addr: tuple[str, int] | None = None
    leader_node_id: str = ""
    leader_term: int = 0

    # Probe state
    reachability: DCReachability = DCReachability.UNKNOWN
    last_probe_sent: float = 0.0
    last_ack_received: float = 0.0
    consecutive_failures: int = 0

    # External incarnation tracking
    incarnation: int = 0

    # Last known health (from ack)
    last_ack: CrossClusterAck | None = None

    # Suspicion timing
    suspected_at: float = 0.0

    @property
    def effective_health(self) -> str:
        """Combine reachability and reported health."""
        if self.reachability == DCReachability.UNKNOWN:
            return "UNKNOWN"
        if self.reachability == DCReachability.UNREACHABLE:
            return "UNREACHABLE"
        if self.reachability == DCReachability.SUSPECTED:
            return "SUSPECTED"
        if self.last_ack:
            return self.last_ack.dc_health
        return "UNKNOWN"

    @property
    def is_healthy_for_jobs(self) -> bool:
        """Can this DC accept new jobs?"""
        if self.reachability in (DCReachability.UNKNOWN, DCReachability.UNREACHABLE):
            return False
        if not self.last_ack:
            return False
        return self.last_ack.dc_health in ("HEALTHY", "DEGRADED", "BUSY")

    @property
    def has_successful_probe(self) -> bool:
        """Return whether this DC has ever answered a federated probe."""
        return self.last_ack is not None and self.last_ack_received > 0.0
