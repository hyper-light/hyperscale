"""``GatePeerState`` -- pickled under the namespace
``hyperscale.distributed.nodes.manager.models.peer_state`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class GatePeerState:
    """
    State for tracking a gate peer.

    Managers track gates for job submission routing and result forwarding.
    """

    node_id: str
    tcp_host: str
    tcp_port: int
    udp_host: str
    udp_port: int
    datacenter_id: str
    is_leader: bool = False
    is_healthy: bool = True
    last_seen: float = 0.0
    epoch: int = 0

    @property
    def tcp_addr(self) -> tuple[str, int]:
        """TCP address tuple."""
        return (self.tcp_host, self.tcp_port)

    @property
    def udp_addr(self) -> tuple[str, int]:
        """UDP address tuple."""
        return (self.udp_host, self.udp_port)
