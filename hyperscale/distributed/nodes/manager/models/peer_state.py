"""
Manager peer state tracking.

Tracks state for peer managers in the SWIM cluster including addresses,
health status, and heartbeat information.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PeerState:
    """
    State for tracking a single manager peer.

    Used for quorum calculations, failure detection, and state sync
    coordination between manager peers.
    """

    node_id: str
    tcp_host: str
    tcp_port: int
    udp_host: str
    udp_port: int
    datacenter_id: str
    is_leader: bool = False
    term: int = 0
    state_version: int = 0
    last_seen: float = 0.0
    is_active: bool = False
    epoch: int = 0

    @property
    def tcp_addr(self) -> tuple[str, int]:
        """TCP address tuple."""
        return (self.tcp_host, self.tcp_port)

    @property
    def udp_addr(self) -> tuple[str, int]:
        """UDP address tuple."""
        return (self.udp_host, self.udp_port)


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
