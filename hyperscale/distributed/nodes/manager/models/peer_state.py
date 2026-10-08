"""
Manager peer state tracking.

Tracks state for peer managers in the SWIM cluster including addresses,
health status, and heartbeat information.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from typing import Any

from .gate_peer_state import GatePeerState


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

_REHOMED = (
    GatePeerState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
