"""
Manager peer state tracking for worker.

Tracks information about known managers including their addresses,
health status, and circuit breaker state.
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class ManagerPeerState:
    """
    State tracking for a manager peer known to this worker.

    Contains all information needed to communicate with and track
    the health of a manager node.
    """

    manager_id: str
    tcp_host: str
    tcp_port: int
    udp_host: str
    udp_port: int
    datacenter: str
    is_leader: bool = False
    is_healthy: bool = True
    unhealthy_since: float | None = None
    state_epoch: int = 0
