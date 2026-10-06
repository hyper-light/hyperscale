from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class JoinedPeer:
    """A node this node was joined to at runtime (`hyperscale join`)."""

    datacenter: str
    tcp_address: tuple[str, int]
    udp_address: tuple[str, int]
