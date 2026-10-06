"""``NodeAddress`` -- pickled under the namespace
``hyperscale.distributed.swim.core.node_id`` (see that module)."""

from dataclasses import dataclass

from .node_id_model import NodeId


@dataclass(slots=True)
class NodeAddress:
    """
    Combines a NodeId with network address information.

    Historically this separated the logical ``node_id`` from the
    network ``(host, port)``. Now that ``NodeId`` is itself
    topology-derived (its identity already includes host/port), this
    wrapper is a thin convenience for the call sites that want the
    address fields alongside the id without re-deriving them.
    """

    node_id: NodeId
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.node_id.short}@{self.host}:{self.port}"

    def __repr__(self) -> str:
        return f"NodeAddress({self.node_id!s}, {self.host}:{self.port})"

    def __hash__(self) -> int:
        return hash(self.node_id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeAddress):
            return self.node_id == other.node_id
        return False

    @property
    def addr_tuple(self) -> tuple[str, int]:
        """Get the (host, port) tuple for socket operations."""
        return (self.host, self.port)

    @property
    def addr_str(self) -> str:
        """Get 'host:port' string."""
        return f"{self.host}:{self.port}"

    def to_bytes(self) -> bytes:
        """Encode for network transmission: node_id|host:port"""
        return f"{self.node_id}|{self.host}:{self.port}".encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "NodeAddress":
        """Decode from network transmission."""
        s = data.decode("utf-8")
        node_id_str, addr = s.split("|", 1)
        host, port_str = addr.rsplit(":", 1)
        return cls(
            node_id=NodeId.parse(node_id_str),
            host=host,
            port=int(port_str),
        )
