from hyperscale.distributed.swim.health_aware_server import HealthAwareServer

class NodeIdentityReader:
    """Reads what every node's dashboard shows in common -- who the node
    is, where it listens, how long it has run, its SWIM view and its local
    health -- from the node's own state, without awaiting or sending
    anything."""

    def __init__(self, node: HealthAwareServer, role: str) -> None:
        self._node = node
        self._role = role

    def identity_lines(self) -> list[str]:
        """Who the node is and where it listens: its role and node id
        (whose short form begins with its datacenter) and its addresses."""
        tcp_host, tcp_port = self._node.tcp_address
        udp_host, udp_port = self._node.udp_address
        return [
            f"{self._role.upper()} {self._node.node_id.short}",
            f"tcp {tcp_host}:{tcp_port}",
            f"udp {udp_host}:{udp_port}",
        ]

    def uptime_seconds(self) -> float:
        """How long the node has run."""
        return self._node.get_metrics()["uptime_seconds"]

    def health_lines(self, overload_state: str) -> list[str]:
        """The node's local health: its overload state, its local health
        multiplier and its degradation level."""
        return [
            f"load {overload_state} lhm {self._node._local_health.score}",
            f"degradation {self._node._degradation.current_level.name.lower()}",
        ]

    def swim_lines(self) -> list[str]:
        """The node's SWIM view: how many peers it holds alive, dead and
        suspect."""
        peer_counts = self._node._incarnation_tracker.get_stats()
        return [
            f"swim ok {peer_counts['ok_nodes']} dead {peer_counts['dead_nodes']}",
            f"swim suspect {peer_counts['suspect_nodes']}",
        ]
