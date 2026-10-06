from hyperscale.distributed.swim.health_aware_server import HealthAwareServer

from .dashboard_formatting import format_duration


class NodeIdentityReader:
    """Reads what every node's dashboard shows in common -- who the node
    is, where it listens, how long it has run, its SWIM view and its local
    health -- from the node's own state, without awaiting or sending
    anything."""

    def __init__(self, node: HealthAwareServer, role: str) -> None:
        self._node = node
        self._role = role

    def identity_lines(self, lifecycle_state: str, overload_state: str) -> list[str]:
        """The identity panel: role and node id, datacenter, addresses,
        uptime and lifecycle state, and local health."""
        node_id = self._node.node_id
        tcp_host, tcp_port = self._node.tcp_address
        udp_host, udp_port = self._node.udp_address
        return [
            f"{self._role.upper()} {node_id.short}",
            f"dc {node_id.datacenter}",
            f"tcp {tcp_host}:{tcp_port}",
            f"udp {udp_host}:{udp_port}",
            f"up {format_duration(self._node.get_metrics()['uptime_seconds'])} {lifecycle_state}",
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
