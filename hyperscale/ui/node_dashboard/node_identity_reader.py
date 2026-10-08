from hyperscale.distributed.swim.health_aware_server import HealthAwareServer
from hyperscale.ui.components.status_badge import StatusBadgeReading
from hyperscale.ui.styling.tones import StatusTone

from .status_tones import joined_label, nonzero_counts, state_tone, worst_tone

# A local health multiplier of zero is a node keeping up with its probes.
HEALTHY_LHM_SCORE = 0
# The degradation level of a node shedding nothing.
NO_DEGRADATION = "normal"

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

    def load_badge(self, overload_state: str) -> StatusBadgeReading:
        """The node's local health as a badge: its overload state, with its
        local health multiplier and degradation level only where they are
        not healthy, in the worst of their tones."""
        lhm_score = self._node._local_health.score
        degradation = self._node._degradation.current_level.name.lower()
        return StatusBadgeReading(
            joined_label([f"load {overload_state}", *self._load_problems(lhm_score, degradation)]),
            worst_tone([state_tone(overload_state), state_tone(degradation), self._lhm_tone(lhm_score)]),
        )

    def _load_problems(self, lhm_score: int, degradation: str) -> list[str]:
        """The local health values that are not healthy."""
        return [
            *([f"lhm {lhm_score}"] if lhm_score > HEALTHY_LHM_SCORE else []),
            *([f"degradation {degradation}"] if degradation != NO_DEGRADATION else []),
        ]

    def _lhm_tone(self, lhm_score: int) -> StatusTone:
        return "ok" if lhm_score <= HEALTHY_LHM_SCORE else "degraded"

    def swim_badge(self) -> StatusBadgeReading:
        """The node's SWIM view as a badge: the peers it holds alive, and
        any suspect or dead (worth a look)."""
        peer_counts = self._node._incarnation_tracker.get_stats()
        problems = nonzero_counts(((peer_counts["suspect_nodes"], "suspect"), (peer_counts["dead_nodes"], "dead")))
        return StatusBadgeReading(
            joined_label([f"swim {peer_counts['ok_nodes']} ok", *problems]), "degraded" if problems else "ok"
        )

    def swim_lines(self) -> list[str]:
        """The node's SWIM view: how many peers it holds alive, dead and
        suspect."""
        peer_counts = self._node._incarnation_tracker.get_stats()
        return [
            f"swim ok {peer_counts['ok_nodes']} dead {peer_counts['dead_nodes']}",
            f"swim suspect {peer_counts['suspect_nodes']}",
        ]
