"""``NodeId`` -- pickled under the namespace
``hyperscale.distributed.swim.core.node_id`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from hyperscale.distributed.runtime import Clock, RealClock

if TYPE_CHECKING:
    from .node_address import NodeAddress

_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(frozen=True)
class NodeId:
    """
    Topology-derived node identifier for multi-datacenter SWIM clusters.

    The ID is:
    - Globally unique (``host:port`` is unique per node in every mode —
      the network stack enforces it in REAL, and the simulation
      coordinator rejects sockname collisions in SIM).
    - Deterministic given the node's placement — no RNG, no wall clock in
      the ordered identity.
    - Lexicographically sortable by ``(datacenter, priority, host, port)``.
    - Human-readable for debugging.
    - Stable across the node's lifetime AND across restarts at the same
      address (topology-stable leadership / hash-ring placement).
    """

    datacenter: str
    priority: int
    host: str
    port: int
    # Non-ordered metadata: start timestamp for human-readable logs.
    # Excluded from ``_ordered_key`` (hence from ==/hash/<) so it can
    # never influence identity or leadership. ``_DEFAULT_CLOCK`` is the
    # swapped VirtualClock under SIM, so even this metadata is
    # deterministic there.
    created_ms: int = field(
        default_factory=lambda: int(_DEFAULT_CLOCK.time() * 1000)
    )

    def __post_init__(self):
        """Validate node ID components."""
        self._validate_placement()
        self._validate_endpoint()

    def _validate_placement(self) -> None:
        """Reject an empty datacenter or a priority outside 0-99."""
        if not self.datacenter:
            raise ValueError("datacenter cannot be empty")
        if not 0 <= self.priority <= 99:
            raise ValueError("priority must be between 0 and 99")

    def _validate_endpoint(self) -> None:
        """Reject an empty host or a port outside 0-65535."""
        if not self.host:
            raise ValueError("host cannot be empty")
        if not 0 <= self.port <= 65535:
            raise ValueError("port must be between 0 and 65535")

    def _ordered_key(self) -> tuple[str, int, str, int]:
        """The topology-derived identity that defines equality, hashing,
        and leadership order. ``created_ms`` is deliberately excluded."""
        return (self.datacenter, self.priority, self.host, self.port)

    def __str__(self) -> str:
        """Full string representation of the node ID."""
        return (
            f"{self.datacenter}-{self.priority:02d}-"
            f"{self.host}-{self.port:05d}-{self.created_ms:013x}"
        )

    def __repr__(self) -> str:
        return f"NodeId({self!s})"

    def __hash__(self) -> int:
        return hash(self._ordered_key())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeId):
            return self._ordered_key() == other._ordered_key()
        if isinstance(other, str):
            return str(self) == other
        return False

    def __lt__(self, other: "NodeId") -> bool:
        """Order by topology: datacenter, then priority (lower = higher
        priority), then host, then port. Deterministic and stable across
        restarts — no wall clock, no randomness participates."""
        return self._ordered_key() < other._ordered_key()

    @property
    def short(self) -> str:
        """Short form for logging: ``DC-EAST-01-9000``."""
        return f"{self.datacenter}-{self.priority:02d}-{self.port}"

    @property
    def full(self) -> str:
        """Full string representation of the node ID (alias for str())."""
        return str(self)

    @property
    def age_seconds(self) -> float:
        """How old this node ID is in seconds (observability only)."""
        return (_DEFAULT_CLOCK.time() * 1000 - self.created_ms) / 1000

    @classmethod
    def parse(cls, s: str) -> "NodeId":
        """
        Parse a node ID string back into a NodeId object.

        Format: ``{datacenter}-{priority:02d}-{host}-{port:05d}-{created_ms:013x}``.
        The datacenter may itself contain dashes; the three trailing
        fields (host, port, created_ms) are peeled off from the right,
        then the priority is peeled off after them, leaving the
        datacenter. Round-trips a ``host`` that contains no dashes
        (IP literals and simple names — the deployment shape); node-id
        strings otherwise travel the wire opaquely and are never
        reconstructed, so this is used only by tests and the
        ``NodeAddress`` byte codec.

        Raises:
            ValueError: If the string is not a valid node ID.
        """
        try:
            head, host, port_str, ts_str = s.rsplit("-", 3)
            datacenter, priority_str = head.rsplit("-", 1)
            return cls(
                datacenter=datacenter,
                priority=int(priority_str),
                host=host,
                port=int(port_str),
                created_ms=int(ts_str, 16),
            )
        except Exception as e:
            raise ValueError(f"Invalid node ID format '{s}': {e}") from e

    @classmethod
    def generate(
        cls,
        datacenter: str,
        priority: int = 50,
        *,
        host: str,
        port: int,
        created_ms: int | None = None,
    ) -> "NodeId":
        """
        Generate a node ID for a node in the given datacenter at the
        given network address.

        Args:
            datacenter: Datacenter identifier.
            priority: Leadership priority (0-99, lower = higher priority).
            host: The node's host (its topology identity, keyword-only).
            port: The node's port (its topology identity, keyword-only).
            created_ms: The start time a resumed identity keeps (D1); a
                fresh one is stamped now.

        Returns:
            New NodeId instance — deterministic given these inputs.
        """
        if created_ms is None:
            return cls(datacenter=datacenter, priority=priority, host=host, port=port)
        return cls(datacenter=datacenter, priority=priority, host=host, port=port, created_ms=created_ms)

    @staticmethod
    def placement_of(node_id_full: str) -> str:
        """A node id's placement -- everything but its start time: two ids
        with one placement name the same node at the same address."""
        return node_id_full.rsplit("-", 1)[0]

    @staticmethod
    def created_ms_of(node_id_full: str) -> int:
        """The start time a node id carries."""
        return int(node_id_full.rsplit("-", 1)[1], 16)

    def to_bytes(self) -> bytes:
        """Encode the node ID as bytes for network transmission."""
        return str(self).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "NodeId":
        """Decode a node ID from bytes."""
        return cls.parse(data.decode("utf-8"))

    def same_datacenter(self, other: "NodeId") -> bool:
        """Check if another node is in the same datacenter."""
        return self.datacenter == other.datacenter

    def has_higher_priority(self, other: "NodeId") -> bool:
        """Check if this node has higher priority (lower number) than another."""
        return self.priority < other.priority
