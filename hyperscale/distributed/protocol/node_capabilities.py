"""``NodeCapabilities`` -- pickled under the namespace
``hyperscale.distributed.protocol.version`` (see that module)."""

from dataclasses import dataclass, field

from .protocol_version import CURRENT_PROTOCOL_VERSION
from .protocol_version import get_features_for_version
from .protocol_version import ProtocolVersion


@dataclass(slots=True)
class NodeCapabilities:
    """
    Capabilities advertised by a node for negotiation.

    Used during handshake to determine which features both nodes support.

    Attributes:
        protocol_version: The node's protocol version.
        capabilities: Set of capability strings (features the node supports).
        node_version: Software version string (e.g., "hyperscale-1.2.3").
    """

    protocol_version: ProtocolVersion
    capabilities: set[str] = field(default_factory=set)
    node_version: str = ""

    def negotiate(self, other: "NodeCapabilities") -> set[str]:
        """
        Negotiate common capabilities with another node.

        Returns the intersection of both nodes' capabilities, limited to
        features supported by the lower protocol version.

        Args:
            other: The other node's capabilities.

        Returns:
            Set of features both nodes support.

        Raises:
            ValueError: If protocol versions are incompatible.
        """
        if not self.protocol_version.is_compatible_with(other.protocol_version):
            raise ValueError(
                f"Incompatible protocol versions: "
                f"{self.protocol_version} vs {other.protocol_version}"
            )

        # Use intersection of capabilities
        common = self.capabilities & other.capabilities

        # Filter to features supported by both versions
        min_version = (
            self.protocol_version
            if self.protocol_version.minor <= other.protocol_version.minor
            else other.protocol_version
        )

        return {
            cap for cap in common
            if min_version.supports_feature(cap)
        }

    def is_compatible_with(self, other: "NodeCapabilities") -> bool:
        """Check if this node is compatible with another."""
        return self.protocol_version.is_compatible_with(other.protocol_version)

    @classmethod
    def current(cls, node_version: str = "") -> "NodeCapabilities":
        """Create capabilities for the current protocol version."""
        return cls(
            protocol_version=CURRENT_PROTOCOL_VERSION,
            capabilities=get_features_for_version(CURRENT_PROTOCOL_VERSION),
            node_version=node_version,
        )
