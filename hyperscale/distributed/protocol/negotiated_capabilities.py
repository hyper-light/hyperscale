"""``NegotiatedCapabilities`` -- pickled under the namespace
``hyperscale.distributed.protocol.version`` (see that module)."""

from dataclasses import dataclass

from .protocol_version import ProtocolVersion


@dataclass(slots=True)
class NegotiatedCapabilities:
    """
    Result of capability negotiation between two nodes.

    Attributes:
        local_version: Our protocol version.
        remote_version: Remote node's protocol version.
        common_features: Features both nodes support.
        compatible: Whether the versions are compatible.
    """

    local_version: ProtocolVersion
    remote_version: ProtocolVersion
    common_features: set[str]
    compatible: bool

    def supports(self, feature: str) -> bool:
        """Check if a feature is available after negotiation."""
        return feature in self.common_features
