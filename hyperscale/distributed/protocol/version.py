"""
Protocol Version and Capability Negotiation (AD-25).

This module provides version skew handling for the distributed system,
enabling rolling upgrades and backwards-compatible protocol evolution.

Key concepts:
- ProtocolVersion: Major.Minor versioning with compatibility checks
- NodeCapabilities: Feature capabilities for negotiation
- Feature version map: Tracks which version introduced each feature

Compatibility Rules:
- Same major version = compatible (may have different features)
- Different major version = incompatible (reject connection)
- Features only used if both nodes support them
"""

from dataclasses import dataclass, field


# =============================================================================
# Protocol Version
# =============================================================================

@dataclass(slots=True, frozen=True)
class ProtocolVersion:
    """
    Semantic version for protocol compatibility.

    Major version changes indicate breaking changes.
    Minor version changes add new features (backwards compatible).

    Compatibility Rules:
    - Compatible if major versions match
    - Features from higher minor versions are optional

    Attributes:
        major: Major version (breaking changes).
        minor: Minor version (new features).
    """

    major: int
    minor: int

    def is_compatible_with(self, other: "ProtocolVersion") -> bool:
        """
        Check if this version is compatible with another.

        Compatibility means same major version. The higher minor version
        node may support features the lower version doesn't, but they
        can still communicate using the common feature set.

        Args:
            other: The other protocol version to check.

        Returns:
            True if versions are compatible.
        """
        return self.major == other.major

    def supports_feature(self, feature: str) -> bool:
        """
        Check if this version supports a specific feature.

        Uses the FEATURE_VERSIONS map to determine if this version
        includes the feature.

        Args:
            feature: Feature name to check.

        Returns:
            True if this version supports the feature.
        """
        required_version = FEATURE_VERSIONS.get(feature)
        if required_version is None:
            return False

        # Feature is supported if our version >= required version
        if self.major > required_version.major:
            return True
        if self.major < required_version.major:
            return False
        return self.minor >= required_version.minor

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"

    def __repr__(self) -> str:
        return f"ProtocolVersion({self.major}, {self.minor})"


# =============================================================================
# Feature Version Map
# =============================================================================

# Maps feature names to the minimum version that introduced them
# Used by ProtocolVersion.supports_feature() and capability negotiation
FEATURE_VERSIONS: dict[str, ProtocolVersion] = {
    # Base protocol features (1.0)
    "job_submission": ProtocolVersion(1, 0),
    "workflow_dispatch": ProtocolVersion(1, 0),
    "heartbeat": ProtocolVersion(1, 0),
    "cancellation": ProtocolVersion(1, 0),

    # Batched stats (1.1)
    "batched_stats": ProtocolVersion(1, 1),
    "stats_compression": ProtocolVersion(1, 1),

    # Client reconnection and fence tokens (1.2)
    "client_reconnection": ProtocolVersion(1, 2),
    "fence_tokens": ProtocolVersion(1, 2),
    "idempotency_keys": ProtocolVersion(1, 2),

    # Rate limiting (1.3)
    "rate_limiting": ProtocolVersion(1, 3),
    "retry_after": ProtocolVersion(1, 3),

    # Health extensions (1.4)
    "healthcheck_extensions": ProtocolVersion(1, 4),
    "health_piggyback": ProtocolVersion(1, 4),
    "three_signal_health": ProtocolVersion(1, 4),
}


# Current protocol version
CURRENT_PROTOCOL_VERSION = ProtocolVersion(1, 4)


def get_all_features() -> set[str]:
    """Get all defined feature names."""
    return set(FEATURE_VERSIONS.keys())


def get_features_for_version(version: ProtocolVersion) -> set[str]:
    """Get all features supported by a specific version."""
    return {
        feature
        for feature, required in FEATURE_VERSIONS.items()
        if version.major > required.major or (
            version.major == required.major and version.minor >= required.minor
        )
    }


# =============================================================================
# Node Capabilities
# =============================================================================

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


# =============================================================================
# Version Negotiation Result
# =============================================================================

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


def negotiate_capabilities(
    local: NodeCapabilities,
    remote: NodeCapabilities,
) -> NegotiatedCapabilities:
    """
    Perform capability negotiation between two nodes.

    Args:
        local: Our capabilities.
        remote: Remote node's capabilities.

    Returns:
        NegotiatedCapabilities with the negotiation result.
    """
    compatible = local.is_compatible_with(remote)

    if compatible:
        common_features = local.negotiate(remote)
    else:
        common_features = set()

    return NegotiatedCapabilities(
        local_version=local.protocol_version,
        remote_version=remote.protocol_version,
        common_features=common_features,
        compatible=compatible,
    )
