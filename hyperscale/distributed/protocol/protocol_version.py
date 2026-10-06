"""``ProtocolVersion`` -- pickled under the namespace
``hyperscale.distributed.protocol.version`` (see that module)."""

from dataclasses import dataclass


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


def get_features_for_version(version: ProtocolVersion) -> set[str]:
    """Get all features supported by a specific version."""
    return {
        feature
        for feature, required in FEATURE_VERSIONS.items()
        if version.major > required.major or (
            version.major == required.major and version.minor >= required.minor
        )
    }
