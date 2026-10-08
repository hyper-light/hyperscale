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

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field

from .protocol_version import FEATURE_VERSIONS
from .protocol_version import CURRENT_PROTOCOL_VERSION
from .protocol_version import get_features_for_version
from .negotiated_capabilities import NegotiatedCapabilities
from .node_capabilities import NodeCapabilities
from .protocol_version import ProtocolVersion


def get_all_features() -> set[str]:
    """Get all defined feature names."""
    return set(FEATURE_VERSIONS.keys())


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

_REHOMED = (
    ProtocolVersion,
    NodeCapabilities,
    NegotiatedCapabilities,
    get_features_for_version,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
