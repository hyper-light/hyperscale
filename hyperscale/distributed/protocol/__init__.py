"""
Protocol module for distributed system communication.

This module provides:
- Version negotiation (AD-25)
- Capability handling
- Future: Message framing, serialization
"""

from hyperscale.distributed.protocol.version import (
    # Protocol versioning
    ProtocolVersion as ProtocolVersion,
    CURRENT_PROTOCOL_VERSION as CURRENT_PROTOCOL_VERSION,
    # Feature versions
    FEATURE_VERSIONS as FEATURE_VERSIONS,
    get_all_features as get_all_features,
    get_features_for_version as get_features_for_version,
    # Capabilities
    NodeCapabilities as NodeCapabilities,
    NegotiatedCapabilities as NegotiatedCapabilities,
    negotiate_capabilities as negotiate_capabilities,
)
