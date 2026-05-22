"""
NodeCapabilitiesRef — opaque wrapper over AD-25 NodeCapabilities so the
cluster module's model package does not directly import the protocol
versioning machinery. The serialized form is whatever AD-25 emits;
this layer just passes it through.

AD-25's actual NodeCapabilities class lives at
hyperscale.distributed.protocol.version. The cluster module references
the wire format only — the protocol-version handshake itself stays in
the AD-25 layer.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NodeCapabilitiesRef:
    """
    Fields:
        version              AD-25 negotiated wire-protocol version.
        capabilities_blob    Opaque bytes — the AD-25 capabilities
                             payload as it would appear on the wire.
                             Sorted by key on serialization for AD-52
                             §15 determinism.
    """

    version: str
    capabilities_blob: bytes = b""
