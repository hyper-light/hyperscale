"""
Gate discovery service module (AD-28).

Provides adaptive peer and manager selection with locality awareness.

Classes:
- DiscoveryService: Peer discovery with adaptive selection
- RoleValidator: mTLS-based role validation

These are re-exported from the discovery package.
"""

from hyperscale.distributed_rewrite.discovery import DiscoveryService
from hyperscale.distributed_rewrite.discovery.security.role_validator import (
    RoleValidator,
    CertificateClaims,
)

__all__ = [
    "DiscoveryService",
    "RoleValidator",
    "CertificateClaims",
]
