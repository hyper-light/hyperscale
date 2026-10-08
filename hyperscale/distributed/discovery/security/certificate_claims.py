"""``CertificateClaims`` -- pickled under the namespace
``hyperscale.distributed.discovery.security.role_validator`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models.distributed import NodeRole


@dataclass(slots=True, frozen=True)
class CertificateClaims:
    """Claims extracted from an mTLS certificate."""

    cluster_id: str
    """Cluster identifier from certificate CN or SAN."""

    environment_id: str
    """Environment identifier (prod, staging, dev)."""

    role: NodeRole
    """Node role from certificate OU or custom extension."""

    node_id: str
    """Unique node identifier."""

    datacenter_id: str = ""
    """Optional datacenter identifier."""

    region_id: str = ""
    """Optional region identifier."""
