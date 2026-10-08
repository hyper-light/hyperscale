"""``DNSSecurityViolation`` -- pickled under the namespace
``hyperscale.distributed.discovery.dns.security`` (see that module)."""

from enum import Enum


class DNSSecurityViolation(Enum):
    """Types of DNS security violations."""

    IP_OUT_OF_RANGE = "ip_out_of_range"
    """Resolved IP is not in any allowed CIDR range."""

    UNEXPECTED_IP_CHANGE = "unexpected_ip_change"
    """IP changed from previously known value (possible hijacking)."""

    RAPID_IP_ROTATION = "rapid_ip_rotation"
    """IP changing too frequently (possible fast-flux attack)."""

    PRIVATE_IP_FOR_PUBLIC_HOST = "private_ip_for_public_host"
    """Private IP returned for a public hostname (possible rebinding)."""
