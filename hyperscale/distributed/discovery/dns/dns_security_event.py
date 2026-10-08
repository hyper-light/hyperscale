"""``DNSSecurityEvent`` -- pickled under the namespace
``hyperscale.distributed.discovery.dns.security`` (see that module)."""

from dataclasses import dataclass, field

from .security_shared import _DEFAULT_CLOCK
from .dns_security_violation import DNSSecurityViolation


@dataclass(slots=True)
class DNSSecurityEvent:
    """Record of a DNS security violation."""

    hostname: str
    """The hostname that triggered the violation."""

    violation_type: DNSSecurityViolation
    """Type of security violation detected."""

    resolved_ip: str
    """The IP address that was resolved."""

    details: str
    """Human-readable description of the violation."""

    timestamp: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())
    """When this violation occurred."""

    previous_ip: str | None = None
    """Previous IP address (for change detection)."""
