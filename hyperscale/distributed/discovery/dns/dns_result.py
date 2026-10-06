"""``DNSResult`` -- pickled under the namespace
``hyperscale.distributed.discovery.dns.resolver`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field

from .resolver_shared import _DEFAULT_CLOCK
from .srv_record import SRVRecord

if TYPE_CHECKING:
    from .dns_error import DNSError


@dataclass(slots=True)
class DNSResult:
    """Result of a DNS lookup."""

    hostname: str
    """The hostname that was resolved."""

    addresses: list[str]
    """Resolved IP addresses."""

    port: int | None = None
    """Port from SRV record (if applicable)."""

    srv_records: list[SRVRecord] = field(default_factory=list)
    """SRV records if this was an SRV query."""

    ttl_seconds: float = 60.0
    """Time-to-live for this result."""

    resolved_at: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())
    """Timestamp when this result was resolved."""

    target_errors: list["DNSError"] = field(default_factory=list)
    """SRV targets that failed to resolve while others answered."""

    @property
    def is_expired(self) -> bool:
        """Check if this result has expired."""
        return _DEFAULT_CLOCK.monotonic() - self.resolved_at > self.ttl_seconds
