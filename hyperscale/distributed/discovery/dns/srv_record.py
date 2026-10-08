"""``SRVRecord`` -- pickled under the namespace
``hyperscale.distributed.discovery.dns.resolver`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class SRVRecord:
    """Represents a DNS SRV record."""

    priority: int
    """Priority of the target host (lower values are preferred)."""

    weight: int
    """Weight for hosts with the same priority (for load balancing)."""

    port: int
    """Port number of the service."""

    target: str
    """Target hostname."""
