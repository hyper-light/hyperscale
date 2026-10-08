"""``NegativeEntry`` -- pickled under the namespace
``hyperscale.distributed.discovery.dns.negative_cache`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class NegativeEntry:
    """A cached negative result for DNS lookup."""

    hostname: str
    """The hostname that failed resolution."""

    error_message: str
    """Description of the failure."""

    cached_at: float
    """Timestamp when this entry was cached."""

    failure_count: int = 1
    """Number of consecutive failures for this hostname."""
