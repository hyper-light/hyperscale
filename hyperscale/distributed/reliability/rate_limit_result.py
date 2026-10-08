"""``RateLimitResult`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    retry_after_seconds: float = 0.0
    tokens_remaining: float = 0.0
