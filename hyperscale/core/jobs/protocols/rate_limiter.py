"""
Rate limiting for protocol message handling.

This module provides:
- RateLimitExceeded exception
- Re-exports ServerRateLimiter from the reliability module
"""


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    pass


# Re-export ServerRateLimiter from reliability module
# This import is placed after RateLimitExceeded to avoid circular import issues
# when other modules need just the exception class.
from hyperscale.distributed_rewrite.reliability.rate_limiting import (
    ServerRateLimiter as ServerRateLimiter,
)
