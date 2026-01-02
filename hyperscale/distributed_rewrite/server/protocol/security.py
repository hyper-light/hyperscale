"""
Security utilities for distributed_rewrite protocols.

Re-exports security components from hyperscale.core.jobs.protocols for
local use in the distributed_rewrite module.
"""

from hyperscale.core.jobs.protocols.replay_guard import (
    ReplayGuard as ReplayGuard,
    ReplayError as ReplayError,
    DEFAULT_MAX_AGE_SECONDS,
    DEFAULT_MAX_FUTURE_SECONDS,
    DEFAULT_WINDOW_SIZE,
)

from hyperscale.core.jobs.protocols.rate_limiter import (
    RateLimiter as RateLimiter,
    RateLimitExceeded as RateLimitExceeded,
    TokenBucket as TokenBucket,
    DEFAULT_REQUESTS_PER_SECOND,
    DEFAULT_BURST_SIZE,
    DEFAULT_MAX_SOURCES,
)


# Message size limits
MAX_MESSAGE_SIZE = 64 * 1024  # 64KB - maximum compressed message size
MAX_DECOMPRESSED_SIZE = 10 * 1024 * 1024  # 10MB - maximum decompressed size
MAX_COMPRESSION_RATIO = 100  # Maximum decompression ratio (compression bomb protection)


class MessageSizeError(Exception):
    """Raised when message size limits are exceeded."""
    pass


def validate_message_size(
    compressed_size: int,
    decompressed_size: int | None = None,
) -> None:
    """
    Validate message sizes for security.
    
    Args:
        compressed_size: Size of compressed message
        decompressed_size: Size after decompression (if known)
        
    Raises:
        MessageSizeError: If limits are exceeded
    """
    if compressed_size > MAX_MESSAGE_SIZE:
        raise MessageSizeError(
            f"Message too large: {compressed_size} > {MAX_MESSAGE_SIZE} bytes"
        )
    
    if decompressed_size is not None:
        if decompressed_size > MAX_DECOMPRESSED_SIZE:
            raise MessageSizeError(
                f"Decompressed message too large: {decompressed_size} > {MAX_DECOMPRESSED_SIZE} bytes"
            )
        
        # Check for compression bomb
        if compressed_size > 0:
            ratio = decompressed_size / compressed_size
            if ratio > MAX_COMPRESSION_RATIO:
                raise MessageSizeError(
                    f"Suspicious compression ratio: {ratio:.1f} > {MAX_COMPRESSION_RATIO}"
                )

