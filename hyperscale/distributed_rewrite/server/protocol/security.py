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

from hyperscale.distributed_rewrite.reliability import (
    ServerRateLimiter as ServerRateLimiter,
)
from hyperscale.core.jobs.protocols.rate_limiter import (
    RateLimitExceeded as RateLimitExceeded,
)


# Message size limits
# Job submissions with workflow classes can be large when pickled
MAX_MESSAGE_SIZE = 1 * 1024 * 1024  # 1MB - maximum compressed message size
MAX_DECOMPRESSED_SIZE = 50 * 1024 * 1024  # 50MB - maximum decompressed size
MAX_COMPRESSION_RATIO = 100  # Maximum decompression ratio (compression bomb protection)


class MessageSizeError(Exception):
    """Raised when message size limits are exceeded."""
    pass


class AddressValidationError(Exception):
    """Raised when address validation fails."""
    pass


# Valid port range
MIN_PORT = 1
MAX_PORT = 65535


def parse_address(address_bytes: bytes) -> tuple[str, int]:
    """
    Safely parse and validate a host:port address from bytes.
    
    Args:
        address_bytes: Bytes containing "host:port" format
        
    Returns:
        Tuple of (host, port)
        
    Raises:
        AddressValidationError: If address is malformed or invalid
    """
    # Decode with error handling
    try:
        address_str = address_bytes.decode('utf-8')
    except UnicodeDecodeError:
        raise AddressValidationError("Address contains invalid UTF-8 bytes")
    
    # Split host:port
    if ':' not in address_str:
        raise AddressValidationError("Address missing port separator ':'")
    
    # Handle IPv6 addresses like [::1]:8080
    if address_str.startswith('['):
        # IPv6 format: [host]:port
        bracket_end = address_str.rfind(']')
        if bracket_end == -1:
            raise AddressValidationError("Invalid IPv6 address format: missing closing bracket")
        if bracket_end + 1 >= len(address_str) or address_str[bracket_end + 1] != ':':
            raise AddressValidationError("Invalid IPv6 address format: missing port after bracket")
        host = address_str[1:bracket_end]
        port_str = address_str[bracket_end + 2:]
    else:
        # IPv4 or hostname format: host:port
        parts = address_str.rsplit(':', 1)
        if len(parts) != 2:
            raise AddressValidationError("Address must be in host:port format")
        host, port_str = parts
    
    # Validate host is not empty
    if not host:
        raise AddressValidationError("Host cannot be empty")
    
    # Validate host doesn't contain dangerous characters
    # Allow: alphanumeric, dots, hyphens, colons (for IPv6)
    allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-:')
    if not all(c in allowed_chars for c in host):
        raise AddressValidationError("Host contains invalid characters")
    
    # Parse and validate port
    try:
        port = int(port_str)
    except ValueError:
        raise AddressValidationError(f"Port is not a valid integer: {port_str!r}")
    
    if port < MIN_PORT or port > MAX_PORT:
        raise AddressValidationError(f"Port {port} out of valid range ({MIN_PORT}-{MAX_PORT})")
    
    return (host, port)


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

