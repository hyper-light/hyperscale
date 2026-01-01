"""
Message size limits and validation.

Provides constants and utilities for enforcing message size limits
to prevent memory exhaustion attacks.
"""

# Maximum sizes (in bytes)
MAX_UDP_MESSAGE_SIZE = 65535  # UDP max datagram size
MAX_COMPRESSED_SIZE = 10 * 1024 * 1024  # 10 MB - max size before decompression
MAX_DECOMPRESSED_SIZE = 100 * 1024 * 1024  # 100 MB - max size after decompression

# Compression ratio limits (to detect compression bombs)
MAX_COMPRESSION_RATIO = 100  # Decompressed can't be >100x compressed size


class MessageSizeError(Exception):
    """Raised when message size limits are exceeded."""
    pass


def validate_compressed_size(data: bytes, raise_on_error: bool = True) -> bool:
    """
    Validate compressed message size before decompression.
    
    Args:
        data: Compressed message data
        raise_on_error: If True, raise MessageSizeError
        
    Returns:
        True if valid, False if too large
        
    Raises:
        MessageSizeError: If raise_on_error and message too large
    """
    size = len(data)
    if size > MAX_COMPRESSED_SIZE:
        if raise_on_error:
            raise MessageSizeError(
                f"Compressed message too large: {size} bytes > {MAX_COMPRESSED_SIZE} bytes"
            )
        return False
    return True


def validate_decompressed_size(
    data: bytes,
    compressed_size: int,
    raise_on_error: bool = True
) -> bool:
    """
    Validate decompressed message size.
    
    Also checks compression ratio to detect compression bombs.
    
    Args:
        data: Decompressed message data
        compressed_size: Size of the compressed data
        raise_on_error: If True, raise MessageSizeError
        
    Returns:
        True if valid, False if limits exceeded
        
    Raises:
        MessageSizeError: If raise_on_error and limits exceeded
    """
    size = len(data)
    
    # Check absolute size
    if size > MAX_DECOMPRESSED_SIZE:
        if raise_on_error:
            raise MessageSizeError(
                f"Decompressed message too large: {size} bytes > {MAX_DECOMPRESSED_SIZE} bytes"
            )
        return False
    
    # Check compression ratio (compression bomb detection)
    if compressed_size > 0:
        ratio = size / compressed_size
        if ratio > MAX_COMPRESSION_RATIO:
            if raise_on_error:
                raise MessageSizeError(
                    f"Suspicious compression ratio: {ratio:.1f}x > {MAX_COMPRESSION_RATIO}x "
                    f"(possible compression bomb)"
                )
            return False
    
    return True


def get_size_stats(compressed: bytes, decompressed: bytes) -> dict:
    """Get size statistics for a message."""
    compressed_size = len(compressed)
    decompressed_size = len(decompressed)
    ratio = decompressed_size / compressed_size if compressed_size > 0 else 0
    
    return {
        'compressed_bytes': compressed_size,
        'decompressed_bytes': decompressed_size,
        'compression_ratio': ratio,
        'compression_savings_percent': (1 - compressed_size / decompressed_size) * 100 if decompressed_size > 0 else 0,
    }

