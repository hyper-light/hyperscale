"""
Pre-allocated byte constants for SWIM protocol.

These constants are defined once at module load time to avoid
repeated string encoding and memory allocation during message
processing. Using pre-allocated bytes for hot-path operations
can provide significant performance improvements.

Usage:
    from ..core.constants import MSG_PROBE, MSG_ACK, STATUS_OK
"""

# =============================================================================
# Message Type Prefixes (used in message parsing/construction)
# =============================================================================

# Core protocol messages
MSG_PROBE = b'PROBE:'
MSG_ACK = b'ACK:'
MSG_PING_REQ = b'PING_REQ:'
MSG_PING_REQ_ACK = b'PING_REQ_ACK:'

# Membership messages
MSG_JOIN = b'JOIN'
MSG_LEAVE = b'LEAVE'
MSG_SUSPECT = b'SUSPECT:'
MSG_ALIVE = b'ALIVE:'

# Leadership messages
MSG_CLAIM = b'CLAIM:'
MSG_VOTE = b'VOTE:'
MSG_PREVOTE_REQ = b'PREVOTE_REQ:'
MSG_PREVOTE_RESP = b'PREVOTE_RESP:'
MSG_ELECTED = b'ELECTED:'
MSG_HEARTBEAT = b'HEARTBEAT:'
MSG_STEPDOWN = b'STEPDOWN:'

# =============================================================================
# Status Bytes (used in node state tracking)
# =============================================================================

STATUS_OK = b'OK'
STATUS_JOIN = b'JOIN'
STATUS_SUSPECT = b'SUSPECT'
STATUS_DEAD = b'DEAD'

# =============================================================================
# Update Type Bytes (used in gossip encoding)
# =============================================================================

UPDATE_ALIVE = b'alive'
UPDATE_SUSPECT = b'suspect'
UPDATE_DEAD = b'dead'
UPDATE_JOIN = b'join'
UPDATE_LEAVE = b'leave'

# =============================================================================
# Delimiters and Separators
# =============================================================================

DELIM_COLON = b':'
DELIM_PIPE = b'|'
DELIM_ARROW = b'>'
DELIM_SEMICOLON = b';'

# Pre-encoded common strings
EMPTY_BYTES = b''

# =============================================================================
# Numeric Encoding Cache (for small numbers 0-255)
# =============================================================================

# Pre-encode small integers for fast lookup
# This avoids str(n).encode() for common values
_SMALL_INT_CACHE: dict[int, bytes] = {i: str(i).encode() for i in range(256)}


def encode_int(n: int) -> bytes:
    """
    Encode an integer to bytes, using cache for small values.
    
    For integers 0-255, returns a pre-allocated bytes object.
    For larger integers, encodes on demand.
    """
    if 0 <= n < 256:
        return _SMALL_INT_CACHE[n]
    return str(n).encode()


# =============================================================================
# Boolean Bytes
# =============================================================================

BOOL_TRUE = b'1'
BOOL_FALSE = b'0'


def encode_bool(b: bool) -> bytes:
    """Encode a boolean to bytes."""
    return BOOL_TRUE if b else BOOL_FALSE

