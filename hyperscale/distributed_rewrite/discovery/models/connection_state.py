"""
Connection state model for the discovery system.
"""

from enum import IntEnum


class ConnectionState(IntEnum):
    """
    State of a connection to a peer.

    Used by the connection pool to track connection lifecycle.
    """

    DISCONNECTED = 0
    """No active connection to the peer."""

    CONNECTING = 1
    """Connection attempt in progress."""

    CONNECTED = 2
    """Connection established and healthy."""

    DRAINING = 3
    """Connection is being gracefully closed (no new requests)."""

    FAILED = 4
    """Connection failed and awaiting retry or eviction."""
