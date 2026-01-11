"""
Result from a message handler.

Encapsulates the response bytes and any metadata
the handler wants to communicate.
"""

from dataclasses import dataclass


@dataclass(slots=True)
class HandlerResult:
    """
    Result from a message handler.

    Encapsulates the response bytes and any side effects
    the handler wants to communicate.
    """

    response: bytes
    """Response bytes to send back."""

    embed_state: bool = True
    """Whether to embed state in the response (handlers can opt out)."""

    is_error: bool = False
    """Whether this was an error response."""
