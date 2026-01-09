"""
Result of parsing a raw UDP message.

Contains the MessageContext plus extracted piggyback data
to be processed separately.
"""

from dataclasses import dataclass

from .message_context import MessageContext


@dataclass(slots=True)
class ParseResult:
    """Result of parsing a raw UDP message."""

    context: MessageContext
    """Parsed message context."""

    health_piggyback: bytes | None = None
    """Extracted health gossip piggyback data."""

    membership_piggyback: bytes | None = None
    """Extracted membership piggyback data."""
