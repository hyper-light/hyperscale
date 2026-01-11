"""
Immutable context for a single SWIM message.

Contains all parsed information about an incoming message,
passed to handlers for processing.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class MessageContext:
    """
    Immutable context for a single SWIM message.

    Contains all parsed information about an incoming message,
    passed to handlers for processing.
    """

    source_addr: tuple[str, int]
    """Source address of the message sender."""

    target: tuple[str, int] | None
    """Target address extracted from message (if present)."""

    target_addr_bytes: bytes | None
    """Raw target address bytes (for forwarding)."""

    message_type: bytes
    """Message type (e.g., b'ack', b'probe', b'leader-claim')."""

    message: bytes
    """Full message content (includes type and payload)."""

    clock_time: int
    """Clock time from the UDP layer."""

    source_addr_string: str = field(init=False)
    """Source address as string (e.g., '127.0.0.1:8001')."""

    def __post_init__(self) -> None:
        """Initialize computed fields."""
        object.__setattr__(
            self,
            "source_addr_string",
            f"{self.source_addr[0]}:{self.source_addr[1]}",
        )

    def get_message_payload(self) -> bytes:
        """Extract payload after the message type (after first colon)."""
        parts = self.message.split(b":", maxsplit=1)
        return parts[1] if len(parts) > 1 else b""
