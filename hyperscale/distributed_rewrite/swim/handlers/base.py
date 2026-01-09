"""
Base classes and protocols for SWIM message handlers.

This module provides the foundation for decomposing the monolithic
receive() function into composable, testable handler classes.
"""

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..health_aware_server import HealthAwareServer


@dataclass(frozen=True, slots=True)
class MessageContext:
    """
    Immutable context for a single SWIM message.

    Contains all parsed information about an incoming message,
    passed to handlers for processing.
    """
    # Source address of the message sender
    source_addr: tuple[str, int]

    # Target address extracted from message (if present)
    target: tuple[str, int] | None

    # Raw target address bytes (for forwarding)
    target_addr_bytes: bytes | None

    # Message type (e.g., b'ack', b'probe', b'leader-claim')
    message_type: bytes

    # Full message content (includes type and payload)
    message: bytes

    # Clock time from the UDP layer
    clock_time: int

    # Source address as string (e.g., "127.0.0.1:8001")
    source_addr_string: str = field(init=False)

    def __post_init__(self) -> None:
        # Use object.__setattr__ because frozen=True
        object.__setattr__(
            self,
            'source_addr_string',
            f'{self.source_addr[0]}:{self.source_addr[1]}'
        )

    def get_message_payload(self) -> bytes:
        """Extract payload after the message type (after first colon)."""
        parts = self.message.split(b':', maxsplit=1)
        return parts[1] if len(parts) > 1 else b''


@dataclass(slots=True)
class HandlerResult:
    """
    Result from a message handler.

    Encapsulates the response bytes and any side effects
    the handler wants to communicate.
    """
    # Response bytes to send back
    response: bytes

    # Whether to embed state in the response
    # (handlers can opt out for specific cases)
    embed_state: bool = True

    # Whether this was an error response
    is_error: bool = False


@runtime_checkable
class MessageHandler(Protocol):
    """
    Protocol for SWIM message handlers.

    Each handler is responsible for processing a specific message type
    or category of messages.
    """

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        """
        Process a message and return a result.

        Args:
            ctx: The parsed message context.
            server: The SWIM server instance for accessing state.

        Returns:
            HandlerResult with response bytes and metadata.
        """
        ...

    @property
    def message_types(self) -> tuple[bytes, ...]:
        """
        The message types this handler processes.

        Returns:
            Tuple of message type bytes (e.g., (b'ack', b'nack')).
        """
        ...


class BaseHandler:
    """
    Base class for message handlers with common utilities.

    Provides helper methods for building responses and
    accessing server state.
    """

    def __init__(self, message_types: tuple[bytes, ...]) -> None:
        self._message_types = message_types

    @property
    def message_types(self) -> tuple[bytes, ...]:
        return self._message_types

    def build_ack(self, server: 'HealthAwareServer') -> HandlerResult:
        """Build a standard ack response with embedded state."""
        return HandlerResult(
            response=server._build_ack_with_state(),
            embed_state=False,  # Already embedded
        )

    def build_nack(
        self,
        server: 'HealthAwareServer',
        reason: str = '',
    ) -> HandlerResult:
        """Build a nack response."""
        if reason:
            response = f'nack:{reason}>'.encode() + server._udp_addr_slug
        else:
            response = b'nack>' + server._udp_addr_slug
        return HandlerResult(response=response, embed_state=False, is_error=True)

    def build_plain_ack(self, server: 'HealthAwareServer') -> HandlerResult:
        """Build a plain ack without embedded state (for duplicates)."""
        return HandlerResult(
            response=b'ack>' + server._udp_addr_slug,
            embed_state=False,
        )
