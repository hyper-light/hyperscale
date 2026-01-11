"""
Base class for all SWIM message handlers.
"""

from abc import ABC, abstractmethod
from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)


class BaseHandler(ABC):
    """
    Base class for SWIM message handlers.

    Each handler processes one or more message types. Handlers are stateless;
    all state comes from the ServerInterface.

    Subclass responsibilities:
    1. Set `message_types` class variable with handled message types
    2. Implement `handle()` method
    """

    message_types: ClassVar[tuple[bytes, ...]] = ()
    """Message types this handler processes (e.g., (b'ack',))."""

    def __init__(self, server: ServerInterface) -> None:
        """
        Initialize handler with server interface.

        Args:
            server: Interface providing server operations.
        """
        self._server = server

    @abstractmethod
    async def handle(self, context: MessageContext) -> HandlerResult:
        """
        Handle a message.

        Args:
            context: Parsed message context.

        Returns:
            HandlerResult with response and metadata.
        """
        ...

    def _ack(self, embed_state: bool = True) -> HandlerResult:
        """
        Build standard ack response.

        Args:
            embed_state: Whether to embed state in response.

        Returns:
            HandlerResult with ack response.
        """
        if embed_state:
            response = self._server.build_ack_with_state()
        else:
            response = b"ack>" + self._server.udp_addr_slug
        return HandlerResult(response=response, embed_state=False)

    def _nack(self, reason: bytes = b"") -> HandlerResult:
        """
        Build standard nack response.

        Args:
            reason: Optional reason for the nack.

        Returns:
            HandlerResult with nack response.
        """
        if reason:
            response = b"nack:" + reason + b">" + self._server.udp_addr_slug
        else:
            response = b"nack>" + self._server.udp_addr_slug
        return HandlerResult(response=response, embed_state=False, is_error=True)

    def _empty(self) -> HandlerResult:
        """
        Build empty response (no reply needed).

        Returns:
            HandlerResult with empty response.
        """
        return HandlerResult(response=b"", embed_state=False)
