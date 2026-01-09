"""
Message dispatcher for SWIM protocol.

Routes messages to appropriate handlers based on message type.
"""

from typing import TYPE_CHECKING

from .base import MessageContext, HandlerResult, MessageHandler

if TYPE_CHECKING:
    from ..health_aware_server import HealthAwareServer


class MessageDispatcher:
    """
    Routes SWIM messages to registered handlers.

    Maintains a mapping of message types to handlers and
    dispatches incoming messages to the appropriate handler.
    """

    def __init__(self) -> None:
        self._handlers: dict[bytes, MessageHandler] = {}
        self._default_handler: MessageHandler | None = None

    def register(self, handler: MessageHandler) -> None:
        """
        Register a handler for its message types.

        Args:
            handler: The handler to register.

        Raises:
            ValueError: If a message type is already registered.
        """
        for msg_type in handler.message_types:
            if msg_type in self._handlers:
                existing = self._handlers[msg_type]
                raise ValueError(
                    f"Message type {msg_type!r} already registered "
                    f"to {type(existing).__name__}"
                )
            self._handlers[msg_type] = handler

    def set_default_handler(self, handler: MessageHandler) -> None:
        """Set a handler for unknown message types."""
        self._default_handler = handler

    async def dispatch(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        """
        Dispatch a message to its handler.

        Args:
            ctx: The parsed message context.
            server: The SWIM server instance.

        Returns:
            HandlerResult from the handler.
        """
        handler = self._handlers.get(ctx.message_type)

        if handler is None:
            if self._default_handler is not None:
                return await self._default_handler.handle(ctx, server)
            # No handler found, return error
            return HandlerResult(
                response=b'nack',
                embed_state=False,
                is_error=True,
            )

        return await handler.handle(ctx, server)

    def get_handler(self, msg_type: bytes) -> MessageHandler | None:
        """Get the handler for a message type."""
        return self._handlers.get(msg_type)

    @property
    def registered_types(self) -> list[bytes]:
        """List of registered message types."""
        return list(self._handlers.keys())
