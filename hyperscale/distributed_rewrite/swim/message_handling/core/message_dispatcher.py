"""
Routes incoming messages to appropriate handlers.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    ParseResult,
    ServerInterface,
)

from .message_parser import MessageParser
from .response_builder import ResponseBuilder

if TYPE_CHECKING:
    from .base_handler import BaseHandler


class MessageDispatcher:
    """
    Routes messages to handlers and coordinates response building.

    This is the main entry point for message handling, replacing the
    giant match statement in HealthAwareServer.receive().

    Usage:
        dispatcher = MessageDispatcher(server)
        dispatcher.register(AckHandler(server))
        dispatcher.register(ProbeHandler(server))
        # ... register all handlers

        result = await dispatcher.dispatch(addr, data, clock_time)
    """

    def __init__(
        self,
        server: ServerInterface,
        parser: MessageParser | None = None,
        response_builder: ResponseBuilder | None = None,
    ) -> None:
        """
        Initialize dispatcher.

        Args:
            server: Server interface for operations.
            parser: Message parser (created if not provided).
            response_builder: Response builder (created if not provided).
        """
        self._server = server
        self._parser = parser or MessageParser(server)
        self._response_builder = response_builder or ResponseBuilder(server)
        self._handlers: dict[bytes, "BaseHandler"] = {}

    def register(self, handler: "BaseHandler") -> None:
        """
        Register a handler instance.

        Args:
            handler: Handler to register.

        Raises:
            ValueError: If message type already registered.
        """
        for msg_type in handler.message_types:
            if msg_type in self._handlers:
                existing = self._handlers[msg_type]
                raise ValueError(
                    f"Message type {msg_type!r} already registered "
                    f"to {type(existing).__name__}"
                )
            self._handlers[msg_type] = handler

    def unregister(self, message_type: bytes) -> bool:
        """
        Unregister a handler for a message type.

        Args:
            message_type: Message type to unregister.

        Returns:
            True if handler was removed, False if not found.
        """
        if message_type in self._handlers:
            del self._handlers[message_type]
            return True
        return False

    async def dispatch(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Parse and dispatch a message to the appropriate handler.

        Args:
            addr: Source address.
            data: Raw message bytes.
            clock_time: Clock time from UDP layer.

        Returns:
            Response bytes to send back.
        """
        # Parse the message
        parse_result = self._parser.parse(addr, data, clock_time)

        # Process piggyback data
        await self._process_piggyback(parse_result)

        context = parse_result.context

        # Find handler
        handler = self._handlers.get(context.message_type)

        if handler is None:
            # No handler found - unknown message type
            await self._server.handle_error(
                ValueError(f"Unknown message type: {context.message_type!r}")
            )
            return self._response_builder.build_nack(b"unknown")

        # Dispatch to handler
        try:
            result = await handler.handle(context)
        except Exception as error:
            await self._server.handle_error(error)
            return self._response_builder.build_nack(b"error")

        # Finalize response
        return self._response_builder.finalize(result)

    async def _process_piggyback(self, parse_result: ParseResult) -> None:
        """
        Process any piggyback data from the message.

        Args:
            parse_result: Parsed message with piggyback data.
        """
        # Health piggyback is processed by the server's health gossip buffer
        # Membership piggyback is processed by the server's gossip buffer
        # These are handled at the server level, not by individual handlers
        pass

    def get_handler(self, message_type: bytes) -> "BaseHandler | None":
        """
        Get the handler for a message type.

        Args:
            message_type: Message type to look up.

        Returns:
            Handler or None if not registered.
        """
        return self._handlers.get(message_type)

    @property
    def registered_types(self) -> list[bytes]:
        """List of registered message types."""
        return list(self._handlers.keys())
