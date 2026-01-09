"""
Handler for XACK messages (cross-cluster health acknowledgments).
"""

from typing import ClassVar

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


class XAckHandler(BaseHandler):
    """
    Handles xack messages (cross-cluster health acknowledgments).

    Response from DC leader with aggregate datacenter health.
    The server's _handle_xack_response method (overridden in GateServer,
    ManagerServer) provides specific behavior.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"xack",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle an xack message."""
        # Delegate to server's handle_xack_response method via ServerInterface
        # This is overridden in GateServer and ManagerServer
        await self._server.handle_xack_response(
            context.source_addr, context.target_addr_bytes or b""
        )

        # No response needed for xack
        return self._empty()
