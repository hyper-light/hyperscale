"""
Handler for LEADER-ELECTED messages.
"""

from typing import ClassVar

from hyperscale.distributed.swim.core.errors import UnexpectedMessageError
from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class LeaderElectedHandler(BaseHandler):
    """
    Handles leader-elected messages.

    Notification that a node has won the election.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"leader-elected",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a leader-elected message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        term = await self._server.parse_term_safe(message, source_addr)

        if target:
            # Check if we received our own election announcement (shouldn't happen)
            self_addr = self._server.get_self_udp_addr()
            if target == self_addr:
                await self._server.handle_error(
                    UnexpectedMessageError(
                        msg_type=b"leader-elected",
                        expected=None,
                        source=source_addr,
                    )
                )
                return self._ack()

            await self._server.leader_election.handle_elected(target, term)

        return self._ack()
