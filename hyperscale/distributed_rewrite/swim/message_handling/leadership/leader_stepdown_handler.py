"""
Handler for LEADER-STEPDOWN messages.
"""

from typing import ClassVar

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


class LeaderStepdownHandler(BaseHandler):
    """
    Handles leader-stepdown messages.

    Notification that a leader is stepping down.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"leader-stepdown",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a leader-stepdown message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        term = await self._server.parse_term_safe(message, source_addr)

        if target:
            await self._server.leader_election.handle_stepdown(target, term)

        return self._ack()
