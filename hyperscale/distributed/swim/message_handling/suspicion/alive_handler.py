"""
Handler for ALIVE messages (refutations).
"""

import time
from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class AliveHandler(BaseHandler):
    """
    Handles ALIVE messages (refutations).

    A node sends ALIVE to prove it's alive when suspected.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"alive",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle an alive message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        msg_incarnation = await self._server.parse_incarnation_safe(
            message, source_addr
        )

        await self._server.confirm_peer(source_addr)

        # Complete any pending probe Future for this address
        # 'alive' is sent as a response when a node is probed about itself
        # This is equivalent to an ACK for probe purposes
        pending_acks = self._server.pending_probe_acks
        pending_future = pending_acks.get(source_addr)
        if pending_future and not pending_future.done():
            pending_future.set_result(True)

        if target:
            if self._server.is_message_fresh(target, msg_incarnation, b"OK"):
                await self._server.refute_suspicion(target, msg_incarnation)
                await self._server.update_node_state(
                    target,
                    b"OK",
                    msg_incarnation,
                    time.monotonic(),
                )
                await self._server.decrease_failure_detector("successful_probe")

        return self._ack()
