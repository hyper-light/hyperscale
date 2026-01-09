"""
Handler for ACK messages.
"""

import time
from typing import ClassVar

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


class AckHandler(BaseHandler):
    """
    Handles ACK messages.

    ACKs indicate successful communication. We:
    - Confirm the peer (AD-29)
    - Complete pending probe futures
    - Update node state to OK
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"ack",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle an ack message."""
        source_addr = context.source_addr
        target = context.target

        # AD-29: Confirm peer on successful communication
        self._server.confirm_peer(source_addr)

        # Complete any pending probe Future for this address
        # This unblocks _probe_with_timeout waiting for ACK
        pending_acks = self._server.pending_probe_acks
        pending_future = pending_acks.get(source_addr)
        if pending_future and not pending_future.done():
            pending_future.set_result(True)

        nodes = self._server.read_nodes()

        if source_addr in nodes:
            # Update node state - triggers recovery callbacks if was DEAD
            self._server.update_node_state(source_addr, b"OK", 0, time.monotonic())
            await self._server.decrease_failure_detector("successful_probe")

        if target:
            if target not in nodes:
                await self._server.increase_failure_detector("missed_nack")
                return self._nack(b"unknown")
            await self._server.decrease_failure_detector("successful_nack")

        return self._ack()
