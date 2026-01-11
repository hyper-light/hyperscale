"""
Handler for NACK messages.
"""

import time
from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class NackHandler(BaseHandler):
    """
    Handles NACK messages.

    NACKs indicate the sender couldn't reach a target.
    We still confirm the peer since they responded.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"nack",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a nack message."""
        source_addr = context.source_addr

        # AD-29: Confirm peer on successful communication (even NACK is communication)
        self._server.confirm_peer(source_addr)

        # The sender is alive since it responded
        nodes = self._server.read_nodes()
        if source_addr in nodes:
            self._server.update_node_state(source_addr, b"OK", 0, time.monotonic())

        return self._ack()
