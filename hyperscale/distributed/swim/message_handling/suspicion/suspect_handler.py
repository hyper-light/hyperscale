"""
Handler for SUSPECT messages.
"""

from base64 import b64encode
from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


# Separator for embedded state
STATE_SEPARATOR = b"#|s"


class SuspectHandler(BaseHandler):
    """
    Handles SUSPECT messages.

    When a node is suspected of being dead:
    - If about self, broadcast refutation
    - Otherwise start suspicion timer
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"suspect",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a suspect message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        msg_incarnation = await self._server.parse_incarnation_safe(
            message, source_addr
        )

        await self._server.confirm_peer(source_addr)

        if target:
            # If suspicion is about self, refute it
            if self._server.udp_target_is_self(target):
                return await self._handle_self_suspicion(msg_incarnation)

            # Start suspicion for target if message is fresh
            if self._server.is_message_fresh(target, msg_incarnation, b"SUSPECT"):
                await self._server.start_suspicion(target, msg_incarnation, source_addr)

                # Check if we should regossip this suspicion
                detector = self._server.hierarchical_detector
                if detector.should_regossip_global(target):
                    detector.mark_regossiped_global(target)
                    await self._server.broadcast_suspicion(target, msg_incarnation)

        return self._ack()

    async def _handle_self_suspicion(self, msg_incarnation: int) -> HandlerResult:
        """Handle suspicion about self - refute it."""
        await self._server.increase_failure_detector("refutation")
        new_incarnation = await self._server.broadcast_refutation()

        base = (
            b"alive:"
            + str(new_incarnation).encode()
            + b">"
            + self._server.udp_addr_slug
        )

        state = self._server.get_embedded_state()
        if state:
            response = base + STATE_SEPARATOR + b64encode(state)
        else:
            response = base

        return HandlerResult(response=response, embed_state=False)
