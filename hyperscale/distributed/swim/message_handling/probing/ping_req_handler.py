"""
Handler for PING-REQ messages (indirect probing).
"""

import asyncio
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


class PingReqHandler(BaseHandler):
    """
    Handles PING-REQ messages (indirect probing).

    Used when direct probe fails - ask other nodes to probe the target.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"ping-req",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a ping-req message."""
        target = context.target
        target_addr_bytes = context.target_addr_bytes

        # Process within context
        async with await self._server.context_with_value(target):
            nodes = self._server.read_nodes()

            # Invalid target
            if target is None:
                return self._nack(b"invalid")

            # If target is self, respond with alive
            if self._server.udp_target_is_self(target):
                return self._build_alive_response()

            # Unknown target
            if target not in nodes:
                return HandlerResult(
                    response=b"ping-req-ack:unknown>" + self._server.udp_addr_slug,
                    embed_state=False,
                )

            # Probe the target and return result
            return await self._probe_target(target, target_addr_bytes)

    def _build_alive_response(self) -> HandlerResult:
        """Build alive response for self-targeted ping-req."""
        base = b"ping-req-ack:alive>" + self._server.udp_addr_slug

        state = self._server.get_embedded_state()
        if state:
            response = base + STATE_SEPARATOR + b64encode(state)
        else:
            response = base

        return HandlerResult(response=response, embed_state=False)

    async def _probe_target(
        self,
        target: tuple[str, int],
        target_addr_bytes: bytes | None,
    ) -> HandlerResult:
        """Probe target and return appropriate response."""
        base_timeout = await self._server.get_current_timeout()
        timeout = self._server.get_lhm_adjusted_timeout(base_timeout)

        try:
            result = await asyncio.wait_for(
                self._server.send_probe_and_wait(target),
                timeout=timeout,
            )

            if result:
                response = b"ping-req-ack:alive>" + (target_addr_bytes or b"")
            else:
                response = b"ping-req-ack:dead>" + (target_addr_bytes or b"")

            return HandlerResult(response=response, embed_state=False)

        except asyncio.TimeoutError:
            response = b"ping-req-ack:timeout>" + (target_addr_bytes or b"")
            return HandlerResult(response=response, embed_state=False)
