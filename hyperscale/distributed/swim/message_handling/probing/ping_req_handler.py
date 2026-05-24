"""
Handler for PING-REQ messages (indirect probing).
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
        request_id = self._parse_request_id(context.message)

        # Process within context
        async with await self._server.context_with_value(target):
            nodes = self._server.read_nodes()

            # Invalid target
            if target is None:
                return self._nack(b"invalid")

            # If target is self, respond with alive
            if self._server.udp_target_is_self(target):
                return self._build_alive_response(request_id)

            # Unknown target
            if target not in nodes:
                return HandlerResult(
                    response=self._build_ping_req_ack(
                        b"unknown",
                        target_addr_bytes,
                        request_id,
                    ),
                    embed_state=False,
                )

            # Probe the target and return result
            return await self._probe_target(target, target_addr_bytes, request_id)

    def _build_alive_response(self, request_id: str | None) -> HandlerResult:
        """Build alive response for self-targeted ping-req."""
        base = self._build_ping_req_ack(
            b"alive",
            self._server.udp_addr_slug,
            request_id,
        )

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
        request_id: str | None,
    ) -> HandlerResult:
        """Probe target and return appropriate response."""
        result = await self._server.send_probe_and_wait(target)
        status = b"alive" if result else b"dead"
        response = self._build_ping_req_ack(status, target_addr_bytes, request_id)
        return HandlerResult(response=response, embed_state=False)

    def _build_ping_req_ack(
        self,
        status: bytes,
        target_addr_bytes: bytes | None,
        request_id: str | None,
    ) -> bytes:
        """Build a request-fenced indirect probe response."""
        if request_id:
            return (
                b"ping-req-ack:"
                + status
                + b":"
                + request_id.encode()
                + b">"
                + (target_addr_bytes or b"")
            )
        return b"ping-req-ack:" + status + b">" + (target_addr_bytes or b"")

    def _parse_request_id(self, message: bytes) -> str | None:
        """Parse optional ``ping-req:{incarnation}:{request_id}`` token."""
        msg_part = message.split(b">", maxsplit=1)[0]
        parts = msg_part.split(b":", maxsplit=2)
        if len(parts) < 3:
            return None
        try:
            request_id = parts[2].decode()
        except UnicodeDecodeError:
            return None
        return request_id or None
