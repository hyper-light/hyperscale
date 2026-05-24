"""
Handler for PING-REQ-ACK messages (indirect probe responses).
"""

from typing import ClassVar

from hyperscale.distributed.swim.core.errors import UnexpectedMessageError
from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class PingReqAckHandler(BaseHandler):
    """
    Handles PING-REQ-ACK messages (indirect probe responses).

    These are responses from nodes we asked to probe a target.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"ping-req-ack",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a ping-req-ack message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        status, request_id = self._parse_status_and_request_id(message)

        # Verify we have a pending indirect probe for this exact request.
        if target is None:
            return self._ack()

        pending_probe = self._server.indirect_probe_manager.get_pending_probe(target)
        if pending_probe is None:
            await self._server.handle_error(
                UnexpectedMessageError(
                    msg_type=b"ping-req-ack",
                    expected=None,
                    source=source_addr,
                )
            )
            return self._ack()

        if not pending_probe.matches_request(request_id):
            self._server.increment_metric("indirect_probe_stale_acks")
            return self._ack()

        if source_addr not in pending_probe.proxies:
            self._server.increment_metric("indirect_probe_unexpected_proxy_acks")
            return self._ack()

        if status == b"alive":
            await self._server.handle_indirect_probe_response(
                target,
                is_alive=True,
                request_id=request_id,
            )
            await self._server.decrease_failure_detector("successful_probe")
        elif status in (b"dead", b"timeout", b"unknown"):
            await self._server.handle_indirect_probe_response(
                target,
                is_alive=False,
                request_id=request_id,
            )

        return self._ack()

    def _parse_status_and_request_id(self, message: bytes) -> tuple[bytes, str | None]:
        """
        Parse status and request id from ping-req-ack message.

        Format: ping-req-ack:status[:request_id]>target_addr

        Returns status bytes (alive, dead, timeout, unknown) plus the
        optional request id token.
        """
        msg_part = message.split(b">", maxsplit=1)[0]
        msg_parts = msg_part.split(b":", maxsplit=2)
        if len(msg_parts) < 2:
            return b"", None

        status = msg_parts[1]
        request_id: str | None = None
        if len(msg_parts) >= 3:
            try:
                request_id = msg_parts[2].decode() or None
            except UnicodeDecodeError:
                request_id = None

        return status, request_id
