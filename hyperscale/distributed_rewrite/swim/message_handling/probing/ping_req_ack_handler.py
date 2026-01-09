"""
Handler for PING-REQ-ACK messages (indirect probe responses).
"""

from typing import ClassVar

from hyperscale.distributed_rewrite.swim.core.errors import UnexpectedMessageError
from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


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

        # Verify we have a pending indirect probe for this target
        if target and not self._server.indirect_probe_manager.get_pending_probe(target):
            await self._server.handle_error(
                UnexpectedMessageError(
                    msg_type=b"ping-req-ack",
                    expected=None,
                    source=source_addr,
                )
            )
            return self._ack()

        # Parse status from message
        status = self._parse_status(message)

        if status == b"alive" and target:
            await self._server.handle_indirect_probe_response(target, is_alive=True)
            await self._server.decrease_failure_detector("successful_probe")
        elif status in (b"dead", b"timeout", b"unknown") and target:
            await self._server.handle_indirect_probe_response(target, is_alive=False)

        return self._ack()

    def _parse_status(self, message: bytes) -> bytes:
        """
        Parse status from ping-req-ack message.

        Format: ping-req-ack:status>target_addr

        Returns:
            Status bytes (alive, dead, timeout, unknown).
        """
        msg_parts = message.split(b":", maxsplit=1)
        if len(msg_parts) > 1:
            # Status is between : and >
            status_part = msg_parts[1]
            if b">" in status_part:
                return status_part.split(b">", maxsplit=1)[0]
            return status_part
        return b""
