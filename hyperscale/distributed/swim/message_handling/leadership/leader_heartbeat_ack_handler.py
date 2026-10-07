"""
Handler for LEADER-HEARTBEAT-ACK messages.
"""

from typing import ClassVar

from hyperscale.distributed.swim.core.errors import MalformedMessageError
from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class LeaderHeartbeatAckHandler(BaseHandler):
    """
    Handles leader-heartbeat-ack messages.

    A follower names the newest beat of the leader's term it applied
    (``leader-heartbeat-ack:{term}:{seq}>{leader_addr}``); the leader
    credits that beat's send instant to its quorum lease, which it steps
    down at the end of.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"leader-heartbeat-ack",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a leader-heartbeat-ack message."""
        source_addr = context.source_addr
        message = context.message

        term = await self._server.parse_term_safe(message, source_addr)
        acknowledgement_fields = message.split(b">", maxsplit=1)[0].split(b":")
        try:
            heartbeat_seq = int(acknowledgement_fields[2])
        except (ValueError, IndexError) as error:
            await self._server.handle_error(
                MalformedMessageError(
                    message,
                    f"Invalid heartbeat sequence: {error}",
                    source_addr,
                )
            )
            return self._empty()

        self._server.leader_election.handle_heartbeat_ack(
            source_addr, term, heartbeat_seq
        )
        return self._empty()
