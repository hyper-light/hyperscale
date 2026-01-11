"""
Handler for LEADER-VOTE messages.
"""

from typing import ClassVar

from hyperscale.distributed.swim.core.errors import UnexpectedMessageError
from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class LeaderVoteHandler(BaseHandler):
    """
    Handles leader-vote messages.

    Vote responses during leader election.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"leader-vote",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a leader-vote message."""
        source_addr = context.source_addr
        message = context.message

        # Verify we're actually expecting votes (are we a candidate?)
        if not self._server.leader_election.state.is_candidate():
            await self._server.handle_error(
                UnexpectedMessageError(
                    msg_type=b"leader-vote",
                    expected=[b"probe", b"ack", b"leader-heartbeat"],
                    source=source_addr,
                )
            )
            return self._ack()

        term = await self._server.parse_term_safe(message, source_addr)

        # Process vote
        if self._server.leader_election.handle_vote(source_addr, term):
            # We won the election
            self._server.leader_election.state.become_leader(term)
            self._server.leader_election.state.current_leader = (
                self._server.get_self_udp_addr()
            )

            self_addr = self._server.get_self_udp_addr()
            elected_msg = (
                b"leader-elected:"
                + str(term).encode()
                + b">"
                + f"{self_addr[0]}:{self_addr[1]}".encode()
            )
            self._server.broadcast_leadership_message(elected_msg)

        return self._ack()
