"""
Handler for PRE-VOTE-RESP messages.
"""

from typing import ClassVar

from hyperscale.distributed_rewrite.swim.core.errors import UnexpectedMessageError
from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


class PreVoteRespHandler(BaseHandler):
    """
    Handles pre-vote-resp messages.

    Response to a pre-vote request.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"pre-vote-resp",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a pre-vote-resp message."""
        source_addr = context.source_addr
        message = context.message

        # Verify we're actually in a pre-voting phase
        if not self._server.leader_election.state.pre_voting_in_progress:
            await self._server.handle_error(
                UnexpectedMessageError(
                    msg_type=b"pre-vote-resp",
                    expected=None,
                    source=source_addr,
                )
            )
            return self._ack()

        term, granted = await self._server.parse_pre_vote_response(
            message, source_addr
        )

        self._server.leader_election.handle_pre_vote_response(
            voter=source_addr,
            term=term,
            granted=granted,
        )

        return self._ack()
