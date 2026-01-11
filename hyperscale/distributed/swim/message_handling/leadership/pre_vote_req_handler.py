"""
Handler for PRE-VOTE-REQ messages (Raft pre-voting).
"""

from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class PreVoteReqHandler(BaseHandler):
    """
    Handles pre-vote-req messages (Raft pre-voting).

    Pre-voting prevents disruption from partitioned nodes.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"pre-vote-req",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a pre-vote-req message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        term, candidate_lhm = await self._server.parse_leadership_claim(
            message, source_addr
        )

        if target:
            resp = self._server.leader_election.handle_pre_vote_request(
                candidate=target,
                term=term,
                candidate_lhm=candidate_lhm,
            )
            if resp:
                self._server.task_runner.run(
                    self._server.send_to_addr,
                    target,
                    resp,
                )

        return self._ack()
