"""
Handler for LEADER-CLAIM messages (election start).
"""

from typing import ClassVar

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


class LeaderClaimHandler(BaseHandler):
    """
    Handles leader-claim messages (election start).

    When a node claims leadership:
    - Parse term and candidate LHM
    - Vote if appropriate
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"leader-claim",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a leader-claim message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        term, candidate_lhm = await self._server.parse_leadership_claim(
            message, source_addr
        )

        if target:
            vote_msg = self._server.leader_election.handle_claim(
                target, term, candidate_lhm
            )
            if vote_msg:
                base_timeout = self._server.get_current_timeout()
                timeout = self._server.get_lhm_adjusted_timeout(base_timeout)
                self._server.task_runner.run(
                    self._server.send,
                    target,
                    vote_msg,
                    timeout=timeout,
                )

        return self._ack()
