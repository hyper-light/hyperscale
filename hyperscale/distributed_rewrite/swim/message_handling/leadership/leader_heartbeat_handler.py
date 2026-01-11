"""
Handler for LEADER-HEARTBEAT messages.
"""

from typing import ClassVar

from hyperscale.distributed_rewrite.swim.core.audit import AuditEventType
from hyperscale.distributed_rewrite.swim.core.errors import (
    UnexpectedMessageError,
    SplitBrainError,
)
from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


class LeaderHeartbeatHandler(BaseHandler):
    """
    Handles leader-heartbeat messages.

    Heartbeats renew the leader lease and detect split-brain scenarios.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"leader-heartbeat",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a leader-heartbeat message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        self._server.increment_metric("heartbeats_received")
        term = await self._server.parse_term_safe(message, source_addr)

        # Check if we received our own heartbeat (shouldn't happen)
        if target:
            self_addr = self._server.get_self_udp_addr()
            if target == self_addr and source_addr != self_addr:
                await self._server.handle_error(
                    UnexpectedMessageError(
                        msg_type=b"leader-heartbeat",
                        expected=None,
                        source=source_addr,
                    )
                )
                return self._ack()

        if target:
            self_addr = self._server.get_self_udp_addr()

            # Check for split-brain: we're leader but received heartbeat from another
            if (
                self._server.leader_election.state.is_leader()
                and target != self_addr
            ):
                should_yield = self._server.leader_election.handle_discovered_leader(
                    target, term
                )

                if should_yield:
                    await self._handle_split_brain(target, term, self_addr)

            await self._server.leader_election.handle_heartbeat(target, term)

        return self._ack()

    async def _handle_split_brain(
        self,
        other_leader: tuple[str, int],
        other_term: int,
        self_addr: tuple[str, int],
    ) -> None:
        """Handle detected split-brain scenario."""
        # Record in audit log
        self._server.audit_log.record(
            AuditEventType.SPLIT_BRAIN_DETECTED,
            node=self_addr,
            other_leader=other_leader,
            self_term=self._server.leader_election.state.current_term,
            other_term=other_term,
        )

        self._server.increment_metric("split_brain_events")

        # Log via error handler
        await self._server.handle_error(
            SplitBrainError(
                self_addr,
                other_leader,
                self._server.leader_election.state.current_term,
                other_term,
            )
        )

        # Step down
        self._server.task_runner.run(self._server.leader_election._step_down)
