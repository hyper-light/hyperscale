"""
Handler for ALIVE messages (refutations).
"""

import time
from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class AliveHandler(BaseHandler):
    """
    Handles ALIVE messages (refutations).

    A node sends ALIVE to prove it's alive when suspected.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"alive",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle an alive message."""
        source_addr = context.source_addr
        target = context.target
        message = context.message

        msg_incarnation = await self._server.parse_incarnation_safe(
            message, source_addr
        )
        node_id = self._server.parse_node_id_from_message(message)
        is_authoritative = self._server.is_authoritative_liveness_evidence(
            source_addr,
            target,
            node_id,
        )
        if not is_authoritative:
            self._server.increment_metric("non_authoritative_alive_suppressed")
            return self._ack()

        await self._server.confirm_peer(source_addr)
        request_id = self._server.parse_probe_request_id_from_message(message)
        request_matches_pending_probe = self._server.probe_request_matches_pending(
            source_addr,
            request_id,
        )

        # Complete any pending probe Future for this address
        # 'alive' is sent as a response when a node is probed about itself
        # This is equivalent to an ACK for probe purposes
        pending_acks = self._server.pending_probe_acks
        pending_future = pending_acks.get(source_addr)
        if (
            pending_future
            and not pending_future.done()
            and request_matches_pending_probe
        ):
            pending_future.set_result(True)

        if target and self._server.is_message_fresh(target, msg_incarnation, b"OK"):
            await self._server.refute_suspicion(target, msg_incarnation)
            await self._server.update_node_state(
                target,
                b"OK",
                msg_incarnation,
                time.monotonic(),
            )
            await self._server.decrease_failure_detector("successful_probe")

        return self._ack()
