"""
Handler for LEAVE messages.
"""

import time
from typing import ClassVar

from hyperscale.distributed.swim.core.audit import AuditEventType
from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class LeaveHandler(BaseHandler):
    """
    Handles LEAVE messages.

    Processes nodes leaving the cluster:
    - Propagates leave to other nodes
    - Updates node state to DEAD
    - Updates probe scheduler
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"leave",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a leave message."""
        source_addr = context.source_addr
        target = context.target
        target_addr_bytes = context.target_addr_bytes
        message = context.message

        # Validate target
        if not await self._server.validate_target(target, b"leave", source_addr):
            return self._nack()

        # Handle self-leave
        if self._server.udp_target_is_self(target):
            return HandlerResult(
                response=b"leave>" + self._server.udp_addr_slug,
                embed_state=False,
            )

        # Process leave within context
        async with self._server.context_with_value(target):
            nodes = self._server.read_nodes()

            if target not in nodes:
                await self._server.increase_failure_detector("missed_nack")
                return self._nack()

            # Record audit event
            self._server.audit_log.record(
                AuditEventType.NODE_LEFT,
                node=target,
                source=source_addr,
            )

            # Propagate leave to other nodes
            await self._propagate_leave(target, target_addr_bytes, message)

            self._server.incarnation_tracker.update_node(
                target, b"DEAD", 0, time.monotonic()
            )
            self._server.update_probe_scheduler_membership()

            return self._ack()

    async def _propagate_leave(
        self,
        target: tuple[str, int],
        target_addr_bytes: bytes | None,
        message: bytes,
    ) -> None:
        """Propagate leave to other cluster members."""
        if target_addr_bytes is None:
            return

        others = self._server.get_other_nodes(target)
        base_timeout = self._server.get_current_timeout()
        gather_timeout = self._server.get_lhm_adjusted_timeout(base_timeout) * 2

        propagate_msg = message + b">" + target_addr_bytes

        coros = [self._server.send_if_ok(node, propagate_msg) for node in others]
        await self._server.gather_with_errors(
            coros, operation="leave_propagation", timeout=gather_timeout
        )
