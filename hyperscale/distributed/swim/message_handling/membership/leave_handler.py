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
        incarnation, node_id = self._parse_leave_metadata(message)

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
        async with await self._server.context_with_value(target):
            nodes = self._server.read_nodes()

            if target not in nodes:
                if self._is_authorized_direct_leave(target, source_addr, node_id):
                    updated = await self._apply_authorized_leave(
                        target,
                        source_addr,
                        incarnation,
                        "direct_leave_handler",
                    )
                    if updated:
                        self._queue_leave_propagation(
                            target,
                            incarnation,
                            target_addr_bytes,
                            message,
                        )
                    return self._ack()

                await self._server.increase_failure_detector("missed_nack")
                return self._nack()

            previous_state = self._server.incarnation_tracker.get_node_state(target)
            was_dead = (
                previous_state is not None and previous_state.status == b"DEAD"
            )
            updated = await self._server.update_node_state(
                target,
                b"DEAD",
                incarnation,
                time.monotonic(),
            )
            if (
                not updated
                and not was_dead
                and previous_state is not None
                and self._is_authorized_direct_leave(target, source_addr, node_id)
            ):
                incarnation = max(incarnation, previous_state.incarnation)
                updated = await self._server.update_node_state(
                    target,
                    b"DEAD",
                    incarnation,
                    time.monotonic(),
                )
            self._server.update_probe_scheduler_membership()

            if updated:
                self._server.audit_log.record(
                    AuditEventType.NODE_LEFT,
                    node=target,
                    source=source_addr,
                )
                if not was_dead:
                    self._server.notify_node_dead(
                        target,
                        incarnation,
                        "leave_handler",
                    )
                self._queue_leave_propagation(
                    target,
                    incarnation,
                    target_addr_bytes,
                    message,
                )

            return self._ack()

    async def _apply_authorized_leave(
        self,
        target: tuple[str, int],
        source_addr: tuple[str, int],
        incarnation: int,
        notification_source: str,
    ) -> bool:
        """Apply a self-originated leave already authorized by node identity."""
        updated = await self._server.update_node_state(
            target,
            b"DEAD",
            incarnation,
            time.monotonic(),
        )
        self._server.update_probe_scheduler_membership()
        if not updated:
            return False

        self._server.audit_log.record(
            AuditEventType.NODE_LEFT,
            node=target,
            source=source_addr,
        )
        self._server.notify_node_dead(
            target,
            incarnation,
            notification_source,
        )
        return True

    def _is_authorized_direct_leave(
        self,
        target: tuple[str, int],
        source_addr: tuple[str, int],
        node_id: str | None,
    ) -> bool:
        """Return True when a direct leave names the currently registered node."""
        if target != source_addr or node_id is None:
            return False
        return self._server.get_registered_node_id_for_addr(target) == node_id

    def _parse_leave_metadata(self, message: bytes) -> tuple[int, str | None]:
        """Parse ``leave:{incarnation}:{node_id}`` with legacy fallback."""
        parts = message.split(b":", maxsplit=2)
        incarnation = 0
        node_id: str | None = None

        if len(parts) > 1:
            try:
                incarnation = int(parts[1].decode())
            except ValueError:
                incarnation = 0

        if len(parts) > 2:
            try:
                node_id = parts[2].decode()
            except UnicodeDecodeError:
                node_id = None

        return incarnation, node_id

    def _queue_leave_propagation(
        self,
        target: tuple[str, int],
        incarnation: int,
        target_addr_bytes: bytes | None,
        message: bytes,
    ) -> None:
        """Queue LEAVE dissemination without delaying local reap/ACK."""
        self._server.queue_gossip_update("leave", target, incarnation)
        if target_addr_bytes is None:
            return

        self._server.task_runner.run(
            self._propagate_leave,
            target,
            target_addr_bytes,
            message,
            alias="leave_propagation",
        )

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
        base_timeout = await self._server.get_current_timeout()
        gather_timeout = self._server.get_lhm_adjusted_timeout(base_timeout) * 2

        propagate_msg = message + b">" + target_addr_bytes

        coros = [self._server.send_if_ok(node, propagate_msg) for node in others]
        await self._server.gather_with_errors(
            coros, operation="leave_propagation", timeout=gather_timeout
        )
