"""
Handler for JOIN messages.
"""

import time
from typing import ClassVar

from hyperscale.distributed.protocol.version import CURRENT_PROTOCOL_VERSION
from hyperscale.distributed.swim.core.audit import AuditEventType
from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


# SWIM protocol version prefix (included in join messages)
SWIM_VERSION_PREFIX = (
    f"v{CURRENT_PROTOCOL_VERSION.major}.{CURRENT_PROTOCOL_VERSION.minor}".encode()
)


class JoinHandler(BaseHandler):
    """
    Handles JOIN messages.

    Processes new nodes joining the cluster:
    - Validates protocol version (AD-25)
    - Clears stale state
    - Propagates join to other nodes
    - Adds to probe scheduler
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"join",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a join message."""
        self._server.increment_metric("joins_received")

        source_addr = context.source_addr
        target_addr_bytes = context.target_addr_bytes

        # Parse version and target from join message
        version, target, target_addr_bytes = self._parse_join_message(
            context.target, target_addr_bytes
        )

        # Validate protocol version (AD-25)
        if version is None:
            self._server.increment_metric("joins_rejected_no_version")
            return self._nack(b"version_required")

        if version[0] != CURRENT_PROTOCOL_VERSION.major:
            self._server.increment_metric("joins_rejected_version_mismatch")
            return self._nack(b"version_mismatch")

        # Validate target
        if not await self._server.validate_target(target, b"join", source_addr):
            return self._nack()

        # Handle self-join
        if self._server.udp_target_is_self(target):
            return self._ack(embed_state=False)

        # Process join within context
        async with await self._server.context_with_value(target):
            nodes = self._server.read_nodes()

            # Check if rejoin
            is_rejoin = target in nodes

            # Clear stale state
            await self._server.clear_stale_state(target)

            # Record audit event
            event_type = (
                AuditEventType.NODE_REJOIN if is_rejoin else AuditEventType.NODE_JOINED
            )
            self._server.audit_log.record(
                event_type,
                node=target,
                source=source_addr,
            )

            # Add to membership
            await self._server.write_context(target, b"OK")

            # Propagate join to other nodes
            await self._propagate_join(target, target_addr_bytes)

            # Update probe scheduler
            self._server.probe_scheduler.add_member(target)

            # AD-29: Confirm both sender and joining node
            self._server.confirm_peer(source_addr)
            self._server.confirm_peer(target)

            # Update incarnation tracker
            self._server.incarnation_tracker.update_node(
                target, b"OK", 0, time.monotonic()
            )

            return self._ack()

    def _parse_join_message(
        self,
        target: tuple[str, int] | None,
        target_addr_bytes: bytes | None,
    ) -> tuple[tuple[int, int] | None, tuple[str, int] | None, bytes | None]:
        """
        Parse version and target from join message.

        Format: v{major}.{minor}|host:port

        Returns:
            Tuple of (version, target, target_addr_bytes).
        """
        if not target_addr_bytes or b"|" not in target_addr_bytes:
            return (None, target, target_addr_bytes)

        version_part, addr_part = target_addr_bytes.split(b"|", maxsplit=1)

        # Parse version
        version: tuple[int, int] | None = None
        if version_part.startswith(b"v"):
            try:
                version_str = version_part[1:].decode()
                parts = version_str.split(".")
                if len(parts) == 2:
                    version = (int(parts[0]), int(parts[1]))
            except (ValueError, UnicodeDecodeError):
                pass

        # Parse target address
        parsed_target: tuple[str, int] | None = None
        try:
            host, port_str = addr_part.decode().split(":", maxsplit=1)
            parsed_target = (host, int(port_str))
        except (ValueError, UnicodeDecodeError):
            pass

        return (version, parsed_target, addr_part)

    async def _propagate_join(
        self, target: tuple[str, int], target_addr_bytes: bytes | None
    ) -> None:
        """Propagate join to other cluster members."""
        if target_addr_bytes is None:
            return

        others = self._server.get_other_nodes(target)
        base_timeout = self._server.get_current_timeout()
        gather_timeout = self._server.get_lhm_adjusted_timeout(base_timeout) * 2

        propagate_msg = b"join>" + SWIM_VERSION_PREFIX + b"|" + target_addr_bytes

        coros = [self._server.send_if_ok(node, propagate_msg) for node in others]
        await self._server.gather_with_errors(
            coros, operation="join_propagation", timeout=gather_timeout
        )
