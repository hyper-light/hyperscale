"""
SWIM Membership Message Handlers.

Handles: ack, nack, join, leave
"""

import time
from typing import TYPE_CHECKING

from .base import BaseHandler, MessageContext, HandlerResult

from ..core.types import Nodes
from ..core.audit import AuditEventType

if TYPE_CHECKING:
    from ..health_aware_server import HealthAwareServer


class AckHandler(BaseHandler):
    """
    Handles ACK messages.

    ACKs indicate successful communication. We:
    - Confirm the peer (AD-29)
    - Complete pending probe futures
    - Update node state to OK
    """

    def __init__(self) -> None:
        super().__init__((b'ack',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target

        # AD-29: Confirm peer on successful communication
        server.confirm_peer(addr)

        # Complete any pending probe Future for this address
        pending_future = server._pending_probe_acks.get(addr)
        if pending_future and not pending_future.done():
            pending_future.set_result(True)

        nodes: Nodes = server._context.read('nodes')

        if addr in nodes:
            # Update node state - triggers recovery callbacks if was DEAD
            server.update_node_state(addr, b'OK', 0, time.monotonic())
            await server.decrease_failure_detector('successful_probe')

        if target:
            if target not in nodes:
                await server.increase_failure_detector('missed_nack')
                return HandlerResult(
                    response=b'nack:unknown>' + server._udp_addr_slug,
                    embed_state=False,
                    is_error=True,
                )
            await server.decrease_failure_detector('successful_nack')

        return self.build_ack(server)


class NackHandler(BaseHandler):
    """
    Handles NACK messages.

    NACKs indicate the sender couldn't reach a target.
    We still confirm the peer since they responded.
    """

    def __init__(self) -> None:
        super().__init__((b'nack',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr

        # AD-29: Confirm peer on successful communication (even NACK is communication)
        server.confirm_peer(addr)

        # The sender is alive since they responded
        nodes: Nodes = server._context.read('nodes')
        if addr in nodes:
            server.update_node_state(addr, b'OK', 0, time.monotonic())

        return self.build_ack(server)


class JoinHandler(BaseHandler):
    """
    Handles JOIN messages.

    Processes new nodes joining the cluster:
    - Validates protocol version (AD-25)
    - Clears stale state
    - Propagates join to other nodes
    - Adds to probe scheduler
    """

    def __init__(self) -> None:
        super().__init__((b'join',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        from ..health_aware_server import SWIM_VERSION_PREFIX
        from hyperscale.distributed_rewrite.protocol.version import CURRENT_PROTOCOL_VERSION

        addr = ctx.source_addr
        target = ctx.target
        target_addr = ctx.target_addr_bytes

        server._metrics.increment('joins_received')

        # Parse version prefix (AD-25)
        join_version_major: int | None = None
        join_version_minor: int | None = None

        if target_addr and b'|' in target_addr:
            version_part, addr_part = target_addr.split(b'|', maxsplit=1)
            if version_part.startswith(b'v'):
                try:
                    version_str = version_part[1:].decode()
                    parts = version_str.split('.')
                    if len(parts) == 2:
                        join_version_major = int(parts[0])
                        join_version_minor = int(parts[1])
                except (ValueError, UnicodeDecodeError):
                    pass

            # Re-parse target from address part
            try:
                host, port = addr_part.decode().split(':', maxsplit=1)
                target = (host, int(port))
                target_addr = addr_part
            except (ValueError, UnicodeDecodeError):
                target = None

        # Validate protocol version (AD-25)
        if join_version_major is None:
            server._metrics.increment('joins_rejected_no_version')
            return self.build_nack(server, 'version_required')

        if join_version_major != CURRENT_PROTOCOL_VERSION.major:
            server._metrics.increment('joins_rejected_version_mismatch')
            return self.build_nack(server, 'version_mismatch')

        if not await server._validate_target(target, b'join', addr):
            return self.build_nack(server)

        async with server._context.with_value(target):
            nodes: Nodes = server._context.read('nodes')

            if server.udp_target_is_self(target):
                return HandlerResult(
                    response=b'ack>' + server._udp_addr_slug,
                    embed_state=False,
                )

            is_rejoin = target in nodes
            await server._clear_stale_state(target)

            # Record audit event
            event_type = AuditEventType.NODE_REJOIN if is_rejoin else AuditEventType.NODE_JOINED
            server._audit_log.record(event_type, node=target, source=addr)

            server._context.write(target, b'OK')

            # Propagate join to others
            others = server.get_other_nodes(target)
            base_timeout = server._context.read('current_timeout')
            gather_timeout = server.get_lhm_adjusted_timeout(base_timeout) * 2
            propagate_msg = b'join>' + SWIM_VERSION_PREFIX + b'|' + target_addr

            await server._gather_with_errors(
                [server.send_if_ok(node, propagate_msg) for node in others],
                operation="join_propagation",
                timeout=gather_timeout,
            )

            await server._safe_queue_put(
                nodes[target],
                (ctx.clock_time, b'OK'),
                target,
            )

            server._probe_scheduler.add_member(target)

            # AD-29: Confirm both sender and joining node
            server.confirm_peer(addr)
            server.confirm_peer(target)

            # Invoke join callbacks
            for callback in server._on_node_join_callbacks:
                try:
                    callback(target)
                except Exception as e:
                    server._task_runner.run(
                        server.handle_exception, e, "on_node_join_callback"
                    )

            server._incarnation_tracker.update_node(
                target, b'OK', 0, time.monotonic()
            )

            return self.build_ack(server)


class LeaveHandler(BaseHandler):
    """
    Handles LEAVE messages.

    Processes nodes leaving the cluster:
    - Propagates leave to other nodes
    - Updates node state to DEAD
    - Updates probe scheduler
    """

    def __init__(self) -> None:
        super().__init__((b'leave',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        target_addr = ctx.target_addr_bytes
        message = ctx.message

        if not await server._validate_target(target, b'leave', addr):
            return self.build_nack(server)

        async with server._context.with_value(target):
            nodes: Nodes = server._context.read('nodes')

            if server.udp_target_is_self(target):
                return HandlerResult(
                    response=b'leave>' + server._udp_addr_slug,
                    embed_state=False,
                )

            if target not in nodes:
                await server.increase_failure_detector('missed_nack')
                return self.build_nack(server)

            # Record audit event
            server._audit_log.record(
                AuditEventType.NODE_LEFT,
                node=target,
                source=addr,
            )

            # Propagate leave to others
            others = server.get_other_nodes(target)
            base_timeout = server._context.read('current_timeout')
            gather_timeout = server.get_lhm_adjusted_timeout(base_timeout) * 2

            await server._gather_with_errors(
                [server.send_if_ok(node, message + b'>' + target_addr) for node in others],
                operation="leave_propagation",
                timeout=gather_timeout,
            )

            await server._safe_queue_put(
                nodes[target],
                (ctx.clock_time, b'DEAD'),
                target,
            )
            server._context.write('nodes', nodes)

            # Update incarnation tracker and probe scheduler
            server._incarnation_tracker.update_node(
                target, b'DEAD', 0, time.monotonic()
            )
            server.update_probe_scheduler_membership()

            return self.build_ack(server)
