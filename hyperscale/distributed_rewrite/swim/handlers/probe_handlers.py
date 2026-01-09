"""
SWIM Probe Message Handlers.

Handles: probe, ping-req, ping-req-ack, alive, suspect
"""

import asyncio
import time
import base64
from typing import TYPE_CHECKING

from .base import BaseHandler, MessageContext, HandlerResult

from ..core.types import Nodes
from ..core.errors import UnexpectedMessageError

if TYPE_CHECKING:
    from ..health_aware_server import HealthAwareServer


class ProbeHandler(BaseHandler):
    """
    Handles PROBE messages.

    Probes check if a node is alive:
    - Confirm the sender (AD-29)
    - If target is self, send refutation with embedded state
    - Otherwise forward probe and send ack
    """

    def __init__(self) -> None:
        super().__init__((b'probe',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        target_addr = ctx.target_addr_bytes
        message = ctx.message

        # AD-29: Confirm the sender
        server.confirm_peer(addr)

        if not await server._validate_target(target, b'probe', addr):
            return self.build_nack(server)

        async with server._context.with_value(target):
            nodes: Nodes = server._context.read('nodes')

            if server.udp_target_is_self(target):
                # Probe about self - send refutation with state
                await server.increase_failure_detector('refutation')
                new_incarnation = await server.broadcast_refutation()

                base = b'alive:' + str(new_incarnation).encode() + b'>' + server._udp_addr_slug
                state = server._get_embedded_state()
                if state:
                    return HandlerResult(
                        response=base + server._STATE_SEPARATOR + base64.b64encode(state),
                        embed_state=False,
                    )
                return HandlerResult(response=base, embed_state=False)

            if target not in nodes:
                return HandlerResult(
                    response=b'nack:unknown>' + server._udp_addr_slug,
                    embed_state=False,
                )

            base_timeout = server._context.read('current_timeout')
            timeout = server.get_lhm_adjusted_timeout(base_timeout)

            # Send ack with state to the target
            ack_with_state = server._build_ack_with_state_for_addr(
                ctx.source_addr_string.encode()
            )
            server._task_runner.run(
                server.send,
                target,
                ack_with_state,
                timeout=timeout,
            )

            # Propagate probe to others
            others = server.get_other_nodes(target)
            gather_timeout = timeout * 2
            await server._gather_with_errors(
                [server.send_if_ok(node, message + b'>' + target_addr) for node in others],
                operation="probe_propagation",
                timeout=gather_timeout,
            )

            return self.build_ack(server)


class PingReqHandler(BaseHandler):
    """
    Handles PING-REQ messages (indirect probing).

    Used when direct probe fails - ask other nodes to probe the target.
    """

    def __init__(self) -> None:
        super().__init__((b'ping-req',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        target = ctx.target
        target_addr = ctx.target_addr_bytes

        async with server._context.with_value(target):
            nodes: Nodes = server._context.read('nodes')

            if target is None:
                return HandlerResult(
                    response=b'nack:invalid>' + server._udp_addr_slug,
                    embed_state=False,
                )

            if server.udp_target_is_self(target):
                # Target is self - respond with alive
                base = b'ping-req-ack:alive>' + server._udp_addr_slug
                state = server._get_embedded_state()
                if state:
                    return HandlerResult(
                        response=base + server._STATE_SEPARATOR + base64.b64encode(state),
                        embed_state=False,
                    )
                return HandlerResult(response=base, embed_state=False)

            if target not in nodes:
                return HandlerResult(
                    response=b'ping-req-ack:unknown>' + server._udp_addr_slug,
                    embed_state=False,
                )

            base_timeout = server._context.read('current_timeout')
            timeout = server.get_lhm_adjusted_timeout(base_timeout)

            try:
                result = await asyncio.wait_for(
                    server._send_probe_and_wait(target),
                    timeout=timeout,
                )
                if result:
                    return HandlerResult(
                        response=b'ping-req-ack:alive>' + target_addr,
                        embed_state=False,
                    )
                else:
                    return HandlerResult(
                        response=b'ping-req-ack:dead>' + target_addr,
                        embed_state=False,
                    )
            except asyncio.TimeoutError:
                return HandlerResult(
                    response=b'ping-req-ack:timeout>' + target_addr,
                    embed_state=False,
                )


class PingReqAckHandler(BaseHandler):
    """
    Handles PING-REQ-ACK messages (indirect probe responses).
    """

    def __init__(self) -> None:
        super().__init__((b'ping-req-ack',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        message = ctx.message

        # Verify we have a pending indirect probe for this target
        if target and not server._indirect_probe_manager.get_pending_probe(target):
            await server.handle_error(
                UnexpectedMessageError(
                    msg_type=b'ping-req-ack',
                    expected=None,
                    source=addr,
                )
            )
            return self.build_ack(server)

        msg_parts = message.split(b':', maxsplit=1)
        if len(msg_parts) > 1:
            status_str = msg_parts[1]
            if status_str == b'alive' and target:
                await server.handle_indirect_probe_response(target, is_alive=True)
                await server.decrease_failure_detector('successful_probe')
                return self.build_ack(server)
            elif status_str in (b'dead', b'timeout', b'unknown') and target:
                await server.handle_indirect_probe_response(target, is_alive=False)

        return self.build_ack(server)


class AliveHandler(BaseHandler):
    """
    Handles ALIVE messages (refutations).

    A node sends ALIVE to prove it's alive when suspected.
    """

    def __init__(self) -> None:
        super().__init__((b'alive',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        message = ctx.message

        msg_incarnation = await server._parse_incarnation_safe(message, addr)

        # AD-29: Confirm the sender
        server.confirm_peer(addr)

        # Complete any pending probe Future for this address
        pending_future = server._pending_probe_acks.get(addr)
        if pending_future and not pending_future.done():
            pending_future.set_result(True)

        if target:
            if server.is_message_fresh(target, msg_incarnation, b'OK'):
                await server.refute_suspicion(target, msg_incarnation)
                server.update_node_state(
                    target,
                    b'OK',
                    msg_incarnation,
                    time.monotonic(),
                )
                await server.decrease_failure_detector('successful_probe')

        return self.build_ack(server)


class SuspectHandler(BaseHandler):
    """
    Handles SUSPECT messages.

    When a node is suspected of being dead:
    - If about self, broadcast refutation
    - Otherwise start suspicion timer
    """

    def __init__(self) -> None:
        super().__init__((b'suspect',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        message = ctx.message

        msg_incarnation = await server._parse_incarnation_safe(message, addr)

        # AD-29: Confirm the sender
        server.confirm_peer(addr)

        if target:
            if server.udp_target_is_self(target):
                # Suspicion about self - refute it
                await server.increase_failure_detector('refutation')
                new_incarnation = await server.broadcast_refutation()

                base = b'alive:' + str(new_incarnation).encode() + b'>' + server._udp_addr_slug
                state = server._get_embedded_state()
                if state:
                    return HandlerResult(
                        response=base + server._STATE_SEPARATOR + base64.b64encode(state),
                        embed_state=False,
                    )
                return HandlerResult(response=base, embed_state=False)

            if server.is_message_fresh(target, msg_incarnation, b'SUSPECT'):
                await server.start_suspicion(target, msg_incarnation, addr)

                suspicion = server._suspicion_manager.get_suspicion(target)
                if suspicion and suspicion.should_regossip():
                    suspicion.mark_regossiped()
                    await server.broadcast_suspicion(target, msg_incarnation)

        return self.build_ack(server)
