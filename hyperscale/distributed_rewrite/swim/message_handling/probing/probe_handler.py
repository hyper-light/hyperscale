"""
Handler for PROBE messages.
"""

from base64 import b64encode
from typing import ClassVar

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


# Separator for embedded state
STATE_SEPARATOR = b"#|s"


class ProbeHandler(BaseHandler):
    """
    Handles PROBE messages.

    Probes check if a node is alive:
    - Confirm the sender (AD-29)
    - If target is self, send refutation with embedded state
    - Otherwise forward probe and send ack
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"probe",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a probe message."""
        source_addr = context.source_addr
        target = context.target
        target_addr_bytes = context.target_addr_bytes
        message = context.message

        # AD-29: Confirm the sender
        self._server.confirm_peer(source_addr)

        # Validate target
        if not await self._server.validate_target(target, b"probe", source_addr):
            return self._nack()

        # Process probe within context
        async with self._server.context_with_value(target):
            nodes = self._server.read_nodes()

            # If probe is about self, send refutation
            if self._server.udp_target_is_self(target):
                return await self._handle_self_probe()

            # Unknown target
            if target not in nodes:
                return self._nack(b"unknown")

            # Forward probe to target
            await self._forward_probe(target, context.source_addr_string)

            # Propagate probe to others
            await self._propagate_probe(target, target_addr_bytes, message)

            return self._ack()

    async def _handle_self_probe(self) -> HandlerResult:
        """Handle probe about self - send refutation."""
        await self._server.increase_failure_detector("refutation")
        new_incarnation = await self._server.broadcast_refutation()

        base = (
            b"alive:"
            + str(new_incarnation).encode()
            + b">"
            + self._server.udp_addr_slug
        )

        state = self._server.get_embedded_state()
        if state:
            response = base + STATE_SEPARATOR + b64encode(state)
        else:
            response = base

        return HandlerResult(response=response, embed_state=False)

    async def _forward_probe(
        self, target: tuple[str, int], source_addr_string: str
    ) -> None:
        """Forward probe to target with ack."""
        base_timeout = self._server.get_current_timeout()
        timeout = self._server.get_lhm_adjusted_timeout(base_timeout)

        ack_with_state = self._server.build_ack_with_state_for_addr(
            source_addr_string.encode()
        )

        self._server.task_runner.run(
            self._server.send,
            target,
            ack_with_state,
            timeout=timeout,
        )

    async def _propagate_probe(
        self,
        target: tuple[str, int],
        target_addr_bytes: bytes | None,
        message: bytes,
    ) -> None:
        """Propagate probe to other cluster members."""
        if target_addr_bytes is None:
            return

        others = self._server.get_other_nodes(target)
        base_timeout = self._server.get_current_timeout()
        timeout = self._server.get_lhm_adjusted_timeout(base_timeout)
        gather_timeout = timeout * 2

        propagate_msg = message + b">" + target_addr_bytes

        coros = [self._server.send_if_ok(node, propagate_msg) for node in others]
        await self._server.gather_with_errors(
            coros, operation="probe_propagation", timeout=gather_timeout
        )
