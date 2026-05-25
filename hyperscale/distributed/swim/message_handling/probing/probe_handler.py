"""
Handler for PROBE messages.
"""

from base64 import b64encode
from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


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

        await self._server.confirm_peer(source_addr)

        # Validate target
        if not await self._server.validate_target(target, b"probe", source_addr):
            return self._nack()

        # Process probe within context
        async with await self._server.context_with_value(target):
            nodes = self._server.read_nodes()

            # If probe is about self, send refutation
            if self._server.udp_target_is_self(target):
                return await self._handle_self_probe(message)

            # Unknown target
            if target not in nodes:
                return self._nack(b"unknown")

            # Forward probe to target
            await self._forward_probe(target, context.source_addr_string)

            # Propagate probe to others
            await self._propagate_probe(target, target_addr_bytes, message)

            return self._ack()

    async def _handle_self_probe(self, message: bytes) -> HandlerResult:
        """Handle probe about self — respond ALIVE.

        A SWIM probe-about-self is a routine health check, not a
        suspicion. The original implementation treated it as a
        refutation event (bumping LHM, broadcasting a full refutation),
        which conflated probe semantics with the SUSPECT-about-self
        path. Two real-world consequences observed:

        1. Every received probe pumped self-LHM. With ``probe_interval``
           ≈ 1 s and one or more peers probing this node, LHM grew at
           ~1/s — the suspicion bracket lengthened proportionally and
           dead-peer detection got unboundedly slower as the cluster
           started up.
        2. Every probe triggered ``broadcast_refutation`` (an
           N-fanout), saturating the gossip channel with redundant
           ALIVE messages on every probe round.

        Per the Lifeguard paper (and AD-30), LHM is bumped on
        ``on_refutation_needed`` — receipt of suspicion gossip about
        self. A probe is not suspicion; it's just "are you alive?".
        Reply with the current incarnation and embedded state.
        """
        new_incarnation = self._server.incarnation_tracker.get_self_incarnation()
        request_id = self._server.parse_probe_request_id_from_message(message)
        base = (
            b"alive:"
            + str(new_incarnation).encode()
            + b":"
            + self._server.get_self_node_id().encode()
        )
        if request_id:
            base += b":" + request_id.encode()
        base += b">" + self._server.udp_addr_slug

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
        base_timeout = await self._server.get_current_timeout()
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
        base_timeout = await self._server.get_current_timeout()
        timeout = self._server.get_lhm_adjusted_timeout(base_timeout)
        gather_timeout = timeout * 2

        propagate_msg = message + b">" + target_addr_bytes

        coros = [self._server.send_if_ok(node, propagate_msg) for node in others]
        await self._server.gather_with_errors(
            coros, operation="probe_propagation", timeout=gather_timeout
        )
