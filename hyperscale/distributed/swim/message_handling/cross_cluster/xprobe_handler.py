"""
Handler for XPROBE messages (cross-cluster health probes).
"""

from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class XProbeHandler(BaseHandler):
    """
    Handles xprobe messages (cross-cluster health probes).

    Cross-cluster probes are sent from gates to DC leader managers
    to check health. The server's _build_xprobe_response method
    (overridden in ManagerServer, GateServer) provides specific behavior.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"xprobe",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle an xprobe message."""
        # Delegate to server's build_xprobe_response method via ServerInterface
        # This is overridden in ManagerServer and GateServer
        xack = await self._server.build_xprobe_response(
            context.source_addr, context.target_addr_bytes or b""
        )

        if xack:
            return HandlerResult(response=b"xack>" + xack, embed_state=False)

        return HandlerResult(
            response=b"xnack>" + self._server.udp_addr_slug, embed_state=False
        )
