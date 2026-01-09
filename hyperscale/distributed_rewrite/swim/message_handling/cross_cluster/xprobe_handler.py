"""
Handler for XPROBE messages (cross-cluster health probes).
"""

from typing import ClassVar

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed_rewrite.swim.message_handling.core import BaseHandler


class XProbeHandler(BaseHandler):
    """
    Handles xprobe messages (cross-cluster health probes).

    Cross-cluster probes are sent from gates to DC leader managers
    to check health. Subclasses (ManagerServer, GateServer) override
    _build_xprobe_response for specific behavior.

    This base implementation returns xnack.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"xprobe",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle an xprobe message."""
        # Delegate to server's _build_xprobe_response method
        # This is overridden in ManagerServer and GateServer
        xack = await self._build_xprobe_response(
            context.source_addr, context.target_addr_bytes or b""
        )

        if xack:
            return HandlerResult(response=b"xack>" + xack, embed_state=False)

        return HandlerResult(
            response=b"xnack>" + self._server.udp_addr_slug, embed_state=False
        )

    async def _build_xprobe_response(
        self, source_addr: tuple[str, int], probe_data: bytes
    ) -> bytes | None:
        """
        Build response to cross-cluster probe.

        Override in subclasses for specific behavior.

        Args:
            source_addr: Address that sent the probe.
            probe_data: Pickled CrossClusterProbe data.

        Returns:
            Pickled CrossClusterAck or None to send xnack.
        """
        # Default implementation: not a DC leader, return None for xnack
        return None
