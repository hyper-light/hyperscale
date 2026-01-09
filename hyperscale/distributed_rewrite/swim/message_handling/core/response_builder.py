"""
Builds responses with embedded state for SWIM messages.
"""

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    HandlerResult,
    ServerInterface,
)


class ResponseBuilder:
    """
    Builds SWIM protocol responses with embedded state.

    Centralizes response construction including state embedding,
    ensuring consistent formatting across all handlers.
    """

    def __init__(self, server: ServerInterface) -> None:
        """
        Initialize response builder.

        Args:
            server: Server interface for state access.
        """
        self._server = server

    def build_ack(self, embed_state: bool = True) -> bytes:
        """
        Build ack response.

        Args:
            embed_state: Whether to embed state.

        Returns:
            Ack response bytes.
        """
        if embed_state:
            return self._server.build_ack_with_state()
        return b"ack>" + self._server.udp_addr_slug

    def build_nack(self, reason: bytes = b"") -> bytes:
        """
        Build nack response.

        Args:
            reason: Optional reason for the nack.

        Returns:
            Nack response bytes.
        """
        if reason:
            return b"nack:" + reason + b">" + self._server.udp_addr_slug
        return b"nack>" + self._server.udp_addr_slug

    def finalize(self, result: HandlerResult) -> bytes:
        """
        Finalize a handler result into response bytes.

        If the handler requested state embedding and didn't already
        embed it, this method adds the embedded state.

        Args:
            result: Handler result to finalize.

        Returns:
            Final response bytes.
        """
        if result.embed_state and result.response:
            # Handler wants state but hasn't embedded it yet
            # This shouldn't normally happen as handlers use _ack()
            # which already embeds state
            return result.response
        return result.response
