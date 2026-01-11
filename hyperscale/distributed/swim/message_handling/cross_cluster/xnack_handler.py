"""
Handler for XNACK messages (cross-cluster probe rejections).
"""

from typing import ClassVar

from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


class XNackHandler(BaseHandler):
    """
    Handles xnack messages (cross-cluster probe rejections).

    Indicates the target is not a DC leader or cannot respond.
    The probe will timeout and try another target.
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"xnack",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle an xnack message."""
        # xnack is a rejection - just ignore, probe will timeout
        return self._empty()
