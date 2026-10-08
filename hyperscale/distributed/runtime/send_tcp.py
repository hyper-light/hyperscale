"""
``SendTcp`` -- the shape of a node's bound ``send_tcp``, for the
components that are handed it as a callback.
"""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    # ``models`` imports the runtime seams; a runtime import back would cycle.
    from hyperscale.distributed.models.message import Message


class SendTcp(Protocol):
    """Send one framed TCP request and await the peer's reply.

    ``MercurySyncBaseServer.send_tcp`` bound on a node matches it. A
    ``Message`` is serialized before it is framed. The reply pairs the
    peer handler's bytes -- or the exception that ended the request, which
    is returned, never raised -- with the logical clock the reply carried.
    """

    async def __call__(
        self,
        address: tuple[str, int],
        action: str,
        data: "bytes | Message",
        timeout: int | float | None = None,
    ) -> tuple[bytes | Exception, int]: ...
