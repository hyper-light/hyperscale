from typing import AsyncIterator, Callable

from hyperscale.core.engines.client.ssh.protocol.ssh.constants import FILEXFER_ATTR_DEFINED_V4

from .protocols.sftp import SFTPAttrs, SFTPClientHandler, SFTPName


class SFTPGlobFilesystem:
    """
    The filesystem SFTPGlob matches patterns against: the interface of
    asyncssh's SFTP client, over a client handler. Paths stat as that
    client stats them, and directories list through the command's listing.
    """

    __slots__ = ("_handler", "_scandir")

    def __init__(
        self,
        handler: SFTPClientHandler,
        scandir: Callable[[bytes], AsyncIterator[SFTPName]],
    ) -> None:
        self._handler = handler
        self._scandir = scandir

    async def stat(self, path: bytes) -> SFTPAttrs:
        return await self._handler.stat(path, FILEXFER_ATTR_DEFINED_V4)

    def scandir(self, path: bytes) -> AsyncIterator[SFTPName]:
        return self._scandir(path)
