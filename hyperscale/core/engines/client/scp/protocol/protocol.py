import asyncio
from pathlib import PurePath
from typing import Sequence
from hyperscale.core.engines.client.ssh.protocol.connection import SSHClientConnection
from hyperscale.core.engines.client.sftp.protocol.sftp import SFTPErrorHandler, SFTPProgressHandler
from .scp import (
    SCPConnPath,
    SCP_BLOCK_SIZE,
    parse_path,
)
from .scp_connection_set import SCPConnectionSet



class SCPProtocol:

    __slots__ = (
        "connected",
        "_dst_connection",
        "_dst_path",
        "_src_paths",
        "_connection_sets",
        "lock"
    )

    def __init__(self):
        self.connected: bool = False
        self._dst_connection: SSHClientConnection | None = None
        self._dst_path: str | None = None
        self._connection_sets: dict[SCPConnPath, SCPConnectionSet] = {}

        self.lock = asyncio.Lock()

    async def connect(
        self,
        srcpaths: SCPConnPath | Sequence[SCPConnPath] | None = None,
        dstpath: SCPConnPath | None = None, *,
        preserve: bool = False,
        recurse: bool = False,
        block_size: int = SCP_BLOCK_SIZE,
        progress_handler: SFTPProgressHandler = None,
        error_handler: SFTPErrorHandler = None, 
        **kwargs,
    ) -> None:
        
        if not isinstance(srcpaths, (list, set, tuple,)):
            srcpaths = [srcpaths] # type: ignore


        has_new_source_path = any(
            path not in self._src_paths
            for path in srcpaths
        )

        has_new_dest_path = dstpath == self._dst_path
        path_diff_exists = has_new_source_path or has_new_dest_path

        # If we have no new paths to copy and are aleady connected,
        # abort.
        if path_diff_exists is False and self.connected is True:
            return
        

        must_be_dir = len(srcpaths) > 1

        dstconn, dstpath, _ = await parse_path(dstpath, **kwargs)

        self._dst_connection = dstconn
        self._dst_path = dstpath
        self._src_paths = srcpaths

        for srcpath in srcpaths:
            connection_set = SCPConnectionSet()
            await connection_set.connect(
                srcpath=srcpath,
                dstconn=dstconn,
                dstpath=dstpath,
                preserve=preserve,
                recurse=recurse,
                must_be_dir=must_be_dir,
                block_size=block_size,
                progress_handler=progress_handler,
                error_handler=error_handler,
            )

            self._connection_sets[srcpath] = connection_set

        self.connected = True

    async def copy(
        self,
        local_source: SCPConnPath,
    ):
        await self._connection_sets[local_source].copier.run()

    async def receive(
        self,
        local_source: SCPConnPath,
    ):
        await self._connection_sets[local_source].sink.run(self._dst_path)

    async def send(
        self,
        local_source: SCPConnPath,
    ):
        await self._connection_sets[local_source].source.run(local_source)