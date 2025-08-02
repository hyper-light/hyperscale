import asyncio
from typing import Sequence
from hyperscale.core.engines.client.ssh.protocol.connection import SSHClientConnection
from hyperscale.core.engines.client.sftp.protocol.sftp import SFTPErrorHandler, SFTPProgressHandler, local_fs
from .scp import (
    SCPConnPath,
    SCP_BLOCK_SIZE,
    parse_path,
    SCPSource,
    start_remote,
)



class SCPProtocol:

    __slots__ = (
        "connected",
        "_dst_connection",
        "_dst_path",
        "_src_paths",
        "connection",
        "lock"
    )

    def __init__(self):
        self.connected: bool = False
        self._dst_connection: SSHClientConnection | None = None
        self._dst_path: str | None = None
        self.connection: SCPSource | None = None

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

        dst_reader, dst_writer = await start_remote(
            dstconn, False, must_be_dir, preserve, recurse, dstpath)
        
        self.connection = SCPSource(local_fs, dst_reader, dst_writer,
                            preserve, recurse, block_size,
                            progress_handler, error_handler)

        self.connected = True

    async def close(self):
        await self.connection.close()