import asyncio
from typing import Sequence, Any
from hyperscale.core.engines.client.ssh.protocol.known_hosts import KnownHostsArg
from hyperscale.core.engines.client.sftp.protocol.sftp import SFTPErrorHandler, SFTPProgressHandler, local_fs
from .scp import (
    SCPConnPath,
    SCP_BLOCK_SIZE,
    SCPClient,
)



class SCPProtocol:

    __slots__ = (
        "connected",
        "_dst_path",
        "_src_paths",
        "connection",
        "lock"
    )

    def __init__(self):
        self.connected: bool = False
        self._src_paths: list[SCPConnPath] = []
        self._dst_path: str | None = None
        self.connection: SCPClient | None = None

        self.lock = asyncio.Lock()

    async def connect(
        self,
        srcpaths: SCPConnPath | Sequence[SCPConnPath] | None,
        dstpath: SCPConnPath | None,
        username: str | None = None,
        password: str | None = None,
        preserve: bool = False,
        recurse: bool = False,
        block_size: int = SCP_BLOCK_SIZE,
        progress_handler: SFTPProgressHandler = None,
        error_handler: SFTPErrorHandler = None, 
        known_hosts: KnownHostsArg = None,
        **kwargs: dict[str, Any],
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

        self.connection = SCPClient(
            local_fs,
            preserve=preserve,
            recurse=recurse,
            must_be_dir=must_be_dir,
            block_size=block_size,
            progress_handler=progress_handler,
            error_handler=error_handler,
            known_hosts=known_hosts,
            username=username,
            password=password,
        )

        await self.connection.connect(
            srcpaths,
            dstpath,
            **kwargs,
        )

        self._src_paths = srcpaths
        self._dst_path = dstpath

        self.connected = True