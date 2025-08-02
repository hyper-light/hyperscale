from hyperscale.core.engines.client.ssh.protocol.connection import SSHClientConnection
from hyperscale.core.engines.client.sftp.protocol.sftp import SFTPErrorHandler, SFTPProgressHandler, local_fs
from .scp import (
    SCPConnPath,
    SCP_BLOCK_SIZE,
    parse_path,
    SCPCopier,
    SCPSink,
    SCPSource,
    start_remote,
)

class SCPConnectionSet:

    def __init__(self):
        self.copier: SCPCopier | None = None
        self.sink: SCPSink | None = None
        self.source: SCPSource | None = None

    async def connect(
        self,
        srcpath: SCPConnPath,
        dstconn: SSHClientConnection,
        dstpath: SCPConnPath,
        preserve: bool = False,
        recurse: bool = False,
        must_be_dir: bool = False,
        block_size: int = SCP_BLOCK_SIZE,
        progress_handler: SFTPProgressHandler = None,
        error_handler: SFTPErrorHandler = None, 
        **kwargs,
    ):

        srcconn, srcpath, _ = await parse_path(srcpath, **kwargs)
        src_reader, src_writer = await start_remote(
            srcconn, 
            True, 
            must_be_dir, 
            preserve, 
            recurse, 
            srcpath,
        )

        dst_reader, dst_writer = await start_remote(
            dstconn, False, must_be_dir, preserve, recurse, dstpath)
        
        self.copier = SCPCopier(src_reader, src_writer, dst_reader,
                            dst_writer, block_size,
                            progress_handler, error_handler)


        self.sink = SCPSink(local_fs, src_reader, src_writer, must_be_dir,
                        preserve, recurse, block_size,
                        progress_handler, error_handler)


        self.source = SCPSource(local_fs, dst_reader, dst_writer,
                            preserve, recurse, block_size,
                            progress_handler, error_handler)