import asyncio
import pathlib
from typing import Any
from hyperscale.core.engines.client.ssh.protocol.connection import connect, SSHClientConnection
from hyperscale.core.engines.client.ssh.protocol.known_hosts import KnownHostsArg
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPErrorHandler, SFTPProgressHandler
from .scp import SCP_BLOCK_SIZE, SCPHandler

class SCPConnection:

    __slots__ = (
        "connected",
        "connection",
        "lock",
        "_path",
        "_command",
    )

    def __init__(self):
        self.connected: bool = False
        self.connection: SSHClientConnection | None = None

        self.lock = asyncio.Lock()
        self._path: str| pathlib.Path | None = None
        self._command: str | None = None
    
    async def make_connection(
        self,
        command: str,
        path: str | pathlib.Path,
        username: str | None = None,
        password: str | None = None,
        must_be_dir: bool = False,
        preserve: bool = False,
        recurse: bool = False,
        block_size: int = SCP_BLOCK_SIZE,
        progress_handler: SFTPProgressHandler = None,
        error_handler: SFTPErrorHandler = None, 
        known_hosts: KnownHostsArg = None,
        **kwargs: dict[str, Any],
    ) -> SCPHandler:
        """Convert an SCP path into an SSHClientConnection and path"""

        if not self.connected or self._path != path or self._command != command:


            # pylint: disable=cyclic-import,import-outside-toplevel

            if isinstance(path, tuple):
                conn, _ = path
            elif isinstance(path, str) and ':' in path:
                conn, _ = path.split(':', 1)
            elif isinstance(path, bytes) and b':' in path:
                conn, _ = path.split(b':', 1)
                conn = conn.decode('utf-8')
            elif isinstance(path, (bytes, str, pathlib.PurePath)):
                conn = None
            else:
                conn = path


            if isinstance(conn, str):
                conn = await connect(
                    conn, 
                    preserve=preserve,
                    recurse=recurse,
                    must_be_dir=must_be_dir,
                    block_size=block_size,
                    progress_handler=progress_handler,
                    error_handler=error_handler,
                    known_hosts=known_hosts,
                    username=username,
                    password=password,
                    **kwargs,)
            elif isinstance(conn, tuple):
                conn = await connect(*conn, **kwargs)
 
            encoded_command += command.encode()
            writer, reader, _ = await conn.open_session(command, encoding=None)

            self.connection = SCPHandler(writer, reader)
            self.connected = True

        return self.connection