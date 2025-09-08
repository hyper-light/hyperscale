import asyncio
import pathlib
from typing import Any, Literal
from hyperscale.core.engines.client.ssh.protocol.connection import (
    SSHClientConnection,
)
from .scp import SCPHandler
from .ssh_connection import SSHConnection


ConnectionType = Literal["SOURCE", "DEST"]


class SCPConnection:

    __slots__ = (
        "connected",
        "connection",
        "handler",
        "lock",
        "_path",
        "_loop",
        "_factory",
        "_base_command",
        "connection_type",
    )

    def __init__(
        self,
        connection_type: ConnectionType,
    ):
        self.connected: bool = False
        self.connection: SSHClientConnection | None = None
        self.handler: SCPHandler | None = None

        self.lock = asyncio.Lock()
        self._path: str| pathlib.Path | None = None
        self._loop = asyncio.get_event_loop()
        self._factory = SSHConnection()
        self._base_command: bytes = b''
        self.connection_type = connection_type
    
    async def make_connection(
        self,
        command: bytes,
        socket_config: tuple[str | int | tuple[str, int], ...]=None,
        config: tuple[str,...] = (),
        must_be_dir: bool = False,
        preserve: bool = False,
        recurse: bool = False,
        **kwargs: dict[str, Any],
    ) -> SSHClientConnection:
        """Convert an SCP path into an SSHClientConnection and path"""

        if not self.connected:
            self.connection = await self._factory.connect(
                socket_config,
                config=config,
                **kwargs,
            )

            if must_be_dir:
                command += b'-d '

            if preserve:
                command += b'-p '

            if recurse:
                command += b'-r '

            self._base_command = command

            self.connected = True

    async def create_session(
        self,
        path: bytes,
    ) -> tuple[SCPHandler, ConnectionType]:
        
        command = self._base_command + path

        writer, reader, _ = await self.connection.open_session(command, encoding=None)

        return SCPHandler(reader, writer), self.connection_type