import asyncio
import pathlib
from typing import Any
from hyperscale.core.engines.client.ssh.protocol.connection import (
    connect,
    connect_with_options,
    SSHClientConnection,
)
from .scp import SCPHandler
from .ssh_connection import SSHConnection

class SCPConnection:

    __slots__ = (
        "connected",
        "connection",
        "handler",
        "lock",
        "_path",
        "_command",
        "_loop",
        "_factory"
    )

    def __init__(self):
        self.connected: bool = False
        self.connection: SSHClientConnection | None = None
        self.handler: SCPHandler | None = None

        self.lock = asyncio.Lock()
        self._path: str| pathlib.Path | None = None
        self._command: bytes | None = None
        self._loop = asyncio.get_event_loop()
        self._factory = SSHConnection()
    
    async def make_connection(
        self,
        command: bytes,
        socket_config: tuple[str | int | tuple[str, int], ...]=None,
        config: tuple[str,...] = (),
        **kwargs: dict[str, Any],
    ) -> SCPHandler:
        """Convert an SCP path into an SSHClientConnection and path"""

        if not self.connected:


            self.connection = await self._factory.connect(
                socket_config,
                config=config,
                **kwargs,
            )

            self.connected = True

        if self._command != command:
            writer, reader, _ = await self.connection.open_session(command, encoding=None)
            self._command = command

            self.handler = SCPHandler(reader, writer)

        return self.handler