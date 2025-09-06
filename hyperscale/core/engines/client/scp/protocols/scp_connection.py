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
        "lock",
        "_path",
        "_command",
        "_loop",
        "_factory"
    )

    def __init__(self):
        self.connected: bool = False
        self.connection: SSHClientConnection | None = None

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

        if not self.connected or self._command != command:


            conn = await self._factory.connect(
                socket_config,
                config=config,
                **kwargs,
            )
           
            writer, reader, _ = await conn.open_session(command, encoding=None)

            self.connection = SCPHandler(writer, reader)
            self.connected = True

        return self.connection