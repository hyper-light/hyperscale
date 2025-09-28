import asyncio
from typing import Literal
from hyperscale.core.engines.client.ssh.protocol.ssh.connection import (
    connect, 
    SSHClientConnection, 
    SSHWriter, 
    SSHReader,
)
from hyperscale.core.engines.client.ssh.protocol.ssh.misc import DefTuple, Env, EnvSeq
from .sftp import (
    MIN_SFTP_VERSION,
    SFTPClientHandler,
)

import asyncio
import pathlib
from typing import Any, Literal
from hyperscale.core.engines.client.ssh.protocol.ssh.connection import (
    SSHClientConnection,
)
from hyperscale.core.engines.client.ssh.protocol.ssh_connection import SSHConnection
from .sftp import SFTPClientHandler


ConnectionType = Literal["SOURCE", "DEST"]


class SFTPConnection:

    __slots__ = (
        "connected",
        "connection",
        "lock",
        "_path",
        "_loop",
        "_factory",
        "_base_command",
    )

    def __init__(self):
        self.connected: bool = False
        self.connection: SSHClientConnection | None = None

        self.lock = asyncio.Lock()
        self._path: str| pathlib.Path | None = None
        self._loop = asyncio.get_event_loop()
        self._factory = SSHConnection()
        self._base_command: bytes = b''
    

    async def make_connection(
        self,
        socket_config: tuple[str | int | tuple[str, int], ...]=None,
        config: tuple[str,...] = (),
        **kwargs: dict[str, Any],
    ):
        if self.connected is False:
            self.connection = await self._factory.connect(
                socket_config,
                config=config,
                **kwargs,
            )

            self.connected = True

    async def create_session(
        self,
        env: DefTuple[Env | None] = (),
        send_env: DefTuple[EnvSeq | None] = (),
        sftp_version: int = MIN_SFTP_VERSION
    ) -> SFTPClientHandler:

        writer, reader, _ = await self.connection.open_session(
            subsystem='sftp',
            env=env,
            send_env=send_env,
            encoding=None,
        )

        handler = SFTPClientHandler(
            self._loop,
            reader,
            writer,
            sftp_version,
        )

        await handler.start()

        self.connection.create_task(handler.recv_packets())

        await handler.request_limits()

        return handler
    
    def close(self):
        if self.connection:
            self.connection.close()


