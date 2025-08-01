import asyncio
from typing import Literal
from hyperscale.core.engines.client.ssh.protocol.connection import (
    connect, 
    SSHClientConnection, 
    SSHWriter, 
    SSHReader,
)
from hyperscale.core.engines.client.ssh.protocol.misc import DefTuple, Env, EnvSeq
from .sftp import start_sftp_client, SFTPClient, MIN_SFTP_VERSION


class SFTPProtocol:
    __slots__ = (
        "dns_address",
        "port",
        "connected",
        "_ssh_connection",
        "_reader",
        "_writer",
        "lock",
    )

    def __init__(self):
        self.dns_address: str = None
        self.port: int = None
        self._ssh_connection: SSHClientConnection | None = None
        self._client: SFTPClient | None = None
        self.connected = False
        self._writer: SSHWriter | None = None
        self._reader: SSHReader | None = None
        self.lock = asyncio.Lock()

    async def connection(
        self,
        dns_address: str,
        port: int,
        env: DefTuple[Env | None] = (),
        send_env: DefTuple[EnvSeq | None] = (),
        path_encoding: str | None = 'utf-8',
        path_errors = 'strict',
        sftp_version: Literal[3] = MIN_SFTP_VERSION
    ):
        if self.connected is False:
            self._ssh_connection = await connect(
                dns_address,
                port,
            )



            writer, reader, _ = await self._ssh_connection.open_session(
                subsystem='sftp',
                env=env,
                send_env=send_env,
                encoding=None,
            )

            self._writer = writer
            self._reader = reader


            self._client = await start_sftp_client(
                self._ssh_connection,
                self._ssh_connection._loop,
                self._reader,
                self._writer,
                path_encoding=path_encoding,
                path_errors=path_errors,
                sftp_version=sftp_version,
            )

            self.connected = True
            
        
