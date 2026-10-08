import asyncio
from typing import Literal
from hyperscale.core.engines.client.ssh.protocol.ssh.connection import SSHClientConnection
from hyperscale.core.engines.client.ssh.protocol.ssh.misc import DefTuple, Env, EnvSeq
from .sftp import (
    MIN_SFTP_VERSION,
    SFTPClientHandler,
)

import asyncio
import pathlib
from typing import Any, Iterator, Literal, Sequence
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
        "target",
        "connection_options",
        "session",
    )

    def __init__(self):
        self.connected: bool = False
        # The address (as the request named it) the open connection serves,
        # and the options -- credentials included -- it was opened with.
        self.target: str | None = None
        self.connection_options: dict[str, Any] | None = None
        # The SFTP session requests on this connection share.
        self.session: SFTPClientHandler | None = None
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

    async def connect_to_any(
        self,
        target: str,
        addresses: Sequence[tuple[str, tuple[str | int | tuple[str, int], ...]]],
        address_rotation: Iterator[int],
        **kwargs: dict[str, Any],
    ) -> tuple[str | None, tuple[str | int | tuple[str, int], ...] | None, bool]:
        """
        Reuse this connection's open SSH connection when it was opened for
        ``target``, the address the request names, with the same options
        (credentials included); any other open connection is dropped first. Otherwise open a new one,
        trying the host's ``addresses`` one at a time from the next offset in
        ``address_rotation`` so a pool's connections spread across all of
        them; the first to connect wins.

        Returns the address and socket config of a new connection (``None``
        for both on reuse), and whether the connection is new.
        """
        if self.connected:
            if self.target == target and self.connection_options == kwargs:
                return None, None, False

            self.reset()

        if not addresses:
            raise ConnectionError("No addresses to connect to")

        offset = next(address_rotation) % len(addresses)
        connection_error: Exception | None = None

        for address, socket_config in (*addresses[offset:], *addresses[:offset]):
            try:
                await self.make_connection(
                    socket_config,
                    **kwargs,
                )

            except Exception as err:
                # Close this attempt's connection before trying the next address.
                connection_error = err
                self.reset()
                continue

            self.target = target
            self.connection_options = kwargs

            return address, socket_config, True

        try:
            raise connection_error

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None

    async def create_session(
        self,
        env: DefTuple[Env | None] = (),
        sftp_version: int = MIN_SFTP_VERSION
    ) -> SFTPClientHandler:

        writer, reader, _ = await self.connection.open_session(
            subsystem='sftp',
            env=env,
            send_env=env,
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

        self.session = handler

        return handler
    
    def close(self):
        if self.connection:
            self.connection.close()

    def reset(self):
        """Drop the SSH connection (and any SFTP session on it); the next
        request connects afresh."""
        if self.connection:
            self.connection.abort()

        self.connection = None
        self.connected = False
        self.target = None
        self.connection_options = None
        self.session = None


