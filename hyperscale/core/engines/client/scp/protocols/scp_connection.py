import asyncio
import pathlib
from typing import Any, Iterator, Literal, Sequence
from hyperscale.core.engines.client.ssh.protocol.ssh.connection import (
    SSHClientConnection,
)
from .scp import SCPHandler
from hyperscale.core.engines.client.ssh.protocol.ssh_connection import SSHConnection


ConnectionType = Literal["SOURCE", "DEST"]


class SCPConnection:

    __slots__ = (
        "connected",
        "connection",
        "lock",
        "_path",
        "_loop",
        "_factory",
        "_base_command",
        "connection_type",
        "target",
        "connection_options",
    )

    def __init__(
        self,
        connection_type: ConnectionType,
    ):
        self.connected: bool = False
        # The address (as the request named it) the open connection serves,
        # and the options -- credentials included -- it was opened with.
        self.target: str | None = None
        self.connection_options: dict[str, Any] | None = None
        self.connection: SSHClientConnection | None = None

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

            self.connected = True

        self._set_command(command, must_be_dir, preserve, recurse)

    def _set_command(
        self,
        command: bytes,
        must_be_dir: bool,
        preserve: bool,
        recurse: bool,
    ) -> None:
        """This request's scp command: its flags belong to the request, not the connection."""
        if must_be_dir:
            command += b'-d '

        if preserve:
            command += b'-p '

        if recurse:
            command += b'-r '

        self._base_command = command

    async def connect_to_any(
        self,
        command: bytes,
        target: str,
        addresses: Sequence[tuple[str, tuple[str | int | tuple[str, int], ...]]],
        address_rotation: Iterator[int],
        must_be_dir: bool = False,
        preserve: bool = False,
        recurse: bool = False,
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
                # A reused connection still takes this request's flags.
                self._set_command(command, must_be_dir, preserve, recurse)
                return None, None, False

            self.reset()

        if not addresses:
            raise ConnectionError("No addresses to connect to")

        offset = next(address_rotation) % len(addresses)
        connection_error: Exception | None = None

        for address, socket_config in (*addresses[offset:], *addresses[:offset]):
            try:
                await self.make_connection(
                    command,
                    socket_config,
                    must_be_dir=must_be_dir,
                    preserve=preserve,
                    recurse=recurse,
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
        path: bytes,
    ) -> tuple[SCPHandler, ConnectionType]:
        
        command = self._base_command + path

        writer, reader, _ = await self.connection.open_session(command, encoding=None)

        return SCPHandler(reader, writer), self.connection_type

    def close(self):
        if self.connection:
            self.connection.close()

    def reset(self):
        """Drop the SSH connection; the next request connects afresh."""
        if self.connection:
            self.connection.abort()

        self.connection = None
        self.connected = False
        self.target = None
        self.connection_options = None
