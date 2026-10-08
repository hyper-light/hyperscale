from __future__ import annotations

import asyncio
from ssl import SSLContext
from typing import Dict, Iterator, Optional, Sequence, Tuple, Literal

from hyperscale.core.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import SocketConfig
from .tcp import IMPLICIT_TLS_PORT, SMTP_LIMIT

from .tcp import TCPConnection


class SMTPConnection:
    __slots__ = (
        "address_info",
        "port",
        "ssl",
        "ip_addr",
        "lock",
        "reader",
        "writer",
        "connected",
        "reset_connections",
        "pending",
        "_connection_factory",
        "_reader_and_writer",
        "reset_connection",
        "server_name",
        "command_encoding",
        "session_key",
        "session_options",
    )

    def __init__(self, reset_connections: bool = False) -> None:
        self.address_info: tuple[
            int,
            int,
            int,
            str,
            tuple[str, int] | tuple[str, int, int , int]
        ] = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.ip_addr = None
        self.lock = asyncio.Lock()

        self.reader: Reader = None
        self.writer: Writer = None

        self._reader_and_writer: Dict[str, Tuple[Reader, Writer]] = {}

        self.connected = False
        self.reset_connection = reset_connections
        self.pending = 0
        self._connection_factory = TCPConnection()
        self.server_name: str | None = None
        self.command_encoding: Literal['ascii', 'utf-8'] = 'ascii'
        # The open SMTP session: the (server, auth) it was established for
        # and its EHLO options. None until a session completes its handshake.
        self.session_key: tuple[str, tuple[str, str] | None] | None = None
        self.session_options: dict[str, str] | None = None
        
    async def make_connection(
        self,
        hostname: str,
        address_info: str,
        ssl: Optional[SSLContext] = None,
        connection_type: Literal['insecure', 'ssl', 'tls'] = 'tls',
        ssl_upgrade: bool = False,
        timeout: int | None = None,
    ) -> None:
        port: int | None = None
        if self._reader_and_writer.get(hostname) is None or ssl_upgrade:
            (
                reader, 
                writer,
                port
            ) = await self._connection_factory.create(
                hostname,
                address_info, 
                ssl=ssl,
                connection_type=connection_type,
                timeout=timeout,
            )

            self.reader = reader
            self.writer = writer

            self._reader_and_writer[hostname] = (reader, writer)

            self.address_info = address_info
            self.port = port
            self.ssl = ssl
        else:
            reader, writer = self._reader_and_writer.get(hostname)

            self.reader = reader
            self.writer = writer

        return port

    async def connect_to_any(
        self,
        hostname: str,
        socket_configs: Sequence[SocketConfig],
        address_rotation: Iterator[int],
        ssl: Optional[SSLContext] = None,
        connection_type: Literal['insecure', 'ssl', 'tls'] = 'tls',
        ssl_upgrade: bool = False,
        timeout: int | None = None,
        implicit_tls: Optional[SSLContext] = None,
    ) -> Tuple[Optional[SocketConfig], Optional[int], bool]:
        """
        Reuse this connection's cached transport for ``hostname`` (unless
        upgrading it). Otherwise open a new one: the server's lookup lists
        its addresses once per port, in the order to try those ports, so
        race each port's addresses (RFC 8305) from the next offset in
        ``address_rotation``, so a pool's connections spread across all of
        them, and fall back to the next port only if all of them fail.

        Returns the socket config and port of a new transport (``None`` for
        both on reuse), and whether the transport is new.
        """
        if ssl_upgrade is False and (cached := self._reader_and_writer.get(hostname)) is not None:
            self.reader, self.writer = cached
            return None, None, False

        if not socket_configs:
            raise ConnectionError(f"No addresses to connect to for {hostname}")

        if self._reader_and_writer:
            # One SMTP session per connection: another server's transport,
            # and the session open on it, end before this server's begins.
            self.reset()

        offset = next(address_rotation)
        connection_error: Exception | None = None

        for port_configs in self._port_groups(socket_configs):
            start = offset % len(port_configs)
            ordered = [*port_configs[start:], *port_configs[:start]]

            # Submission over implicit TLS (RFC 8314): TLS from the first
            # byte, not a STARTTLS upgrade after the greeting.
            if port_configs[0][4][1] == IMPLICIT_TLS_PORT:
                group_ssl, group_connection_type = implicit_tls, 'ssl'

            else:
                group_ssl, group_connection_type = ssl, connection_type

            try:
                reader, writer, port, winner_index = await self._connection_factory.create_racing(
                    hostname,
                    ordered,
                    ssl=group_ssl,
                    connection_type=group_connection_type,
                    timeout=timeout,
                )

            except Exception as err:
                # Close what this port's attempt left open before the next port.
                connection_error = err
                self._connection_factory.reset()
                continue

            self.reader = reader
            self.writer = writer

            self._reader_and_writer[hostname] = (reader, writer)

            self.address_info = ordered[winner_index]
            self.port = port
            self.ssl = group_ssl

            return self.address_info, port, True

        try:
            raise connection_error

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None

    @staticmethod
    def _port_groups(socket_configs: Sequence[SocketConfig]):
        """The configs of each port, in the lookup's order (consecutive runs of one port)."""
        group_start = 0
        for index in range(1, len(socket_configs)):
            if socket_configs[index][4][1] != socket_configs[group_start][4][1]:
                yield socket_configs[group_start:index]
                group_start = index

        yield socket_configs[group_start:]

    @property
    def empty(self):
        return not self.reader._buffer

    def read(self, num_bytes: int = SMTP_LIMIT):
        return self.reader.read(n=num_bytes)

    def readexactly(self, n_bytes: int):
        return self.reader.readexactly(n=n_bytes)

    def readuntil(self, sep=b"\n"):
        return self.reader.readuntil(separator=sep)

    def readline(self):
        return self.reader.readline()

    def write(self, data):
        self.writer.write(data)

    def reset_buffer(self):
        self.reader._buffer = bytearray()

    def read_headers(self):
        return self.reader.read_headers()

    def close(self):
        # The factory closes only its newest transport: abort every one held.
        for _, writer in self._reader_and_writer.values():
            writer.abort()

        self._reader_and_writer.clear()
        self.reader = None
        self.writer = None
        self.session_key = None
        self.session_options = None

        self._connection_factory.close()

    def reset(self):
        # The factory closes only its newest transport: abort every one held.
        for _, writer in self._reader_and_writer.values():
            writer.abort()

        self._reader_and_writer.clear()
        self.reader = None
        self.writer = None
        self.session_key = None
        self.session_options = None
        self.command_encoding = 'ascii'

        self._connection_factory.reset()
