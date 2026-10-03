from __future__ import annotations

import asyncio
from ssl import SSLContext
from typing import Dict, Iterator, Optional, Sequence, Tuple

from hyperscale.core.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import SocketConfig

from .tcp import TCPConnectionFactory


class TCPConnection:
    __slots__ = (
        "dns_address",
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
    )

    def __init__(self, reset_connections: bool = False) -> None:
        self.dns_address: str = None
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
        self._connection_factory = TCPConnectionFactory()

    async def make_connection(
        self,
        hostname: str,
        dns_address: str,
        port: int,
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        ssl: Optional[SSLContext] = None,
        ssl_upgrade: bool = False,
    ) -> None:
        if self._reader_and_writer.get(hostname) is None or ssl_upgrade:
            reader, writer = await self._connection_factory.create(
                hostname, socket_config, ssl=ssl
            )

            self.reader = reader
            self.writer = writer

            self._reader_and_writer[hostname] = (reader, writer)

            self.dns_address = dns_address
            self.port = port
            self.ssl = ssl
        else:
            reader, writer = self._reader_and_writer.get(hostname)

            self.reader = reader
            self.writer = writer

    async def connect_to_any(
        self,
        hostname: str,
        addresses: Sequence[Tuple[str, SocketConfig]],
        port: int,
        address_rotation: Iterator[int],
        ssl: Optional[SSLContext] = None,
    ) -> Tuple[Optional[str], Optional[SocketConfig], bool]:
        """
        Reuse this connection's cached transport for ``hostname``. Otherwise
        open a new one, racing the host's ``addresses`` (RFC 8305) from the
        next offset in ``address_rotation`` so a pool's connections spread
        across all of them.

        Returns the address and socket config of a new transport (``None``
        for both on reuse), and whether the transport is new.
        """
        if (cached := self._reader_and_writer.get(hostname)) is not None:
            self.reader, self.writer = cached
            return None, None, False

        if not addresses:
            raise ConnectionError(f"No addresses to connect to for {hostname}")

        offset = next(address_rotation) % len(addresses)
        ordered = [*addresses[offset:], *addresses[:offset]]

        reader, writer, winner_index = await self._connection_factory.create_racing(
            hostname,
            [socket_config for _, socket_config in ordered],
            ssl=ssl,
        )

        address, socket_config = ordered[winner_index]

        self.reader = reader
        self.writer = writer

        self._reader_and_writer[hostname] = (reader, writer)

        self.dns_address = address
        self.port = port
        self.ssl = ssl

        return address, socket_config, True

    @property
    def empty(self):
        return not self.reader._buffer

    def read(self):
        return self.reader.read(n=_DEFAULT_LIMIT)

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
        self._reader_and_writer.clear()

        if self.reader:
            self.reader = None

        if self.writer:
            self.writer.clear()

        self._connection_factory.close()

    def reset(self, hostname: str | None = None):
        if hostname:
            self._reader_and_writer[hostname] = None
        else:
            self._reader_and_writer.clear()

        if self.reader:
            self.reader = None

        if self.writer:
            self.writer.clear()

        self._connection_factory.reset()
