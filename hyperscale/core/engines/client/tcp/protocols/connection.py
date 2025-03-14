from __future__ import annotations

import asyncio
from ssl import SSLContext
from typing import Dict, Optional, Tuple

from hyperscale.core.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)

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
        self._connection_factory.close()
