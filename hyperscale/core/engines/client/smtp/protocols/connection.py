from __future__ import annotations

import asyncio
from ssl import SSLContext
from typing import Dict, Optional, Tuple, Literal

from hyperscale.core.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)
from .tcp import SMTP_LIMIT

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
        self._connection_factory.close()
