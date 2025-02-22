from __future__ import annotations

from ssl import SSLContext
from typing import Optional, Tuple

from hyperscale.core.engines.client.http2.streams import Stream
from hyperscale.core.engines.client.shared.protocols import _DEFAULT_LIMIT

from .tcp import TCPConnection


class HTTP2Connection:
    __slots__ = (
        "dns_address",
        "port",
        "ssl",
        "stream_id",
        "stream",
        "connected",
        "reset_connections",
        "_connection_factory",
    )

    def __init__(
        self,
        stream_id: int = 1,
        reset_connections: bool = False,
    ) -> None:
        if stream_id % 2 == 0:
            stream_id += 1

        self.dns_address: str = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.stream_id = stream_id

        self.stream = Stream(
            stream_id=stream_id,
            reset_connections=reset_connections,
        )

        self.connected = False
        self.reset_connections = reset_connections
        self._connection_factory = TCPConnection()

    async def make_connection(
        self,
        hostname: str,
        dns_address: str,
        port: int,
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        ssl: Optional[SSLContext] = None,
        timeout: Optional[float] = None,
        ssl_upgrade: bool = False,
    ):
        if self.connected is False or self.dns_address != dns_address or ssl_upgrade:
            reader, writer = await self._connection_factory.create_http2(
                hostname,
                socket_config,
                ssl=ssl,
            )

            self.stream.reader = reader
            self.stream.writer = writer

            self.connected = True
            self.dns_address = dns_address
            self.port = port
            self.ssl = ssl
        else:
            self.stream.update_stream_id()

    @property
    def empty(self):
        return not self.stream.reader._buffer

    def read(self, limit: int = _DEFAULT_LIMIT):
        return self.stream.reader.read(n=_DEFAULT_LIMIT)

    def readexactly(self, n_bytes: int):
        return self.stream.reader.readexactly(n=n_bytes)

    def readuntil(self, sep=b"\n"):
        return self.stream.reader.readuntil(separator=sep)

    def readline(self):
        return self.stream.reader.readline()

    def write(self, data):
        self.stream.writer.write(data)

    def reset_buffer(self):
        self.stream.reader._buffer = bytearray()

    def read_headers(self):
        return self.stream.reader.read_headers()

    def close(self):
        try:
            self._connection_factory.close()
        except Exception:
            pass
