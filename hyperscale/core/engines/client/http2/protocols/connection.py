from __future__ import annotations

from ssl import SSLContext
from typing import Iterator, Optional, Sequence, Tuple

from hyperscale.core.engines.client.http2.frames import FrameBuffer
from hyperscale.core.engines.client.http2.streams import Stream
from hyperscale.core.engines.client.shared.protocols import _DEFAULT_LIMIT
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import SocketConfig

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
        "consecutive_read_timeouts",
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
        # Read timeouts in a row on this transport; a second means it is dead.
        self.consecutive_read_timeouts = 0
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

    async def connect_to_any(
        self,
        hostname: str,
        addresses: Sequence[Tuple[str, SocketConfig]],
        port: int,
        address_rotation: Iterator[int],
        ssl: Optional[SSLContext] = None,
        ssl_upgrade: bool = False,
    ) -> Tuple[str, SocketConfig, bool]:
        """
        Reuse this connection's transport when it already reaches one of the
        host's ``addresses`` on ``port``. Otherwise open a new one, racing the
        addresses (RFC 8305) from the next offset in ``address_rotation`` so
        a pool's connections spread across all of them.

        Returns the address and socket config connected to, and whether the
        transport is new.
        """
        if self.connected and ssl_upgrade is False and self.port == port:
            for address, socket_config in addresses:
                if address == self.dns_address:
                    self.stream.update_stream_id()
                    return address, socket_config, False

        if not addresses:
            raise ConnectionError(f"No addresses to connect to for {hostname}")

        if self.connected:
            # The transport reaches a different host: close it first.
            self.reset()

        offset = next(address_rotation) % len(addresses)
        ordered = [*addresses[offset:], *addresses[:offset]]

        reader, writer, winner_index = await self._connection_factory.create_http2_racing(
            hostname,
            [socket_config for _, socket_config in ordered],
            ssl=ssl,
        )

        address, socket_config = ordered[winner_index]

        self.stream.reader = reader
        self.stream.writer = writer

        self.connected = True
        self.dns_address = address
        self.port = port
        self.ssl = ssl

        return address, socket_config, True

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
        if self.stream.reader:
            self.stream.reader = None

        if self.stream.writer:
            self.stream.writer.clear()

        try:
            self._connection_factory.close()
        except Exception:
            pass

    def reset(self):
        self.connected = False
        self.consecutive_read_timeouts = 0

        # Bytes buffered from the old transport mean nothing on the next one.
        self.stream.frame_buffer = FrameBuffer()

        if self.stream.reader:
            self.stream.reader = None

        if self.stream.writer:
            self.stream.writer.clear()

        self._connection_factory.reset()
