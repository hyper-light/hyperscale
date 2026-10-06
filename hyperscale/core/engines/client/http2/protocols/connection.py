from __future__ import annotations

from ssl import SSLContext
from typing import Iterator, Optional, Sequence, Tuple

from hyperscale.core.engines.client.http2.frames import FrameBuffer
from hyperscale.core.engines.client.http2.streams import Stream
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
        "target",
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
        # The origin (scheme and authority) the open transport serves.
        self.target: Tuple[str, str] | None = None
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
            # The next stream: client streams are odd (RFC 9113 5.1.1), and
            # every id starts odd, so a step of two keeps it odd.
            self.stream.stream_id += 2

    def reuse_transport(
        self,
        target: Tuple[str, str],
        addresses: Sequence[Tuple[str, SocketConfig]],
    ) -> Optional[Tuple[str, SocketConfig]]:
        """
        When this connection's transport serves ``target`` (the request's
        scheme and authority) at one of ``addresses``, move to the next
        stream and return that address and its socket config; otherwise
        None. Another host on the same address and port gets its own
        transport: this one's TLS session names this host. Synchronous, so
        the common case -- a request on an open connection -- awaits nothing.
        """
        if self.connected and self.target == target:
            # A transport the server closed while it sat in the pool -- its
            # reader at the end of the stream, or holding the error -- is not
            # reused: the request opens a new one.
            reader = self.stream.reader
            if reader is None or reader._eof or reader._exception is not None:
                return None

            for address, socket_config in addresses:
                if address == self.dns_address:
                    # The next stream: client streams are odd (RFC 9113
                    # 5.1.1), and every id starts odd, so a step of two keeps
                    # it odd.
                    self.stream.stream_id += 2
                    return address, socket_config

        return None

    async def connect_to_any(
        self,
        target: Tuple[str, str],
        hostname: str,
        addresses: Sequence[Tuple[str, SocketConfig]],
        port: int,
        address_rotation: Iterator[int],
        ssl: Optional[SSLContext] = None,
    ) -> Tuple[str, SocketConfig, bool]:
        """
        Reuse this connection's transport when it already serves ``target``
        at one of the host's ``addresses``. Otherwise open a new one, racing the
        addresses (RFC 8305) from the next offset in ``address_rotation`` so
        a pool's connections spread across all of them.

        Returns the address and socket config connected to, and whether the
        transport is new.
        """
        if (reused := self.reuse_transport(target, addresses)) is not None:
            return *reused, False

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
        self.target = target
        self.dns_address = address
        self.port = port
        self.ssl = ssl

        return address, socket_config, True

    def write(self, data):
        self.stream.writer.write(data)

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
        self.target = None
        self.consecutive_read_timeouts = 0

        # Bytes buffered from the old transport mean nothing on the next one.
        self.stream.frame_buffer = FrameBuffer()

        if self.stream.reader:
            self.stream.reader = None

        if self.stream.writer:
            self.stream.writer.clear()

        self._connection_factory.reset()
