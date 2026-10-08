import asyncio
import socket
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
from ssl import SSLContext
from typing import Optional, Sequence

from hyperscale.core.engines.client.shared.protocols import Writer
from hyperscale.core.engines.client.shared.protocols.client_ssl_protocol import (
    open_ssl_protocol_transport,
)
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import (
    SocketConfig,
    connect_first_responding,
)
from hyperscale.core.engines.client.shared.protocols.tls_transport import (
    RECEIVE_BUFFER_SIZE,
    open_tls_transport,
    supports_readiness_callbacks,
)

from .http2_protocol import HTTP2Protocol


class TCPConnection:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport = None
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None
        # Every TLS transport this connection opens reads the socket into
        # this, one read callback at a time.
        self._receive_buffer = memoryview(bytearray(RECEIVE_BUFFER_SIZE))

    async def create_http2(
        self,
        hostname=None,
        socket_config=None,
        ssl: Optional[SSLContext] = None,
        ssl_timeout: int = SSL_HANDSHAKE_TIMEOUT,
    ):
        reader, writer, _ = await self.create_http2_racing(
            hostname,
            (socket_config,),
            ssl=ssl,
            ssl_timeout=ssl_timeout,
        )

        return reader, writer

    async def create_http2_racing(
        self,
        hostname=None,
        socket_configs: Sequence[SocketConfig] = (),
        ssl: Optional[SSLContext] = None,
        ssl_timeout: int = SSL_HANDSHAKE_TIMEOUT,
    ):
        # What loop.open_connection(ssl=...) does, over the socket that won
        # the connection race.
        self.loop = asyncio.get_event_loop()

        # RFC 8305: the first address to answer gets the connection.
        self.socket, winner_index = await connect_first_responding(
            self.loop,
            socket_configs,
        )

        # The protocol is the connection's reader too.
        protocol = HTTP2Protocol(loop=self.loop)

        # The handshake's outcome, or its error, is the connect's: a failed
        # handshake fails the connect rather than a later read.
        if supports_readiness_callbacks(self.loop, self.socket):
            self.transport = await open_tls_transport(
                self.loop,
                self.socket,
                ssl,
                hostname,
                protocol,
                self._receive_buffer,
                handshake_timeout=ssl_timeout,
            )

        else:
            self.transport = await open_ssl_protocol_transport(
                self.loop,
                self.socket,
                socket_configs[winner_index][0],
                ssl,
                hostname,
                protocol,
                ssl_timeout,
            )

        # RFC 9113 3.2: HTTP/2 over TLS only once ALPN has selected "h2".
        negotiated = self.transport.get_extra_info("ssl_object").selected_alpn_protocol()
        if negotiated != "h2":
            raise ConnectionError(
                f"{hostname} negotiated {negotiated!r} over ALPN, not HTTP/2 (h2)"
            )

        self._writer = Writer(self.transport, protocol, protocol, self.loop)

        return protocol, self._writer, winner_index

    def close(self):
        try:

            self.transport.abort()

        except Exception:
            pass

        try:
            self.transport.close()

        except Exception:
            pass

        try:
            if self.socket:
                self.socket.shutdown(socket.SHUT_RDWR)

        except Exception:
            pass

        try:
            if self.socket:
                self.socket.close()

        except Exception:
            pass

    def reset(self):
        self.close()
        self.transport = None
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None
