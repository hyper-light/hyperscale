import asyncio
import socket
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
from asyncio.sslproto import SSLProtocol
from ssl import SSLContext
from typing import Optional, Sequence

from hyperscale.core.engines.client.shared.protocols import (
    HTTP2_LIMIT,
    Reader,
    Writer,
)
from hyperscale.core.engines.client.shared.protocols.client_ssl_protocol import (
    ClientSSLProtocol,
)
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import (
    SocketConfig,
    connect_first_responding,
)

from .tls_protocol import TLSProtocol


class TCPConnection:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport = None
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None

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
        # this does the same as loop.open_connection(), but TLS upgrade is done
        # manually after connection be established.

        self.loop = asyncio.get_event_loop()

        # RFC 8305: the first address to answer gets the connection.
        self.socket, winner_index = await connect_first_responding(
            self.loop,
            socket_configs,
        )

        family = socket_configs[winner_index][0]

        reader = Reader(limit=HTTP2_LIMIT, loop=self.loop)

        protocol = TLSProtocol(reader, loop=self.loop)

        self.transport, _ = await self.loop.create_connection(
            lambda: protocol, sock=self.socket, family=family
        )

        handshake_complete = self.loop.create_future()

        ssl_protocol = ClientSSLProtocol(
            self.loop,
            protocol,
            ssl,
            handshake_complete,
            False,
            hostname,
            ssl_handshake_timeout=ssl_timeout,
            call_connection_made=False,
        )

        # Pause early so that "ssl_protocol.data_received()" doesn't
        # have a chance to get called before "ssl_protocol.connection_made()".
        self.transport.pause_reading()

        self.transport.set_protocol(ssl_protocol)

        # Starts the handshake without blocking. It must run on the loop's
        # thread: it schedules timers and writes to the transport.
        ssl_protocol.connection_made(self.transport)
        self.transport.resume_reading()

        # The handshake's outcome, or its error, arrives on this future, so a
        # failed handshake fails the connect rather than a later read.
        await handshake_complete

        self.transport = ssl_protocol._app_transport

        # RFC 9113 3.2: HTTP/2 over TLS only once ALPN has selected "h2".
        negotiated = self.transport.get_extra_info("ssl_object").selected_alpn_protocol()
        if negotiated != "h2":
            raise ConnectionError(
                f"{hostname} negotiated {negotiated!r} over ALPN, not HTTP/2 (h2)"
            )

        reader = Reader(limit=HTTP2_LIMIT, loop=self.loop)

        protocol.upgrade_reader(reader)  # update reader
        protocol.connection_made(self.transport)  # update transport

        self._writer = Writer(
            self.transport, ssl_protocol, reader, self.loop
        )  # update writer

        return reader, self._writer, winner_index

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
