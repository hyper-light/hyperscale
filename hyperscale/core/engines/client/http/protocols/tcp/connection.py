import asyncio
import socket
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
from asyncio.sslproto import SSLProtocol
from typing import Sequence

from hyperscale.core.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)
from hyperscale.core.engines.client.shared.protocols.client_ssl_protocol import (
    open_ssl_protocol_transport,
)
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import (
    SocketConfig,
    connect_first_responding,
)
from hyperscale.core.engines.client.shared.protocols.tls_transport import (
    RECEIVE_BUFFER_SIZE,
    TLSTransport,
    open_tls_transport,
    supports_readiness_callbacks,
)

from .protocol import TCPProtocol


class TCPConnection:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport = None
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None
        # Every TLS transport this connection opens reads the socket into
        # this, one read callback at a time; allocated on the first one.
        self._receive_buffer: memoryview | None = None

    async def create(
        self, hostname=None, socket_config=None, *, limit=_DEFAULT_LIMIT, ssl=None
    ):
        reader, writer, _ = await self.create_racing(
            hostname,
            (socket_config,),
            limit=limit,
            ssl=ssl,
        )

        return reader, writer

    async def create_racing(
        self,
        hostname=None,
        socket_configs: Sequence[SocketConfig] = (),
        *,
        limit=_DEFAULT_LIMIT,
        ssl=None,
    ):
        self.loop = asyncio.get_event_loop()

        # RFC 8305: the first address to answer gets the connection.
        self.socket, winner_index = await connect_first_responding(
            self.loop,
            socket_configs,
        )

        family = socket_configs[winner_index][0]

        reader = Reader(limit=limit, loop=self.loop)
        reader_protocol = TCPProtocol(reader, loop=self.loop)

        if ssl is None:
            self.transport, _ = await self.loop.create_connection(
                lambda: reader_protocol,
                sock=self.socket,
                family=family,
            )

        else:
            self.transport = await self._connect_tls(reader_protocol, family, hostname, ssl)

        self._writer = Writer(self.transport, reader_protocol, reader, self.loop)

        return reader, self._writer, winner_index

    async def _connect_tls(self, app_protocol, family, hostname, ssl):
        """
        What ``loop.create_connection(ssl=...)`` does: TLS over the connected
        socket, returning the TLS transport once the handshake completes --
        a TLSTransport where the loop can watch the socket for readiness,
        asyncio's TLS path with ClientSSLProtocol where it cannot.
        """
        if hostname is None:
            raise ValueError("You must set server_hostname when using ssl without a host")

        if supports_readiness_callbacks(self.loop, self.socket):
            if self._receive_buffer is None:
                self._receive_buffer = memoryview(bytearray(RECEIVE_BUFFER_SIZE))

            return await open_tls_transport(
                self.loop,
                self.socket,
                ssl,
                hostname,
                app_protocol,
                self._receive_buffer,
                handshake_timeout=SSL_HANDSHAKE_TIMEOUT,
            )

        return await open_ssl_protocol_transport(
            self.loop,
            self.socket,
            family,
            ssl,
            hostname,
            app_protocol,
            SSL_HANDSHAKE_TIMEOUT,
        )

    def close(self):
        try:
            if isinstance(self.transport, TLSTransport) or (
                hasattr(self.transport, "_ssl_protocol")
                and isinstance(self.transport._ssl_protocol, SSLProtocol)
            ):
                # close() starts a TLS shutdown that cannot finish once the
                # socket below is closed, leaving the raw transport registered
                # on a freed fd number the next socket reuses. abort() tears
                # down both layers now.
                self.transport.abort()

        except Exception:
            pass

        try:
            if self.transport and not self.transport.is_closing():
                self.transport.pause_reading()
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
