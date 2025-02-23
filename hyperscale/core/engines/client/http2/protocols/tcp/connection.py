import asyncio
import socket
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
from asyncio.sslproto import SSLProtocol
from ssl import SSLContext
from typing import Optional

from hyperscale.core.engines.client.shared.protocols import (
    HTTP2_LIMIT,
    Reader,
    Writer,
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
        # this does the same as loop.open_connection(), but TLS upgrade is done
        # manually after connection be established.

        self.loop = asyncio.get_event_loop()

        family, _, _, _, address = socket_config

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        await self.loop.run_in_executor(None, self.socket.connect, address)

        self.socket.setblocking(False)

        reader = Reader(limit=HTTP2_LIMIT, loop=self.loop)

        protocol = TLSProtocol(reader, loop=self.loop)

        self.transport, _ = await self.loop.create_connection(
            lambda: protocol, sock=self.socket, family=family
        )

        ssl_protocol = SSLProtocol(
            self.loop,
            protocol,
            ssl,
            None,
            False,
            hostname,
            ssl_handshake_timeout=ssl_timeout,
            call_connection_made=False,
        )

        # Pause early so that "ssl_protocol.data_received()" doesn't
        # have a chance to get called before "ssl_protocol.connection_made()".
        self.transport.pause_reading()

        self.transport.set_protocol(ssl_protocol)

        await self.loop.run_in_executor(
            None, ssl_protocol.connection_made, self.transport
        )
        self.transport.resume_reading()

        self.transport = ssl_protocol._app_transport

        reader = Reader(limit=HTTP2_LIMIT, loop=self.loop)

        protocol.upgrade_reader(reader)  # update reader
        protocol.connection_made(self.transport)  # update transport

        self._writer = Writer(
            self.transport, ssl_protocol, reader, self.loop
        )  # update writer

        return reader, self._writer

    def close(self):
        try:
            if hasattr(self.transport, "_ssl_protocol") and isinstance(
                self.transport._ssl_protocol, SSLProtocol
            ):
                self.transport._ssl_protocol.pause_writing()

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
