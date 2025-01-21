import asyncio
import socket
import ssl
from asyncio.sslproto import SSLProtocol
from typing import Callable

from hyperscale.core.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)

from .protocol import UDPProtocol

QuicStreamHandler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], None]


class UDPConnection:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport: asyncio.DatagramTransport = None
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None

    async def create_udp(
        self, socket_config=None, *, limit=_DEFAULT_LIMIT, tls: ssl.SSLContext = None
    ):
        self.loop = asyncio.get_event_loop()

        family, type_, _, _, address = socket_config

        self.socket = socket.socket(family=family, type=type_)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        await self.loop.run_in_executor(None, self.socket.connect, address)

        self.socket.setblocking(False)

        if tls:
            self.socket = tls.wrap_socket(self.socket)

        reader = Reader(limit=limit, loop=self.loop)
        reader_protocol = UDPProtocol(reader, loop=self.loop)

        self.transport, _ = await self.loop.create_datagram_endpoint(
            lambda: reader_protocol, sock=self.socket
        )

        self._writer = Writer(self.transport, reader_protocol, reader, self.loop)

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
