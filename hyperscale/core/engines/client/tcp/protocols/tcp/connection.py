import asyncio
import socket
from asyncio.sslproto import SSLProtocol

from hyperscale.core.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)

from .protocol import TCPProtocol


class TCPConnectionFactory:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport = None
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None

    async def create(
        self, hostname=None, socket_config=None, *, limit=_DEFAULT_LIMIT, ssl=None
    ):
        self.loop = asyncio.get_event_loop()

        family, type_, proto, _, address = socket_config

        self.socket = socket.socket(family=family, type=type_, proto=proto)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        await self.loop.run_in_executor(None, self.socket.connect, address)

        self.socket.setblocking(False)

        reader = Reader(limit=limit, loop=self.loop)
        reader_protocol = TCPProtocol(reader, loop=self.loop)

        if ssl is None:
            hostname = None

        self.transport, _ = await self.loop.create_connection(
            lambda: reader_protocol,
            sock=self.socket,
            family=family,
            server_hostname=hostname,
            ssl=ssl,
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
