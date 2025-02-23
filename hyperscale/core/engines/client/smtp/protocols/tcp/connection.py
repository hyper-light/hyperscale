import asyncio
import ssl
import socket
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
from asyncio.sslproto import SSLProtocol
from typing import Literal
from hyperscale.core.engines.client.shared.protocols import (
    Reader,
    Writer,
)
from typing import cast
from .limits import SMTP_LIMIT
from .protocol import TCPProtocol
from .tls_protocol import TLSProtocol


class TCPConnection:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport = None
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None

    async def create(
        self, 
        hostname: str=None, 
        socket_config=None,
        ssl: ssl.SSLContext=None,
        connection_type: Literal['insecure', 'ssl', 'tls'] = 'tls',
        ssl_timeout: int = SSL_HANDSHAKE_TIMEOUT,
        timeout: int | None = None

    ):
        self.loop = asyncio.get_event_loop()

        family, type_, proto, _, address = socket_config


        socket_family = socket.AF_INET
        if len(address) == 4:
            socket_family = socket.AF_INET6
            

        self.socket = socket.socket(family=socket_family, type=type_, proto=proto)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        await asyncio.wait_for(
            self.loop.run_in_executor(None, self.socket.connect, address),
            timeout=timeout,
        )

        self.socket.setblocking(False)
        reader = Reader(limit=SMTP_LIMIT, loop=self.loop)

        if connection_type == 'tls':
            reader_protocol = TCPProtocol(reader, loop=self.loop)

            if ssl is None:
                hostname = None

            if self.transport:
 
                self.transport = await self.loop.start_tls(
                    cast(asyncio.WriteTransport, self.transport),
                    reader_protocol,
                    ssl,
                    server_side=False,
                    server_hostname=hostname,
                    ssl_handshake_timeout=timeout,
                )

            else:
                self.transport, _ = await self.loop.create_connection(
                    lambda: reader_protocol,
                    sock=self.socket,
                    family=family,
                    server_hostname=hostname,
                    ssl=ssl,
                )

                self.transport = await self.loop.start_tls(
                    cast(asyncio.WriteTransport, self.transport),
                    reader_protocol,
                    ssl,
                    server_side=False,
                    server_hostname=hostname,
                    ssl_handshake_timeout=timeout,
                )


            self._writer = self.transport

        else:
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

        return (
            reader,
            self._writer,
            address[1],
        )

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
