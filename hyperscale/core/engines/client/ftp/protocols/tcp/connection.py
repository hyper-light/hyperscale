import asyncio
import ssl
import socket
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
from typing import Literal, Sequence
from hyperscale.core.engines.client.shared.protocols import (
    Reader,
    Writer,
)
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import (
    SocketConfig,
    connect_first_responding,
)
from typing import cast
from .limits import MAXLINE
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
        reader, writer, port, _ = await self.create_racing(
            hostname,
            (socket_config,),
            ssl=ssl,
            connection_type=connection_type,
            timeout=timeout,
        )

        return (
            reader,
            writer,
            port,
        )

    async def create_racing(
        self,
        hostname: str = None,
        socket_configs: Sequence[SocketConfig] = (),
        ssl: ssl.SSLContext = None,
        connection_type: Literal['insecure', 'ssl', 'tls'] = 'tls',
        timeout: int | None = None,
    ):
        self.loop = asyncio.get_event_loop()

        # RFC 8305: the first address to answer gets the connection.
        self.socket, winner_index = await asyncio.wait_for(
            connect_first_responding(self.loop, socket_configs),
            timeout=timeout,
        )

        family, _, _, _, address = socket_configs[winner_index]
        reader = Reader(limit=MAXLINE, loop=self.loop)

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
            winner_index,
        )

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
