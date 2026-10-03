import asyncio
import socket
from typing import Sequence

from hyperscale.core.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import (
    SocketConfig,
    connect_first_responding,
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
            hostname = None

        self.transport, _ = await self.loop.create_connection(
            lambda: reader_protocol,
            sock=self.socket,
            family=family,
            server_hostname=hostname,
            ssl=ssl,
        )

        self._writer = Writer(self.transport, reader_protocol, reader, self.loop)

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
