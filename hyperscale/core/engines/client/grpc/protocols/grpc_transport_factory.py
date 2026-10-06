import asyncio
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
from ssl import SSLContext
from typing import Optional, Sequence

from hyperscale.core.engines.client.http2.protocols.tcp import TCPConnection
from hyperscale.core.engines.client.http2.protocols.tcp.http2_protocol import HTTP2Protocol
from hyperscale.core.engines.client.shared.protocols import Writer
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import (
    SocketConfig,
    connect_first_responding,
)


class GRPCTransportFactory(TCPConnection):
    """
    The HTTP/2 transports gRPC runs on: TLS with ALPN "h2" for https://
    targets, exactly as for HTTP/2, and cleartext HTTP/2 with prior knowledge
    (RFC 9113, 3.3) for http:// targets -- insecure gRPC channels, the usual
    form of local and in-cluster gRPC servers.
    """

    async def create_http2_racing(
        self,
        hostname=None,
        socket_configs: Sequence[SocketConfig] = (),
        ssl: Optional[SSLContext] = None,
        ssl_timeout: int = SSL_HANDSHAKE_TIMEOUT,
    ):
        if ssl is not None:
            return await super().create_http2_racing(
                hostname,
                socket_configs,
                ssl=ssl,
                ssl_timeout=ssl_timeout,
            )

        self.loop = asyncio.get_running_loop()

        # RFC 8305: the first address to answer gets the connection.
        self.socket, winner_index = await connect_first_responding(
            self.loop,
            socket_configs,
        )

        # The protocol is the connection's reader too.
        protocol = HTTP2Protocol(loop=self.loop)

        # No TLS, so no ALPN: the connection preface the pipe sends first is
        # what opens HTTP/2 with a server known to speak it.
        self.transport, _ = await self.loop.create_connection(
            lambda: protocol,
            sock=self.socket,
        )

        self._writer = Writer(self.transport, protocol, protocol, self.loop)

        return protocol, self._writer, winner_index
