"""
SIM-mode implementation of the ``TransportFactory`` seam.

Adapts the production ``MercurySyncBaseServer`` socket-creation sites
onto the ``InProcessTransport`` registry. One instance is shared across
every server in a SIM cluster — each server passes its own sockname to
the register / connect calls, so a single factory over a single shared
``InProcessTransport`` routes the whole cluster.

The three methods map one-to-one onto ``InProcessTransport``:

- ``register_datagram_endpoint`` → ``register_udp`` (fires
  ``connection_made``, returns the ``FakeUDPTransport``).
- ``register_stream_server`` → ``register_tcp`` (stores the factory).
- ``close_stream_server`` → ``close_tcp`` (drops the factory: further
  dials are refused).
- ``connect_stream`` → ``connect_tcp`` (builds the paired
  transport/protocol, schedules ``connection_made`` on both sides).

``connect_stream`` is ``async`` so the server's connect path can
``await`` it exactly as it awaits ``loop.create_connection``, even
though the underlying ``connect_tcp`` resolves synchronously — the
awaitable shape is the contract, not the timing.
"""

import asyncio
from typing import Callable

from .in_process_transport import InProcessTransport


class SimTransportFactory:
    """``TransportFactory`` backed by an ``InProcessTransport``.

    Construct with the shared ``InProcessTransport`` for the cluster;
    pass the same instance to every SIM server's ``transport_factory``
    kwarg.
    """

    def __init__(self, in_process_transport: InProcessTransport) -> None:
        self._transport = in_process_transport

    def register_datagram_endpoint(
        self,
        sockname: tuple[str, int],
        protocol: asyncio.DatagramProtocol,
    ) -> asyncio.DatagramTransport:
        return self._transport.register_udp(sockname, protocol)

    def register_stream_server(
        self,
        sockname: tuple[str, int],
        protocol_factory: Callable[[], asyncio.Protocol],
    ) -> None:
        self._transport.register_tcp(sockname, protocol_factory)

    def close_stream_server(self, sockname: tuple[str, int]) -> None:
        self._transport.close_tcp(sockname)

    async def connect_stream(
        self,
        self_sockname: tuple[str, int],
        peer_sockname: tuple[str, int],
        protocol_factory: Callable[[], asyncio.Protocol],
    ) -> tuple[asyncio.Transport, asyncio.Protocol]:
        return self._transport.connect_tcp(
            client_sockname=self_sockname,
            peer_sockname=peer_sockname,
            client_protocol_factory=protocol_factory,
        )
