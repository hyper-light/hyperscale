"""
TransportFactory interface — the dependency-injection seam for the
three points where ``MercurySyncBaseServer`` creates OS sockets.

Why this exists
--------------

Phase 4 proved that ``send_tcp`` / ``send_udp`` are the right seam for
message-level fault injection, but SIM mode (Phase 6) needs to replace
the *socket* layer entirely: no real ``socket.socket``, no
``loop.create_server`` / ``create_datagram_endpoint`` /
``create_connection``, no ``run_in_executor`` connect. The server's
three socket-creation sites —

1. ``_start_udp_server``  (bind the datagram endpoint),
2. ``_start_tcp_server``  (bind the stream server),
3. ``_connect_tcp_client`` (dial a peer),

— each consult ``self._transport_factory``. In REAL mode the factory
is ``None`` and every site runs its existing OS-socket code unchanged
(zero behavior change, zero regression risk). In SIM mode the factory
is a ``SimTransportFactory`` (living under ``tests/simulation/``) that
routes through the in-process transport registry against fake asyncio
transports, so the production receive path — framing, decryption,
dispatch, queues — runs byte-for-byte as it does in REAL.

The Protocol is deliberately SIM-shaped rather than a general socket
abstraction: it exposes exactly the three operations the server needs,
in the terms the server uses (sockname tuples, protocol instances /
factories), so the SIM branch at each call site is a couple of lines.
It is NOT an ``asyncio``-signature mirror — REAL mode never goes
through it.
"""

from typing import Awaitable, Callable, Protocol

import asyncio


class TransportFactory(Protocol):
    """Create the datagram endpoint, stream server, and stream
    connections for a server running in SIM mode.

    All addresses are ``(host, port)`` sockname tuples in the SIM
    address namespace (the server's configured host/ports). Only SIM
    implementations exist; REAL mode passes ``None`` and never calls
    these.
    """

    def register_datagram_endpoint(
        self,
        sockname: tuple[str, int],
        protocol: asyncio.DatagramProtocol,
    ) -> asyncio.DatagramTransport:
        """Register ``protocol`` as the server's UDP endpoint at
        ``sockname`` and return the transport it should send on.

        Mirrors ``loop.create_datagram_endpoint``'s contract: the
        implementation fires ``protocol.connection_made(transport)``
        before returning, so the caller does NOT call it itself.
        """
        ...

    def register_stream_server(
        self,
        sockname: tuple[str, int],
        protocol_factory: Callable[[], asyncio.Protocol],
    ) -> None:
        """Register ``protocol_factory`` as the server's TCP listener
        at ``sockname``.

        A fresh protocol instance is built from the factory per
        accepted connection (matches ``loop.create_server``). No
        transport is returned — the server side gets its transport via
        ``connection_made`` when a peer dials in.
        """
        ...

    def close_stream_server(self, sockname: tuple[str, int]) -> None:
        """Close the server's TCP listener at ``sockname``: further dials
        are refused, as at a closed port. The connections it accepted are
        the server's to close (it tracks them, as in REAL)."""
        ...

    def connect_stream(
        self,
        self_sockname: tuple[str, int],
        peer_sockname: tuple[str, int],
        protocol_factory: Callable[[], asyncio.Protocol],
    ) -> Awaitable[tuple[asyncio.Transport, asyncio.Protocol]]:
        """Dial ``peer_sockname`` from ``self_sockname``, building the
        client protocol from ``protocol_factory``.

        Returns an awaitable yielding ``(transport, protocol)`` —
        matching the shape of ``loop.create_connection`` — so the
        server's connect path can ``await`` it uniformly. Raises
        ``ConnectionRefusedError`` when no server listens at
        ``peer_sockname`` (closed-port semantics).
        """
        ...
