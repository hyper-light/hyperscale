"""
In-process transport coordinator. Maintains the registry of
``(host, port) → MercurySyncBaseServer`` and wires up paired
``FakeTCPTransport`` / ``FakeUDPTransport`` instances so a sender's
``transport.write`` / ``transport.sendto`` routes to the right
peer's protocol via the SimulationLoop.

Why centralize routing
----------------------

In REAL mode, kernel-level TCP / UDP routing is implicit — the OS
knows where ``(host, port)`` lives. In SIM, we need an explicit
table. Every ``MercurySyncBaseServer`` instance in the cluster
registers itself with ``InProcessTransport`` at startup and
de-registers at shutdown. Every send consults the table at the
moment of delivery so a kill / restart between registration and
delivery sees the up-to-date routing (matches REAL where a peer
that has died refuses connections / drops datagrams immediately).

Why fault injection lives here
------------------------------

The Phase 4 ``fault_transport.py`` intercepts ``send_tcp`` /
``send_udp`` *on the sender side* — it wraps the bound methods so
the FaultMatrix can drop, delay, or partition messages. Under SIM,
the same FaultMatrix rules apply but the interception point shifts
to *delivery* time: drops short-circuit at the registry lookup,
delays schedule ``call_later`` instead of ``call_soon``, partitions
silently discard. Routing all delivery decisions through one
chokepoint (``InProcessTransport``) means the fault semantics are
captured exactly once.

For Phase 6a (this commit), the FaultMatrix integration point is
sketched as a ``fault_check`` callback the harness installs in
Phase 6d. The class accepts it as an optional constructor
parameter so the wire-up is a one-line change later.

TCP connection model
--------------------

A new ``connect_tcp`` call creates a *paired* pair of transports —
one for each direction. The client transport routes to the server
protocol's ``data_received``; the server transport routes back to
the client protocol's ``data_received``. A fresh server protocol
instance is created on every ``connect_tcp`` call (matching REAL,
where ``loop.create_server`` calls ``protocol_factory()`` once per
accepted connection). The pair lives until either side calls
``close``.

UDP datagram model
------------------

UDP is symmetric: a single ``FakeUDPTransport`` per server. To
send, the sender's transport calls back into ``InProcessTransport.
route_udp`` with ``(data, target_addr, sender_addr)``; we look up
``target_addr``'s protocol and schedule its
``datagram_received(data, sender_addr)`` via ``loop.call_soon``.
"""

import asyncio
from typing import Callable

from .fake_tcp_transport import FakeTCPTransport
from .fake_udp_transport import FakeUDPTransport


# Optional fault-check signature. Returns a tuple
# ``(should_deliver: bool, delay_seconds: float)``. ``False``
# = drop; ``delay_seconds > 0`` = schedule delivery via
# ``call_later`` instead of ``call_soon``.
FaultCheck = Callable[
    [str, tuple[str, int], tuple[str, int]],
    tuple[bool, float],
]


class _Registration:
    """Bound server entry in the transport registry.

    Holds the protocol factories ``MercurySyncBaseServer`` would
    normally pass to ``loop.create_server`` /
    ``loop.create_datagram_endpoint`` plus the live UDP protocol
    instance (single per server, persistent across the server's
    lifetime).
    """

    __slots__ = (
        "tcp_protocol_factory",
        "udp_protocol",
        "udp_transport",
    )

    def __init__(
        self,
        tcp_protocol_factory: Callable[[], asyncio.Protocol],
        udp_protocol: asyncio.DatagramProtocol,
        udp_transport: FakeUDPTransport,
    ) -> None:
        self.tcp_protocol_factory = tcp_protocol_factory
        self.udp_protocol = udp_protocol
        self.udp_transport = udp_transport


class InProcessTransport:
    """In-process delivery coordinator.

    One instance per SIM cluster. Servers register at startup
    (``register_server``), de-register at shutdown
    (``deregister_server``). Senders use ``connect_tcp`` to open a
    TCP connection (returns a transport-protocol pair on the
    client side) and the UDP transport returned by
    ``register_server`` for outbound datagrams.

    Construct with the bound ``SimulationLoop``. Optional
    ``fault_check`` parameter is consulted at every delivery for
    drops / delays.
    """

    def __init__(
        self,
        loop,
        fault_check: FaultCheck | None = None,
    ) -> None:
        self._loop = loop
        self._fault_check = fault_check
        self._registry: dict[tuple[str, int], _Registration] = {}

    def register_server(
        self,
        sockname: tuple[str, int],
        tcp_protocol_factory: Callable[[], asyncio.Protocol],
        udp_protocol: asyncio.DatagramProtocol,
    ) -> FakeUDPTransport:
        """Register a server's TCP / UDP protocols at ``sockname``.

        Returns the ``FakeUDPTransport`` the server should use as
        its global UDP transport — wired so ``sendto`` routes
        through this coordinator's ``route_udp``. Callers should
        immediately invoke
        ``udp_protocol.connection_made(returned_transport)``.

        The TCP side registers the factory only; a new
        ``MercurySyncTCPProtocol`` instance is constructed on each
        ``connect_tcp`` (matches REAL: ``loop.create_server`` calls
        the factory once per accepted connection).
        """
        if sockname in self._registry:
            raise ValueError(
                f"server at {sockname} is already registered"
            )
        udp_transport = FakeUDPTransport(
            loop=self._loop,
            route=self._route_udp,
            sockname=sockname,
        )
        udp_transport.set_protocol(udp_protocol)
        self._registry[sockname] = _Registration(
            tcp_protocol_factory=tcp_protocol_factory,
            udp_protocol=udp_protocol,
            udp_transport=udp_transport,
        )
        return udp_transport

    def deregister_server(self, sockname: tuple[str, int]) -> None:
        """Remove the server at ``sockname`` from the registry.

        After deregistration, subsequent ``connect_tcp`` attempts
        targeting this address raise ``ConnectionRefusedError`` and
        UDP sendto targeting this address silently drops (matches
        REAL: kernel refuses on a closed TCP port; OS drops
        datagrams to closed UDP ports without error).
        """
        registration = self._registry.pop(sockname, None)
        if registration is not None:
            registration.udp_transport.close()

    def connect_tcp(
        self,
        client_sockname: tuple[str, int],
        peer_sockname: tuple[str, int],
        client_protocol_factory: Callable[[], asyncio.Protocol],
    ) -> tuple[FakeTCPTransport, asyncio.Protocol]:
        """Open a paired TCP connection.

        Returns ``(client_transport, client_protocol)``. The peer's
        protocol is constructed from its registered factory and
        wired so the two sides can exchange bytes via their
        respective ``FakeTCPTransport.write`` calls.

        Raises ``ConnectionRefusedError`` if ``peer_sockname`` is not
        registered — matches the REAL behavior of trying to connect
        to a closed port.
        """
        registration = self._registry.get(peer_sockname)
        if registration is None:
            raise ConnectionRefusedError(
                f"no SIM server registered at {peer_sockname}"
            )

        # Build the two protocol instances.
        client_protocol = client_protocol_factory()
        server_protocol = registration.tcp_protocol_factory()

        # Build the two transports. Each routes ``write`` to the
        # *other side's* ``data_received`` via ``call_soon`` (or
        # ``call_later`` when faults inject a delay).
        client_transport = FakeTCPTransport(
            loop=self._loop,
            deliver=lambda data: self._deliver_tcp(
                data,
                from_addr=client_sockname,
                to_addr=peer_sockname,
                target_protocol=server_protocol,
            ),
            peername=peer_sockname,
            sockname=client_sockname,
        )
        client_transport.set_protocol(client_protocol)

        server_transport = FakeTCPTransport(
            loop=self._loop,
            deliver=lambda data: self._deliver_tcp(
                data,
                from_addr=peer_sockname,
                to_addr=client_sockname,
                target_protocol=client_protocol,
            ),
            peername=client_sockname,
            sockname=peer_sockname,
        )
        server_transport.set_protocol(server_protocol)

        # Notify both sides; matches asyncio's contract that
        # ``connection_made`` is called after the transport-protocol
        # pair is fully wired.
        self._loop.call_soon(
            client_protocol.connection_made, client_transport
        )
        self._loop.call_soon(
            server_protocol.connection_made, server_transport
        )
        return client_transport, client_protocol

    def _deliver_tcp(
        self,
        data: bytes,
        *,
        from_addr: tuple[str, int],
        to_addr: tuple[str, int],
        target_protocol: asyncio.Protocol,
    ) -> None:
        """Deliver a TCP byte chunk to ``target_protocol``.

        Consults ``fault_check`` (when provided): drop short-circuits,
        delay reschedules via ``call_later``. Otherwise calls
        ``target_protocol.data_received(data)`` directly — note this
        is called from within a ``call_soon`` callback already (set
        up by ``FakeTCPTransport.write``), so the asyncio ordering
        contract is preserved.
        """
        if self._fault_check is not None:
            should_deliver, delay = self._fault_check(
                "tcp", from_addr, to_addr
            )
            if not should_deliver:
                return
            if delay > 0:
                self._loop.call_later(
                    delay, target_protocol.data_received, data
                )
                return
        target_protocol.data_received(data)

    def _route_udp(
        self,
        data: bytes,
        target_addr: tuple[str, int],
        sender_addr: tuple[str, int],
    ) -> None:
        """Route a UDP datagram from ``sender_addr`` to ``target_addr``.

        Looks up the target's UDP protocol; if missing, silently
        drops (matches kernel behavior for closed-port datagrams).
        Otherwise schedules ``datagram_received(data, sender_addr)``
        via ``call_soon`` (or ``call_later`` if faults inject a
        delay). Drops short-circuit at the lookup step.
        """
        if self._fault_check is not None:
            should_deliver, delay = self._fault_check(
                "udp", sender_addr, target_addr
            )
            if not should_deliver:
                return
        else:
            delay = 0.0

        registration = self._registry.get(target_addr)
        if registration is None:
            return  # silently drop, matches REAL UDP semantics

        if delay > 0:
            self._loop.call_later(
                delay,
                registration.udp_protocol.datagram_received,
                data,
                sender_addr,
            )
        else:
            self._loop.call_soon(
                registration.udp_protocol.datagram_received,
                data,
                sender_addr,
            )

    def registered_addresses(self) -> tuple[tuple[str, int], ...]:
        """Snapshot of currently-registered server addresses.

        Used by the SIM harness for diagnostics and by the smoke
        scenarios for assertions about cluster membership.
        """
        return tuple(self._registry)
