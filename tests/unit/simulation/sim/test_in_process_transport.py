"""
Unit tests for ``InProcessTransport`` + ``FakeTCPTransport`` +
``FakeUDPTransport``.

These primitives implement the SIM byte-transit layer. The
production-side protocols (``MercurySyncTCPProtocol`` /
``MercurySyncUDPProtocol``) are unchanged; the only difference
between REAL and SIM is the transport that carries bytes between
``write`` / ``sendto`` callers and ``data_received`` /
``datagram_received`` callbacks.

Tests verify:

1. ``register_server`` returns a usable ``FakeUDPTransport``.
2. ``connect_tcp`` creates a paired transport-protocol set; bytes
   sent on one side arrive at the other side's
   ``data_received``.
3. ``sendto`` UDP routes correctly and invokes the target's
   ``datagram_received`` with the sender's address.
4. ``ConnectionRefusedError`` when targeting an unregistered TCP
   peer (matches kernel semantics).
5. UDP send to unregistered peer silently drops (matches kernel
   semantics).
6. Fault-check hook drops / delays as configured.
7. Closing transports cleanly de-registers.
"""

import asyncio

import pytest

from tests.simulation.harness.sim import (
    FakeTCPTransport,
    FakeUDPTransport,
    InProcessTransport,
    SimulationLoop,
)


@pytest.fixture
def loop():
    loop_instance = SimulationLoop()
    asyncio.set_event_loop(loop_instance)
    yield loop_instance
    if not loop_instance.is_closed():
        loop_instance.close()
    asyncio.set_event_loop(None)


class _ProtocolStub(asyncio.Protocol):
    """Minimal asyncio.Protocol that records every event for
    assertion."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.received: list[bytes] = []
        self.connected_at: asyncio.Transport | None = None
        self.lost_at: int = 0

    def connection_made(self, transport: asyncio.Transport) -> None:
        self.connected_at = transport

    def data_received(self, data: bytes) -> None:
        self.received.append(data)

    def connection_lost(self, exc) -> None:
        self.lost_at += 1


class _DatagramProtocolStub(asyncio.DatagramProtocol):
    """Minimal asyncio.DatagramProtocol that records every datagram."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.received: list[tuple[bytes, tuple[str, int]]] = []
        self.connected_at: asyncio.DatagramTransport | None = None

    def connection_made(self, transport) -> None:
        self.connected_at = transport

    def datagram_received(
        self,
        data: bytes,
        addr: tuple[str, int],
    ) -> None:
        self.received.append((data, addr))

    def error_received(self, exc) -> None:
        pass


def test_register_server_returns_fake_udp_transport(loop: SimulationLoop) -> None:
    """``register_server`` returns a ``FakeUDPTransport`` ready for
    the caller's ``connection_made``."""
    transport = InProcessTransport(loop)
    udp_protocol = _DatagramProtocolStub("server")
    udp_transport = transport.register_server(
        sockname=("127.0.0.1", 9000),
        tcp_protocol_factory=lambda: _ProtocolStub("server-tcp"),
        udp_protocol=udp_protocol,
    )
    assert isinstance(udp_transport, FakeUDPTransport)


def test_connect_tcp_pairs_protocols(loop: SimulationLoop) -> None:
    """A ``connect_tcp`` call results in both sides receiving
    ``connection_made`` and then the bytes one side writes
    arriving on the other side's ``data_received``."""
    transport = InProcessTransport(loop)

    server_protocol_stub = _ProtocolStub("server")
    transport.register_server(
        sockname=("127.0.0.1", 9001),
        tcp_protocol_factory=lambda: server_protocol_stub,
        udp_protocol=_DatagramProtocolStub("server"),
    )

    async def scenario() -> _ProtocolStub:
        client_tx, client_protocol = transport.connect_tcp(
            client_sockname=("127.0.0.1", 5000),
            peer_sockname=("127.0.0.1", 9001),
            client_protocol_factory=lambda: _ProtocolStub("client"),
        )
        client_tx.write(b"hello")
        await asyncio.sleep(0)
        await asyncio.sleep(0)  # ensure call_soon's drain
        return client_protocol

    client_protocol = loop.run_until_complete(scenario())
    assert isinstance(client_protocol, _ProtocolStub)
    assert client_protocol.connected_at is not None
    assert server_protocol_stub.connected_at is not None
    assert server_protocol_stub.received == [b"hello"]


def test_connect_tcp_unregistered_peer_refuses(loop: SimulationLoop) -> None:
    """Connecting to a sockname with no registered server raises
    ``ConnectionRefusedError`` (matches REAL kernel behavior)."""
    transport = InProcessTransport(loop)
    with pytest.raises(ConnectionRefusedError):
        transport.connect_tcp(
            client_sockname=("127.0.0.1", 5000),
            peer_sockname=("127.0.0.1", 9999),
            client_protocol_factory=lambda: _ProtocolStub("client"),
        )


def test_udp_routing_delivers_to_target(loop: SimulationLoop) -> None:
    """A ``FakeUDPTransport.sendto`` routes the bytes to the target's
    UDP protocol with the sender's sockname."""
    transport = InProcessTransport(loop)

    target_protocol = _DatagramProtocolStub("target")
    transport.register_server(
        sockname=("127.0.0.1", 9100),
        tcp_protocol_factory=lambda: _ProtocolStub("target-tcp"),
        udp_protocol=target_protocol,
    )
    sender_udp_transport = transport.register_server(
        sockname=("127.0.0.1", 9101),
        tcp_protocol_factory=lambda: _ProtocolStub("sender-tcp"),
        udp_protocol=_DatagramProtocolStub("sender"),
    )

    async def scenario() -> None:
        sender_udp_transport.sendto(b"datagram-1", ("127.0.0.1", 9100))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    loop.run_until_complete(scenario())
    assert target_protocol.received == [(b"datagram-1", ("127.0.0.1", 9101))]


def test_udp_to_unregistered_target_silently_drops(loop: SimulationLoop) -> None:
    """Sending UDP to a target that isn't registered silently
    drops — matches REAL kernel behavior on a closed UDP port.

    Crucially does NOT raise; UDP is fire-and-forget.
    """
    transport = InProcessTransport(loop)
    sender_udp_transport = transport.register_server(
        sockname=("127.0.0.1", 9200),
        tcp_protocol_factory=lambda: _ProtocolStub("sender-tcp"),
        udp_protocol=_DatagramProtocolStub("sender"),
    )

    async def scenario() -> None:
        sender_udp_transport.sendto(b"datagram", ("127.0.0.1", 9999))
        await asyncio.sleep(0)

    loop.run_until_complete(scenario())  # no exception


def test_deregister_drops_subsequent_traffic(loop: SimulationLoop) -> None:
    """After ``deregister_server``, subsequent TCP connects refuse
    and UDP sends silently drop."""
    transport = InProcessTransport(loop)
    transport.register_server(
        sockname=("127.0.0.1", 9300),
        tcp_protocol_factory=lambda: _ProtocolStub("server-tcp"),
        udp_protocol=_DatagramProtocolStub("server"),
    )
    transport.deregister_server(("127.0.0.1", 9300))

    with pytest.raises(ConnectionRefusedError):
        transport.connect_tcp(
            client_sockname=("127.0.0.1", 5000),
            peer_sockname=("127.0.0.1", 9300),
            client_protocol_factory=lambda: _ProtocolStub("client"),
        )


def test_fault_check_drop_short_circuits(loop: SimulationLoop) -> None:
    """A fault-check that returns ``(False, 0)`` causes the
    delivery to be silently dropped."""
    def fault_check(
        kind: str,
        from_addr: tuple[str, int],
        to_addr: tuple[str, int],
    ) -> tuple[bool, float]:
        return False, 0.0

    transport = InProcessTransport(loop, fault_check=fault_check)

    target_protocol = _DatagramProtocolStub("target")
    transport.register_server(
        sockname=("127.0.0.1", 9400),
        tcp_protocol_factory=lambda: _ProtocolStub("target-tcp"),
        udp_protocol=target_protocol,
    )
    sender_udp_transport = transport.register_server(
        sockname=("127.0.0.1", 9401),
        tcp_protocol_factory=lambda: _ProtocolStub("sender-tcp"),
        udp_protocol=_DatagramProtocolStub("sender"),
    )

    async def scenario() -> None:
        sender_udp_transport.sendto(b"dropped", ("127.0.0.1", 9400))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    loop.run_until_complete(scenario())
    assert target_protocol.received == []


def test_fault_check_delay_uses_call_later(loop: SimulationLoop) -> None:
    """A fault-check that returns ``(True, delay)`` schedules
    delivery at virtual time + delay."""
    def fault_check(
        kind: str,
        from_addr: tuple[str, int],
        to_addr: tuple[str, int],
    ) -> tuple[bool, float]:
        return True, 5.0

    transport = InProcessTransport(loop, fault_check=fault_check)

    target_protocol = _DatagramProtocolStub("target")
    transport.register_server(
        sockname=("127.0.0.1", 9500),
        tcp_protocol_factory=lambda: _ProtocolStub("target-tcp"),
        udp_protocol=target_protocol,
    )
    sender_udp_transport = transport.register_server(
        sockname=("127.0.0.1", 9501),
        tcp_protocol_factory=lambda: _ProtocolStub("sender-tcp"),
        udp_protocol=_DatagramProtocolStub("sender"),
    )

    async def scenario() -> float:
        sender_udp_transport.sendto(b"delayed", ("127.0.0.1", 9500))
        await asyncio.sleep(10.0)
        return loop.time()

    end_time = loop.run_until_complete(scenario())
    # Delivery should fire at virtual time 5.0 (the configured delay).
    assert target_protocol.received == [(b"delayed", ("127.0.0.1", 9501))]
    assert end_time == pytest.approx(10.0, abs=1e-6)


def test_fake_tcp_transport_close_is_idempotent(loop: SimulationLoop) -> None:
    """Calling ``close`` twice is a no-op the second time."""
    def deliver(data: bytes) -> None:
        pass

    tx = FakeTCPTransport(
        loop=loop,
        deliver=deliver,
        peername=("127.0.0.1", 9000),
        sockname=("127.0.0.1", 5000),
    )
    assert not tx.is_closing()
    tx.close()
    assert tx.is_closing()
    tx.close()
    assert tx.is_closing()


def test_fake_tcp_transport_write_after_close_raises(loop: SimulationLoop) -> None:
    """Writing to a closed transport raises ``ConnectionError``."""
    tx = FakeTCPTransport(
        loop=loop,
        deliver=lambda data: None,
        peername=("127.0.0.1", 9000),
        sockname=("127.0.0.1", 5000),
    )
    tx.close()
    with pytest.raises(ConnectionError):
        tx.write(b"after close")
