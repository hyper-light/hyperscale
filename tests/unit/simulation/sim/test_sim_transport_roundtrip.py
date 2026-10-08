"""
Phase 6d transport-seam integration test.

Proves that a real ``MercurySyncBaseServer`` — the production base
server, unmodified except for the ``transport_factory`` seam — starts
and exchanges messages under the SIM stack (``SimulationLoop`` +
``VirtualClock`` + ``SeededRandom`` + ``InProcessTransport`` +
``SimTransportFactory``) with NO real sockets and NO banned event-loop
operations.

The round-trip exercises the *entire* production send/receive path:
``send_tcp`` / ``send_udp`` → encode → compress → encrypt → frame →
``FakeTransport.write``/``sendto`` → ``InProcessTransport`` route →
the peer's ``MercurySyncTCPProtocol.data_received`` /
``MercurySyncUDPProtocol.datagram_received`` → deframe → decrypt →
dispatch to the ``@tcp.receive()`` / ``@udp.receive()`` handler →
response routed back. The only thing the seam replaces is the OS-socket
byte-transit layer; every higher layer runs byte-for-byte as in REAL.

This is the foundation the rest of Phase 6d builds on: if the base
server transacts correctly under SIM, the ``SimulationRuntime`` /
``ClusterHarness`` wiring is a matter of constructing the real
manager/worker/gate subclasses with these same injected dependencies.
"""

import asyncio
import os

import pytest

os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", "sim-test-secret-000000000000")

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp, udp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)
from tests.simulation.harness.sim import (
    InProcessTransport,
    SeededRandom,
    SimTransportFactory,
    SimulationLoop,
    VirtualClock,
)


class _EchoServer(MercurySyncBaseServer):
    """Minimal server with one TCP and one UDP echo handler."""

    @tcp.receive()
    async def echo_tcp(self, addr, data, clock_time) -> bytes:
        return b"tcp-echo:" + data

    @udp.receive()
    async def echo_udp(self, addr, data, clock_time) -> bytes:
        return b"udp-echo:" + data


def _make_sim_cluster():
    """Build the SIM stack + two echo servers sharing it.

    Returns ``(loop, server_a, server_b)``. The caller drives the loop.
    """
    loop = SimulationLoop()
    asyncio.set_event_loop(loop)
    in_process_transport = InProcessTransport(loop)
    factory = SimTransportFactory(in_process_transport)
    clock = VirtualClock(loop)
    env = Env(MERCURY_SYNC_AUTH_SECRET="sim-transport-roundtrip-secret-0123456")

    server_a = _EchoServer(
        "127.0.0.1", 9000, 9001, env,
        clock=clock, random_source=SeededRandom(1), transport_factory=factory,
    )
    server_b = _EchoServer(
        "127.0.0.1", 9002, 9003, env,
        clock=clock, random_source=SeededRandom(2), transport_factory=factory,
    )
    return loop, server_a, server_b, in_process_transport


def _close(loop: SimulationLoop, *started_servers: _EchoServer) -> None:
    """Shut the started servers down, then close the loop. Closing the
    loop under running servers left their background loops and task
    runners pending, destroyed only when the test process exited."""
    try:
        loop.run_until_complete(
            asyncio.gather(*(server.shutdown() for server in started_servers))
        )
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_base_server_starts_under_sim_with_fake_transports():
    """``start_server`` under SIM binds fake transports, no real sockets."""
    from tests.simulation.harness.sim import FakeUDPTransport

    loop, server_a, _server_b, in_process_transport = _make_sim_cluster()
    try:
        loop.run_until_complete(server_a.start_server())
        assert isinstance(server_a._udp_transport, FakeUDPTransport)
        assert server_a._tcp_connected is True
        # No real OS socket was created for the UDP server.
        assert server_a._udp_server_socket is None
        assert ("127.0.0.1", 9001) in in_process_transport.registered_addresses()
        assert ("127.0.0.1", 9000) in in_process_transport.registered_addresses()
    finally:
        _close(loop, server_a)


def test_tcp_roundtrip_under_sim():
    """A full ``send_tcp`` → handler → response round-trip under SIM."""
    loop, server_a, server_b, _ = _make_sim_cluster()

    async def scenario() -> bytes:
        await server_a.start_server()
        await server_b.start_server()
        response, _clock = await server_a.send_tcp(
            ("127.0.0.1", 9002), "echo_tcp", b"hello"
        )
        return response

    try:
        response = loop.run_until_complete(scenario())
        assert response == b"tcp-echo:hello"
    finally:
        _close(loop, server_a, server_b)


def test_udp_roundtrip_under_sim():
    """A full ``send_udp`` → handler → response round-trip under SIM."""
    loop, server_a, server_b, _ = _make_sim_cluster()

    async def scenario() -> bytes:
        await server_a.start_server()
        await server_b.start_server()
        response, _clock = await server_a.send_udp(
            ("127.0.0.1", 9003), "echo_udp", b"world"
        )
        return response

    try:
        response = loop.run_until_complete(scenario())
        assert response == b"udp-echo:world"
    finally:
        _close(loop, server_a, server_b)


def test_roundtrip_uses_zero_wall_time():
    """The round-trip completes without real time elapsing — proof the
    virtual clock, not wall-clock I/O, drives the exchange."""
    import time as wall_time

    loop, server_a, server_b, _ = _make_sim_cluster()

    async def scenario() -> None:
        await server_a.start_server()
        await server_b.start_server()
        for _ in range(20):
            await server_a.send_tcp(("127.0.0.1", 9002), "echo_tcp", b"x")

    try:
        wall_start = wall_time.monotonic()
        loop.run_until_complete(scenario())
        wall_elapsed = wall_time.monotonic() - wall_start
        # 20 in-process round-trips must complete near-instantly; if this
        # is slow, something is doing real I/O or real sleeping.
        assert wall_elapsed < 2.0
    finally:
        _close(loop, server_a, server_b)
