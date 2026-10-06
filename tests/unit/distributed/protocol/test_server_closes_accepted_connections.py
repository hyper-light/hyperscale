"""
A node's TCP connections close with the node, and its cap on them holds.

Closing a listener leaves the connections it accepted open. An aborted or
shut-down node kept answering on every connection a peer had already
pooled -- under its old identity, so a peer reaching the restarted node
at the same address re-registered the dead incarnation. Graceful shutdown
closed them through ``asyncio.Server.abort_clients``, which exists from
Python 3.13 only (the package supports 3.11+). A node now tracks the
connections it accepted and closes them itself.

Each connection's protocol built its own server state, so the cap on a
node's connections (connection-storm mitigation) counted one connection:
it never held. The state is now the node's, shared by every connection
it accepts.

A request waiting on a connection its peer closed got no reply and waited
out its whole timeout -- a peer crashing mid-request held its caller that
long. It now fails when the connection closes.

Real sockets on loopback.
"""

import asyncio
import socket

import pytest

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp, udp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)

LOOPBACK = "127.0.0.1"
AUTH_SECRET = "server-closes-accepted-secret-0000"
REQUEST_TIMEOUT_SECONDS = 5.0


class EchoNode(MercurySyncBaseServer):
    @udp.receive()
    async def echo_udp(self, addr, data, clock_time) -> bytes:
        return data

    @tcp.receive()
    async def echo(self, addr, data, clock_time) -> bytes:
        return b"echo:" + data


class SilentNode(EchoNode):
    """Takes ``hold`` requests and never answers them."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.request_arrived = asyncio.Event()

    @tcp.receive()
    async def hold(self, addr, data, clock_time) -> bytes:
        self.request_arrived.set()
        await asyncio.Event().wait()
        return b""


def free_port(socket_type: socket.SocketKind) -> int:
    with socket.socket(socket.AF_INET, socket_type) as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


def make_node[NodeType: EchoNode](
    maximum_accepted_connections: int = 0,
    node_type: type[NodeType] = EchoNode,
) -> NodeType:
    return node_type(
        LOOPBACK,
        free_port(socket.SOCK_STREAM),
        free_port(socket.SOCK_DGRAM),
        Env(
            MERCURY_SYNC_AUTH_SECRET=AUTH_SECRET,
            MERCURY_SYNC_MAX_ACCEPTED_TCP_CONNECTIONS=maximum_accepted_connections,
        ),
    )


async def pooled_connection(requester: EchoNode, responder: EchoNode) -> asyncio.Transport:
    """Open (and use) the requester's pooled connection to the responder."""
    responder_address = (LOOPBACK, responder._tcp_port)
    response, _ = await requester.send_tcp(
        responder_address, "echo", b"warm", timeout=REQUEST_TIMEOUT_SECONDS
    )
    assert response == b"echo:warm"
    return requester._tcp_client_transports[responder_address]


async def until_closed(transport: asyncio.Transport) -> bool:
    for _ in range(100):
        if transport.is_closing():
            return True
        await asyncio.sleep(0.01)
    return False


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["abort", "shutdown"])
async def test_a_node_that_ends_closes_the_connections_it_accepted(ending: str) -> None:
    requester = make_node()
    responder = make_node()
    await requester.start_server()
    await responder.start_server()
    try:
        connection = await pooled_connection(requester, responder)

        if ending == "abort":
            responder.abort()
        else:
            await responder.shutdown()

        assert await until_closed(connection)
        # Nothing answers at the address any more.
        response, _ = await requester.send_tcp(
            (LOOPBACK, responder._tcp_port), "echo", b"after", timeout=REQUEST_TIMEOUT_SECONDS
        )
        assert isinstance(response, Exception)
    finally:
        await requester.shutdown()
        if ending == "abort":
            await responder.shutdown()


@pytest.mark.asyncio
async def test_a_node_holds_no_more_connections_than_its_cap() -> None:
    responder = make_node(maximum_accepted_connections=1)
    first_requester = make_node()
    second_requester = make_node()
    for node in (responder, first_requester, second_requester):
        await node.start_server()
    try:
        await pooled_connection(first_requester, responder)

        refused, _ = await second_requester.send_tcp(
            (LOOPBACK, responder._tcp_port), "echo", b"second", timeout=REQUEST_TIMEOUT_SECONDS
        )

        assert isinstance(refused, Exception)
        assert responder._tcp_server_state.connections_rejected == 1
        assert len(responder._tcp_server_state.connections) == 1
    finally:
        for node in (first_requester, second_requester, responder):
            await node.shutdown()


@pytest.mark.asyncio
async def test_a_request_fails_when_its_connection_closes_not_at_its_timeout() -> None:
    requester = make_node()
    responder = make_node(node_type=SilentNode)
    await requester.start_server()
    await responder.start_server()
    try:
        await pooled_connection(requester, responder)
        request = asyncio.create_task(
            requester.send_tcp(
                (LOOPBACK, responder._tcp_port), "hold", b"", timeout=REQUEST_TIMEOUT_SECONDS
            )
        )
        await asyncio.wait_for(responder.request_arrived.wait(), timeout=REQUEST_TIMEOUT_SECONDS)

        responder.abort()
        response, _ = await request

        # The connection's loss, not the timeout.
        assert isinstance(response, ConnectionResetError)
    finally:
        await requester.shutdown()
        await responder.shutdown()
