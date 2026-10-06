"""
Nodes started with a DNS name are reached by that name over real sockets.

A node is identified by the host it is started with: every frame it sends
declares that host, and a requester matches each reply to the address it
dialed. A node started with a stable name -- a Kubernetes StatefulSet
pod's -- must therefore keep the name after binding and be reachable by
it. Before, the server replaced the name with its bound IP once the TCP
listener started (so its heartbeats and registrations named the IP while
its frames named the host), and datagrams addressed to a name went out
through a blocking lookup per datagram.

The name is "localhost", which RFC 6761 reserves for loopback.
"""

import socket

import pytest

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp, udp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)

NAMED_HOST = "localhost"
AUTH_SECRET = "named-host-addressing-secret-0000"
REQUEST_TIMEOUT_SECONDS = 5.0


class EchoNode(MercurySyncBaseServer):
    """A real base server with one UDP and one TCP echo handler."""

    @udp.receive()
    async def echo_udp(self, addr, data, clock_time) -> bytes:
        return b"udp-echo:" + data

    @tcp.receive()
    async def echo_tcp(self, addr, data, clock_time) -> bytes:
        return b"tcp-echo:" + data


def free_port(socket_type: socket.SocketKind) -> int:
    with socket.socket(socket.AF_INET, socket_type) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def start_node() -> EchoNode:
    return EchoNode(
        NAMED_HOST,
        free_port(socket.SOCK_STREAM),
        free_port(socket.SOCK_DGRAM),
        Env(MERCURY_SYNC_AUTH_SECRET=AUTH_SECRET),
    )


@pytest.mark.asyncio
async def test_a_node_started_with_a_name_keeps_it_and_answers_by_it():
    requester = start_node()
    responder = start_node()
    await requester.start_server()
    await responder.start_server()

    try:
        # Binding resolved the name; the node still advertises the name.
        assert responder._host == NAMED_HOST
        assert responder._tcp_addr_slug == f"{NAMED_HOST}:{responder._tcp_port}".encode()

        udp_response, _ = await requester.send_udp(
            (NAMED_HOST, responder._udp_port),
            "echo_udp",
            b"ping",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        assert udp_response == b"udp-echo:ping"

        tcp_response, _ = await requester.send_tcp(
            (NAMED_HOST, responder._tcp_port),
            "echo_tcp",
            b"ping",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        assert tcp_response == b"tcp-echo:ping"

    finally:
        await requester.shutdown()
        await responder.shutdown()
