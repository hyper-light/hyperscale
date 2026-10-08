"""
A TCP reply reaches exactly the request it answers.

Replies were matched by queue: a requester waited under the address it
dialed and the action, while each reply was filed under the replier's
own address. A requester that reached a node at any other address -- a
Kubernetes Service, a forwarded port -- never saw a reply, and two
concurrent requests for one action took each other's replies whenever
the peer answered them out of order. Each request now carries an id its
reply echoes.

* a node is answered when reached through a port forward;
* concurrent requests for one action each get their own reply, though
  the peer answers them out of order;
* a reply whose request already gave up reaches no other request.

A connection retired from new requests -- its address invalidated (a
peer suspected of restarting), or a request on it failed -- was closed
under the requests still waiting on it, losing their replies: each
waited out its whole timeout though the peer answered. A retired
connection now closes once no request waits on it:

* a request keeps its reply when its connection is invalidated, and
  the next request dials a new connection;
* a request that times out leaves the other requests on its
  connection their replies;
* a request that fails leaves a newer connection to the address open.

Real sockets; the node's name is "localhost", which RFC 6761 reserves
for loopback.
"""

import asyncio
import socket

import pytest

from hyperscale.distributed.models.message import generate_message_id

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp, udp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)

NAMED_HOST = "localhost"
LOOPBACK = "127.0.0.1"
AUTH_SECRET = "tcp-reply-correlation-secret-000"
REQUEST_TIMEOUT_SECONDS = 5.0
SHORT_TIMEOUT_SECONDS = 0.5
RELAY_READ_BYTES = 65536


class DelayedEchoNode(MercurySyncBaseServer):
    """A real base server whose TCP echo answers after the requested delay."""

    @udp.receive()
    async def echo_udp(self, addr, data, clock_time) -> bytes:
        return data

    @tcp.receive()
    async def delayed_echo(self, addr, data, clock_time) -> bytes:
        await asyncio.sleep(float(data.decode()))
        return b"echo:" + data


class HeldEchoNode(DelayedEchoNode):
    """Also holds ``held_echo`` requests, answering them once released."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.request_arrived = asyncio.Event()
        self.release_replies = asyncio.Event()

    @tcp.receive()
    async def held_echo(self, addr, data, clock_time) -> bytes:
        self.request_arrived.set()
        await self.release_replies.wait()
        return b"held:" + data


def free_port(socket_type: socket.SocketKind) -> int:
    with socket.socket(socket.AF_INET, socket_type) as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


def make_node[NodeType: DelayedEchoNode](node_type: type[NodeType] = DelayedEchoNode) -> NodeType:
    return node_type(
        NAMED_HOST,
        free_port(socket.SOCK_STREAM),
        free_port(socket.SOCK_DGRAM),
        Env(MERCURY_SYNC_AUTH_SECRET=AUTH_SECRET),
    )


class PortForward:
    """Relays every connection to its own port to the node's, as
    `kubectl port-forward` does."""

    def __init__(self, target_port: int) -> None:
        self._target_port = target_port
        self._server: asyncio.Server | None = None

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._relay, LOOPBACK, 0)
        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def _relay(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection(LOOPBACK, self._target_port)
        await asyncio.gather(
            self._pipe(reader, upstream_writer),
            self._pipe(upstream_reader, writer),
        )

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(RELAY_READ_BYTES):
                writer.write(data)
                await writer.drain()
        finally:
            writer.close()


@pytest.mark.asyncio
async def test_a_node_is_answered_through_a_port_forward() -> None:
    requester = make_node()
    responder = make_node()
    await requester.start_server()
    await responder.start_server()
    port_forward = PortForward(responder._tcp_port)
    forwarded_port = await port_forward.start()

    try:
        response, _ = await requester.send_tcp(
            (LOOPBACK, forwarded_port),
            "delayed_echo",
            b"0",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        assert response == b"echo:0"

    finally:
        await requester.shutdown()
        await responder.shutdown()
        await port_forward.stop()


@pytest.mark.asyncio
async def test_concurrent_requests_for_one_action_each_get_their_own_reply() -> None:
    requester = make_node()
    responder = make_node()
    await requester.start_server()
    await responder.start_server()
    responder_address = (NAMED_HOST, responder._tcp_port)

    try:
        # One connection, so both requests below share it.
        warm_up, _ = await requester.send_tcp(
            responder_address, "delayed_echo", b"0", timeout=REQUEST_TIMEOUT_SECONDS
        )
        assert warm_up == b"echo:0"

        slow, fast = await asyncio.gather(
            requester.send_tcp(responder_address, "delayed_echo", b"0.3", timeout=REQUEST_TIMEOUT_SECONDS),
            requester.send_tcp(responder_address, "delayed_echo", b"0.0", timeout=REQUEST_TIMEOUT_SECONDS),
        )

        assert (slow[0], fast[0]) == (b"echo:0.3", b"echo:0.0")

    finally:
        await requester.shutdown()
        await responder.shutdown()


class ClosedTransport:
    """The connection a reply arrived on, closed by the time the node
    shuts down."""

    def is_closing(self) -> bool:
        return True


def reply_frame(node: DelayedEchoNode, request_id: int, data: bytes) -> bytes:
    """A reply to request ``request_id``, framed, compressed and encrypted
    as a peer sends it: address<handler<clock(64)request_id(8)frame_id(8)data_len(4)data."""
    return node._encryptor.encrypt(
        node._compressor.compress(
            node._tcp_addr_slug
            + b"<delayed_echo<"
            + (0).to_bytes(64)
            + request_id.to_bytes(8, "big") + generate_message_id().to_bytes(8, "big")
            + len(data).to_bytes(4, "big")
            + data
        )
    )


@pytest.mark.asyncio
async def test_a_reply_without_a_waiting_request_reaches_no_other_request() -> None:
    node = make_node()
    await node.start_server()
    waiting_request = asyncio.get_running_loop().create_future()
    node._tcp_request_waiters[2] = waiting_request

    try:
        await node.process_tcp_client_response(reply_frame(node, 1, b"late reply"), ClosedTransport())

        assert not waiting_request.done()
        assert list(node._tcp_request_waiters) == [2]

        await node.process_tcp_client_response(reply_frame(node, 2, b"its reply"), ClosedTransport())

        assert waiting_request.result() == (b"its reply", 0)
        assert node._tcp_request_waiters == {}

    finally:
        await node.shutdown()


async def start_pair() -> tuple[DelayedEchoNode, HeldEchoNode, tuple[str, int]]:
    requester = make_node()
    responder = make_node(HeldEchoNode)
    await requester.start_server()
    await responder.start_server()
    return requester, responder, (NAMED_HOST, responder._tcp_port)


async def stop_pair(requester: DelayedEchoNode, responder: HeldEchoNode) -> None:
    responder.release_replies.set()
    await requester.shutdown()
    await responder.shutdown()


@pytest.mark.asyncio
async def test_a_request_keeps_its_reply_when_its_connection_is_invalidated() -> None:
    requester, responder, responder_address = await start_pair()

    async def invalidate_while_in_flight() -> asyncio.Transport:
        await responder.request_arrived.wait()
        first_connection = requester._tcp_client_transports[responder_address]
        requester._invalidate_tcp_client_transport(responder_address)
        responder.release_replies.set()
        return first_connection

    try:
        (reply, _), first_connection = await asyncio.gather(
            requester.send_tcp(responder_address, "held_echo", b"in-flight", timeout=REQUEST_TIMEOUT_SECONDS),
            invalidate_while_in_flight(),
        )

        assert reply == b"held:in-flight"
        assert first_connection.is_closing()
        assert requester._retired_tcp_client_transports == set()

        next_reply, _ = await requester.send_tcp(
            responder_address, "held_echo", b"next", timeout=REQUEST_TIMEOUT_SECONDS
        )

        assert next_reply == b"held:next"
        assert requester._tcp_client_transports[responder_address] is not first_connection
        assert requester._tcp_transport_requests == {}

    finally:
        await stop_pair(requester, responder)


@pytest.mark.asyncio
async def test_a_timed_out_request_leaves_other_requests_their_replies() -> None:
    requester, responder, responder_address = await start_pair()

    async def time_out_then_release() -> object:
        timed_out, _ = await requester.send_tcp(
            responder_address, "held_echo", b"impatient", timeout=SHORT_TIMEOUT_SECONDS
        )
        responder.release_replies.set()
        return timed_out

    try:
        # One connection, so both requests below share it.
        warm_up, _ = await requester.send_tcp(
            responder_address, "delayed_echo", b"0", timeout=REQUEST_TIMEOUT_SECONDS
        )
        assert warm_up == b"echo:0"
        shared_connection = requester._tcp_client_transports[responder_address]

        (reply, _), timed_out = await asyncio.gather(
            requester.send_tcp(responder_address, "held_echo", b"patient", timeout=REQUEST_TIMEOUT_SECONDS),
            time_out_then_release(),
        )

        assert isinstance(timed_out, TimeoutError)
        assert reply == b"held:patient"
        assert shared_connection.is_closing()
        assert (requester._retired_tcp_client_transports, requester._tcp_transport_requests) == (set(), {})

    finally:
        await stop_pair(requester, responder)


@pytest.mark.asyncio
async def test_a_failed_request_leaves_a_newer_connection_open() -> None:
    requester, responder, responder_address = await start_pair()

    async def reconnect_while_in_flight() -> bytes:
        await responder.request_arrived.wait()
        requester._invalidate_tcp_client_transport(responder_address)
        reply, _ = await requester.send_tcp(
            responder_address, "delayed_echo", b"0", timeout=REQUEST_TIMEOUT_SECONDS
        )
        return reply

    try:
        (timed_out, _), newer_reply = await asyncio.gather(
            requester.send_tcp(responder_address, "held_echo", b"stale", timeout=SHORT_TIMEOUT_SECONDS),
            reconnect_while_in_flight(),
        )

        assert isinstance(timed_out, TimeoutError)
        assert newer_reply == b"echo:0"
        assert not requester._tcp_client_transports[responder_address].is_closing()

    finally:
        await stop_pair(requester, responder)
