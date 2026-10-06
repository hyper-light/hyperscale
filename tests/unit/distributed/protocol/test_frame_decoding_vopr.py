"""
Adversarial frame decoding: a node's TCP and UDP read paths survive any
byte sequence a peer, a broken peer, or an attacker can put on the wire.

VOPR over the real ``MercurySyncTCPProtocol`` / ``ReceiveBuffer`` and the
real ``MercurySyncBaseServer`` read paths. Each seed draws a stream of
frames and cuts it at seeded (or every) byte boundary:

* the length-prefixed stream reassembles exactly -- every frame, in
  order, once -- however the reads split it, a zero-length frame
  included, and nothing but a strictly incomplete frame is ever left
  buffered;
* a length prefix above the frame limit is refused before a byte of its
  body is buffered: the frames before it are delivered, the peer is told
  FRAME_TOO_LARGE, the connection closes, the buffer is emptied; a
  prefix exactly at the limit is a frame like any other;
* a read that would grow the buffer past its limit closes the
  connection and empties the buffer;
* through a live node, malformed frames -- random bytes, truncated or
  bit-flipped ciphertext, authenticated plaintext that is not a request,
  compression bombs, lying data lengths -- are dropped or answered with
  the sanitized error, never reach a handler, never escape their task,
  and never close the connection: every valid request interleaved with
  them is answered with its own reply;
* the same holds for UDP datagrams, oversize ones included;
* a malformed reply on a dialed connection, and an unsolicited UDP reply
  naming addresses no request awaits, are dropped without escaping
  their task or growing the node's state.
"""

from __future__ import annotations

import asyncio
import json
import random
import socket
from collections import Counter

import pytest

from hyperscale.core.jobs.protocols.constants import MAX_DECOMPRESSED_SIZE, MAX_MESSAGE_SIZE
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp, udp
from hyperscale.distributed.server.protocol.drop_counter import DropCounter
from hyperscale.distributed.server.protocol.mercury_sync_tcp_protocol import MercurySyncTCPProtocol
from hyperscale.distributed.server.protocol.receive_buffer import (
    MAX_BUFFER_SIZE,
    MAX_FRAME_LENGTH,
    frame_message,
)
from hyperscale.distributed.server.protocol.receive_buffer_shared import LENGTH_PREFIX_SIZE
from hyperscale.distributed.server.server.mercury_sync_base_server import MercurySyncBaseServer

SEEDS = range(24)
LOOPBACK = "127.0.0.1"
AUTH_SECRET = "frame-decoding-vopr-secret-00000"
PEER_ADDRESS = ("127.0.0.1", 47001)
MAX_SMALL_FRAME = 96
FRAMES_PER_STREAM = 12
REQUESTS_PER_SEED = 24
MALFORMED_KINDS = (
    "random_bytes",
    "truncated_ciphertext",
    "bit_flipped_ciphertext",
    "zero_length",
    "plaintext_without_separators",
    "unknown_handler",
    "bad_address",
    "not_zstd",
    "compression_bomb",
)
UNAUTHENTICATED_KINDS = frozenset(("random_bytes", "truncated_ciphertext", "bit_flipped_ciphertext", "zero_length"))


class RecordingStreamTransport:
    """The accepted connection's transport: records what the node writes
    and whether it closed the connection."""

    def __init__(self, peername: tuple[str, int] | None = PEER_ADDRESS) -> None:
        self.written: list[bytes] = []
        self.closed = False
        self._peername = peername

    def write(self, data: bytes) -> None:
        self.written.append(bytes(data))

    def close(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed

    def get_extra_info(self, name: str, default: object = None) -> object:
        return self._peername if name == "peername" else default

    def pause_reading(self) -> None:
        pass

    def resume_reading(self) -> None:
        pass


class RecordingDatagramTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple[str, int]]] = []

    def sendto(self, data: bytes, address: tuple[str, int]) -> None:
        self.sent.append((bytes(data), address))

    def get_extra_info(self, name: str, default: object = None) -> object:
        return default


class FrameRecordingConnection:
    """The protocol's connection: records each frame the protocol hands it."""

    def __init__(self) -> None:
        self.frames: list[bytes] = []
        self._tcp_drop_counter = DropCounter()

    def read_server_tcp(self, data: bytes, transport: RecordingStreamTransport) -> None:
        self.frames.append(data)

    def read_client_tcp(self, data: bytes, transport: RecordingStreamTransport) -> None:
        self.frames.append(data)

    def lose_client_tcp(self, transport: RecordingStreamTransport) -> None:
        pass


class EchoNode(MercurySyncBaseServer):
    """A real base server whose handlers echo, counting what reached them."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.handled_payloads: list[bytes] = []

    @tcp.receive()
    async def echo(self, addr, data, clock_time) -> bytes:
        self.handled_payloads.append(data)
        return b"echo:" + data

    @udp.receive()
    async def echo_udp(self, addr, data, clock_time) -> bytes:
        self.handled_payloads.append(data)
        return b"echo:" + data


def free_port(socket_type: socket.SocketKind) -> int:
    with socket.socket(socket.AF_INET, socket_type) as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


def make_node() -> EchoNode:
    return EchoNode(
        LOOPBACK,
        free_port(socket.SOCK_STREAM),
        free_port(socket.SOCK_DGRAM),
        Env(MERCURY_SYNC_AUTH_SECRET=AUTH_SECRET),
    )


def make_protocol(connection: FrameRecordingConnection | EchoNode, mode: str = "server"):
    protocol = MercurySyncTCPProtocol(connection, mode=mode)
    transport = RecordingStreamTransport()
    protocol.connection_made(transport)
    return protocol, transport


def seeded_frames(seeded_random: random.Random) -> list[bytes]:
    frames = [seeded_random.randbytes(seeded_random.randint(0, MAX_SMALL_FRAME)) for _ in range(FRAMES_PER_STREAM)]
    frames[seeded_random.randrange(FRAMES_PER_STREAM)] = b""
    return frames


def seeded_cuts(seeded_random: random.Random, stream: bytes, maximum_chunk: int) -> list[bytes]:
    chunks: list[bytes] = []
    offset = 0
    while offset < len(stream):
        chunk_length = seeded_random.randint(1, maximum_chunk)
        chunks.append(stream[offset : offset + chunk_length])
        offset += chunk_length
    return chunks


def feed(protocol: MercurySyncTCPProtocol, transport: RecordingStreamTransport, chunks: list[bytes]) -> None:
    """Feed the chunks as the event loop would: none after the close.
    After every read, only a strictly incomplete frame may stay buffered."""
    for chunk in chunks:
        if transport.closed:
            return
        protocol.data_received(chunk)
        buffered = bytes(protocol._receive_buffer)
        assert len(buffered) <= MAX_BUFFER_SIZE
        if len(buffered) >= LENGTH_PREFIX_SIZE:
            assert len(buffered) < LENGTH_PREFIX_SIZE + int.from_bytes(buffered[:LENGTH_PREFIX_SIZE], "big")


def request_plaintext(node: EchoNode, handler: bytes, request_id: int, data: bytes, address: bytes) -> bytes:
    return (
        address + b"<" + handler + b"<" + (0).to_bytes(64) + request_id.to_bytes(8, "big")
        + len(data).to_bytes(4, "big") + data
    )


def sealed(node: EchoNode, plaintext: bytes) -> bytes:
    return node._encryptor.encrypt(node._compressor.compress(plaintext))


def malformed_frame_body(node: EchoNode, seeded_random: random.Random, kind: str, request_id: int) -> bytes:
    peer_slug = f"{PEER_ADDRESS[0]}:{PEER_ADDRESS[1]}".encode()
    valid_ciphertext = sealed(node, request_plaintext(node, b"echo", request_id, b"never-handled", peer_slug))
    match kind:
        case "random_bytes":
            return seeded_random.randbytes(seeded_random.randint(1, 400))
        case "truncated_ciphertext":
            return valid_ciphertext[: seeded_random.randrange(len(valid_ciphertext))]
        case "bit_flipped_ciphertext":
            flipped = bytearray(valid_ciphertext)
            flipped[seeded_random.randrange(len(flipped))] ^= 1 << seeded_random.randrange(8)
            return bytes(flipped)
        case "zero_length":
            return b""
        case "plaintext_without_separators":
            return sealed(node, seeded_random.randbytes(seeded_random.randint(0, 120)).replace(b"<", b">"))
        case "unknown_handler":
            return sealed(node, request_plaintext(node, b"no_such_handler", request_id, b"never-handled", peer_slug))
        case "bad_address":
            return sealed(node, request_plaintext(node, b"echo", request_id, b"never-handled", b"no-port\x00here"))
        case "not_zstd":
            return node._encryptor.encrypt(seeded_random.randbytes(seeded_random.randint(1, 200)))
        case "compression_bomb":
            return sealed(node, request_plaintext(node, b"echo", request_id, bytes(MAX_DECOMPRESSED_SIZE // 2), peer_slug))
    raise AssertionError(f"unknown malformed kind {kind}")


def decoded_replies(node: EchoNode, written: list[bytes]) -> list[tuple[bytes, int, bytes]]:
    """(handler, request_id, data) of every framed reply the node wrote."""
    stream = b"".join(written)
    replies: list[tuple[bytes, int, bytes]] = []
    offset = 0
    while offset < len(stream):
        frame_length = int.from_bytes(stream[offset : offset + LENGTH_PREFIX_SIZE], "big")
        body = stream[offset + LENGTH_PREFIX_SIZE : offset + LENGTH_PREFIX_SIZE + frame_length]
        assert len(body) == frame_length
        offset += LENGTH_PREFIX_SIZE + frame_length
        plaintext = node._decompressor.decompress(node._encryptor.decrypt(body))
        _, handler, rest = plaintext.split(b"<", maxsplit=2)
        data_length = int.from_bytes(rest[72:76], "big")
        replies.append((handler, int.from_bytes(rest[64:72], "big"), rest[76 : 76 + data_length]))
    return replies


async def settle_tasks(tasks: list[asyncio.Task]) -> None:
    """Await every response task and require that none escaped."""
    await asyncio.gather(*tasks, return_exceptions=True)
    escaped = [task.exception() for task in tasks if not task.cancelled() and task.exception() is not None]
    assert escaped == []


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_stream_reassembles_exactly_however_reads_split_it(seed: int) -> None:
    seeded_random = random.Random(seed)
    frames = seeded_frames(seeded_random)
    stream = b"".join(frame_message(frame) for frame in frames)

    # Every two-read split, at every byte boundary.
    for boundary in range(len(stream) + 1):
        connection = FrameRecordingConnection()
        protocol, transport = make_protocol(connection)
        feed(protocol, transport, [stream[:boundary], stream[boundary:]])
        assert connection.frames == frames
        assert len(protocol._receive_buffer) == 0
        assert not transport.closed

    # A byte at a time, and seeded many-read partitions.
    for maximum_chunk in (1, 3, 17, len(stream)):
        connection = FrameRecordingConnection()
        protocol, transport = make_protocol(connection)
        feed(protocol, transport, seeded_cuts(seeded_random, stream, maximum_chunk))
        assert connection.frames == frames
        assert len(protocol._receive_buffer) == 0
        assert not transport.closed
        assert connection._tcp_drop_counter.total == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_oversize_length_prefix_is_refused_before_its_body_is_buffered(seed: int) -> None:
    seeded_random = random.Random(seed)
    frames = seeded_frames(seeded_random)
    oversize_length = seeded_random.choice(
        (MAX_FRAME_LENGTH + 1, seeded_random.randint(MAX_FRAME_LENGTH + 2, 2**32 - 1), 2**32 - 1)
    )
    stream = (
        b"".join(frame_message(frame) for frame in frames)
        + oversize_length.to_bytes(LENGTH_PREFIX_SIZE, "big")
        + seeded_random.randbytes(seeded_random.randint(0, 64))
        + frame_message(b"after the refusal")
    )
    connection = FrameRecordingConnection()
    protocol, transport = make_protocol(connection)

    feed(protocol, transport, seeded_cuts(seeded_random, stream, seeded_random.randint(1, 64)))

    assert connection.frames == frames
    assert transport.closed
    assert len(protocol._receive_buffer) == 0
    assert connection._tcp_drop_counter.message_too_large == 1
    assert len(transport.written) == 1
    refusal = transport.written[0]
    assert int.from_bytes(refusal[:LENGTH_PREFIX_SIZE], "big") == len(refusal) - LENGTH_PREFIX_SIZE
    assert json.loads(refusal[LENGTH_PREFIX_SIZE:]) == {
        "error_type": "FRAME_TOO_LARGE",
        "actual_size": oversize_length,
        "max_size": MAX_FRAME_LENGTH,
        "suggestion": "Split payload into smaller chunks or compress data",
    }


@pytest.mark.asyncio
async def test_frame_exactly_at_the_limit_is_delivered() -> None:
    seeded_random = random.Random(0)
    largest_frame = seeded_random.randbytes(MAX_FRAME_LENGTH)
    stream = frame_message(largest_frame) + frame_message(b"next")
    connection = FrameRecordingConnection()
    protocol, transport = make_protocol(connection)

    feed(protocol, transport, seeded_cuts(seeded_random, stream, 256 * 1024))

    assert connection.frames == [largest_frame, b"next"]
    assert not transport.closed
    assert len(protocol._receive_buffer) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_read_that_would_overflow_the_buffer_closes_the_connection(seed: int) -> None:
    seeded_random = random.Random(seed)
    # A frame at the limit, all but its last byte buffered...
    pending_length = seeded_random.randint(MAX_FRAME_LENGTH // 2, MAX_FRAME_LENGTH)
    pending_prefix = pending_length.to_bytes(LENGTH_PREFIX_SIZE, "big")
    buffered_part = pending_prefix + bytes(pending_length - 1)
    # ...then one read carrying more than the buffer has room for.
    overflowing_read = bytes(MAX_BUFFER_SIZE - len(buffered_part) + seeded_random.randint(1, 4096))
    connection = FrameRecordingConnection()
    protocol, transport = make_protocol(connection)

    feed(protocol, transport, seeded_cuts(seeded_random, buffered_part, 256 * 1024))
    assert not transport.closed
    assert len(protocol._receive_buffer) == len(buffered_part)

    feed(protocol, transport, [overflowing_read, frame_message(b"after the close")])

    assert transport.closed
    assert connection.frames == []
    assert len(protocol._receive_buffer) == 0
    assert connection._tcp_drop_counter.message_too_large == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_malformed_tcp_frames_never_kill_the_connection_for_valid_requests(seed: int) -> None:
    seeded_random = random.Random(seed)
    node = make_node()
    await node.start_server()
    try:
        protocol, transport = make_protocol(node)
        peer_slug = f"{PEER_ADDRESS[0]}:{PEER_ADDRESS[1]}".encode()
        expected_replies: dict[int, bytes] = {}
        malformed_request_ids: set[int] = set()
        malformed_kinds: Counter[str] = Counter()
        stream_parts: list[bytes] = []
        for request_id in range(1, REQUESTS_PER_SEED + 1):
            if seeded_random.random() < 0.5:
                data = seeded_random.randbytes(seeded_random.randint(0, 64))
                expected_replies[request_id] = b"echo:" + data
                stream_parts.append(
                    frame_message(sealed(node, request_plaintext(node, b"echo", request_id, data, peer_slug)))
                )
                continue
            malformed_request_ids.add(request_id)
            kind = seeded_random.choice(MALFORMED_KINDS)
            malformed_kinds[kind] += 1
            stream_parts.append(frame_message(malformed_frame_body(node, seeded_random, kind, request_id)))

        feed(protocol, transport, seeded_cuts(seeded_random, b"".join(stream_parts), seeded_random.randint(1, 512)))
        await settle_tasks(list(node._pending_tcp_server_responses))

        replies = decoded_replies(node, transport.written)
        answered = {request_id: data for _, request_id, data in replies if request_id in expected_replies}
        assert answered == expected_replies
        # Each malformed frame is dropped at the check that catches it; the
        # authenticated ones that fail to parse are answered with the
        # sanitized error -- never a handler's answer.
        unparseable_count = (
            malformed_kinds["plaintext_without_separators"]
            + malformed_kinds["unknown_handler"]
            + malformed_kinds["not_zstd"]
        )
        error_replies = [data for _, request_id, data in replies if request_id not in expected_replies]
        assert error_replies == [b"Request processing failed"] * unparseable_count
        drop_counter = node._tcp_drop_counter
        assert drop_counter.decryption_failed == sum(malformed_kinds[kind] for kind in UNAUTHENTICATED_KINDS)
        assert drop_counter.decompression_too_large == malformed_kinds["compression_bomb"]
        assert drop_counter.malformed_message == unparseable_count
        assert drop_counter.message_too_large == 0
        assert sorted(node.handled_payloads) == sorted(reply[len(b"echo:"):] for reply in expected_replies.values())
        assert not transport.closed
        assert len(protocol._receive_buffer) == 0
        assert set(node._tcp_server_request_transports) <= {PEER_ADDRESS}
    finally:
        await node.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_malformed_datagrams_are_dropped_and_valid_ones_answered(seed: int) -> None:
    seeded_random = random.Random(seed)
    node = make_node()
    await node.start_server()
    try:
        transport = RecordingDatagramTransport()
        peer_slug = f"{PEER_ADDRESS[0]}:{PEER_ADDRESS[1]}".encode()
        expected_echoes: list[bytes] = []
        expected_request_ids: list[int] = []
        malformed_kinds: Counter[str] = Counter()
        for request_index in range(REQUESTS_PER_SEED):
            if seeded_random.random() < 0.5:
                data = seeded_random.randbytes(seeded_random.randint(0, 64))
                expected_echoes.append(b"echo:" + data)
                expected_request_ids.append(request_index + 1)
                datagram = sealed(
                    node,
                    b"c<" + peer_slug + b"<echo_udp<" + (0).to_bytes(64) + (request_index + 1).to_bytes(8, "big")
                    + len(data).to_bytes(4, "big") + data,
                )
            else:
                kind = seeded_random.choice(MALFORMED_KINDS + ("oversize", "lying_length", "unknown_type"))
                malformed_kinds[kind] += 1
                match kind:
                    case "oversize":
                        datagram = seeded_random.randbytes(16) + bytes(MAX_MESSAGE_SIZE)
                    case "lying_length":
                        datagram = sealed(
                            node,
                            b"c<" + b"no-port\x00here" + b"<echo_udp<" + (0).to_bytes(64) + (0).to_bytes(8)
                            + (2**32 - 1).to_bytes(4, "big") + b"short",
                        )
                    case "unknown_type":
                        datagram = sealed(node, b"x<" + peer_slug + b"<echo_udp<" + (0).to_bytes(76))
                    case _:
                        datagram = malformed_frame_body(node, seeded_random, kind, request_index)
            node.read_udp(datagram, transport, None)
        await settle_tasks(list(node._pending_udp_server_responses))

        echoes: list[bytes] = []
        answered_request_ids: list[int] = []
        for datagram, address in transport.sent:
            assert address == PEER_ADDRESS
            plaintext = node._decompressor.decompress(node._encryptor.decrypt(datagram))
            request_type, _, handler, rest = plaintext.split(b"<", maxsplit=3)
            assert (request_type, handler) == (b"s", b"echo_udp")
            echo = rest[76 : 76 + int.from_bytes(rest[72:76], "big")]
            echoes.append(echo)
            if echo != b"Request processing failed":
                answered_request_ids.append(int.from_bytes(rest[64:72], "big"))
        answered_echoes = [echo for echo in echoes if echo != b"Request processing failed"]
        assert sorted(answered_echoes) == sorted(expected_echoes)
        # Every reply names the request it answers.
        assert sorted(answered_request_ids) == expected_request_ids
        assert sorted(node.handled_payloads) == sorted(echo[len(b"echo:"):] for echo in expected_echoes)
        drop_counter = node._udp_drop_counter
        assert drop_counter.message_too_large == malformed_kinds["oversize"]
        assert drop_counter.decryption_failed == sum(malformed_kinds[kind] for kind in UNAUTHENTICATED_KINDS)
        assert drop_counter.decompression_too_large == malformed_kinds["compression_bomb"]
        # The TCP-layout plaintexts carry two separators where a datagram
        # needs three: they fail to parse, as does what is not a datagram,
        # and an authenticated datagram that is neither a request nor a
        # reply is counted too.
        assert drop_counter.malformed_message == (
            malformed_kinds["plaintext_without_separators"]
            + malformed_kinds["not_zstd"]
            + malformed_kinds["unknown_handler"]
            + malformed_kinds["bad_address"]
            + malformed_kinds["unknown_type"]
        )
    finally:
        await node.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_malformed_reply_on_a_dialed_connection_is_dropped_without_escaping(seed: int) -> None:
    seeded_random = random.Random(seed)
    node = make_node()
    await node.start_server()
    try:
        protocol, transport = make_protocol(node, mode="client")
        waiting_request = asyncio.get_running_loop().create_future()
        node._tcp_request_waiters[7] = waiting_request
        malformed_kinds = [seeded_random.choice(MALFORMED_KINDS) for _ in range(8)]
        reply_parts = [
            # Request id 0 answers no request: whatever these frames carry,
            # none may complete the waiting one.
            frame_message(malformed_frame_body(node, seeded_random, kind, 0))
            for kind in malformed_kinds
        ]
        reply_parts.append(
            frame_message(sealed(node, request_plaintext(node, b"echo", 7, b"its reply", node._tcp_addr_slug)))
        )

        feed(protocol, transport, seeded_cuts(seeded_random, b"".join(reply_parts), seeded_random.randint(1, 256)))
        await settle_tasks(list(node._pending_tcp_server_responses))

        assert not transport.closed
        assert waiting_request.done()
        assert waiting_request.result()[0] == b"its reply"
        assert node._tcp_request_waiters == {}
        assert node._tcp_drop_counter.malformed_message == malformed_kinds.count("plaintext_without_separators")
    finally:
        await node.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_unsolicited_udp_replies_do_not_grow_the_node(seed: int) -> None:
    seeded_random = random.Random(seed)
    node = make_node()
    responder = make_node()
    await node.start_server()
    await responder.start_server()
    try:
        transport = RecordingDatagramTransport()
        for _ in range(64):
            forged_address = seeded_random.randbytes(seeded_random.randint(1, 24)).replace(b"<", b">")
            forged_handler = seeded_random.randbytes(seeded_random.randint(1, 24)).replace(b"<", b">")
            node.read_udp(
                sealed(
                    node,
                    b"s<" + forged_address + b"<" + forged_handler + b"<" + (0).to_bytes(64)
                    + seeded_random.getrandbits(64).to_bytes(8, "big") + (4).to_bytes(4, "big") + b"data",
                ),
                transport,
                None,
            )
        await settle_tasks(list(node._pending_udp_server_responses))

        assert node._udp_request_waiters == {}
        assert transport.sent == []

        # A reply a request awaits is still delivered to it, and nothing is
        # kept for the peer once it settles.
        reply, _ = await node.send_udp((LOOPBACK, responder._udp_port), "echo_udp", b"solicited", timeout=5.0)
        assert reply == b"echo:solicited"
        assert node._udp_request_waiters == {}
    finally:
        await node.shutdown()
        await responder.shutdown()


@pytest.mark.asyncio
async def test_a_reply_after_its_request_gave_up_never_answers_the_next_request() -> None:
    """A UDP reply resolves only the request whose id it echoes: one that
    lands after its request timed out is dropped -- it is not handed to the
    next request to the same peer and action, as one queue per (peer,
    action) once did (a stale cross-cluster ``xack`` read as a fresh one)."""
    node = make_node()
    responder = make_node()
    await node.start_server()
    await responder.start_server()
    try:
        live_transport = node._udp_transport
        recording = RecordingDatagramTransport()
        node._udp_transport = recording
        late = await node.send_udp((LOOPBACK, responder._udp_port), "echo_udp", b"first", timeout=0.05)
        node._udp_transport = live_transport
        assert isinstance(late[0], asyncio.TimeoutError | TimeoutError)
        assert node._udp_request_waiters == {}

        request_plaintext_sent = node._decompressor.decompress(node._encryptor.decrypt(recording.sent[0][0]))
        timed_out_request_id = int.from_bytes(request_plaintext_sent.split(b"<", maxsplit=3)[3][64:72], "big")
        responder_slug = f"{LOOPBACK}:{responder._udp_port}".encode()
        node.read_udp(
            sealed(
                node,
                b"s<" + responder_slug + b"<echo_udp<" + (0).to_bytes(64) + timed_out_request_id.to_bytes(8, "big")
                + len(b"echo:first").to_bytes(4, "big") + b"echo:first",
            ),
            live_transport,
            None,
        )
        await settle_tasks(list(node._pending_udp_server_responses))

        reply, _ = await node.send_udp((LOOPBACK, responder._udp_port), "echo_udp", b"second", timeout=5.0)
        assert reply == b"echo:second"
        assert node._udp_request_waiters == {}
    finally:
        await node.shutdown()
        await responder.shutdown()
