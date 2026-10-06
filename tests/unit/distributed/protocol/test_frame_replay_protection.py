"""
A captured frame cannot be replayed; a resend always goes through.

Distributed nodes had no working replay protection: the transport checked
only payloads it decoded itself (``msgspec`` models), every distributed
message is a dataclass its handler decodes, and the message id it would
have checked was never pickled. A captured frame replayed byte for byte ran
its handler again, every time.

Every frame now carries a per-send frame id (a Snowflake) inside its
AES-GCM-authenticated body, and ``ReplayGuard.validate_frame`` checks it:
duplicates are keyed on the frame's encryption nonce, and a watermark (the
newest timestamp evicted from the bounded nonce set) refuses anything that
could have been forgotten.

VOPR over the guard, against an independent model of the watermark:
* a replay of any accepted frame is never accepted again, however long ago
  it was accepted and however small the remembered window;
* a fresh frame is refused only when its timestamp is at or below the
  model's watermark -- a delayed frame, never a timely one -- including a
  frame whose Snowflake collides with another sender's (same millisecond,
  instance and sequence) under its own nonce;
* the guard never remembers more than its window.

Real sockets, two nodes:
* a TCP request frame captured on the wire and written again runs its
  handler once, and the replay is counted;
* a UDP request datagram captured and sent again runs its handler once;
* the same payload bytes sent twice (a resend) run the handler twice.
"""

from __future__ import annotations

import asyncio
import random
import secrets
import socket

import pytest

from hyperscale.core.jobs.protocols.replay_guard import SNOWFLAKE_TIMESTAMP_SHIFT, ReplayGuard
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp, udp
from hyperscale.distributed.server.server.mercury_sync_base_server import MercurySyncBaseServer

SEEDS = range(40)
OPERATIONS_PER_SEED = 600
LOOPBACK = "127.0.0.1"
AUTH_SECRET = "frame-replay-protection-secret-0"
REQUEST_TIMEOUT_SECONDS = 5.0
RELAY_READ_BYTES = 65536
NONCE_SIZE = 12


def frame_id_at(frame_ms: int, sequence: int) -> int:
    """A Snowflake for ``frame_ms`` (instance 0)."""
    return frame_ms << SNOWFLAKE_TIMESTAMP_SHIFT | sequence


@pytest.mark.parametrize("seed", SEEDS)
def test_the_guard_never_accepts_a_replay_and_refuses_only_frames_below_the_watermark(seed: int) -> None:
    seeded_random = random.Random(seed)
    window_size = seeded_random.randint(1, 24)
    guard = ReplayGuard(max_window_size=window_size)
    initial_watermark_ms = guard._frame_watermark_ms
    current_ms = initial_watermark_ms + 1_000
    sequence = 0
    accepted: list[tuple[int, bytes, int]] = []  # (frame_id, nonce, frame_ms) in acceptance order
    sent: list[tuple[int, bytes]] = []
    model_watermark_ms = initial_watermark_ms

    for _ in range(OPERATIONS_PER_SEED):
        action = seeded_random.random()
        if action < 0.35 and sent:
            frame_id, nonce = seeded_random.choice(sent)
            assert not guard.validate_frame(frame_id, nonce), "a replayed frame was accepted"
            continue

        sequence += 1
        if action < 0.45 and sent:
            # Another sender whose Snowflake collides with one already seen:
            # same frame id, its own encryption nonce.
            frame_id = seeded_random.choice(sent)[0]
            frame_ms = frame_id >> SNOWFLAKE_TIMESTAMP_SHIFT
        elif action < 0.55:
            # A fresh frame delayed in transit: stamped up to 40 ms ago.
            frame_ms = current_ms - seeded_random.randint(0, 40)
            frame_id = frame_id_at(frame_ms, sequence % 4096)
        else:
            current_ms += seeded_random.randint(0, 3)
            frame_ms = current_ms
            frame_id = frame_id_at(frame_ms, sequence % 4096)
        nonce = secrets.token_bytes(NONCE_SIZE)
        was_accepted = guard.validate_frame(frame_id, nonce)
        sent.append((frame_id, nonce))

        assert was_accepted == (frame_ms > model_watermark_ms), (
            f"frame at {frame_ms} with watermark {model_watermark_ms}: accepted={was_accepted}"
        )
        if was_accepted:
            accepted.append((frame_id, nonce, frame_ms))
            if len(accepted) > window_size:
                model_watermark_ms = max(model_watermark_ms, accepted[-window_size - 1][2])
        assert len(guard._seen_frame_nonces) <= window_size


def test_a_frame_stamped_before_the_guard_started_is_refused() -> None:
    guard = ReplayGuard()
    assert not guard.validate_frame(frame_id_at(guard._frame_watermark_ms, 0), secrets.token_bytes(NONCE_SIZE))
    assert guard.validate_frame(frame_id_at(guard._frame_watermark_ms + 1, 0), secrets.token_bytes(NONCE_SIZE))


class CountingNode(MercurySyncBaseServer):
    """A real base server counting each request its handlers run."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.tcp_handled: list[bytes] = []
        self.udp_handled: list[bytes] = []

    @tcp.receive()
    async def count_tcp(self, addr, data, clock_time) -> bytes:
        self.tcp_handled.append(data)
        return b"counted"

    @udp.receive()
    async def count_udp(self, addr, data, clock_time) -> bytes:
        self.udp_handled.append(data)
        return b"counted"


def free_port(socket_type: socket.SocketKind) -> int:
    with socket.socket(socket.AF_INET, socket_type) as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


def make_node() -> CountingNode:
    return CountingNode(
        LOOPBACK,
        free_port(socket.SOCK_STREAM),
        free_port(socket.SOCK_DGRAM),
        Env(MERCURY_SYNC_AUTH_SECRET=AUTH_SECRET),
    )


class RecordingRelay:
    """Relays connections to the node's port, recording every byte the
    requester sends -- what an attacker on the path captures."""

    def __init__(self, target_port: int) -> None:
        self._target_port = target_port
        self._server: asyncio.Server | None = None
        self.captured = bytearray()

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._relay, LOOPBACK, 0)
        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def _relay(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection(LOOPBACK, self._target_port)
        await asyncio.gather(
            self._pipe(reader, upstream_writer, record=True),
            self._pipe(upstream_reader, writer, record=False),
        )

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, record: bool) -> None:
        try:
            while data := await reader.read(RELAY_READ_BYTES):
                if record:
                    self.captured.extend(data)
                writer.write(data)
                await writer.drain()
        finally:
            writer.close()


async def wait_until(condition, timeout_seconds: float) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not condition():
        assert asyncio.get_running_loop().time() < deadline, "condition not reached"
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_a_captured_tcp_request_replayed_on_the_wire_runs_its_handler_once() -> None:
    requester = make_node()
    responder = make_node()
    await requester.start_server()
    await responder.start_server()
    relay = RecordingRelay(responder._tcp_port)
    relay_port = await relay.start()

    try:
        response, _ = await requester.send_tcp(
            (LOOPBACK, relay_port), "count_tcp", b"once", timeout=REQUEST_TIMEOUT_SECONDS
        )
        assert response == b"counted"
        assert responder.tcp_handled == [b"once"]

        # The attacker writes the captured request frame again.
        _, attacker_writer = await asyncio.open_connection(LOOPBACK, responder._tcp_port)
        attacker_writer.write(bytes(relay.captured))
        await attacker_writer.drain()
        await wait_until(lambda: responder._tcp_drop_counter.replay_detected == 1, REQUEST_TIMEOUT_SECONDS)
        attacker_writer.close()

        assert responder.tcp_handled == [b"once"]

    finally:
        await requester.shutdown()
        await responder.shutdown()
        await relay.stop()


@pytest.mark.asyncio
async def test_a_captured_udp_request_sent_again_runs_its_handler_once() -> None:
    requester = make_node()
    responder = make_node()
    await requester.start_server()
    await responder.start_server()
    captured: list[bytes] = []
    send_datagram = requester._udp_transport.sendto

    def capturing_sendto(datagram: bytes, address: tuple[str, int]) -> None:
        captured.append(datagram)
        send_datagram(datagram, address)

    requester._udp_transport.sendto = capturing_sendto

    try:
        response, _ = await requester.send_udp(
            (LOOPBACK, responder._udp_port), "count_udp", b"once", timeout=REQUEST_TIMEOUT_SECONDS
        )
        assert response == b"counted"
        assert responder.udp_handled == [b"once"]

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as attacker:
            attacker.sendto(captured[0], (LOOPBACK, responder._udp_port))
        await wait_until(lambda: responder._udp_drop_counter.replay_detected == 1, REQUEST_TIMEOUT_SECONDS)

        assert responder.udp_handled == [b"once"]

    finally:
        requester._udp_transport.sendto = send_datagram
        await requester.shutdown()
        await responder.shutdown()


@pytest.mark.asyncio
async def test_the_same_payload_sent_twice_runs_its_handler_twice() -> None:
    requester = make_node()
    responder = make_node()
    await requester.start_server()
    await responder.start_server()
    payload = b"resent"

    try:
        for _ in range(2):
            tcp_response, _ = await requester.send_tcp(
                (LOOPBACK, responder._tcp_port), "count_tcp", payload, timeout=REQUEST_TIMEOUT_SECONDS
            )
            udp_response, _ = await requester.send_udp(
                (LOOPBACK, responder._udp_port), "count_udp", payload, timeout=REQUEST_TIMEOUT_SECONDS
            )
            assert (tcp_response, udp_response) == (b"counted", b"counted")

        assert responder.tcp_handled == [payload, payload]
        assert responder.udp_handled == [payload, payload]
        assert responder._tcp_drop_counter.replay_detected == 0
        assert responder._udp_drop_counter.replay_detected == 0

    finally:
        await requester.shutdown()
        await responder.shutdown()
