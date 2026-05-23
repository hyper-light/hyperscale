"""Unit coverage for SWIM LEAVE transport handling."""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.swim.health_aware_server import HealthAwareServer


class RecordingDispatcher:
    """Minimal dispatcher double for ``HealthAwareServer.receive`` tests."""

    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, int], bytes, int]] = []

    async def dispatch(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        self.calls.append((addr, data, clock_time))
        return b"ack>127.0.0.1:9000"


class RecordingLogger:
    """Minimal async logger double."""

    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


def _make_receive_server(
    registered_node_ids: dict[tuple[str, int], str],
) -> tuple[HealthAwareServer, RecordingDispatcher, list[tuple[str, int]]]:
    server = object.__new__(HealthAwareServer)
    dispatcher = RecordingDispatcher()
    rate_limit_calls: list[tuple[str, int]] = []

    server._udp_addr_slug = b"127.0.0.1:9000"
    server._message_dispatcher = dispatcher
    server._rate_limit_stats = {
        "accepted": 0,
        "rejected": 0,
        "protected_direct_leave": 0,
    }
    server._get_registered_node_id_for_addr = registered_node_ids.get
    server._is_duplicate_message = lambda addr, data: False

    async def check_rate_limit(addr: tuple[str, int]) -> bool:
        rate_limit_calls.append(addr)
        return False

    async def extract_embedded_state(
        data: bytes,
        addr: tuple[str, int],
    ) -> bytes:
        return data

    async def handle_error(error: Exception) -> None:
        pass

    async def handle_exception(error: Exception, operation: str) -> None:
        pass

    server._check_rate_limit = check_rate_limit
    server._extract_embedded_state = extract_embedded_state
    server.handle_error = handle_error
    server.handle_exception = handle_exception

    return server, dispatcher, rate_limit_calls


@pytest.mark.asyncio
async def test_authorized_direct_leave_bypasses_generic_rate_limit() -> None:
    """Registered self-originated LEAVE reaches dispatch despite token exhaustion."""
    source_addr = ("127.0.0.1", 46024)
    server, dispatcher, rate_limit_calls = _make_receive_server(
        {source_addr: "worker-1"}
    )

    response = await HealthAwareServer.receive(
        server,
        source_addr,
        b"leave:7:worker-1>127.0.0.1:46024#|mdead:1:127.0.0.1:1",
        123,
    )

    assert response == b"ack>127.0.0.1:9000"
    assert rate_limit_calls == []
    assert server._rate_limit_stats["protected_direct_leave"] == 1
    assert dispatcher.calls == [
        (
            source_addr,
            b"leave:7:worker-1>127.0.0.1:46024#|mdead:1:127.0.0.1:1",
            123,
        )
    ]


@pytest.mark.asyncio
async def test_spoofed_direct_leave_uses_generic_rate_limit() -> None:
    """A direct LEAVE with the wrong node identity is not protected."""
    source_addr = ("127.0.0.1", 46024)
    server, dispatcher, rate_limit_calls = _make_receive_server(
        {source_addr: "worker-actual"}
    )

    response = await HealthAwareServer.receive(
        server,
        source_addr,
        b"leave:7:worker-spoofed>127.0.0.1:46024",
        123,
    )

    assert response == b"nack>127.0.0.1:9000"
    assert rate_limit_calls == [source_addr]
    assert server._rate_limit_stats["protected_direct_leave"] == 0
    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_propagated_leave_uses_generic_rate_limit() -> None:
    """A LEAVE about another node remains normal rate-limited gossip traffic."""
    source_addr = ("127.0.0.1", 46024)
    target_addr = ("127.0.0.1", 46088)
    server, dispatcher, rate_limit_calls = _make_receive_server(
        {target_addr: "worker-2"}
    )

    response = await HealthAwareServer.receive(
        server,
        source_addr,
        b"leave:7:worker-2>127.0.0.1:46088",
        123,
    )

    assert response == b"nack>127.0.0.1:9000"
    assert rate_limit_calls == [source_addr]
    assert server._rate_limit_stats["protected_direct_leave"] == 0
    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_broadcast_leave_retries_nack_response() -> None:
    """``_broadcast_leave`` treats NACK as failure and retries boundedly."""
    server = object.__new__(HealthAwareServer)
    target_addr = ("127.0.0.1", 9001)
    send_calls: list[tuple[tuple[str, int], bytes, float]] = []
    responses = [(b"nack>127.0.0.1:9001", 1), (b"ack>127.0.0.1:9001", 2)]

    async def prepare_leave_incarnation() -> int:
        return 7

    async def send(
        node: tuple[str, int],
        message: bytes,
        timeout: float,
    ) -> tuple[bytes, int]:
        send_calls.append((node, message, timeout))
        return responses.pop(0)

    server._get_self_udp_addr = lambda: ("127.0.0.1", 9000)
    server._prepare_leave_incarnation = prepare_leave_incarnation
    server._get_leave_targets = lambda: [target_addr]
    server.get_lhm_adjusted_timeout = lambda timeout: 0.5
    server.send = send
    server._udp_logger = RecordingLogger()
    server._host = "127.0.0.1"
    server._port = 9000
    server._node_id = SimpleNamespace(full="worker-1", numeric_id=1)

    await HealthAwareServer._broadcast_leave(server)

    assert len(send_calls) == 2
    assert send_calls[0] == (
        target_addr,
        b"leave:7:worker-1>127.0.0.1:9000",
        0.5,
    )
    assert send_calls[1] == send_calls[0]
    assert server._udp_logger.entries == []
