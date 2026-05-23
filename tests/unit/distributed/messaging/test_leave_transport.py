"""Unit coverage for SWIM LEAVE transport handling."""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.server.protocol import MessagePriority
from hyperscale.distributed.server.protocol.in_flight_tracker import (
    PriorityLimits,
    ProtocolInFlightTracker,
)
from hyperscale.distributed.swim.admission import classify_swim_payload
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
    rate_limit_allowed: bool,
) -> tuple[HealthAwareServer, RecordingDispatcher, list[tuple[tuple[str, int], str]]]:
    server = object.__new__(HealthAwareServer)
    dispatcher = RecordingDispatcher()
    rate_limit_calls: list[tuple[tuple[str, int], str]] = []

    server._udp_addr_slug = b"127.0.0.1:9000"
    server._udp_logger = RecordingLogger()
    server._host = "127.0.0.1"
    server._udp_port = 9000
    server._node_id = SimpleNamespace(short="server-1")
    server._message_dispatcher = dispatcher
    server._rate_limit_stats = {
        "accepted": 0,
        "rejected": 0,
    }
    server._is_duplicate_message = lambda addr, data: False

    async def check_rate_limit(
        addr: tuple[str, int],
        admission_class: str = "unknown",
        *,
        log_rejection: bool = True,
    ) -> bool:
        rate_limit_calls.append((addr, admission_class))
        return rate_limit_allowed

    async def extract_embedded_state(
        data: bytes,
        addr: tuple[str, int],
        process_piggybacks: bool = True,
    ) -> bytes:
        return data

    async def handle_error(error: Exception) -> None:
        pass

    async def handle_exception(error: Exception, operation: str) -> None:
        pass

    async def should_process_auxiliary_piggyback(
        addr: tuple[str, int],
        data: bytes,
    ) -> bool:
        return False

    server._check_rate_limit = check_rate_limit
    server._extract_embedded_state = extract_embedded_state
    server._classify_swim_admission = lambda addr, data: "lifecycle_direct"
    server._should_process_auxiliary_piggyback = should_process_auxiliary_piggyback
    server.handle_error = handle_error
    server.handle_exception = handle_exception

    return server, dispatcher, rate_limit_calls


@pytest.mark.asyncio
async def test_direct_leave_uses_generic_rate_limit() -> None:
    """Direct LEAVE is still subject to the generic SWIM token bucket."""
    source_addr = ("127.0.0.1", 46024)
    server, dispatcher, rate_limit_calls = _make_receive_server(False)

    response = await HealthAwareServer.receive(
        server,
        source_addr,
        b"leave:7:worker-1>127.0.0.1:46024#|mdead:1:127.0.0.1:1",
        123,
    )

    assert response == b"nack>127.0.0.1:9000"
    assert rate_limit_calls == [(source_addr, "lifecycle_direct")]
    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_direct_leave_dispatches_when_rate_limit_allows() -> None:
    """Direct LEAVE reaches dispatch through the normal receive path."""
    source_addr = ("127.0.0.1", 46024)
    server, dispatcher, rate_limit_calls = _make_receive_server(True)

    response = await HealthAwareServer.receive(
        server,
        source_addr,
        b"leave:7:worker-1>127.0.0.1:46024#|mdead:1:127.0.0.1:1",
        123,
    )

    assert response == b"ack>127.0.0.1:9000"
    assert rate_limit_calls == [(source_addr, "lifecycle_direct")]
    assert dispatcher.calls == [
        (
            source_addr,
            b"leave:7:worker-1>127.0.0.1:46024#|mdead:1:127.0.0.1:1",
            123,
        )
    ]


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
    server._udp_port = 9000
    server._node_id = SimpleNamespace(full="worker-1", numeric_id=1, short="worker-1")

    await HealthAwareServer._broadcast_leave(server)

    assert len(send_calls) == 2
    assert send_calls[0] == (
        target_addr,
        b"leave:7:worker-1>127.0.0.1:9000",
        0.5,
    )
    assert send_calls[1] == send_calls[0]


def test_swim_admission_group_is_bounded_independent_of_normal_limit() -> None:
    """SWIM traffic has a bounded reserve separate from NORMAL data traffic."""
    tracker = ProtocolInFlightTracker(
        limits=PriorityLimits(
            swim=2,
            high=1,
            normal=1,
            low=1,
            global_limit=1,
        )
    )

    assert tracker.try_acquire(MessagePriority.NORMAL)
    assert tracker.try_acquire(MessagePriority.CRITICAL, admission_group="swim")
    assert tracker.try_acquire(MessagePriority.CRITICAL, admission_group="swim")
    assert not tracker.try_acquire(MessagePriority.CRITICAL, admission_group="swim")

    tracker.release(MessagePriority.CRITICAL, admission_group="swim")
    assert tracker.try_acquire(MessagePriority.CRITICAL, admission_group="swim")


def test_swim_admission_classifies_authorized_direct_leave() -> None:
    """Direct self-LEAVE uses the bounded lifecycle admission class."""
    source_addr = ("127.0.0.1", 46024)

    admission_class = classify_swim_payload(
        source_addr,
        b"leave:7:worker-1>127.0.0.1:46024#|mdead:1:127.0.0.1:1",
        registered_node_id="worker-1",
    )

    assert admission_class == "lifecycle_direct"


def test_swim_admission_classifies_propagated_leave_as_gossip() -> None:
    """Forwarded LEAVE does not consume direct lifecycle reserve."""
    admission_class = classify_swim_payload(
        ("127.0.0.1", 46024),
        b"leave:7:worker-1>127.0.0.1:46088",
        registered_node_id="worker-forwarder",
    )

    assert admission_class == "membership_gossip"
