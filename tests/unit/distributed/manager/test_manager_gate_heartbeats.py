"""
A manager heartbeats every gate at once.

The manager sent its gate heartbeats one gate at a time, so an
unreachable gate held every gate after it for a full send timeout each
round -- long enough for healthy gates to judge the datacenter's
heartbeats stale. The round's sends now run concurrently.

* a gate whose send never completes does not hold up the others: every
  gate's send starts before any of them finishes.
"""

import asyncio
from types import SimpleNamespace

import pytest

from hyperscale.distributed.nodes.manager.server import ManagerServer

GATES = [("gate-0", 8431), ("gate-1", 8431), ("gate-2", 8431)]
HEARTBEAT_INTERVAL_SECONDS = 5.0
ROUND_WAIT_SECONDS = 5.0


class OneRoundClock:
    """Lets the first round run, and ends the loop at the next sleep the
    way a cancelled loop ends."""

    def __init__(self) -> None:
        self.sleeps = 0

    def monotonic(self) -> float:
        return 0.0

    def time(self) -> float:
        return 0.0

    async def sleep(self, seconds: float) -> None:
        self.sleeps += 1
        if self.sleeps > 1:
            raise asyncio.CancelledError()


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


@pytest.mark.asyncio
async def test_an_unreachable_gate_does_not_hold_up_the_others() -> None:
    started: list[tuple[str, int]] = []
    every_send_started = asyncio.Event()
    manager = object.__new__(ManagerServer)
    manager._running = True
    manager._clock = OneRoundClock()
    manager._config = SimpleNamespace(heartbeat_interval_seconds=HEARTBEAT_INTERVAL_SECONDS)
    manager._udp_logger = RecordingLogger()
    manager._host = "manager-0"
    manager._tcp_port = 8231
    manager._node_id = SimpleNamespace(short="manager-0")
    manager._seed_gates = list(GATES)
    manager._get_healthy_gate_tcp_addrs = lambda: list(GATES)
    manager._build_manager_heartbeat = lambda: SimpleNamespace(dump=lambda: b"heartbeat")

    async def send_gate_heartbeat(gate_addr: tuple[str, int], payload: bytes) -> bool:
        started.append(gate_addr)
        if len(started) == len(GATES):
            every_send_started.set()
        # The first gate never answers until every send has begun; sent
        # one at a time, the others would never start.
        await every_send_started.wait()
        return True

    manager._send_gate_heartbeat = send_gate_heartbeat

    await asyncio.wait_for(ManagerServer._gate_heartbeat_loop(manager), timeout=ROUND_WAIT_SECONDS)

    assert sorted(started) == sorted(GATES)
