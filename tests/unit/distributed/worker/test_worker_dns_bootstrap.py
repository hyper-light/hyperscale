"""
A worker joins through the managers its DNS names resolve to (AD-28).

The worker's discovery loop resolved its DNS names into discovery peers
and stopped there: nothing registered with a discovered manager, so a
worker configured with only a headless service name never joined a
cluster. Its first lookup also waited out a whole interval.

* the first round runs at once and registers with every manager found
  while the worker knows no healthy manager (any one answers with the
  whole cohort);
* a worker that knows a healthy manager does not register again;
* a round without DNS names configured registers with nothing.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.nodes.worker import background_loops as background_loops_module
from hyperscale.distributed.nodes.worker.background_loops import WorkerBackgroundLoops

MANAGER_ADDRESSES = [("10.244.1.10", 9000), ("10.244.1.11", 9000)]


class OneRoundClock:
    """Ends the loop at its first sleep."""

    def __init__(self, loops: WorkerBackgroundLoops) -> None:
        self._loops = loops
        self.sleeps = 0

    def monotonic(self) -> float:
        return 0.0

    def time(self) -> float:
        return 0.0

    async def sleep(self, seconds: float) -> None:
        self.sleeps += 1
        self._loops._running = False


def make_loops(
    dns_names: list[str],
    healthy_manager_addresses: list[tuple[str, int]],
) -> tuple[WorkerBackgroundLoops, AsyncMock]:
    discover_peers = AsyncMock(return_value=[])
    discovery_service = SimpleNamespace(
        config=SimpleNamespace(dns_names=dns_names),
        discover_peers=discover_peers,
        get_dns_peer_addresses=lambda: list(MANAGER_ADDRESSES),
        decay_failures=lambda: 0,
        cleanup_expired_dns=lambda: (0, 0),
    )
    registry = SimpleNamespace(get_healthy_manager_tcp_addrs=lambda: list(healthy_manager_addresses))
    loops = WorkerBackgroundLoops(
        registry=registry,
        state=SimpleNamespace(),
        discovery_service=discovery_service,
    )
    return loops, discover_peers


async def run_one_round(
    loops: WorkerBackgroundLoops,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[AsyncMock, OneRoundClock]:
    clock = OneRoundClock(loops)
    monkeypatch.setattr(background_loops_module, "_DEFAULT_CLOCK", clock)
    register_with_manager = AsyncMock(return_value=True)
    await loops.run_discovery_maintenance_loop(
        is_running=lambda: True,
        register_with_manager=register_with_manager,
    )
    return register_with_manager, clock


@pytest.mark.asyncio
async def test_the_first_round_registers_with_every_manager_found(monkeypatch: pytest.MonkeyPatch) -> None:
    loops, discover_peers = make_loops(["managers.hyperscale.svc"], healthy_manager_addresses=[])

    register_with_manager, clock = await run_one_round(loops, monkeypatch)

    discover_peers.assert_awaited_once()
    assert sorted(call.args[0] for call in register_with_manager.await_args_list) == MANAGER_ADDRESSES
    assert clock.sleeps == 1


@pytest.mark.asyncio
async def test_a_worker_with_a_healthy_manager_does_not_register_again(monkeypatch: pytest.MonkeyPatch) -> None:
    loops, discover_peers = make_loops(["managers.hyperscale.svc"], healthy_manager_addresses=[MANAGER_ADDRESSES[0]])

    register_with_manager, _ = await run_one_round(loops, monkeypatch)

    discover_peers.assert_awaited_once()
    register_with_manager.assert_not_awaited()


@pytest.mark.asyncio
async def test_without_dns_names_nothing_is_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    loops, discover_peers = make_loops([], healthy_manager_addresses=[])

    register_with_manager, _ = await run_one_round(loops, monkeypatch)

    discover_peers.assert_not_awaited()
    register_with_manager.assert_not_awaited()
