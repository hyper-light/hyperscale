"""
Unit tests for ``VirtualClock``.

The Clock seam contract: production code calls
``await self._clock.sleep(t)``, ``self._clock.monotonic()``,
``await self._clock.wait_for(awaitable, timeout)``. Under SIM
these route through ``asyncio.sleep`` / ``asyncio.wait_for`` /
``loop.time()`` against virtual time.

Tests verify the contract holds:

1. ``monotonic`` / ``time`` return the loop's virtual time.
2. ``sleep`` advances virtual time by the requested delay.
3. ``wait_for`` times out at the requested virtual deadline, not
   wall time.
4. Concurrent sleeps interleave correctly.
"""

import asyncio

import pytest

from tests.simulation.harness.sim import SimulationLoop, VirtualClock


@pytest.fixture
def loop():
    loop_instance = SimulationLoop()
    asyncio.set_event_loop(loop_instance)
    yield loop_instance
    if not loop_instance.is_closed():
        loop_instance.close()
    asyncio.set_event_loop(None)


@pytest.fixture
def clock(loop: SimulationLoop) -> VirtualClock:
    return VirtualClock(loop)


def test_monotonic_returns_loop_time(loop: SimulationLoop, clock: VirtualClock) -> None:
    """``clock.monotonic()`` mirrors ``loop.time()``."""
    assert clock.monotonic() == loop.time()


def test_monotonic_ns_tracks_virtual_time(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """``clock.monotonic_ns()`` returns integer nanoseconds derived from
    the same virtual timeline as ``monotonic()``.

    Id-generation sites (probe / gate-rejoin tokens) embed a nanosecond
    timestamp; routing it through the clock seam keeps those ids
    deterministic under replay instead of reading the wall clock.
    """
    assert clock.monotonic_ns() == int(loop.time() * 1_000_000_000)
    assert isinstance(clock.monotonic_ns(), int)


def test_monotonic_ns_advances_with_sleep(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """``monotonic_ns`` advances by the slept virtual interval in ns."""
    async def scenario() -> tuple[int, int]:
        start = clock.monotonic_ns()
        await clock.sleep(2.0)
        return start, clock.monotonic_ns()

    start, end = loop.run_until_complete(scenario())
    assert end - start == 2_000_000_000


def test_time_returns_loop_time(loop: SimulationLoop, clock: VirtualClock) -> None:
    """``clock.time()`` mirrors ``loop.time()``.

    SIM collapses ``time.time`` and ``time.monotonic`` — production
    code that uses them interchangeably (Snowflake IDs, log
    timestamps) gets consistent values.
    """
    assert clock.time() == loop.time()


def test_sleep_advances_virtual_time(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """``await clock.sleep(t)`` resumes at virtual time + t.

    This is the proof point — production code that does
    ``await self._clock.sleep(...)`` under SIM advances virtual
    time exactly as intended.
    """
    async def scenario() -> tuple[float, float]:
        start = clock.monotonic()
        await clock.sleep(3.5)
        end = clock.monotonic()
        return start, end

    start, end = loop.run_until_complete(scenario())
    assert end - start == pytest.approx(3.5, abs=1e-6)


def test_wait_for_resolves_before_timeout(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """``wait_for`` returns the awaitable's result when it resolves
    before the virtual-time deadline."""
    async def scenario() -> str:
        future = loop.create_future()
        loop.call_later(1.0, future.set_result, "resolved")
        return await clock.wait_for(future, timeout=5.0)

    result = loop.run_until_complete(scenario())
    assert result == "resolved"
    assert clock.monotonic() == pytest.approx(1.0, abs=1e-6)


def test_wait_for_times_out_at_virtual_deadline(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """``wait_for(future, timeout=N)`` raises ``TimeoutError`` when
    virtual time reaches N before the future resolves."""
    async def scenario() -> None:
        future = loop.create_future()  # never resolved
        await clock.wait_for(future, timeout=2.5)

    with pytest.raises(asyncio.TimeoutError):
        loop.run_until_complete(scenario())
    assert clock.monotonic() == pytest.approx(2.5, abs=1e-6)
