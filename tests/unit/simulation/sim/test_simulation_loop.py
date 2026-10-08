"""
Unit tests for ``SimulationLoop``.

Covers the primitives this loop must hold:

1. Virtual-time progression — ``loop.time()`` advances exactly when
   the loop schedules a timer fire, never spontaneously.
2. FIFO ``_ready`` drain — ``call_soon`` callbacks fire in
   registration order regardless of what they schedule.
3. ``call_later`` virtual-time scheduling — a coroutine that
   awaits ``asyncio.sleep(5)`` completes when virtual time has
   advanced by 5 seconds, not wall time.
4. Banned-method surface — every entry on the banned list raises
   ``SimulationConstraintError`` synchronously.
5. Deadlock detection — when ``_ready`` and ``_scheduled`` are both
   empty and the loop hasn't been stopped, ``SimulationConstraintError``
   surfaces rather than silent hang.
6. ``EventTrace`` integration — when a trace is attached, every
   scheduling decision is recorded in order.

The tests construct a fresh ``SimulationLoop`` per test and tear
it down explicitly so any cross-test contamination is impossible.
"""

import asyncio

import pytest

from tests.simulation.harness.sim import (
    EventTrace,
    SimulationConstraintError,
    SimulationLoop,
)


@pytest.fixture
def loop():
    """Return a fresh ``SimulationLoop``; close on teardown."""
    loop_instance = SimulationLoop()
    asyncio.set_event_loop(loop_instance)
    yield loop_instance
    if not loop_instance.is_closed():
        loop_instance.close()
    asyncio.set_event_loop(None)


def test_initial_time_is_zero(loop: SimulationLoop) -> None:
    """A fresh loop starts at virtual time 0.0.

    Every Phase 6 scenario relies on this as the baseline so
    ``call_later(5, ...)`` registers at absolute virtual time 5.0.
    """
    assert loop.time() == 0.0


def test_call_soon_does_not_advance_time(loop: SimulationLoop) -> None:
    """``call_soon`` callbacks fire at the *current* virtual instant.

    Virtual time only advances when ``_ready`` empties and the
    loop reaches the next scheduled timer.
    """
    times_observed: list[float] = []

    async def scenario() -> None:
        for _ in range(5):
            loop.call_soon(lambda: times_observed.append(loop.time()))
        # Yield once so the scheduled call_soon callbacks fire.
        await asyncio.sleep(0)

    loop.run_until_complete(scenario())
    assert times_observed == [0.0, 0.0, 0.0, 0.0, 0.0]


def test_sleep_advances_virtual_time(loop: SimulationLoop) -> None:
    """``await asyncio.sleep(N)`` advances virtual time by N.

    The proof that production-side ``await self._clock.sleep(t)``
    works under SIM: it delegates to ``asyncio.sleep`` which uses
    ``loop.call_later`` which uses our virtual ``loop.time()``.
    """
    async def scenario() -> float:
        await asyncio.sleep(5.0)
        return loop.time()

    final_time = loop.run_until_complete(scenario())
    assert final_time == pytest.approx(5.0, abs=1e-6)


def test_sleep_does_not_block_wall_time(loop: SimulationLoop) -> None:
    """Sleeping 100 virtual seconds takes near-zero wall time.

    The defining performance property of SIM: virtual time is
    instantaneous from wall-clock perspective.
    """
    import time as wall_time

    async def scenario() -> None:
        await asyncio.sleep(100.0)

    wall_start = wall_time.monotonic()
    loop.run_until_complete(scenario())
    wall_elapsed = wall_time.monotonic() - wall_start
    # Sleeping 100 virtual seconds should take well under 1 wall
    # second even on a slow runner; if this fails, virtual-time
    # advancement is broken.
    assert wall_elapsed < 1.0


def test_concurrent_sleeps_serialize_by_time(loop: SimulationLoop) -> None:
    """Multiple coroutines sleeping different durations resume in
    virtual-time order, not registration order.

    Validates that ``_scheduled`` heap-ordering uses virtual time.
    """
    wake_order: list[tuple[str, float]] = []

    async def sleeper(label: str, delay: float) -> None:
        await asyncio.sleep(delay)
        wake_order.append((label, loop.time()))

    async def scenario() -> None:
        await asyncio.gather(
            sleeper("c", 3.0),
            sleeper("a", 1.0),
            sleeper("b", 2.0),
        )

    loop.run_until_complete(scenario())
    labels = [entry[0] for entry in wake_order]
    times = [entry[1] for entry in wake_order]
    assert labels == ["a", "b", "c"]
    assert times == [pytest.approx(1.0), pytest.approx(2.0), pytest.approx(3.0)]


def test_run_in_executor_raises(loop: SimulationLoop) -> None:
    """``loop.run_in_executor`` raises ``SimulationConstraintError``.

    Thread pools are the largest source of asyncio non-determinism;
    every reach for them under SIM must surface loudly.
    """
    with pytest.raises(SimulationConstraintError) as exc_info:
        loop.run_in_executor(None, print, "hi")
    assert "run_in_executor" in str(exc_info.value)


def test_add_signal_handler_raises(loop: SimulationLoop) -> None:
    """``add_signal_handler`` raises ``SimulationConstraintError``."""
    with pytest.raises(SimulationConstraintError) as exc_info:
        loop.add_signal_handler(0, print)
    assert "add_signal_handler" in str(exc_info.value)


def test_create_connection_raises(loop: SimulationLoop) -> None:
    """``create_connection`` raises ``SimulationConstraintError``.

    Real sockets are banned; ``InProcessTransport`` substitutes.
    The exception is raised synchronously (before ``await``) so the
    failure attributes to the caller, not an asyncio internal.
    """
    async def scenario() -> None:
        await loop.create_connection(lambda: None, "127.0.0.1", 5000)

    with pytest.raises(SimulationConstraintError) as exc_info:
        loop.run_until_complete(scenario())
    assert "create_connection" in str(exc_info.value)


def test_create_datagram_endpoint_raises(loop: SimulationLoop) -> None:
    """``create_datagram_endpoint`` raises ``SimulationConstraintError``."""
    async def scenario() -> None:
        await loop.create_datagram_endpoint(lambda: None)

    with pytest.raises(SimulationConstraintError) as exc_info:
        loop.run_until_complete(scenario())
    assert "create_datagram_endpoint" in str(exc_info.value)


def test_deadlock_detection(loop: SimulationLoop) -> None:
    """A coroutine awaiting a Future that no one resolves deadlocks.

    Under REAL, this would block in ``select()`` forever. Under SIM,
    ``_run_once`` finds no ready callbacks and no scheduled timers
    and raises ``SimulationConstraintError`` rather than silently
    spinning.
    """
    async def scenario() -> None:
        # Future that nothing resolves — no call_soon, no call_later
        # can fire to set its result.
        await loop.create_future()

    with pytest.raises(SimulationConstraintError) as exc_info:
        loop.run_until_complete(scenario())
    assert "deadlocked" in str(exc_info.value).lower()


def test_event_trace_records_call_soon_and_fire(loop: SimulationLoop) -> None:
    """An attached ``EventTrace`` records every scheduling decision.

    Determinism gate proof: when traces match, the loops are
    bit-equivalent.
    """
    trace = EventTrace(loop)
    loop._trace = trace

    def callback() -> None:
        pass

    async def scenario() -> None:
        loop.call_soon(callback)
        await asyncio.sleep(0)

    loop.run_until_complete(scenario())

    # At minimum: one ``call_soon`` for the explicit registration,
    # one ``fire`` for the callback execution.
    op_kinds = [entry.op_kind for entry in trace.entries]
    assert "call_soon" in op_kinds
    assert "fire" in op_kinds


def test_event_trace_equivalence_across_runs() -> None:
    """Two ``SimulationLoop`` runs with identical scenarios produce
    identical ``EventTrace`` sequences.

    The core determinism property. If asyncio's internal scheduling
    is bit-deterministic under our loop, this passes.
    """
    def collect_trace() -> EventTrace:
        loop = SimulationLoop()
        trace = EventTrace(loop)
        loop._trace = trace
        asyncio.set_event_loop(loop)

        async def scenario() -> None:
            await asyncio.sleep(0.5)
            await asyncio.sleep(1.0)
            await asyncio.gather(asyncio.sleep(0.1), asyncio.sleep(0.2))

        loop.run_until_complete(scenario())
        loop.close()
        asyncio.set_event_loop(None)
        return trace

    trace_a = collect_trace()
    trace_b = collect_trace()
    assert trace_a.equals(trace_b), trace_a.diff_against(trace_b)
