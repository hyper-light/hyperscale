"""
D1/D2 per-node wall-clock skew — ``VirtualClock.set_wall_offset``.

The knob models an NTP step: real steps move ``time.time`` but never
``CLOCK_MONOTONIC``, so ONLY ``time()`` (wall reads) shifts by the
armed delta while ``monotonic()`` / ``monotonic_ns()`` / ``sleep`` /
``wait_for`` and every loop timer stay on the loop's virtual timeline.
Negative deltas (backwards steps) and mid-run re-sets (repeated jumps)
are legal — both are things NTP does. Lockstep coherence is unaffected:
the offset shifts one process's wall READS, never the loop's global
virtual time.

The invariants this knob exists to exercise (wave-2): HLC never
regresses under inter-node skew, ``LogicalIdGenerator`` ids stay
unique/monotone across a backwards wall read, deadline arithmetic
never mixes a skewed ``time()`` into ``monotonic()`` math.
"""

import asyncio
import math

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


def test_default_offset_zero_preserves_wall_monotonic_collapse(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """Un-armed, SIM's ``time()`` == ``monotonic()`` collapse holds —
    the knob changes nothing until a scenario arms it."""
    assert clock.time() == clock.monotonic() == loop.time()


def test_wall_offset_shifts_time_only(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """``time()`` returns loop time + delta; ``monotonic()`` and
    ``monotonic_ns()`` are untouched — the NTP-step contract."""
    clock.set_wall_offset(5.25)

    assert clock.time() == loop.time() + 5.25
    assert clock.monotonic() == loop.time()
    assert clock.monotonic_ns() == int(loop.time() * 1_000_000_000)


def test_negative_offset_steps_wall_backwards(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """A backwards NTP step: wall reads land BEFORE the monotonic
    timeline; monotonic never regresses."""
    clock.set_wall_offset(-3.25)

    assert clock.time() == pytest.approx(clock.monotonic() - 3.25, abs=1e-9)
    assert clock.monotonic() == loop.time()


def test_offset_is_absolute_and_resettable_mid_run(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """Mid-run re-sets are legal and ABSOLUTE (each call models one
    step to a definite skew): +10 then -4 leaves the wall 4 behind,
    not 6 ahead. Elapsed-wall deltas between jumps equal elapsed
    monotonic — the offset is constant between steps."""

    async def scenario() -> list[tuple[float, float]]:
        readings = [(clock.monotonic(), clock.time())]
        await clock.sleep(1.0)
        clock.set_wall_offset(10.0)
        readings.append((clock.monotonic(), clock.time()))
        await clock.sleep(2.0)
        readings.append((clock.monotonic(), clock.time()))
        clock.set_wall_offset(-4.0)
        readings.append((clock.monotonic(), clock.time()))
        return readings

    readings = loop.run_until_complete(scenario())

    assert readings[0] == (0.0, 0.0)
    assert readings[1] == (1.0, 11.0)
    # Between jumps the wall advances exactly with monotonic.
    assert readings[2] == (3.0, 13.0)
    # Absolute re-set: monotonic 3.0, wall 3.0 - 4.0 — a backwards jump.
    assert readings[3] == (3.0, -1.0)


def test_sleep_durations_unaffected_by_offset(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """``sleep`` measures the loop's virtual timeline: a huge negative
    wall skew changes nothing about how long a sleep takes."""

    async def scenario() -> float:
        clock.set_wall_offset(-1000.0)
        sleep_start = clock.monotonic()
        await clock.sleep(3.5)
        return clock.monotonic() - sleep_start

    slept = loop.run_until_complete(scenario())
    assert slept == pytest.approx(3.5, abs=1e-6)


def test_timers_and_wait_for_deadlines_unaffected_by_offset(
    loop: SimulationLoop, clock: VirtualClock
) -> None:
    """Timer deadlines live on the loop's virtual timeline: an offset
    armed between scheduling and firing moves nothing."""

    async def scenario() -> None:
        future = loop.create_future()  # never resolved
        clock.set_wall_offset(500.0)
        await clock.wait_for(future, timeout=2.5)

    with pytest.raises(asyncio.TimeoutError):
        loop.run_until_complete(scenario())
    # The timeout fired at monotonic 2.5 exactly; the wall reads 502.5.
    assert clock.monotonic() == pytest.approx(2.5, abs=1e-6)
    assert clock.time() == pytest.approx(502.5, abs=1e-6)


def test_wall_offset_readings_are_deterministic() -> None:
    """Twin scripted runs on fresh loops produce identical reading
    sequences — the knob is pure data over the virtual timeline."""

    def scripted_readings() -> list[tuple[float, float, int]]:
        loop_instance = SimulationLoop()
        asyncio.set_event_loop(loop_instance)
        try:
            scripted_clock = VirtualClock(loop_instance)

            async def scenario() -> list[tuple[float, float, int]]:
                readings: list[tuple[float, float, int]] = []
                for step_index, delta in enumerate((2.5, -7.0, 0.0)):
                    scripted_clock.set_wall_offset(delta)
                    await scripted_clock.sleep(0.5 + step_index)
                    readings.append(
                        (
                            scripted_clock.monotonic(),
                            scripted_clock.time(),
                            scripted_clock.monotonic_ns(),
                        )
                    )
                return readings

            return loop_instance.run_until_complete(scenario())
        finally:
            loop_instance.close()
            asyncio.set_event_loop(None)

    assert scripted_readings() == scripted_readings()


def test_wall_offset_rejects_non_finite(clock: VirtualClock) -> None:
    """NaN/inf offsets would silently poison every wall read — refused
    loudly at the knob."""
    with pytest.raises(ValueError):
        clock.set_wall_offset(math.nan)
    with pytest.raises(ValueError):
        clock.set_wall_offset(math.inf)
    with pytest.raises(ValueError):
        clock.set_wall_offset(-math.inf)
