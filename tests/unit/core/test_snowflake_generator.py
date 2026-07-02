"""
Snowflake generator hardening: total and monotone.

The realtime clock is not monotone (NTP slew steps it backwards) and
virtual-time bursts can exhaust a millisecond's sequence space. The
generators used to return ``None`` in both cases — an unhandleable
escape hatch: ``message.py`` retried around it with a blocking
``time.sleep`` on the event-loop thread (a non-terminating loop under a
frozen virtual clock), and a ``None`` shard id reaching the UDP receive
path exploded as ``TypeError`` inside replay validation, silently
killing one request and shifting the deterministic schedule — the exact
mechanism behind a once-observed, unreproducible simulation-test
failure.

These tests pin the hardened contract for all three generator variants
(core, logging, taskex) and the replay guard's malformed-id rejection.
"""

from unittest.mock import patch

import pytest

from hyperscale.core.jobs.protocols.replay_guard import ReplayError, ReplayGuard
from hyperscale.core.snowflake import Snowflake
from hyperscale.core.snowflake.constants import MAX_SEQ
from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator
from hyperscale.distributed.taskex.snowflake.snowflake_generator import (
    SnowflakeGenerator as TaskexSnowflakeGenerator,
)
from hyperscale.logging.snowflake.snowflake_generator import (
    SnowflakeGenerator as LoggingSnowflakeGenerator,
)


class _SteppingClock:
    """Deterministic ``time()`` source scripted per call (seconds)."""

    def __init__(self, readings_seconds: list[float]) -> None:
        self._readings = list(readings_seconds)
        self._last = self._readings[0]

    def __call__(self) -> float:
        if self._readings:
            self._last = self._readings.pop(0)
        return self._last

    # Clock-Protocol surface for the taskex variant.
    def time(self) -> float:
        return self()


def test_backwards_wall_step_still_generates_increasing_unique_ids():
    """A realtime regression between calls must not fail generation —
    the cursor holds and sequencing continues."""
    clock = _SteppingClock([100.0, 100.05, 100.05 - 0.003, 100.06])
    with patch(
        "hyperscale.core.snowflake.snowflake_generator.time", clock
    ):
        generator = SnowflakeGenerator(7)
        first = generator.generate()   # at 100.05
        second = generator.generate()  # wall stepped BACK 3ms
        third = generator.generate()   # wall recovered forward

    assert first < second < third
    assert len({first, second, third}) == 3


def test_same_millisecond_burst_borrows_the_next_millisecond():
    """Exhausting the 12-bit sequence within one clock millisecond must
    borrow the next logical millisecond, never fail."""
    frozen = _SteppingClock([200.0])
    with patch(
        "hyperscale.core.snowflake.snowflake_generator.time", frozen
    ):
        generator = SnowflakeGenerator(3)
        identifiers = [generator.generate() for _ in range(MAX_SEQ + 10)]

    assert all(identifier is not None for identifier in identifiers)
    assert len(set(identifiers)) == len(identifiers)
    assert identifiers == sorted(identifiers)
    # The overflow crossed into a borrowed millisecond.
    first_ms = Snowflake.parse(identifiers[0], 0).timestamp
    last_ms = Snowflake.parse(identifiers[-1], 0).timestamp
    assert last_ms > first_ms


def test_instance_bits_round_trip():
    generator = SnowflakeGenerator(513)
    parsed = Snowflake.parse(generator.generate(), 0)
    assert parsed.instance == 513


def test_logging_generator_is_total_under_backwards_step():
    clock = _SteppingClock([300.0, 300.02, 300.02 - 0.005, 300.03])
    with patch(
        "hyperscale.logging.snowflake.snowflake_generator.time", clock
    ):
        generator = LoggingSnowflakeGenerator(1)
        identifiers = [generator.generate() for _ in range(3)]

    assert all(identifier is not None for identifier in identifiers)
    assert identifiers == sorted(set(identifiers))


def test_taskex_generator_is_total_under_backwards_step():
    """The taskex variant takes the Clock seam directly — inject a
    stepping clock and exercise both the sync and async paths."""
    import asyncio

    clock = _SteppingClock([400.0, 400.01, 400.01 - 0.002, 400.02, 400.02])
    generator = TaskexSnowflakeGenerator(2, clock=clock)

    first = generator.generate_sync()
    second = generator.generate_sync()  # backwards step absorbed

    async def generate_async() -> int:
        return await generator.generate()

    third = asyncio.run(generate_async())

    assert first < second < third


def test_replay_guard_rejects_malformed_ids_as_replay_errors():
    """A non-int shard id (hostile or corrupted frame) must be REJECTED
    through the normal drop path, not crash the receive dispatch with a
    TypeError that ``except ReplayError`` can never catch."""
    guard = ReplayGuard()

    with pytest.raises(ReplayError):
        guard.validate(None, raise_on_error=True)

    is_valid, error = guard.validate("not-an-id", raise_on_error=False)
    assert is_valid is False
    assert "Malformed" in error
    assert guard.get_stats()["malformed_rejected"] == 2
