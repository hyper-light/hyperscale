"""AD-23/AD-37: a manager's stats buffer leaves REJECT once pressure drops.

At base commit 2e6d0532, ``StatsBuffer.record`` returned at REJECT
(``reliability/stats_buffer.py:83-86``) before ``_maybe_promote_tiers``
(``:95``), the only code that ages HOT entries out. The level is the HOT
fill (``:122``), so once 95% full it never fell and every progress ack said
REJECT for the life of the process.

AD-23 gives HOT a 0-60 s window and AD-37's worker state diagram returns to
NO_BACKPRESSURE when the level falls below THROTTLE. The hysteresis is that
window: the level holds while the entries that filled HOT are younger than
``hot_max_age_seconds``, then steps down level by level as they age out.
"""

import pytest

from hyperscale.distributed.reliability import stats_buffer as stats_buffer_module
from hyperscale.distributed.reliability.backpressure_level import BackpressureLevel
from hyperscale.distributed.reliability.stats_buffer import StatsBuffer
from hyperscale.distributed.reliability.stats_buffer_config import StatsBufferConfig

HOT_CAPACITY = 100


class SteppedClock:
    """A monotonic clock the test advances by hand."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def stepped_clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    clock = SteppedClock()
    monkeypatch.setattr(stats_buffer_module, "_DEFAULT_CLOCK", clock)
    return clock


def record_at(stats_buffer: StatsBuffer, clock: SteppedClock, offset_seconds: float, count: int) -> None:
    """Record ``count`` values at ``offset_seconds`` past the clock's start."""
    clock.now = 1000.0 + offset_seconds
    for _ in range(count):
        assert stats_buffer.record(1.0)


def filled_to_reject(clock: SteppedClock) -> StatsBuffer:
    """A buffer at REJECT whose HOT entries were written at +0 s, +20 s and +40 s."""
    stats_buffer = StatsBuffer(StatsBufferConfig(hot_max_entries=HOT_CAPACITY))
    record_at(stats_buffer, clock, 0.0, 10)
    record_at(stats_buffer, clock, 20.0, 10)
    record_at(stats_buffer, clock, 40.0, 75)
    assert stats_buffer.get_backpressure_level() == BackpressureLevel.REJECT
    return stats_buffer


def test_reject_holds_while_hot_entries_are_young(stepped_clock: SteppedClock) -> None:
    stats_buffer = filled_to_reject(stepped_clock)

    stepped_clock.now = 1000.0 + 59.0

    assert stats_buffer.record(1.0) is False
    assert stats_buffer.get_backpressure_level() == BackpressureLevel.REJECT


def test_reject_steps_down_to_none_as_hot_entries_age_out(stepped_clock: SteppedClock) -> None:
    stats_buffer = filled_to_reject(stepped_clock)
    observed_levels: list[BackpressureLevel] = []

    for offset_seconds in (65.0, 85.0, 105.0):
        stepped_clock.now = 1000.0 + offset_seconds
        observed_levels.append(stats_buffer.get_backpressure_signal().level)

    assert observed_levels == [BackpressureLevel.BATCH, BackpressureLevel.THROTTLE, BackpressureLevel.NONE]


def test_record_is_accepted_again_once_pressure_drops(stepped_clock: SteppedClock) -> None:
    stats_buffer = filled_to_reject(stepped_clock)
    stepped_clock.now = 1000.0 + 105.0

    assert stats_buffer.record(1.0) is True
    assert stats_buffer.get_backpressure_level() == BackpressureLevel.NONE
    # The aged entries moved to WARM rather than being lost.
    assert [warm_entry.count for warm_entry in stats_buffer.get_warm_stats()] == [95]
