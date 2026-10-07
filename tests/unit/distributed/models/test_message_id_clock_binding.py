"""
Message ids follow the clock the runtime seam currently binds.

``generate_message_id`` mints Snowflake ids whose timestamp receivers'
replay guards judge for freshness. A generator first built while a SIM
``VirtualClock`` is swapped in must not outlive that swap: once the
defaults are restored, every id must carry REAL wall time again, or
every frame a later server sends is rejected as stale (the order-
dependent ``TimeoutError`` in the SIM transport round-trip tests).
"""

import time

from hyperscale.distributed.models.message import generate_message_id
from hyperscale.distributed.runtime import restore_defaults, snapshot_defaults, swap_defaults
from hyperscale.distributed.taskex.snowflake.snowflake import Snowflake
from tests.simulation.harness.sim import SimulationLoop, VirtualClock

# A Snowflake timestamp has millisecond resolution; REAL ids are minted
# against ``time.time``, so one read either side brackets them exactly
# (the generator's cursor never regresses, so a borrowed millisecond
# may put an id at most a burst's length ahead -- these mint one id).
MILLISECONDS_PER_SECOND = 1000


def _id_milliseconds(message_id: int) -> int:
    return Snowflake.parse(message_id).milliseconds


def _assert_minted_on_real_time() -> None:
    before_milliseconds = int(time.time() * MILLISECONDS_PER_SECOND)
    minted_milliseconds = _id_milliseconds(generate_message_id())
    after_milliseconds = int(time.time() * MILLISECONDS_PER_SECOND)
    assert before_milliseconds <= minted_milliseconds <= after_milliseconds + 1


def _mint_under_swapped_virtual_clock() -> tuple[int, float]:
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    virtual_clock = VirtualClock(loop)
    try:
        swap_defaults(clock=virtual_clock)
        return _id_milliseconds(generate_message_id()), virtual_clock.time()
    finally:
        restore_defaults(snapshot)
        loop.close()


def test_ids_minted_under_a_swap_follow_the_virtual_clock() -> None:
    minted_milliseconds, virtual_seconds = _mint_under_swapped_virtual_clock()
    assert minted_milliseconds == int(virtual_seconds * MILLISECONDS_PER_SECOND)


def test_ids_return_to_real_time_after_the_swap_is_restored() -> None:
    _assert_minted_on_real_time()
    _mint_under_swapped_virtual_clock()
    _assert_minted_on_real_time()


def test_real_axis_ids_stay_strictly_monotone_across_swaps() -> None:
    previous_message_id = generate_message_id()
    for _swap_index in range(8):
        _mint_under_swapped_virtual_clock()
        message_id = generate_message_id()
        assert message_id > previous_message_id
        previous_message_id = message_id
