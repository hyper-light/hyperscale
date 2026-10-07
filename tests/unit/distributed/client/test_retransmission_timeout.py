"""
``RetransmissionTimeout`` follows RFC 6298: before any measurement the
timeout is the minimum (section 2.1); the first round trip R sets
SRTT = R and RTTVAR = R/2 (section 2.2); each later one R' updates
RTTVAR = 3/4 RTTVAR + 1/4 |SRTT - R'| and then SRTT = 7/8 SRTT + 1/8 R'
(section 2.3); the timeout is SRTT + 4 RTTVAR, never below the minimum
(section 2.4).
"""

import random

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client.retransmission_timeout import RetransmissionTimeout

MINIMUM_SECONDS = Env().CLIENT_RETRANSMISSION_TIMEOUT_MIN_SECONDS


def rfc_6298_timeout(round_trips: list[float]) -> float:
    """The RFC's timeout after ``round_trips``, computed from the text of sections 2.1-2.4."""
    if not round_trips:
        return MINIMUM_SECONDS
    smoothed, variation = round_trips[0], round_trips[0] / 2
    for round_trip in round_trips[1:]:
        variation = 0.75 * variation + 0.25 * abs(smoothed - round_trip)
        smoothed = 0.875 * smoothed + 0.125 * round_trip
    return max(MINIMUM_SECONDS, smoothed + 4 * variation)


def test_before_any_round_trip_the_timeout_is_the_minimum() -> None:
    assert RetransmissionTimeout(MINIMUM_SECONDS).seconds == MINIMUM_SECONDS


def test_round_trips_far_below_the_minimum_leave_it_at_the_minimum() -> None:
    timeout = RetransmissionTimeout(MINIMUM_SECONDS)
    for _ in range(50):
        timeout.record_round_trip(MINIMUM_SECONDS / 100)
    assert timeout.seconds == MINIMUM_SECONDS


def test_one_slow_round_trip_sets_three_of_it() -> None:
    timeout = RetransmissionTimeout(MINIMUM_SECONDS)
    timeout.record_round_trip(MINIMUM_SECONDS)
    assert timeout.seconds == pytest.approx(3 * MINIMUM_SECONDS)


@pytest.mark.parametrize("seed", [3, 17, 101, 4242])
def test_any_sequence_of_round_trips_matches_the_rfc(seed: int) -> None:
    """Fuzzed: wide, skewed round-trip sequences (sub-millisecond to many seconds, with outliers)."""
    generator = random.Random(seed)
    round_trips = [generator.lognormvariate(-1.0, 1.5) for _ in range(generator.randint(1, 200))]
    timeout = RetransmissionTimeout(MINIMUM_SECONDS)
    for round_trip in round_trips:
        timeout.record_round_trip(round_trip)
    assert timeout.seconds == pytest.approx(rfc_6298_timeout(round_trips))
    assert timeout.seconds >= MINIMUM_SECONDS
