"""
AD-39 hybrid logical clock invariants.

* Monotonic: every timestamp a node issues exceeds the previous one.
* Causal: a receive is timestamped above both the local clock and the
  received timestamp, so causally ordered events have ordered timestamps
  across nodes -- checked over seeded randomized message exchanges
  between nodes whose physical clocks are skewed within the bound.
* Bounded: a timestamp's physical component never exceeds the issuing
  node's physical time by more than the maximum offset (plus counter
  carry), and a remote timestamp beyond the bound is refused without
  changing the clock.
* Recovery: witnessing persisted history never lets a node reissue a
  timestamp at or below it, even after its physical clock went back.
"""

import random

import pytest

from hyperscale.distributed.hlc import (
    ClockOffsetExceededError,
    HLCTimestamp,
    HybridLogicalClock,
    hlc_node_id,
)
from hyperscale.distributed.hlc.hlc_timestamp import MAX_LOGICAL

MAX_OFFSET_MS = 500
EPOCH_MS = 1_790_000_000_000
FUZZ_SEEDS = range(20)
FUZZ_EVENTS = 2_000
FUZZ_NODES = 4


class SettableClock:
    def __init__(self, unix_ms: int) -> None:
        self.unix_ms = unix_ms

    def time(self) -> float:
        return self.unix_ms / 1000.0

    def monotonic(self) -> float:
        return self.unix_ms / 1000.0


def make_clock(node_id: int = 1, unix_ms: int = EPOCH_MS) -> tuple[HybridLogicalClock, SettableClock]:
    physical = SettableClock(unix_ms)
    return HybridLogicalClock(node_id, physical, MAX_OFFSET_MS), physical


def test_issued_timestamps_strictly_increase_even_when_physical_time_stalls_or_regresses() -> None:
    clock, physical = make_clock()
    issued = [clock.now()]
    for step in (0, 0, 5, -100, 0, 1):
        physical.unix_ms += step
        issued.append(clock.now())
    assert all(earlier < later for earlier, later in zip(issued, issued[1:]))


def test_receive_orders_after_both_local_and_remote() -> None:
    clock, _physical = make_clock(node_id=1)
    local_before = clock.now()
    remote = HLCTimestamp(EPOCH_MS + MAX_OFFSET_MS, 7, 2)

    received = clock.receive(remote)

    assert received > local_before and received > remote


@pytest.mark.parametrize("seed", FUZZ_SEEDS)
def test_causality_and_bounded_offset_over_random_exchanges(seed: int) -> None:
    randomness = random.Random(seed)
    nodes = [make_clock(node_id=index + 1) for index in range(FUZZ_NODES)]
    # Physical clocks skewed within half the bound of each other.
    for _, physical in nodes:
        physical.unix_ms = EPOCH_MS + randomness.randrange(MAX_OFFSET_MS // 2)
    last_issued = [clock.current for clock, _ in nodes]
    in_flight: list[tuple[int, HLCTimestamp]] = []

    for _ in range(FUZZ_EVENTS):
        for _, physical in nodes:
            physical.unix_ms += randomness.randrange(3)
        sender = randomness.randrange(FUZZ_NODES)
        clock, physical = nodes[sender]
        if in_flight and randomness.random() < 0.5:
            receiver, message = in_flight.pop(randomness.randrange(len(in_flight)))
            clock, physical = nodes[receiver]
            stamped = clock.receive(message)
            assert stamped > message
            sender = receiver
        else:
            stamped = clock.now()
            in_flight.append((randomness.randrange(FUZZ_NODES), stamped))
        assert stamped > last_issued[sender]
        last_issued[sender] = stamped
        assert stamped.wall_ms - physical.unix_ms <= MAX_OFFSET_MS


def test_remote_beyond_the_offset_bound_is_refused_and_changes_nothing() -> None:
    clock, physical = make_clock()
    before = clock.now()
    too_far_ahead = HLCTimestamp(physical.unix_ms + MAX_OFFSET_MS + 1, 0, 2)

    with pytest.raises(ClockOffsetExceededError):
        clock.receive(too_far_ahead)
    with pytest.raises(ClockOffsetExceededError):
        clock.check(too_far_ahead)

    assert clock.current == before


def test_remote_exactly_at_the_bound_is_accepted() -> None:
    clock, physical = make_clock()
    at_bound = HLCTimestamp(physical.unix_ms + MAX_OFFSET_MS, 0, 2)
    assert clock.receive(at_bound) > at_bound


def test_witnessed_history_is_never_reissued_after_physical_regression() -> None:
    clock, physical = make_clock()
    persisted = HLCTimestamp(EPOCH_MS + 60_000, 3, 1)
    physical.unix_ms = EPOCH_MS  # restarted with the clock a minute behind

    clock.witness(persisted)

    assert clock.now() > persisted


def test_counter_overflow_carries_into_the_physical_component() -> None:
    clock, _physical = make_clock()
    first = clock.now()
    issued = [clock.now() for _ in range(MAX_LOGICAL + 1)]
    assert issued[-1].wall_ms == first.wall_ms + 1
    assert all(earlier < later for earlier, later in zip(issued, issued[1:]))


def test_bytes_round_trip_preserves_value_and_order() -> None:
    randomness = random.Random(7)
    stamps = sorted(
        HLCTimestamp(randomness.randrange(1 << 48), randomness.randrange(1 << 16), randomness.randrange(1 << 64))
        for _ in range(500)
    )
    decoded = [HLCTimestamp.from_bytes(stamp.to_bytes()) for stamp in stamps]
    assert decoded == stamps


def test_node_id_is_stable_and_distinguishes_identities() -> None:
    assert hlc_node_id("manager:dc-1:10.0.0.1:9000") == hlc_node_id("manager:dc-1:10.0.0.1:9000")
    assert hlc_node_id("manager:dc-1:10.0.0.1:9000") != hlc_node_id("manager:dc-1:10.0.0.1:9001")


def test_invalid_configuration_is_refused() -> None:
    with pytest.raises(ValueError):
        HybridLogicalClock(-1, SettableClock(EPOCH_MS), MAX_OFFSET_MS)
    with pytest.raises(ValueError):
        HybridLogicalClock(1, SettableClock(EPOCH_MS), 0)
