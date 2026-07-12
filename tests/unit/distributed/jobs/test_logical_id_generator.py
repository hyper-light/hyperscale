"""
LogicalIdGenerator — deterministic-unique logical ids.

The construction (scope + injected-Clock monotonic ns + monotone
counter) must be: deterministic given the same clock readings (SIM
replay — job ids appear in every downstream message and WAL record),
unique within an instance even at identical nanoseconds (the counter),
and unique across owners (the scope embeds identity). It must consume
NOTHING from the shared seeded Random (schedule coupling) and nothing
from wall entropy.
"""

from hyperscale.distributed.jobs.logical_id_generator import (
    LogicalIdGenerator,
)


class _FixedClock:
    """Clock stub: constant monotonic_ns — the worst case for
    uniqueness, which the counter must carry alone."""

    def __init__(self, nanoseconds: int) -> None:
        self._nanoseconds = nanoseconds

    def monotonic_ns(self) -> int:
        return self._nanoseconds


def test_same_clock_readings_reproduce_identical_ids() -> None:
    first = LogicalIdGenerator(scope="10.0.0.1-8500", clock=_FixedClock(42))
    second = LogicalIdGenerator(scope="10.0.0.1-8500", clock=_FixedClock(42))

    assert [first.generate("job") for _ in range(3)] == [
        second.generate("job") for _ in range(3)
    ]


def test_ids_are_unique_at_identical_nanoseconds() -> None:
    generator = LogicalIdGenerator(scope="10.0.0.1-8500", clock=_FixedClock(7))
    generated = [generator.generate("wf") for _ in range(100)]
    assert len(set(generated)) == 100


def test_scope_separates_owners() -> None:
    clock = _FixedClock(7)
    first_client = LogicalIdGenerator(scope="10.0.0.1-8500", clock=clock)
    second_client = LogicalIdGenerator(scope="10.0.0.2-8500", clock=clock)
    assert first_client.generate("job") != second_client.generate("job")


def test_id_shape_carries_prefix_scope_time_sequence() -> None:
    generator = LogicalIdGenerator(scope="10.0.0.1-8500", clock=_FixedClock(9))
    job_id = generator.generate("job")
    assert job_id == "job-10.0.0.1-8500-9-0"
    assert generator.generate("job") == "job-10.0.0.1-8500-9-1"
