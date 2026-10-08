"""
D-63 capacity reservation in the D-65 derived datacenter cap
(``JobConcurrencyCaps``): what a reserve admits, how it shrinks with the
registered cores, and the configuration it refuses.
"""

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.job_concurrency_caps import JobConcurrencyCaps
from hyperscale.distributed.jobs.models import JobAdmissionRecord

BURST_CLASS = "SpikeTest"
ORDINARY_CLASS = "SoakTest"
REGISTERED_CORES = 4
RESERVED_CORES = 2
TIMEOUT_SECONDS = 10.0
# Two shared cores over the timeout.
SHARED_CORE_SECONDS = (REGISTERED_CORES - RESERVED_CORES) * TIMEOUT_SECONDS


def record(job_class: str, core_seconds: float) -> JobAdmissionRecord:
    return JobAdmissionRecord(job_class=job_class, core_seconds=core_seconds, admitted_at=0.0, expected_end_at=1.0)


def reserving_caps(reservations: str = f"{BURST_CLASS}={RESERVED_CORES}") -> JobConcurrencyCaps:
    return JobConcurrencyCaps(Env(JOB_CLASS_RESERVED_CORES=reservations), "dc-test")


def refusal(caps: JobConcurrencyCaps, candidate: JobAdmissionRecord, unfinished: list[JobAdmissionRecord], cores: int):
    return caps.refusal(candidate, unfinished, cores, TIMEOUT_SECONDS, 0.0)


def test_ordinary_work_is_held_to_the_shared_cores_while_the_reserve_takes_its_class() -> None:
    caps = reserving_caps()
    shared_full = [record(ORDINARY_CLASS, SHARED_CORE_SECONDS)]

    assert refusal(caps, record(ORDINARY_CLASS, 1.0), shared_full, REGISTERED_CORES) is not None
    assert refusal(caps, record(BURST_CLASS, RESERVED_CORES * TIMEOUT_SECONDS), shared_full, REGISTERED_CORES) is None
    # Past its reserve, the class competes for the shared cores like any other.
    assert refusal(caps, record(BURST_CLASS, RESERVED_CORES * TIMEOUT_SECONDS + 1.0), shared_full, REGISTERED_CORES)


def test_without_reservations_the_rule_is_the_plain_work_rule() -> None:
    caps = JobConcurrencyCaps(Env(), "dc-test")
    capacity = REGISTERED_CORES * TIMEOUT_SECONDS

    half_full = [record(BURST_CLASS, capacity / 2)]

    assert refusal(caps, record(ORDINARY_CLASS, capacity / 2), half_full, REGISTERED_CORES) is None
    assert refusal(caps, record(ORDINARY_CLASS, 1.0), [record(BURST_CLASS, capacity)], REGISTERED_CORES) is not None


def test_reserves_larger_than_the_registered_cores_shrink_to_their_share() -> None:
    # Two classes reserving four cores each of four registered: two each,
    # and nothing shared.
    caps = reserving_caps(f"{BURST_CLASS}={REGISTERED_CORES},{ORDINARY_CLASS}={REGISTERED_CORES}")
    share_core_seconds = REGISTERED_CORES / 2 * TIMEOUT_SECONDS

    other_share_full = [record(ORDINARY_CLASS, share_core_seconds)]

    assert refusal(caps, record(BURST_CLASS, share_core_seconds), other_share_full, REGISTERED_CORES) is None
    assert refusal(caps, record(BURST_CLASS, share_core_seconds + 1.0), [], REGISTERED_CORES) is not None
    assert refusal(caps, record("Unreserved", 1.0), [], REGISTERED_CORES) is not None


def test_a_refused_burst_job_is_hinted_by_the_cores_that_retire_its_way() -> None:
    caps = reserving_caps()
    overflow_core_seconds = 4.0
    refused = refusal(
        caps,
        record(BURST_CLASS, RESERVED_CORES * TIMEOUT_SECONDS + overflow_core_seconds),
        [record(ORDINARY_CLASS, SHARED_CORE_SECONDS)],
        REGISTERED_CORES,
    )

    # The overflow, retired by the shared cores and the class's reserve.
    assert refused.retry_after_seconds == max(
        Env().OVERLOAD_SAMPLE_INTERVAL_SECONDS, overflow_core_seconds / REGISTERED_CORES
    )


def test_reserving_cores_beside_a_datacenter_count_cap_is_refused() -> None:
    with pytest.raises(ValueError, match="JOB_CONCURRENCY_CAP_PER_DC"):
        JobConcurrencyCaps(
            Env(JOB_CLASS_RESERVED_CORES=f"{BURST_CLASS}={RESERVED_CORES}", JOB_CONCURRENCY_CAP_PER_DC=2),
            "dc-test",
        )


def test_a_malformed_reservation_names_its_setting() -> None:
    with pytest.raises(ValueError, match="JOB_CLASS_RESERVED_CORES"):
        reserving_caps(BURST_CLASS)


@pytest.mark.parametrize("reserved", ["0", "-2"])
def test_a_reservation_of_no_cores_is_refused(reserved: str) -> None:
    with pytest.raises(ValueError, match="at least one core"):
        reserving_caps(f"{BURST_CLASS}={reserved}")


def test_a_job_larger_than_the_datacenter_is_admitted_when_nothing_else_is_unfinished() -> None:
    # Waiting cannot make room for work the whole datacenter cannot do in
    # the job's timeout: refusing it would refuse it forever, while admitted
    # it runs exactly as it would uncapped and meets its own deadline.
    caps = JobConcurrencyCaps(Env(), "dc-test")
    oversize = record(ORDINARY_CLASS, REGISTERED_CORES * TIMEOUT_SECONDS * 3)

    assert refusal(caps, oversize, [], REGISTERED_CORES) is None


def test_an_oversize_job_beside_unfinished_work_waits_only_for_that_work() -> None:
    caps = JobConcurrencyCaps(Env(), "dc-test")
    capacity = REGISTERED_CORES * TIMEOUT_SECONDS
    unfinished_core_seconds = 12.0
    refused = refusal(
        caps, record(ORDINARY_CLASS, capacity * 3), [record(BURST_CLASS, unfinished_core_seconds)], REGISTERED_CORES
    )

    assert refused is not None
    # The unfinished share of the excess, retired by every core: the
    # candidate's own impossible share is never waited for.
    assert refused.retry_after_seconds == max(
        Env().OVERLOAD_SAMPLE_INTERVAL_SECONDS, unfinished_core_seconds / REGISTERED_CORES
    )


def test_work_only_another_class_reserve_could_hold_is_refused_for_good() -> None:
    caps = reserving_caps(f"{BURST_CLASS}={REGISTERED_CORES}")

    refused = refusal(caps, record(ORDINARY_CLASS, 1.0), [], REGISTERED_CORES)

    assert refused is not None
    assert refused.retry_after_seconds == 0.0
