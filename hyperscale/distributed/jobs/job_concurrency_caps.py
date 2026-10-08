"""
D-65 concurrency caps: per datacenter and per job class.

A policy over the unfinished jobs a datacenter's leader manager counts
(``JobAdmissionRecord``): given them and a job being admitted, it answers
whether the job fits or the ``JobAdmissionRefusal`` to send back. It holds
only its configuration; the records are its caller's.

Caps, in the order they are checked:

* A job class named in ``JOB_CLASS_CONCURRENCY_CAPS``: at most that many
  unfinished jobs of the class.
* The datacenter: at most ``JOB_CONCURRENCY_CAP_PER_DC`` unfinished jobs
  when set; unset, the work of the unfinished jobs plus the new job's must
  fit the registered cores over the new job's timeout (the derivation is
  on the Env fields). A job class with no configured cap is held by this
  rule alone: the same rule over the class's own jobs never binds first.
  The rule bounds contention between jobs, so it never refuses a job on a
  datacenter with no unfinished jobs: a job whose own work alone exceeds
  its timeout's capacity would otherwise be refused forever, though it
  runs exactly as it would uncapped and meets its own deadline.

D-63 capacity reservation. ``JOB_CLASS_RESERVED_CORES`` holds cores back
for job classes -- bursty tests that must start when they arrive. Under
the work rule, a reserved class's work fills its reserve (its cores over
the new job's timeout) first and only what overflows it counts against
the shared cores; every other class has the shared cores alone -- the
registered cores less every reserve. A reserve is the configured cores
while the reserves fit the registered cores, else each reserve's share
of them: the reserved fraction of a datacenter is derived from its live
registered cores, never fixed. With no reservations the rule is the
plain work rule. Reservations are cores, so they need the work rule:
a configured ``JOB_CONCURRENCY_CAP_PER_DC`` counts jobs and is refused
beside them.

Retry hints. A count cap frees a slot when one of the jobs it counts ends,
so the hint is the soonest any of them is expected to end. The work rule
is over by ``excess`` shared core-seconds, of which only the unfinished
jobs' share can retire before the job is admitted: the hint is
``min(excess, unfinished shared work)`` over the cores that retire it --
the shared cores and the job class's own reserve -- fully busy. Neither
hint is shorter than
``OVERLOAD_SAMPLE_INTERVAL_SECONDS``, the hint every other refusal of a
submission for load carries and the client's base back-off.
"""

from hyperscale.distributed.env import Env

from .models import JobAdmissionRecord
from .models import JobAdmissionRefusal
from .job_shape import JOB_CLASS_SEPARATOR, job_class_name

CONCURRENCY_CAP_CONTROL = "concurrency_cap"
JOB_CLASS_CAP_SEPARATOR = ","
JOB_CLASS_CAP_ASSIGNMENT = "="


def parse_job_class_caps(configured: str, setting_name: str = "JOB_CLASS_CONCURRENCY_CAPS") -> dict[str, int]:
    """Parse a per-class setting -- "<class>=<count>" entries, comma-separated
    (``JOB_CLASS_CONCURRENCY_CAPS``, ``JOB_CLASS_RESERVED_CORES``) -- into
    counts by normalized job class.

    Raises:
        ValueError: an entry has no "=" or its count is not an integer.
    """
    return dict(
        _parse_job_class_cap(entry, setting_name)
        for entry in configured.split(JOB_CLASS_CAP_SEPARATOR)
        if entry.strip()
    )


def _parse_job_class_cap(entry: str, setting_name: str) -> tuple[str, int]:
    """One "<class>=<count>" entry: its job class, normalized as
    ``job_class_name`` names it, and its count."""
    job_class, assignment, count = entry.partition(JOB_CLASS_CAP_ASSIGNMENT)
    if not assignment:
        raise ValueError(f"{setting_name} entry {entry!r} is not <class>=<count>")
    workflow_names = [name.strip() for name in job_class.split(JOB_CLASS_SEPARATOR)]
    return job_class_name(workflow_names), int(count)


class JobConcurrencyCaps:
    """The D-65 caps a datacenter's leader applies to each job it admits."""

    __slots__ = (
        "_datacenter",
        "_datacenter_job_cap",
        "_job_class_caps",
        "_minimum_retry_after_seconds",
        "_reserved_cores",
    )

    def __init__(self, env: Env, datacenter: str) -> None:
        """
        Raises:
            ValueError: a per-class setting is malformed, or cores are
                reserved beside a datacenter count cap.
        """
        self._datacenter = datacenter
        self._datacenter_job_cap = env.JOB_CONCURRENCY_CAP_PER_DC
        self._job_class_caps = parse_job_class_caps(env.JOB_CLASS_CONCURRENCY_CAPS)
        self._reserved_cores = self._parse_reserved_cores(env.JOB_CLASS_RESERVED_CORES)
        self._minimum_retry_after_seconds = env.OVERLOAD_SAMPLE_INTERVAL_SECONDS
        if self._reserved_cores and self._datacenter_job_cap is not None:
            raise ValueError(
                "JOB_CLASS_RESERVED_CORES reserves cores under the derived datacenter cap; "
                "JOB_CONCURRENCY_CAP_PER_DC counts jobs instead -- unset it to reserve cores"
            )

    @staticmethod
    def _parse_reserved_cores(configured: str) -> dict[str, int]:
        """``JOB_CLASS_RESERVED_CORES`` by normalized job class.

        Raises:
            ValueError: an entry is malformed or reserves no cores.
        """
        reserved_cores = parse_job_class_caps(configured, "JOB_CLASS_RESERVED_CORES")
        if min(reserved_cores.values(), default=1) <= 0:
            raise ValueError(f"JOB_CLASS_RESERVED_CORES must reserve at least one core per class: {configured!r}")
        return reserved_cores

    def refusal(
        self,
        candidate: JobAdmissionRecord,
        unfinished: list[JobAdmissionRecord],
        registered_cores: int,
        timeout_seconds: float,
        now: float,
    ) -> JobAdmissionRefusal | None:
        """The refusal for admitting ``candidate`` beside the ``unfinished``
        jobs, or None when every cap has room for it."""
        return self._job_class_cap_refusal(candidate, unfinished, now) or self._datacenter_cap_refusal(
            candidate, unfinished, registered_cores, timeout_seconds, now
        )

    def _job_class_cap_refusal(
        self,
        candidate: JobAdmissionRecord,
        unfinished: list[JobAdmissionRecord],
        now: float,
    ) -> JobAdmissionRefusal | None:
        """Refuse a job whose class has a configured cap its unfinished jobs fill."""
        if (job_class_cap := self._job_class_caps.get(candidate.job_class)) is None:
            return None
        return self._count_cap_refusal(
            self._records_of_class(unfinished, candidate.job_class),
            job_class_cap,
            f"job class {candidate.job_class}",
            now,
        )

    @staticmethod
    def _records_of_class(records: list[JobAdmissionRecord], job_class: str) -> list[JobAdmissionRecord]:
        """The records of jobs of ``job_class``."""
        return [record for record in records if record.job_class == job_class]

    def _datacenter_cap_refusal(
        self,
        candidate: JobAdmissionRecord,
        unfinished: list[JobAdmissionRecord],
        registered_cores: int,
        timeout_seconds: float,
        now: float,
    ) -> JobAdmissionRefusal | None:
        """Refuse a job the datacenter has no room for: by its configured
        count cap, else by the work its cores can do in the job's timeout."""
        if self._datacenter_job_cap is not None:
            return self._count_cap_refusal(unfinished, self._datacenter_job_cap, f"datacenter {self._datacenter}", now)
        return self._work_cap_refusal(candidate, unfinished, registered_cores, timeout_seconds)

    def _count_cap_refusal(
        self,
        counted: list[JobAdmissionRecord],
        cap: int,
        scope: str,
        now: float,
    ) -> JobAdmissionRefusal | None:
        """Refuse when ``counted`` already holds ``cap`` jobs; retry once the
        soonest of them is expected to end."""
        if len(counted) < cap:
            return None
        soonest_end_seconds = min((record.expected_end_at - now for record in counted), default=0.0)
        return JobAdmissionRefusal(
            control=CONCURRENCY_CAP_CONTROL,
            reason=f"{scope} has no room for another job: its concurrency cap of {cap} unfinished jobs is reached",
            retry_after_seconds=max(self._minimum_retry_after_seconds, soonest_end_seconds),
        )

    def _work_cap_refusal(
        self,
        candidate: JobAdmissionRecord,
        unfinished: list[JobAdmissionRecord],
        registered_cores: int,
        timeout_seconds: float,
    ) -> JobAdmissionRefusal | None:
        """Refuse a job whose work, after the unfinished jobs', its
        datacenter's shared cores -- the registered cores less the D-63
        reserves -- cannot do within its timeout. A reserved class's work
        fills its own reserve first.

        Only the unfinished jobs' share of the excess can be waited out, so
        that share decides a retryable refusal. A job whose own work alone
        exceeds the cores it may use is admitted when no other class holds a
        reserve (refusing it would refuse it forever, while it runs exactly
        as it would uncapped), and is refused for good otherwise: the other
        classes' reserves are never lent."""
        reserved_cores = self._effective_reserved_cores(registered_cores)
        shared_cores = registered_cores - sum(reserved_cores.values())
        excess_core_seconds = (
            self._shared_core_seconds([candidate, *unfinished], reserved_cores, timeout_seconds)
            - shared_cores * timeout_seconds
        )
        if excess_core_seconds <= 0.0:
            return None
        contended_core_seconds = min(
            excess_core_seconds, self._shared_core_seconds(unfinished, reserved_cores, timeout_seconds)
        )
        if contended_core_seconds > 0.0:
            # The cores that retire the work in this job's way: the shared
            # cores, and its class's own reserve.
            retiring_cores = shared_cores + reserved_cores.get(candidate.job_class, 0.0)
            return JobAdmissionRefusal(
                control=CONCURRENCY_CAP_CONTROL,
                reason=(
                    f"datacenter {self._datacenter} has no room for another job: unfinished work exceeds "
                    f"its {shared_cores:.1f} shared cores (of {registered_cores} registered) over the job's "
                    f"{timeout_seconds:.1f}s timeout by {contended_core_seconds:.1f} core-seconds"
                ),
                retry_after_seconds=max(
                    self._minimum_retry_after_seconds,
                    contended_core_seconds / max(1.0, retiring_cores),
                ),
            )
        return self._reserved_for_other_classes_refusal(candidate, reserved_cores, shared_cores, registered_cores)

    def _reserved_for_other_classes_refusal(
        self,
        candidate: JobAdmissionRecord,
        reserved_cores: dict[str, float],
        shared_cores: float,
        registered_cores: int,
    ) -> JobAdmissionRefusal | None:
        """For a job whose own work alone exceeds the cores it may use: None
        when no other class holds a reserve, else a refusal no wait can
        clear -- no retry hint, so the client fails at once and a gate
        places the job elsewhere."""
        other_reserved_cores = sum(reserved_cores.values()) - reserved_cores.get(candidate.job_class, 0.0)
        if other_reserved_cores <= 0.0:
            return None
        return JobAdmissionRefusal(
            control=CONCURRENCY_CAP_CONTROL,
            reason=(
                f"datacenter {self._datacenter} holds {other_reserved_cores:.1f} of its {registered_cores} cores "
                f"for other job classes; this job's work exceeds the {shared_cores:.1f} shared cores and its "
                f"class's reserve over its timeout"
            ),
            retry_after_seconds=0.0,
        )

    def _effective_reserved_cores(self, registered_cores: int) -> dict[str, float]:
        """Each reserved class's cores: as configured while the reserves fit
        the registered cores, else its share of them."""
        if not (configured_total := sum(self._reserved_cores.values())):
            return {}
        scale = min(1.0, registered_cores / configured_total)
        return {job_class: cores * scale for job_class, cores in self._reserved_cores.items()}

    @staticmethod
    def _shared_core_seconds(
        records: list[JobAdmissionRecord],
        reserved_cores: dict[str, float],
        timeout_seconds: float,
    ) -> float:
        """The core-seconds of ``records`` the shared cores hold: each class's
        work past its reserve over ``timeout_seconds`` (none for a class
        without one)."""
        work_by_class: dict[str, float] = {}
        for record in records:
            work_by_class[record.job_class] = work_by_class.get(record.job_class, 0.0) + record.core_seconds
        return sum(
            max(0.0, work - reserved_cores.get(job_class, 0.0) * timeout_seconds)
            for job_class, work in work_by_class.items()
        )
