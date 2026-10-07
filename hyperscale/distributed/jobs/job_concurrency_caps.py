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

Retry hints. A count cap frees a slot when one of the jobs it counts ends,
so the hint is the soonest any of them is expected to end. The work rule
is over by ``excess`` core-seconds, of which only the unfinished jobs'
share can retire before the job is admitted: the hint is
``min(excess, unfinished work) / C`` seconds, the cores fully busy. Neither hint is shorter than
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


def parse_job_class_caps(configured: str) -> dict[str, int]:
    """Parse ``JOB_CLASS_CONCURRENCY_CAPS`` -- "<class>=<count>" entries,
    comma-separated -- into caps by normalized job class.

    Raises:
        ValueError: an entry has no "=" or its count is not an integer.
    """
    return dict(
        _parse_job_class_cap(entry)
        for entry in configured.split(JOB_CLASS_CAP_SEPARATOR)
        if entry.strip()
    )


def _parse_job_class_cap(entry: str) -> tuple[str, int]:
    """One "<class>=<count>" entry: its job class, normalized as
    ``job_class_name`` names it, and its count."""
    job_class, assignment, count = entry.partition(JOB_CLASS_CAP_ASSIGNMENT)
    if not assignment:
        raise ValueError(f"JOB_CLASS_CONCURRENCY_CAPS entry {entry!r} is not <class>=<count>")
    workflow_names = [name.strip() for name in job_class.split(JOB_CLASS_SEPARATOR)]
    return job_class_name(workflow_names), int(count)


class JobConcurrencyCaps:
    """The D-65 caps a datacenter's leader applies to each job it admits."""

    __slots__ = ("_datacenter", "_datacenter_job_cap", "_job_class_caps", "_minimum_retry_after_seconds")

    def __init__(self, env: Env, datacenter: str) -> None:
        self._datacenter = datacenter
        self._datacenter_job_cap = env.JOB_CONCURRENCY_CAP_PER_DC
        self._job_class_caps = parse_job_class_caps(env.JOB_CLASS_CONCURRENCY_CAPS)
        self._minimum_retry_after_seconds = env.OVERLOAD_SAMPLE_INTERVAL_SECONDS

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
        datacenter's registered cores cannot do within its timeout -- never
        on a datacenter with no unfinished jobs, where waiting cannot help."""
        unfinished_core_seconds = sum(record.core_seconds for record in unfinished)
        admitted_core_seconds = candidate.core_seconds + unfinished_core_seconds
        capacity_core_seconds = registered_cores * timeout_seconds
        # Only the unfinished jobs' share of the excess is contention: with
        # none unfinished, or the work fitting, there is nothing to wait out.
        contended_core_seconds = min(admitted_core_seconds - capacity_core_seconds, unfinished_core_seconds)
        if contended_core_seconds <= 0.0:
            return None
        return JobAdmissionRefusal(
            control=CONCURRENCY_CAP_CONTROL,
            reason=(
                f"datacenter {self._datacenter} has no room for another job: unfinished work of "
                f"{admitted_core_seconds:.1f} core-seconds with this job's exceeds its {registered_cores} "
                f"cores over the job's {timeout_seconds:.1f}s timeout"
            ),
            retry_after_seconds=max(
                self._minimum_retry_after_seconds,
                contended_core_seconds / max(1, registered_cores),
            ),
        )
