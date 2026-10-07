"""
D-65 / D-67 job admission control at a datacenter's leader manager.

Every job a datacenter runs is admitted by its leader manager -- whether a
gate dispatched it or a client submitted it directly (gateless) -- so this
is where the datacenter's concurrency caps (``JobConcurrencyCaps``) and the
noisy-job breaker (``JobClassCircuitBreaker``) are enforced. A refusal is
logged (``JobAdmissionRefused``) and raised as ``JobAdmissionRefusedError``
carrying the ``JobAck`` with its retry hint; the manager answers with it.

There is no bounded job queue to hold a capped job in -- the dispatcher's
pending workflows are an unbounded list of admitted work -- so a capped job
is refused, never queued: the submitter holds it and comes back after the
hint (the client waits every hint out; a gate tries another datacenter).

Counting. Each admitted job is recorded (``JobAdmissionRecord``) before the
admission decision yields, so concurrent submissions see each other. A job
this leader did not admit -- one a previous leader admitted -- is recorded
the first time an admission finds it unfinished in the job manager. A
record is released when its job completes, when its state is cleaned up,
and -- whatever happened to it -- once its job is neither unfinished in
the job manager nor still being decided: a submission that failed after
its record was made leaves nothing behind.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.env import Env
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.events.job_completed import JobCompleted
from hyperscale.distributed.ledger.events.job_failed import JobFailed
from hyperscale.distributed.models import JobAck, JobInfo, JobSubmission
from hyperscale.distributed.models.distributed import JobStatus
from hyperscale.distributed.protocol.version import CURRENT_PROTOCOL_VERSION
from hyperscale.distributed.runtime import Clock
from hyperscale.distributed.swim.core import CircuitState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    JobAdmissionRefused,
    NoisyJobClassQuarantined,
    NoisyJobClassRecovered,
)

from .models import JobAdmissionRecord
from .models import JobAdmissionRefusal
from .job_admission_refused_error import JobAdmissionRefusedError
from .job_class_circuit_breaker import JobClassCircuitBreaker
from .job_concurrency_caps import JobConcurrencyCaps
from .job_shape import job_class_name, job_core_seconds
from .job_status_order import JobStatusOrder
from .workflow_dependencies import resolve_job_deadline_seconds, workflow_name

if TYPE_CHECKING:
    from .job_manager import JobManager

# A job is expected to end after its longest chain of workflows ran for
# their durations: the deadline rule with no slack on any duration.
DURATION_MULTIPLIER = 1.0
MILLISECONDS_PER_SECOND = 1000.0
# How each committed terminal outcome decodes (``record_replicated_job_outcome``).
TERMINAL_EVENT_DECODERS: dict[JobEventType, Callable[[bytes], JobCompleted | JobFailed]] = {
    JobEventType.JOB_COMPLETED: JobCompleted.from_bytes,
    JobEventType.JOB_FAILED: JobFailed.from_bytes,
}


class JobAdmissionControl:
    """The D-65 caps and the D-67 noisy-job breaker over one datacenter's
    unfinished jobs, applied by its leader to each job it admits."""

    __slots__ = (
        "_breaker",
        "_caps",
        "_clock",
        "_datacenter",
        "_get_registered_cores",
        "_is_submission_in_progress",
        "_job_manager",
        "_logger",
        "_node_id",
        "_records",
        "_status_order",
    )

    def __init__(
        self,
        env: Env,
        job_manager: "JobManager",
        is_submission_in_progress: Callable[[str], bool],
        get_registered_cores: Callable[[], int],
        clock: Clock,
        logger: Logger,
        node_id: str,
        datacenter: str,
    ) -> None:
        """
        Args:
            env: The caps (``JOB_CONCURRENCY_CAP_PER_DC``,
                ``JOB_CLASS_CONCURRENCY_CAPS``) and the retry-hint floor.
            job_manager: The manager's jobs, read for unfinished jobs.
            is_submission_in_progress: Whether a submission of a job id is
                being decided right now -- its job may not exist yet.
            get_registered_cores: The cores of the datacenter's registered
                workers.
            clock: The manager's clock.
            logger: Where refusals and breaker transitions are logged.
        """
        self._caps = JobConcurrencyCaps(env, datacenter)
        self._breaker = JobClassCircuitBreaker()
        self._job_manager = job_manager
        self._is_submission_in_progress = is_submission_in_progress
        self._get_registered_cores = get_registered_cores
        self._clock = clock
        self._logger = logger
        self._node_id = node_id
        self._datacenter = datacenter
        self._records: dict[str, JobAdmissionRecord] = {}
        self._status_order = JobStatusOrder()

    async def admit(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
    ) -> None:
        """Admit a job, recording it against the caps.

        Raises:
            JobAdmissionRefusedError: a cap has no room for the job, or its
                class is quarantined; the error carries the refusal to send.
        """
        now = self._clock.monotonic()
        registered_cores = self._get_registered_cores()
        candidate = self._candidate_record(submission, workflows, registered_cores, now)
        refusal = self._caps.refusal(
            candidate,
            self._unfinished_records(),
            registered_cores,
            submission.timeout_seconds,
            now,
        ) or self._breaker.admission_refusal(candidate.job_class, submission.job_id)
        if refusal is None:
            self._records[submission.job_id] = candidate
            return
        await self._refuse(submission.job_id, candidate.job_class, refusal)

    async def record_job_outcome(self, job_id: str, final_status: str, refused_retries: int) -> None:
        """Record how a job ended: a job with a refused retry opens its
        class's breaker; one that completed clean closes a half-open one.
        Its record is released."""
        if (record := self._records.get(job_id) or self._record_of_known_job(job_id)) is None:
            return
        lifetime_seconds = self._clock.monotonic() - record.admitted_at
        transition = self._breaker.record_job_outcome(
            record.job_class,
            job_id,
            refused_retries,
            lifetime_seconds,
            final_status == JobStatus.COMPLETED.value,
        )
        self.release(job_id)
        await self._log_breaker_transition(transition, job_id, record.job_class, refused_retries, lifetime_seconds)

    def record_replicated_job_outcome(
        self,
        event_type: JobEventType,
        payload: bytes,
        created_hlc: HLCTimestamp,
    ) -> None:
        """Record a job outcome committed to the AD-38 ledger -- on every
        member of the job's group -- into the breaker, counted from when the
        job ended. Its lifetime runs from the job's creation to its terminal,
        both durable: a job that never started still lived that long. An
        outcome recorded before its class was (an older member's record)
        carries no class and is skipped."""
        event = TERMINAL_EVENT_DECODERS[event_type](payload)
        if not event.job_class:
            return
        self._breaker.record_job_outcome(
            event.job_class,
            event.job_id,
            event.refused_retries,
            (event.hlc.wall_ms - created_hlc.wall_ms) / MILLISECONDS_PER_SECOND,
            event_type == JobEventType.JOB_COMPLETED and event.final_status == JobStatus.COMPLETED.value,
            max(0.0, self._clock.time() - event.hlc.wall_ms / MILLISECONDS_PER_SECOND),
        )

    def job_class_of(self, job_id: str) -> str:
        """The class of a job counted here or held by the job manager; "" for neither."""
        record = self._records.get(job_id) or self._record_of_known_job(job_id)
        return record.job_class if record is not None else ""

    def release(self, job_id: str) -> None:
        """Stop counting a job, freeing its class's probe slot if it held it."""
        if (record := self._records.pop(job_id, None)) is not None:
            self._breaker.release_probe(record.job_class, job_id)

    def counted_job_ids(self) -> list[str]:
        """The job ids counted against the caps right now."""
        return list(self._records)

    def quarantined_job_classes(self) -> dict[str, str]:
        """Every job class with a breaker held, by its state's name."""
        return self._breaker.quarantined_job_classes()

    def _candidate_record(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
        registered_cores: int,
        now: float,
    ) -> JobAdmissionRecord:
        """The record a submitted job is counted by once admitted."""
        instances = [instance for _workflow_id, _dependencies, instance in workflows]
        return JobAdmissionRecord(
            job_class=job_class_name(workflow_name(instance) for instance in instances),
            core_seconds=job_core_seconds(instances, submission.vus, registered_cores),
            admitted_at=now,
            expected_end_at=now + resolve_job_deadline_seconds(workflows, DURATION_MULTIPLIER),
        )

    def _unfinished_records(self) -> list[JobAdmissionRecord]:
        """The records of every unfinished job: finished ones released, and
        unfinished ones this leader did not admit recorded first."""
        self._record_unrecorded_jobs()
        for job_id in self._finished_job_ids():
            self.release(job_id)
        return list(self._records.values())

    def _record_unrecorded_jobs(self) -> None:
        """Record every unfinished job of the job manager not yet counted --
        a job of a half-open class among them is taken as its probe."""
        unrecorded = self._unrecorded_job_records()
        for job_id, record in unrecorded.items():
            self._breaker.claim_probe_if_half_open(record.job_class, job_id)
        self._records.update(unrecorded)

    def _unrecorded_job_records(self) -> dict[str, JobAdmissionRecord]:
        """Records of every unfinished job of the job manager not yet counted."""
        return {
            job.job_id: self._known_job_record(job)
            for job in self._job_manager.iter_jobs()
            if self._is_unrecorded_unfinished(job)
        }

    def _is_unrecorded_unfinished(self, job: JobInfo) -> bool:
        """Whether a job is not counted yet and has not ended."""
        return job.job_id not in self._records and not self._status_order.is_terminal(job.status)

    def _finished_job_ids(self) -> list[str]:
        """The counted job ids whose jobs ended, or never came to be."""
        return [job_id for job_id in self._records if not self._is_unfinished(job_id)]

    def _is_unfinished(self, job_id: str) -> bool:
        """Whether a job is being decided, or exists and has not ended."""
        if self._is_submission_in_progress(job_id):
            return True
        job = self._job_manager.get_job_by_id(job_id)
        return job is not None and not self._status_order.is_terminal(job.status)

    def _record_of_known_job(self, job_id: str) -> JobAdmissionRecord | None:
        """The record of a job the job manager holds, or None."""
        if (job := self._job_manager.get_job_by_id(job_id)) is None:
            return None
        return self._known_job_record(job)

    def _known_job_record(self, job: JobInfo) -> JobAdmissionRecord:
        """Count a job from the job manager's state of it: its workflows'
        names and instances, the VUs it was submitted with, first seen now."""
        now = self._clock.monotonic()
        return JobAdmissionRecord(
            job_class=job_class_name(workflow_info.name for workflow_info in job.workflows.values()),
            core_seconds=job_core_seconds(
                self._job_workflow_instances(job),
                self._submission_vus(job),
                self._get_registered_cores(),
            ),
            admitted_at=now,
            expected_end_at=now,
        )

    @staticmethod
    def _job_workflow_instances(job: JobInfo) -> list[Workflow]:
        """The workflow instances a job's state holds."""
        return [
            workflow_info.workflow for workflow_info in job.workflows.values() if workflow_info.workflow is not None
        ]

    @staticmethod
    def _submission_vus(job: JobInfo) -> int:
        """The VUs a job was submitted with; 0 for one held without its submission."""
        return job.submission.vus if job.submission is not None else 0

    async def _refuse(self, job_id: str, job_class: str, refusal: JobAdmissionRefusal) -> None:
        """Log a refusal and raise it with the ``JobAck`` to answer with."""
        await self._logger.log(
            JobAdmissionRefused(
                message=(
                    f"Job {job_id} refused ({refusal.control}): {refusal.reason}; "
                    f"retry after {refusal.retry_after_seconds:.2f}s"
                ),
                node_id=self._node_id,
                datacenter=self._datacenter,
                job_id=job_id,
                job_class=job_class,
                control=refusal.control,
                reason=refusal.reason,
                retry_after_seconds=refusal.retry_after_seconds,
            )
        )
        raise JobAdmissionRefusedError(
            JobAck(
                job_id=job_id,
                accepted=False,
                error=refusal.reason,
                retry_after_seconds=refusal.retry_after_seconds,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump(),
            refusal.reason,
        )

    async def _log_breaker_transition(
        self,
        transition: CircuitState | None,
        job_id: str,
        job_class: str,
        refused_retries: int,
        lifetime_seconds: float,
    ) -> None:
        """Log a job class's breaker opening or closing."""
        if transition == CircuitState.OPEN:
            await self._logger.log(
                NoisyJobClassQuarantined(
                    message=(
                        f"Job class {job_class} quarantined for {lifetime_seconds:.2f}s: job {job_id} "
                        f"ended with {refused_retries} retries refused for a spent retry budget"
                    ),
                    node_id=self._node_id,
                    datacenter=self._datacenter,
                    job_id=job_id,
                    job_class=job_class,
                    refused_retries=refused_retries,
                    quarantine_seconds=lifetime_seconds,
                )
            )
        elif transition == CircuitState.CLOSED:
            await self._logger.log(
                NoisyJobClassRecovered(
                    message=f"Job class {job_class} recovered: job {job_id} completed clean while half-open",
                    node_id=self._node_id,
                    datacenter=self._datacenter,
                    job_id=job_id,
                    job_class=job_class,
                )
            )
