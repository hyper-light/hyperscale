"""
D-67: a circuit breaker per job class, for noisy jobs.

*Noisy* is a job that ended with at least one workflow retry refused for a
spent AD-44 retry budget. A workflow's retry is refused once it failed to
run ``retry_budget_per_workflow`` + 1 times (every worker refused or lost
it), or its job spent ``retry_budget`` re-dispatches: the job turned into
a retry storm up to the cap AD-44 allows a single job. The breaker keeps a
storm from following on the next submission of the same test.

Thresholds and windows come from the signal itself:

* Opens on ONE noisy job (``max_errors`` 1). The usual "N errors in a
  window" evidence is already inside it: the refused retry comes after its
  workflow's whole per-workflow budget of consecutive failures. Waiting for
  a second noisy job would let it burn a second job budget first.
* Stays open for the noisy job's own lifetime (``half_open_after``) -- the
  time the class took to burn one budget. A quarantine as long as the
  storm, then a single probe job, at least halves the class's storm rate
  however it behaves, and a probe that is noisy again re-opens it for its
  own lifetime.
* Its evidence lasts the quarantine plus one probe's run (``window_seconds``
  = twice the lifetime). A class half-open with no probe once its evidence
  aged out of that window has nothing holding it: the breaker forgets it,
  so a class never resubmitted leaves nothing behind.

A class is isolated alone: its OPEN breaker refuses only its own jobs, and
a HALF_OPEN one admits one probe job of the class and refuses the rest of
the class until the probe ends. A probe ending clean (completed, no
refused retry) closes the breaker; one ending otherwise without being
noisy (cancelled, timed out) frees the probe slot for the next submission.

The state machine is the cluster's per-peer breaker, ``ErrorStats``.

Every manager of the datacenter runs the breaker from the same facts: the
job leader records the outcome when the job completes, and every member of
the job's group -- the leader included -- again when the job's terminal
commits to the AD-38 ledger, carrying the class and refused retries. An
outcome is applied once per job (a breaker opened by a job is not opened
by it again), and one that ended a while ago -- a member catching up --
counts from when it ended: its quarantine is what remains of it, and
evidence already aged out opens nothing. A follower that becomes the
leader therefore refuses a quarantined class exactly as its predecessor
would have. A job of a half-open class found running by a new leader is
taken as the class's probe.
"""

from hyperscale.distributed.swim.core import CircuitState, ErrorStats

from .models import JobAdmissionRefusal

NOISY_JOB_BREAKER_CONTROL = "noisy_job_breaker"
# The breaker's evidence lasts its quarantine and one probe's run: two
# lifetimes of the job that opened it (module docstring).
EVIDENCE_LIFETIMES = 2


class JobClassCircuitBreaker:
    """Per job class breakers and their half-open probe jobs."""

    __slots__ = ("_circuits", "_opened_by_job_ids", "_probe_job_ids")

    def __init__(self) -> None:
        self._circuits: dict[str, ErrorStats] = {}
        self._opened_by_job_ids: dict[str, str] = {}
        self._probe_job_ids: dict[str, str] = {}

    def admission_refusal(self, job_class: str, job_id: str) -> JobAdmissionRefusal | None:
        """Refuse a job of a quarantined class, or admit it -- as the class's
        probe when its breaker is half-open. None admits it."""
        self._forget_aged_out_quarantines()
        if (circuit := self._circuits.get(job_class)) is None:
            return None
        if circuit.circuit_state == CircuitState.OPEN:
            return JobAdmissionRefusal(
                control=NOISY_JOB_BREAKER_CONTROL,
                reason=f"job class {job_class} is quarantined: a job of it exhausted its retry budget",
                retry_after_seconds=circuit.seconds_until_half_open,
            )
        return self._probe_refusal(job_class, job_id, circuit)

    def _probe_refusal(self, job_class: str, job_id: str, circuit: ErrorStats) -> JobAdmissionRefusal | None:
        """Admit ``job_id`` as the half-open class's probe, or refuse it while
        another probe runs -- until that probe's expected lifetime passes."""
        if (probe_job_id := self._probe_job_ids.setdefault(job_class, job_id)) == job_id:
            return None
        return JobAdmissionRefusal(
            control=NOISY_JOB_BREAKER_CONTROL,
            reason=f"job class {job_class} is quarantined: probe job {probe_job_id} is testing its recovery",
            retry_after_seconds=circuit.half_open_after,
        )

    def record_job_outcome(
        self,
        job_class: str,
        job_id: str,
        refused_retries: int,
        lifetime_seconds: float,
        completed: bool,
        ended_seconds_ago: float = 0.0,
    ) -> CircuitState | None:
        """Record how a job of ``job_class`` ended, ``ended_seconds_ago``.
        Returns the breaker's new state when the outcome opened it (OPEN) or
        closed it (CLOSED), else None."""
        self.release_probe(job_class, job_id)
        if refused_retries > 0:
            return self._open(job_class, job_id, lifetime_seconds, ended_seconds_ago)
        return self._close_on_clean_completion(job_class, completed)

    def claim_probe_if_half_open(self, job_class: str, job_id: str) -> None:
        """Take a running job of a half-open class as its probe, when the
        class has none: a new leader finds its predecessor's probe this way."""
        if (circuit := self._circuits.get(job_class)) is not None and circuit.circuit_state == CircuitState.HALF_OPEN:
            self._probe_job_ids.setdefault(job_class, job_id)

    def release_probe(self, job_class: str, job_id: str) -> None:
        """Free the class's probe slot when ``job_id`` holds it."""
        if self._probe_job_ids.get(job_class) == job_id:
            del self._probe_job_ids[job_class]

    def _open(
        self,
        job_class: str,
        job_id: str,
        lifetime_seconds: float,
        ended_seconds_ago: float,
    ) -> CircuitState | None:
        """Open (or re-open) the class's breaker for what remains of the
        noisy job's lifetime since it ended; None when this job already
        opened it, or its evidence has aged out."""
        evidence_seconds = EVIDENCE_LIFETIMES * lifetime_seconds - ended_seconds_ago
        if self._opened_by_job_ids.get(job_class) == job_id or evidence_seconds <= 0.0:
            return None
        circuit = self._circuits.setdefault(
            job_class,
            ErrorStats(max_errors=1, error_rate_threshold=0.0),
        )
        circuit.half_open_after = max(0.0, lifetime_seconds - ended_seconds_ago)
        circuit.window_seconds = evidence_seconds
        circuit.record_error()
        self._opened_by_job_ids[job_class] = job_id
        return CircuitState.OPEN

    def _close_on_clean_completion(self, job_class: str, completed: bool) -> CircuitState | None:
        """Close a half-open class's breaker on a job of it that completed."""
        if not completed or (circuit := self._circuits.get(job_class)) is None:
            return None
        circuit.record_success()
        return self._forget_closed(job_class, circuit)

    def _forget_closed(self, job_class: str, circuit: ErrorStats) -> CircuitState | None:
        """Drop a class whose breaker closed; CLOSED when it did."""
        if circuit.circuit_state != CircuitState.CLOSED:
            return None
        del self._circuits[job_class]
        self._opened_by_job_ids.pop(job_class, None)
        return CircuitState.CLOSED

    def _forget_aged_out_quarantines(self) -> None:
        """Drop every half-open class with no probe whose evidence aged out."""
        self._circuits = {
            job_class: circuit
            for job_class, circuit in self._circuits.items()
            if not self._is_aged_out(job_class, circuit)
        }
        self._forget_openers_of_dropped_classes()

    def _forget_openers_of_dropped_classes(self) -> None:
        """Drop the opening job of every class whose breaker was dropped."""
        self._opened_by_job_ids = {
            job_class: job_id for job_class, job_id in self._opened_by_job_ids.items() if job_class in self._circuits
        }

    def _is_aged_out(self, job_class: str, circuit: ErrorStats) -> bool:
        """Whether a class's breaker is half-open, without a probe, and holds
        no evidence inside its window."""
        return (
            circuit.circuit_state == CircuitState.HALF_OPEN
            and job_class not in self._probe_job_ids
            and circuit.error_count == 0
        )

    def quarantined_job_classes(self) -> dict[str, str]:
        """Every class with a breaker held, by its state's name."""
        return {job_class: circuit.circuit_state.name for job_class, circuit in self._circuits.items()}
