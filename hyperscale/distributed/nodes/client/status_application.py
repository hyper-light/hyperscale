"""
JobStatusApplier — the ONE chokepoint through which every client-side
job-status write flows.

Before this existed, the client had four independent blind-assignment
sites (the status poll, both TCP push handlers, and the local
cancel/fail marks), so a stale poll response arriving after a fresher
push regressed the observed status — even out of a terminal state.
Every site now routes here, and the ``JobStatusOrder`` spec (monotone
ranks, absorbing terminals) is enforced in one place.

Stats application rides the same freshness reasoning:

* ``total_completed`` / ``total_failed`` are monotone counters in
  reality, so they apply via ``max`` — a stale push can never wind
  them back.
* ``overall_rate`` and ``elapsed_seconds`` are point-in-time readings
  with no sequence number on the wire (JobStatusPush carries none), so
  ``elapsed_seconds`` itself is the freshness signal: readings only
  apply when elapsed is not regressing.
* A terminal job is FROZEN entirely — its final push carried its final
  stats.
"""

from hyperscale.distributed.jobs.job_status_order import JobStatusOrder
from hyperscale.distributed.models.client import ClientJobResult


class JobStatusApplier:
    """Order-guarded application of status/stats updates to a
    ``ClientJobResult``."""

    __slots__ = ("_order",)

    def __init__(self) -> None:
        self._order = JobStatusOrder()

    @property
    def order(self) -> JobStatusOrder:
        return self._order

    def apply_status(self, job: ClientJobResult, new_status: str) -> bool:
        """Apply a bare status transition; returns whether it applied."""
        if not self._order.should_apply(job.status, new_status):
            return False
        job.status = new_status
        return True

    def apply_push(
        self,
        job: ClientJobResult,
        status: str,
        total_completed: int,
        total_failed: int,
        overall_rate: float,
        elapsed_seconds: float,
    ) -> bool:
        """Apply a full status+stats update (push or poll response).

        Returns whether the STATUS advanced. Stats apply under their
        own monotonicity rules unless the job is already terminal
        (frozen).
        """
        if self._order.is_terminal(job.status):
            return False

        status_applied = self.apply_status(job, status)

        job.total_completed = max(job.total_completed, total_completed)
        job.total_failed = max(job.total_failed, total_failed)

        if elapsed_seconds >= job.elapsed_seconds:
            job.elapsed_seconds = elapsed_seconds
            job.overall_rate = overall_rate

        return status_applied
