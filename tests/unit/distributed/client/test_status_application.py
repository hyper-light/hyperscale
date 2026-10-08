"""
JobStatusOrder + JobStatusApplier — the client-side ordering guard.

Pins the exact failure modes the linearizability-oracle survey found
in the four blind-assignment sites the applier replaced:

* a stale poll response regressing a fresher pushed status;
* a stale response un-terminaling a finished job;
* a local CANCELLED mark overwriting COMPLETED (AD-20: the cancel ack
  means REQUESTED — completed-after-cancel keeps COMPLETED);
* stale stats winding back monotone counters or overwriting fresher
  rate/elapsed readings.
"""

from hyperscale.distributed.jobs.job_status_order import JobStatusOrder
from hyperscale.distributed.models.client import ClientJobResult
from hyperscale.distributed.models.distributed import JobStatus
from hyperscale.distributed.nodes.client.status_application import (
    JobStatusApplier,
)
from hyperscale.distributed.nodes.client.status_apply_outcome import (
    StatusApplyOutcome,
)


def _job(status: str = JobStatus.SUBMITTED.value) -> ClientJobResult:
    return ClientJobResult(job_id="job-1", status=status)


class TestJobStatusOrder:
    def test_forward_and_skip_transitions_apply(self) -> None:
        order = JobStatusOrder()
        assert order.should_apply("submitted", "running")
        assert order.should_apply("running", "completing")
        assert order.should_apply("submitted", "completed")

    def test_backward_transitions_rejected(self) -> None:
        order = JobStatusOrder()
        assert not order.should_apply("running", "submitted")
        assert not order.should_apply("completing", "queued")

    def test_terminals_absorb_everything(self) -> None:
        order = JobStatusOrder()
        for terminal in ("completed", "failed", "cancelled", "timeout"):
            assert not order.should_apply(terminal, "running")
            assert not order.should_apply(terminal, "completed")
            assert not order.should_apply(terminal, "cancelled")

    def test_both_timeout_spellings_rank_terminal(self) -> None:
        order = JobStatusOrder()
        assert order.is_terminal("timeout")
        assert order.is_terminal("timed_out")

    def test_unrecognized_statuses(self) -> None:
        order = JobStatusOrder()
        assert not order.should_apply("running", "definitely-not-a-status")
        # Unknown CURRENT recovers to known vocabulary.
        assert order.should_apply("garbage", "running")


class TestJobStatusApplier:
    def test_stale_poll_cannot_regress_pushed_status(self) -> None:
        applier = JobStatusApplier()
        job = _job("running")
        outcome = applier.apply_status(job, "submitted")
        assert outcome is StatusApplyOutcome.REJECTED_STALE
        assert job.status == "running"

    def test_stale_response_cannot_unterminal_a_finished_job(self) -> None:
        applier = JobStatusApplier()
        job = _job("completed")
        applier.apply_push(job, "running", 5, 0, 10.0, 3.0)
        assert job.status == "completed"

    def test_completed_after_cancel_keeps_completed(self) -> None:
        applier = JobStatusApplier()
        job = _job("completed")
        outcome = applier.apply_status(job, JobStatus.CANCELLED.value)
        assert outcome is StatusApplyOutcome.REJECTED_STALE
        assert job.status == "completed"

    def test_unknown_vocabulary_is_distinguished_from_stale(self) -> None:
        """REJECTED_UNKNOWN is a protocol surprise the callers must
        log — it must never be conflated with a routine ordering
        rejection (and enum members are all truthy, so boolean use of
        the outcome would hide exactly this)."""
        applier = JobStatusApplier()
        job = _job("running")
        outcome = applier.apply_status(job, "definitely-not-a-status")
        assert outcome is StatusApplyOutcome.REJECTED_UNKNOWN
        assert outcome.unknown_vocabulary
        assert not outcome.applied
        assert job.status == "running"

    def test_counters_never_wind_back(self) -> None:
        applier = JobStatusApplier()
        job = _job("running")
        applier.apply_push(job, "running", 40, 2, 8.0, 10.0)
        # A stale push (lower elapsed) with lower counters.
        applier.apply_push(job, "running", 20, 1, 4.0, 5.0)
        assert job.total_completed == 40
        assert job.total_failed == 2
        # Stale rate/elapsed rejected by the elapsed freshness gate.
        assert job.overall_rate == 8.0
        assert job.elapsed_seconds == 10.0

    def test_fresher_stats_apply_within_same_status(self) -> None:
        applier = JobStatusApplier()
        job = _job("running")
        applier.apply_push(job, "running", 20, 1, 4.0, 5.0)
        applier.apply_push(job, "running", 40, 2, 8.0, 10.0)
        assert job.total_completed == 40
        assert job.overall_rate == 8.0
        assert job.elapsed_seconds == 10.0

    def test_terminal_job_is_fully_frozen(self) -> None:
        applier = JobStatusApplier()
        job = _job("completed")
        job.total_completed = 100
        job.elapsed_seconds = 30.0
        applier.apply_push(job, "completed", 999, 999, 99.0, 99.0)
        assert job.total_completed == 100
        assert job.elapsed_seconds == 30.0

    def test_forward_push_applies_status_and_stats(self) -> None:
        applier = JobStatusApplier()
        job = _job("submitted")
        outcome = applier.apply_push(job, "running", 10, 0, 2.0, 1.5)
        assert outcome.applied
        assert job.status == "running"
        assert job.total_completed == 10
        assert job.elapsed_seconds == 1.5
