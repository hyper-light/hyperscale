"""
Job-status vocabulary contract — the three fixes the linearizability
oracle survey pre-identified, pinned so they cannot regress:

1. ``JobStatus.UNKNOWN`` exists: ``JobManager.get_job_status`` returns
   ``JobStatus.UNKNOWN.value`` for jobs it does not know — before the
   member existed, the FIRST unknown-job status query raised
   ``AttributeError``.
2. The client's terminal set includes ``timeout``: the manager really
   produces ``JobStatus.TIMEOUT`` (deadline enforcement), and without
   it ``wait_for_job`` never releases on a timed-out job.
3. The ledger's terminal set accepts BOTH live timeout spellings —
   managers write ``"timeout"`` (the enum), the gate timeout tracker
   records ``"timed_out"``; the gate-side normalizers accept both and
   the ledger must too, or canonical manager timeouts read as
   non-terminal (checkpoint/archival logic would treat a finished job
   as still running).
"""

from hyperscale.distributed.ledger.job_state import (
    TERMINAL_STATUSES as LEDGER_TERMINAL_STATUSES,
)
from hyperscale.distributed.models.distributed import JobStatus
from hyperscale.distributed.nodes.client.tracking import (
    TERMINAL_STATUSES as CLIENT_TERMINAL_STATUSES,
)


def test_job_status_has_unknown_member() -> None:
    assert JobStatus.UNKNOWN.value == "unknown"


def test_client_terminal_statuses_cover_every_terminal_outcome() -> None:
    assert CLIENT_TERMINAL_STATUSES == frozenset(
        {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }
    )


def test_ledger_terminal_statuses_accept_both_timeout_spellings() -> None:
    assert JobStatus.TIMEOUT.value in LEDGER_TERMINAL_STATUSES
    assert "timed_out" in LEDGER_TERMINAL_STATUSES
    for status in (
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
    ):
        assert status in LEDGER_TERMINAL_STATUSES

    # Non-terminal states must stay out.
    for status in (
        JobStatus.SUBMITTED.value,
        JobStatus.RUNNING.value,
        JobStatus.UNKNOWN.value,
    ):
        assert status not in LEDGER_TERMINAL_STATUSES
        assert status not in CLIENT_TERMINAL_STATUSES
