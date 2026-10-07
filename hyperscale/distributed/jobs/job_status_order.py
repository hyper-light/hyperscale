"""
JobStatusOrder — the monotone rank order of the job-status lifecycle.

This is the SPEC the client-observed history must obey (and the one the
SIM linearizability oracle checks against): statuses advance through

    submitted -> queued -> dispatching -> running -> completing
        -> {completed | failed | cancelled | timeout}

Observers may legitimately MISS intermediate states (pushes are
periodic, polls sample), so forward skips are legal. What is never
legal is moving BACKWARD — a stale poll response or reordered push
must not regress a fresher status — and terminal states are ABSORBING:
once a job is completed/failed/cancelled/timed-out, nothing may change
it (including to a different terminal — completed-after-cancel keeps
COMPLETED, per AD-20's cancellation semantics: the ack means the
cancel was REQUESTED, not that it won).

Both live timeout spellings rank terminal: managers write the enum's
``timeout``, the gate timeout tracker records ``timed_out``.
"""

from hyperscale.distributed.models.distributed import JobStatus

_TERMINAL_RANK = 5

_STATUS_RANKS: dict[str, int] = {
    JobStatus.SUBMITTED.value: 0,
    JobStatus.QUEUED.value: 1,
    JobStatus.DISPATCHING.value: 2,
    JobStatus.RUNNING.value: 3,
    JobStatus.COMPLETING.value: 4,
    JobStatus.COMPLETED.value: _TERMINAL_RANK,
    JobStatus.FAILED.value: _TERMINAL_RANK,
    JobStatus.CANCELLED.value: _TERMINAL_RANK,
    JobStatus.TIMEOUT.value: _TERMINAL_RANK,
    "timed_out": _TERMINAL_RANK,
}


class JobStatusOrder:
    """Rank queries and the single transition-legality rule.

    Stateless — safe to share; instantiate per consumer for clarity.
    """

    __slots__ = ()

    def rank(self, status: str) -> int | None:
        """The status's lifecycle rank, or None if unrecognized."""
        return _STATUS_RANKS.get(status)

    def is_terminal(self, status: str) -> bool:
        return _STATUS_RANKS.get(status) == _TERMINAL_RANK

    def should_apply(self, current_status: str, new_status: str) -> bool:
        """Whether ``new_status`` may replace ``current_status``.

        Unrecognized new statuses never apply (the caller logs them —
        silently storing an unrankable status would blind every later
        ordering decision). An unrecognized CURRENT status accepts any
        recognized new one — recovery toward known vocabulary.
        Terminals absorb; equal-or-forward rank applies; backward never
        does.
        """
        new_rank = _STATUS_RANKS.get(new_status)
        if new_rank is None:
            return False

        current_rank = _STATUS_RANKS.get(current_status)
        if current_rank is None:
            return True

        return self._advances(current_rank, new_rank, current_status, new_status)

    @staticmethod
    def _advances(current_rank: int, new_rank: int, current_status: str, new_status: str) -> bool:
        """Between ranked statuses: terminals absorb, a repeat never applies, forward rank applies."""
        return (
            current_rank != _TERMINAL_RANK
            and new_status != current_status
            and new_rank >= current_rank
        )
