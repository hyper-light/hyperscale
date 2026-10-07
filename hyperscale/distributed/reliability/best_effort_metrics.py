"""
AD-44 best-effort completion metrics a gate holds.
"""

BEST_EFFORT_REASON_PREFIX = "best_effort: "


class BestEffortMetrics:
    """The AD-44 best-effort metrics of one gate.

    * ``best_effort_completions_total{reason}`` -- jobs completed on their
      best-effort policy, by the kind of reason (``min_dcs_reached``,
      ``deadline_expired``, ``all_dcs_reported``); a job counts once, when
      its result first went out.
    * ``best_effort_completion_ratio{job_id}`` -- the share of each job's
      target datacenters that completed, as of its latest result; held
      while the gate holds the job (``forget_job`` at its cleanup).
    * ``best_effort_late_results_total{outcome}`` -- datacenter results
      that arrived after their job completed: ``logged`` or ``updated``.
    """

    __slots__ = ("_completions_by_reason", "_completion_ratio_by_job", "_late_results_by_outcome")

    def __init__(self) -> None:
        self._completions_by_reason: dict[str, int] = {}
        self._completion_ratio_by_job: dict[str, float] = {}
        self._late_results_by_outcome: dict[str, int] = {}

    def record_completion(self, job_id: str, completion_reason: str, completion_ratio: float) -> None:
        """A best-effort job's result went out for the first time."""
        reason_kind = completion_reason.removeprefix(BEST_EFFORT_REASON_PREFIX).partition(" ")[0]
        self._completions_by_reason[reason_kind] = self._completions_by_reason.get(reason_kind, 0) + 1
        self._completion_ratio_by_job[job_id] = completion_ratio

    def record_ratio(self, job_id: str, completion_reason: str, completion_ratio: float) -> None:
        """A released job's result changed: its ratio moves, its completion
        was already counted (same signature as ``record_completion``)."""
        self._completion_ratio_by_job[job_id] = completion_ratio

    def record_late_result(self, outcome: str) -> None:
        """A datacenter result arrived after its job completed."""
        self._late_results_by_outcome[outcome] = self._late_results_by_outcome.get(outcome, 0) + 1

    def forget_job(self, job_id: str) -> None:
        """Drop the job's ratio (the gate no longer holds the job)."""
        self._completion_ratio_by_job.pop(job_id, None)

    def completions_by_reason(self) -> dict[str, int]:
        return dict(self._completions_by_reason)

    def completion_ratio_by_job(self) -> dict[str, float]:
        return dict(self._completion_ratio_by_job)

    def late_results_by_outcome(self) -> dict[str, int]:
        return dict(self._late_results_by_outcome)
