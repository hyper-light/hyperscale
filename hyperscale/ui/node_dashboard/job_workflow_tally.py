from hyperscale.distributed.models import JobInfo


class JobWorkflowTally:
    """Running totals of the workflows a manager's jobs have completed and
    failed since the dashboard began watching.

    A job's own counts only grow while the manager holds it and vanish when
    it is cleaned up, so the totals add each job's growth between two
    samples (a job first seen adds its counts so far). It holds only the
    counts of the jobs present at the last sample: its memory is bounded by
    the manager's own job table.
    """

    def __init__(self) -> None:
        self._last_counts: dict[str, tuple[int, int]] = {}
        self.completed_total = 0
        self.failed_total = 0

    def advance(self, jobs: list[JobInfo]) -> None:
        """Add the growth of each job's completed and failed workflows."""
        current_counts = {job.job_id: (job.workflows_completed, job.workflows_failed) for job in jobs}
        last_counts = self._last_counts
        for job_id, (workflows_completed, workflows_failed) in current_counts.items():
            last_completed, last_failed = last_counts.get(job_id, (0, 0))
            self.completed_total += max(workflows_completed - last_completed, 0)
            self.failed_total += max(workflows_failed - last_failed, 0)

        self._last_counts = current_counts
