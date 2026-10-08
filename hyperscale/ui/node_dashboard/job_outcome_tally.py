def jobs_in(statuses: dict[str, str], wanted_statuses: tuple[str, ...]) -> set[str]:
    """The jobs whose status is one of ``wanted_statuses``."""
    return {job_id for job_id, status in statuses.items() if status in wanted_statuses}


def count_entered(statuses: dict[str, str], last_statuses: dict[str, str], entered_statuses: tuple[str, ...]) -> int:
    """How many jobs are in one of ``entered_statuses`` now but were not at
    the last sample."""
    return len(jobs_in(statuses, entered_statuses) - jobs_in(last_statuses, entered_statuses))


class JobOutcomeTally:
    """Running totals of the jobs a gate has admitted, completed and failed
    since the dashboard began watching.

    The gate keeps no such counters: the totals count transitions between
    two samples of its job table -- a job not held at the last sample was
    admitted, a job whose status entered ``completed_statuses`` or
    ``failed_statuses`` completed or failed. A gate holds a finished job
    until its cleanup sweep (``GATE_JOB_CLEANUP_INTERVAL``), far longer
    than a sample interval, so every job is seen. It holds only the
    statuses at the last sample: its memory is bounded by the gate's own
    job table.
    """

    def __init__(self, completed_statuses: tuple[str, ...], failed_statuses: tuple[str, ...]) -> None:
        self._completed_statuses = completed_statuses
        self._failed_statuses = failed_statuses
        self._last_statuses: dict[str, str] = {}
        self.admitted_total = 0
        self.completed_total = 0
        self.failed_total = 0

    def advance(self, statuses: dict[str, str]) -> None:
        """Count the jobs admitted, completed and failed since the last
        sample, from each held job's status now."""
        last_statuses = self._last_statuses
        self.admitted_total += len(statuses.keys() - last_statuses.keys())
        self.completed_total += count_entered(statuses, last_statuses, self._completed_statuses)
        self.failed_total += count_entered(statuses, last_statuses, self._failed_statuses)
        self._last_statuses = statuses
