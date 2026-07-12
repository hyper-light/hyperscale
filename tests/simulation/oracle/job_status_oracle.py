"""
JobStatusOracle — checks a client-observed status history against the
job-lifecycle spec.

The spec is ``JobStatusOrder`` (production's own rank table — the
oracle verifies the OBSERVED SEQUENCES, which no production component
computes): statuses may only advance in rank (forward skips are legal —
pushes are periodic and polls sample, so observers miss intermediates),
terminal states are absorbing (nothing follows one, not even a
different terminal), and the ``job-finished`` milestone must agree with
the terminal status the observation stream ended on.

Histories are the milestone logs the SIM client entries already
record — ``("status-seen", status, virtual_time)`` for every observed
status transition and ``("job-finished", status, virtual_time)`` at
result delivery — so the oracle needs no production hooks and judges
exactly what the client SAW, in the order it saw it.
"""

from hyperscale.distributed.jobs.job_status_order import JobStatusOrder


class JobStatusOracle:
    """Judge client-observed job-status histories.

    ``check_history`` takes one job's observed sequence and returns
    human-readable violations (empty = the history linearizes).
    ``check_client_log`` adapts the SIM client milestone-log shape.
    """

    __slots__ = ("_order",)

    def __init__(self) -> None:
        self._order = JobStatusOrder()

    def check_history(
        self,
        observed_statuses: list[str],
        finished_status: str | None = None,
    ) -> list[str]:
        violations: list[str] = []

        previous_status: str | None = None
        terminal_seen: str | None = None
        for index, status in enumerate(observed_statuses):
            status_rank = self._order.rank(status)
            if status_rank is None:
                violations.append(
                    f"observation {index}: unknown status {status!r} "
                    "(not in the lifecycle vocabulary)"
                )
                continue

            if terminal_seen is not None and status != terminal_seen:
                violations.append(
                    f"observation {index}: {status!r} observed AFTER "
                    f"terminal {terminal_seen!r} — terminals are absorbing"
                )
                continue

            if previous_status is not None:
                previous_rank = self._order.rank(previous_status)
                if previous_rank is not None and status_rank < previous_rank:
                    violations.append(
                        f"observation {index}: {status!r} (rank "
                        f"{status_rank}) regressed from "
                        f"{previous_status!r} (rank {previous_rank})"
                    )
                    continue

            if self._order.is_terminal(status):
                terminal_seen = status
            previous_status = status

        if finished_status is not None:
            if not self._order.is_terminal(finished_status):
                violations.append(
                    f"job-finished carried non-terminal status "
                    f"{finished_status!r}"
                )
            if terminal_seen is not None and finished_status != terminal_seen:
                violations.append(
                    f"job-finished status {finished_status!r} disagrees "
                    f"with the observed terminal {terminal_seen!r}"
                )

        return violations

    def check_client_log(self, client_log: list[tuple]) -> list[str]:
        """Adapt a SIM client milestone log and judge it.

        Recognizes ``("status-seen", status, t)`` and
        ``("job-finished", status, t)`` entries; everything else in the
        log (submission markers, custom milestones) is ignored.
        """
        observed_statuses = [
            entry[1] for entry in client_log if entry[0] == "status-seen"
        ]
        finished = [
            entry[1] for entry in client_log if entry[0] == "job-finished"
        ]

        violations = self.check_history(
            observed_statuses,
            finished_status=finished[0] if finished else None,
        )
        if len(finished) > 1:
            violations.append(
                f"job finished more than once: {finished!r} — result "
                "delivery must be exactly-once per job"
            )
        return violations
