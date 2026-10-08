"""
Per-job dispatch-failure cooldowns (AD-36 Part 5).
"""

from hyperscale.distributed.runtime import Clock


class JobDispatchCooldowns:
    """
    Datacenters that recently failed to accept a job, per job.

    A datacenter that failed a job's dispatch is routed after every other
    eligible datacenter for that job until ``cooldown_seconds`` pass: it
    is not excluded (it may be the only one left), it is tried last.
    Entries expire on their own and a job's are dropped with the job;
    every recorded failure also sweeps the expired entries of all jobs,
    so a job that is never routed or cleaned up again cannot pin its
    entries.
    """

    def __init__(self, clock: Clock, cooldown_seconds: float) -> None:
        self._clock = clock
        self._cooldown_seconds = cooldown_seconds
        self._cooling_until_by_job: dict[str, dict[str, float]] = {}

    def record_failure(self, job_id: str, datacenter_id: str) -> None:
        now = self._clock.monotonic()
        expired_job_ids = self._expired_job_ids(now)
        for expired_job_id in expired_job_ids:
            del self._cooling_until_by_job[expired_job_id]

        self._cooling_until_by_job.setdefault(job_id, {})[datacenter_id] = (
            now + self._cooldown_seconds
        )

    def cooling_datacenters(self, job_id: str) -> frozenset[str]:
        """The datacenters still cooling down for ``job_id``."""
        cooling_until = self._cooling_until_by_job.get(job_id)
        if cooling_until is None:
            return frozenset()

        now = self._clock.monotonic()
        cooling = self._still_cooling(cooling_until, now)
        if len(cooling) < len(cooling_until):
            self._keep_only_cooling(job_id, cooling_until, cooling)

        return cooling

    @staticmethod
    def _still_cooling(cooling_until: dict[str, float], now: float) -> frozenset[str]:
        """The datacenters whose cooldown runs past ``now``."""
        return frozenset(
            datacenter_id
            for datacenter_id, until in cooling_until.items()
            if until > now
        )

    def _expired_job_ids(self, now: float) -> list[str]:
        """The jobs none of whose datacenters is still cooling at ``now``."""
        return [
            expired_job_id
            for expired_job_id, cooling_until in self._cooling_until_by_job.items()
            if max(cooling_until.values()) <= now
        ]

    def _keep_only_cooling(
        self,
        job_id: str,
        cooling_until: dict[str, float],
        cooling: frozenset[str],
    ) -> None:
        """Drop ``job_id``'s expired cooldowns, and the job once none is left."""
        if cooling:
            self._cooling_until_by_job[job_id] = {
                datacenter_id: cooling_until[datacenter_id]
                for datacenter_id in cooling
            }
        else:
            del self._cooling_until_by_job[job_id]

    def clear_job(self, job_id: str) -> None:
        self._cooling_until_by_job.pop(job_id, None)

    def tracked_job_count(self) -> int:
        return len(self._cooling_until_by_job)
