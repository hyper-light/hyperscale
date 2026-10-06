"""
Datacenter capacity aggregation for gate routing (AD-43).
"""

from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.models.distributed import ManagerHeartbeat


@dataclass(slots=True)
class DatacenterCapacity:
    """
    Aggregated capacity metrics for a datacenter.

    Cores are the datacenter's: every manager tracks every worker (workers
    register with each manager, AD-48), so each heartbeat already reports
    the whole pool and the most authoritative one is taken -- summing them
    multiplied the datacenter by its manager count. Pending and active work
    is per manager (each counts only the jobs it leads), so it is summed.
    ``last_updated`` is when the newest heartbeat arrived; without any, the
    capacity is unknown and reads as infinitely stale.

    ``release_schedule`` is when held cores come free (AD-43 Part 4):
    ``(seconds from when the capacity was read, cores)``, soonest first --
    every manager's executing workflows, from after the authoritative
    heartbeat (whose available cores count the cores freed before it).
    """

    datacenter_id: str
    total_cores: int
    available_cores: int
    pending_workflow_count: int
    pending_duration_seconds: float
    active_remaining_seconds: float
    last_updated: float
    release_schedule: tuple[tuple[float, int], ...] = ()

    @classmethod
    def aggregate(
        cls,
        datacenter_id: str,
        heartbeats: list[tuple[ManagerHeartbeat, float]],
        now: float,
    ) -> DatacenterCapacity:
        """
        Aggregate a datacenter's capacity from its managers' heartbeats,
        each paired with the gate's monotonic time it arrived, as of
        ``now`` (the gate's monotonic time).

        The authoritative heartbeat is the leader's with the highest term
        (a deposed leader's last claim loses), else the most recent one.
        """
        if not heartbeats:
            return cls(
                datacenter_id=datacenter_id,
                total_cores=0,
                available_cores=0,
                pending_workflow_count=0,
                pending_duration_seconds=0.0,
                active_remaining_seconds=0.0,
                last_updated=float("-inf"),
            )

        authoritative_heartbeat, authoritative_received_at = max(
            heartbeats,
            key=lambda entry: (entry[0].is_leader, entry[0].term, entry[1]),
        )
        return cls(
            datacenter_id=datacenter_id,
            total_cores=authoritative_heartbeat.total_cores,
            available_cores=authoritative_heartbeat.available_cores,
            pending_workflow_count=sum(
                heartbeat.pending_workflow_count for heartbeat, _ in heartbeats
            ),
            pending_duration_seconds=sum(
                heartbeat.pending_duration_seconds for heartbeat, _ in heartbeats
            ),
            active_remaining_seconds=sum(
                heartbeat.active_remaining_seconds for heartbeat, _ in heartbeats
            ),
            last_updated=max(received_at for _, received_at in heartbeats),
            release_schedule=tuple(
                sorted(
                    (max(received_at + release_offset - now, 0.0), released_cores)
                    for heartbeat, received_at in heartbeats
                    for release_offset, released_cores in heartbeat.cores_freeing_schedule
                    if received_at + release_offset > authoritative_received_at
                )
            ),
        )

    def can_serve_immediately(self, cores_required: int) -> bool:
        """
        Check whether the datacenter can serve the cores immediately -- all
        of them, or every core it has when the job wants more.
        """
        if self.total_cores > 0:
            cores_required = min(cores_required, self.total_cores)
        return self.available_cores >= cores_required

    def estimated_wait_for_cores(self, cores_required: int) -> float:
        """
        Estimate the wait for a given core requirement: the later of two
        bounds it cannot beat. Its cores must come free -- the release
        schedule walked until enough have (AD-43 Part 4) -- and the work
        ahead of it, executing and queued, must drain at one core-second
        per core. The drain bound alone took one workflow holding every
        core for a hundred seconds as ten seconds of work on ten cores.

        A job uses at most every core of the datacenter (a manager's
        dispatcher starts it on what is free and grows it as more comes
        free), so a requirement beyond them waits for them all. A shortfall
        the schedule does not explain (a manager that reports none) waits at
        least for every release it does report -- stopping short of them let
        a larger job wait less than a smaller one -- and the drain bound.
        """
        if cores_required <= 0:
            return 0.0
        if self.total_cores <= 0:
            return float("inf")
        # Capped before the free cores are compared, as
        # ``can_serve_immediately`` caps: a requirement beyond the
        # datacenter waits exactly as one for all of it does, so with every
        # core free it waits for nothing.
        cores_required = min(cores_required, self.total_cores)
        if self.available_cores >= cores_required:
            return 0.0

        free_cores = self.available_cores
        cores_free_after = 0.0
        for release_offset, released_cores in self.release_schedule:
            free_cores += released_cores
            cores_free_after = release_offset
            if free_cores >= cores_required:
                break

        return max(
            cores_free_after,
            (self.active_remaining_seconds + self.pending_duration_seconds)
            / self.total_cores,
        )

    def is_stale(self, now: float, staleness_threshold_seconds: float) -> bool:
        """
        Check whether capacity data is stale relative to a threshold.
        """
        return (now - self.last_updated) > staleness_threshold_seconds
