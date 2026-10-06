"""
Datacenter capacity aggregation for gate routing (AD-43).
"""

from hyperscale.distributed.models.distributed import ManagerHeartbeat
from hyperscale.distributed.runtime import Clock

from .datacenter_capacity import DatacenterCapacity


class DatacenterCapacityAggregator:
    """
    Aggregates manager heartbeats into datacenter-wide capacity metrics.

    Holds the latest heartbeat of each manager, keyed by its node id, for as
    long as it is fresh: stale entries are dropped whenever a heartbeat is
    recorded or a capacity read, so the store is bounded by the managers
    heard from within the staleness threshold (a restarted manager's old
    incarnation ages out).
    """

    def __init__(self, clock: Clock, staleness_threshold_seconds: float) -> None:
        if staleness_threshold_seconds <= 0.0:
            raise ValueError(
                "staleness_threshold_seconds must be positive: without it no "
                f"heartbeat ever ages out (got {staleness_threshold_seconds})"
            )
        self._clock = clock
        self._staleness_threshold_seconds = staleness_threshold_seconds
        self._manager_heartbeats: dict[str, tuple[ManagerHeartbeat, float]] = {}

    @property
    def staleness_threshold_seconds(self) -> float:
        """How long a manager's heartbeat counts toward its datacenter."""
        return self._staleness_threshold_seconds

    def record_heartbeat(self, heartbeat: ManagerHeartbeat) -> None:
        now = self._clock.monotonic()
        self._prune_stale(now)
        self._manager_heartbeats[heartbeat.node_id] = (heartbeat, now)

    def get_capacity(self, datacenter_id: str) -> DatacenterCapacity:
        """
        Aggregate capacity metrics for a given datacenter, as of now.
        """
        now = self._clock.monotonic()
        self._prune_stale(now)
        return DatacenterCapacity.aggregate(
            datacenter_id=datacenter_id,
            heartbeats=[
                entry
                for entry in self._manager_heartbeats.values()
                if entry[0].datacenter == datacenter_id
            ],
            now=now,
        )

    def _prune_stale(self, now: float) -> None:
        stale_manager_ids = [
            manager_id
            for manager_id, (_, received_at) in self._manager_heartbeats.items()
            if (now - received_at) > self._staleness_threshold_seconds
        ]
        for manager_id in stale_manager_ids:
            self._manager_heartbeats.pop(manager_id, None)
