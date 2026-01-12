"""
Datacenter capacity aggregation for gate routing (AD-43).
"""

import time

from hyperscale.distributed.models.distributed import ManagerHeartbeat

from .datacenter_capacity import DatacenterCapacity


class DatacenterCapacityAggregator:
    """
    Aggregates manager heartbeats into datacenter-wide capacity metrics.
    """

    def __init__(self, staleness_threshold_seconds: float = 30.0) -> None:
        self._staleness_threshold_seconds = staleness_threshold_seconds
        self._manager_heartbeats: dict[str, tuple[ManagerHeartbeat, float]] = {}

    def record_heartbeat(self, heartbeat: ManagerHeartbeat) -> None:
        """
        Record a manager heartbeat for aggregation.
        """
        self._manager_heartbeats[heartbeat.node_id] = (heartbeat, time.monotonic())

    def get_capacity(
        self, datacenter_id: str, health_bucket: str = "healthy"
    ) -> DatacenterCapacity:
        """
        Aggregate capacity metrics for a given datacenter.
        """
        now = time.monotonic()
        self._prune_stale(now)
        heartbeats, last_updated = self._collect_heartbeats(datacenter_id)
        return DatacenterCapacity.aggregate(
            datacenter_id=datacenter_id,
            heartbeats=heartbeats,
            health_bucket=health_bucket,
            last_updated=last_updated,
        )

    def _collect_heartbeats(
        self, datacenter_id: str
    ) -> tuple[list[ManagerHeartbeat], float | None]:
        heartbeats: list[ManagerHeartbeat] = []
        latest_update: float | None = None
        for heartbeat, received_at in self._manager_heartbeats.values():
            if heartbeat.datacenter != datacenter_id:
                continue
            heartbeats.append(heartbeat)
            if latest_update is None or received_at > latest_update:
                latest_update = received_at
        return heartbeats, latest_update

    def _prune_stale(self, now: float) -> None:
        if self._staleness_threshold_seconds <= 0:
            return

        stale_manager_ids = [
            manager_id
            for manager_id, (_, received_at) in self._manager_heartbeats.items()
            if (now - received_at) > self._staleness_threshold_seconds
        ]
        for manager_id in stale_manager_ids:
            self._manager_heartbeats.pop(manager_id, None)
