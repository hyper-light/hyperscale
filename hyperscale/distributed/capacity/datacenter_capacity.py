"""
Datacenter capacity aggregation for gate routing (AD-43).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from hyperscale.distributed.models.distributed import ManagerHeartbeat


@dataclass(slots=True)
class DatacenterCapacity:
    """
    Aggregated capacity metrics for a datacenter.
    """

    datacenter_id: str
    total_cores: int
    available_cores: int
    pending_workflow_count: int
    pending_duration_seconds: float
    active_remaining_seconds: float
    estimated_wait_seconds: float
    utilization: float
    health_bucket: str
    last_updated: float

    @classmethod
    def aggregate(
        cls,
        datacenter_id: str,
        heartbeats: list[ManagerHeartbeat],
        health_bucket: str,
        last_updated: float | None = None,
    ):
        """
        Aggregate capacity metrics from manager heartbeats.
        """
        updated_time = last_updated if last_updated is not None else time.monotonic()
        if not heartbeats:
            return cls(
                datacenter_id=datacenter_id,
                total_cores=0,
                available_cores=0,
                pending_workflow_count=0,
                pending_duration_seconds=0.0,
                active_remaining_seconds=0.0,
                estimated_wait_seconds=float("inf"),
                utilization=0.0,
                health_bucket=health_bucket,
                last_updated=updated_time,
            )

        total_cores = sum(heartbeat.total_cores for heartbeat in heartbeats)
        available_cores = sum(heartbeat.available_cores for heartbeat in heartbeats)
        pending_count = sum(
            heartbeat.pending_workflow_count for heartbeat in heartbeats
        )
        pending_duration = sum(
            heartbeat.pending_duration_seconds for heartbeat in heartbeats
        )
        active_remaining = sum(
            heartbeat.active_remaining_seconds for heartbeat in heartbeats
        )

        estimated_wait = _estimate_wait_time(
            available_cores,
            total_cores,
            pending_duration,
            pending_count,
        )
        utilization = _calculate_utilization(available_cores, total_cores)

        return cls(
            datacenter_id=datacenter_id,
            total_cores=total_cores,
            available_cores=available_cores,
            pending_workflow_count=pending_count,
            pending_duration_seconds=pending_duration,
            active_remaining_seconds=active_remaining,
            estimated_wait_seconds=estimated_wait,
            utilization=utilization,
            health_bucket=health_bucket,
            last_updated=updated_time,
        )

    def can_serve_immediately(self, cores_required: int) -> bool:
        """
        Check whether the datacenter can serve the cores immediately.
        """
        return self.available_cores >= cores_required

    def estimated_wait_for_cores(self, cores_required: int) -> float:
        """
        Estimate the wait time for a given core requirement.
        """
        if cores_required <= 0:
            return 0.0
        if self.available_cores >= cores_required:
            return 0.0
        if self.total_cores <= 0:
            return float("inf")

        total_work_remaining = (
            self.active_remaining_seconds + self.pending_duration_seconds
        )
        throughput = self.total_cores
        if throughput <= 0:
            return float("inf")

        return total_work_remaining / throughput

    def is_stale(self, now: float, staleness_threshold_seconds: float) -> bool:
        """
        Check whether capacity data is stale relative to a threshold.
        """
        if staleness_threshold_seconds <= 0:
            return False
        return (now - self.last_updated) > staleness_threshold_seconds


def _estimate_wait_time(
    available_cores: int,
    total_cores: int,
    pending_duration: float,
    pending_count: int,
) -> float:
    if available_cores > 0:
        return 0.0
    if total_cores <= 0:
        return float("inf")

    average_duration = pending_duration / max(1, pending_count)
    return (pending_count * average_duration) / total_cores


def _calculate_utilization(available_cores: int, total_cores: int) -> float:
    if total_cores <= 0:
        return 1.0
    return 1.0 - (available_cores / total_cores)
