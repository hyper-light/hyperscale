"""``ManagerHealthMonitor.get_health_metrics`` -- worker health and AD-30 job suspicion counts."""

from __future__ import annotations

from typing import TypedDict


class ManagerHealthMetrics(TypedDict):
    """Healthy, unhealthy and total workers, latency targets sampled, and AD-30 job suspicions."""

    healthy_workers: int
    unhealthy_workers: int
    total_workers: int
    tracked_latency_targets: int
    job_suspicions: int
