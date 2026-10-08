"""``GateOrphanJobCoordinator.get_orphan_stats`` -- the gate's orphaned-job tracking."""

from __future__ import annotations

from typing import TypedDict


class OrphanJobStats(TypedDict):
    """Orphaned and confirmed-orphaned job counts, those past the grace period, the grace period
    and scan interval, and whether the scan loop runs."""

    total_orphaned: int
    confirmed_orphaned: int
    past_grace_period: int
    grace_period_seconds: float
    check_interval_seconds: float
    running: bool
