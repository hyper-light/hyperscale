"""
Leadership tracking state for client.

Tracks gate and manager leaders, fence tokens, and orphaned job status.
"""

from dataclasses import dataclass

from hyperscale.distributed.models import (
    GateLeaderInfo,
    ManagerLeaderInfo,
    OrphanedJobInfo,
)


@dataclass(slots=True)
class GateLeaderTracking:
    """Tracks gate leader for a job."""

    job_id: str
    leader_info: GateLeaderInfo
    last_updated: float


@dataclass(slots=True)
class ManagerLeaderTracking:
    """Tracks manager leader for a job+datacenter."""

    job_id: str
    datacenter_id: str
    leader_info: ManagerLeaderInfo
    last_updated: float


@dataclass(slots=True)
class OrphanedJob:
    """Tracks orphaned job state."""

    job_id: str
    orphan_info: OrphanedJobInfo
    orphaned_at: float
