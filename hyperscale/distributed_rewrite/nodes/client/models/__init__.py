"""
Client-specific data models with slots for memory efficiency.

All state containers use dataclasses with slots=True per REFACTOR.md.
Shared protocol message models remain in distributed_rewrite/models/.
"""

from .job_tracking_state import JobTrackingState
from .cancellation_state import CancellationState
from .leader_tracking import GateLeaderTracking, ManagerLeaderTracking
from .request_routing import RequestRouting

__all__ = [
    "JobTrackingState",
    "CancellationState",
    "GateLeaderTracking",
    "ManagerLeaderTracking",
    "RequestRouting",
]
