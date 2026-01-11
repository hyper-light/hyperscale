"""
Gate-specific data models with slots for memory efficiency.

All state containers use dataclasses with slots=True per REFACTOR.md.
Shared protocol message models remain in distributed_rewrite/models/.
"""

from .gate_peer_state import GatePeerState, GatePeerTracking
from .dc_health_state import DCHealthState, ManagerTracking
from .job_forwarding_state import JobForwardingState, ForwardingMetrics
from .lease_state import LeaseState, LeaseTracking

__all__ = [
    "GatePeerState",
    "GatePeerTracking",
    "DCHealthState",
    "ManagerTracking",
    "JobForwardingState",
    "ForwardingMetrics",
    "LeaseState",
    "LeaseTracking",
]
