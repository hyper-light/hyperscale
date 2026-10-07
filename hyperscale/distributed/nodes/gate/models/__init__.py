"""
Gate-specific data models with slots for memory efficiency.

All state containers use dataclasses with slots=True per REFACTOR.md.
Shared protocol message models remain in distributed_rewrite/models/.
"""

from .gate_peer_state import GatePeerState, GatePeerTracking
from .dc_health_state import DCHealthState, ManagerTracking
from .transient_dispatch_error import TransientDispatchError
from .gate_job_replica_durable_state import GateJobReplicaDurableState
from .gate_job_replica_rollback import GateJobReplicaRollback

__all__ = [
    "GatePeerState",
    "GatePeerTracking",
    "DCHealthState",
    "ManagerTracking",
    "TransientDispatchError",
    "GateJobReplicaDurableState",
    "GateJobReplicaRollback",
]
