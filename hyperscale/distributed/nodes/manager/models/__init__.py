"""
Manager-specific data models with slots for memory efficiency.

All state containers use dataclasses with slots=True per REFACTOR.md.
Shared protocol message models remain in distributed_rewrite/models/.
"""

from .peer_state import PeerState, GatePeerState
from .worker_sync_state import WorkerSyncState
from .job_sync_state import JobSyncState
from .workflow_lifecycle_state import WorkflowLifecycleState
from .provision_state import ProvisionState

__all__ = [
    "PeerState",
    "GatePeerState",
    "WorkerSyncState",
    "JobSyncState",
    "WorkflowLifecycleState",
    "ProvisionState",
]
