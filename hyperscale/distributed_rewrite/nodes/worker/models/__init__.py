"""
Worker-specific data models with slots for memory efficiency.

All state containers use dataclasses with slots=True per REFACTOR.md.
Shared protocol message models remain in distributed_rewrite/models/.
"""

from .manager_peer_state import ManagerPeerState
from .workflow_runtime_state import WorkflowRuntimeState
from .cancel_state import CancelState
from .execution_metrics import ExecutionMetrics, CompletionTimeTracker
from .transfer_state import TransferMetrics, PendingTransferState

__all__ = [
    "ManagerPeerState",
    "WorkflowRuntimeState",
    "CancelState",
    "ExecutionMetrics",
    "CompletionTimeTracker",
    "TransferMetrics",
    "PendingTransferState",
]
