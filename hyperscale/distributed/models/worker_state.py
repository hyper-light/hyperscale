"""
Worker state update models for cross-manager dissemination (AD-48).

These models support worker visibility across managers using:
- TCP broadcast for critical events (registration, death)
- UDP gossip piggyback for steady-state convergence

Each worker has ONE owner manager that is authoritative; other managers
track workers as "remote" with reduced trust.

This module is the wire namespace of the models below. Each lives in a
file of its own and is re-homed here -- its ``__module__`` set to this
module -- so its pickled form names this module, exactly as before the
split: mixed-version clusters keep talking and data written earlier
keeps loading.
"""

from .worker_list_request import WorkerListRequest
from .worker_list_response import WorkerListResponse
from .worker_state_piggyback_update import WorkerStatePiggybackUpdate
from .worker_state_update import WorkerStateUpdate
from .workflow_reassignment_batch import WorkflowReassignmentBatch
from .workflow_reassignment_notification import WorkflowReassignmentNotification

_WIRE_MODELS = (
    WorkerStateUpdate,
    WorkerStatePiggybackUpdate,
    WorkerListResponse,
    WorkerListRequest,
    WorkflowReassignmentNotification,
    WorkflowReassignmentBatch,
)

for _wire_model in _WIRE_MODELS:
    _wire_model.__module__ = __name__
