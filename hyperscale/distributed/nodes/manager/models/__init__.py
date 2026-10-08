"""
Manager-specific data models with slots for memory efficiency.

All state containers use dataclasses with slots=True per REFACTOR.md.
Shared protocol message models remain in distributed_rewrite/models/.
"""

from .eviction_notice_state import WorkerEvictionNoticeState
from .peer_state import PeerState, GatePeerState
from .worker_sync_state import WorkerSyncState
from .job_sync_state import JobSyncState
from .parsed_cancel_request import ParsedCancelRequest
from .state_sync_not_ready_error import StateSyncNotReadyError

__all__ = [
    "ParsedCancelRequest",
    "StateSyncNotReadyError",
    "WorkerEvictionNoticeState",
    "PeerState",
    "GatePeerState",
    "WorkerSyncState",
    "JobSyncState",
]
