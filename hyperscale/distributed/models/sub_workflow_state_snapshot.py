"""One sub-workflow's replicated state in a ``JobStateSyncMessage``
(``sub_workflow_snapshots``: sub-workflow token -> snapshot)."""

from __future__ import annotations

from typing import TypedDict

from .workflow_final_result import WorkflowFinalResult
from .workflow_progress import WorkflowProgress


class SubWorkflowStateSnapshot(TypedDict):
    """A sub-workflow's dispatch, progress and result as its job leader
    holds them."""

    token: str
    parent_token: str
    cores_allocated: int
    fence_token: int
    progress: WorkflowProgress | None
    result: WorkflowFinalResult | None
    dispatched_context: bytes
    dispatched_version: int
    superseded: bool
