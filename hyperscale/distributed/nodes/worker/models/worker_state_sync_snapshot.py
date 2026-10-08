"""``WorkerStateSync.generate_snapshot`` -- the worker state a manager sync request receives."""

from __future__ import annotations

from typing import TypedDict

from .worker_workflow_sync_snapshot import WorkerWorkflowSyncSnapshot


class WorkerStateSyncSnapshot(TypedDict):
    """The worker's state version, cores, and active workflows by workflow id."""

    state_version: int
    total_cores: int
    available_cores: int
    active_workflow_count: int
    workflows: dict[str, WorkerWorkflowSyncSnapshot]
