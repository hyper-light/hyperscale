"""One active workflow in a ``WorkerStateSyncSnapshot``."""

from __future__ import annotations

from typing import TypedDict


class WorkerWorkflowSyncSnapshot(TypedDict):
    """The workflow's job, status, counts, assigned cores and job leader address."""

    job_id: str
    status: str
    completed_count: int
    failed_count: int
    assigned_cores: list[int]
    job_leader: tuple[str, int] | None
