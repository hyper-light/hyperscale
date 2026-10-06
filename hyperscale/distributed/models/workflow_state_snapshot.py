"""One workflow's replicated state in a ``JobStateSyncMessage``
(``workflow_snapshots``: workflow token -> snapshot)."""

from __future__ import annotations

from typing import NotRequired, TypedDict


class WorkflowStateSnapshot(TypedDict):
    """A workflow's identity, status and AD-54 lifecycle as its job leader
    holds them. ``lifecycle_state``, ``retry_generation`` and
    ``dependency_workflow_ids`` are absent from an older peer's snapshot;
    ``is_test`` from one built off a worker's report, which cannot know it."""

    token: str
    name: str
    status: str
    lifecycle_state: NotRequired[str | None]
    retry_generation: NotRequired[int]
    dependency_workflow_ids: NotRequired[list[str]]
    is_test: NotRequired[bool]
    sub_workflow_tokens: list[str]
    error: str | None
    aggregation_error: str | None
    terminal_pushed: bool
    terminal_status: str | None
