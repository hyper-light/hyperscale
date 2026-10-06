"""The builtin form of a ``JobState`` that checkpoints and the job archive
persist (``JobState.to_dict`` / ``JobState.from_dict``)."""

from __future__ import annotations

from typing import TypedDict


class JobStateRecord(TypedDict):
    """A ``JobState`` as msgpack builtins. Each HLC is its three components
    ``[wall_ms, logical, node_id]``; each datacenter reassignment is the
    array form of ``DatacenterReassignment`` (``[lost_datacenter,
    replacement_datacenter, completed_workflow_ids, total_completed,
    total_failed]``)."""

    job_id: str
    status: str
    fence_token: int
    assigned_datacenters: list[str]
    accepted_datacenters: list[str]
    cancelled: bool
    completed_count: int
    failed_count: int
    created_hlc: list[int | str]
    last_hlc: list[int | str]
    requestor_id: str
    timeout_seconds: float
    last_progress_hlc: list[int | str] | None
    cancellation_acked_datacenters: list[str]
    datacenter_reassignments: list[list[str | list[str] | int]]
    leader_id: str
