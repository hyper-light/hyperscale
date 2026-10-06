from __future__ import annotations

import msgspec


class DatacenterReassignment(msgspec.Struct, frozen=True, array_like=True):
    """A datacenter a job lost while it ran there, as the job's record
    keeps it (AD-36 mid-flight failover): where its unfinished workflows
    re-ran ("" when it had delivered every workflow's result, so nothing
    re-ran), the workflows whose results it delivered before it was lost
    (their result slots stay with it), and its work until it was lost."""

    lost_datacenter: str
    replacement_datacenter: str
    completed_workflow_ids: tuple[str, ...]
    total_completed: int
    total_failed: int
