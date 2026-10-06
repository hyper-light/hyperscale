"""Wire model ``PendingTransfer`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class PendingTransfer:
    """
    Tracks a transfer that arrived before the job/workflow was known (Section 8.3).

    This handles the edge case where a transfer notification arrives
    before the original workflow dispatch.
    """

    job_id: str
    workflow_ids: list[str]
    new_manager_id: str
    new_manager_addr: tuple[str, int]
    fence_token: int
    old_manager_id: str | None
    received_at: float
