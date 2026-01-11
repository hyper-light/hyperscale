"""
Transfer state tracking for worker job leadership transfers.

Tracks metrics and pending transfers for Section 8 robust
job leadership transfer handling.
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class TransferMetrics:
    """
    Metrics for job leadership transfer tracking (Section 8.6).

    Tracks transfer acceptance/rejection statistics for
    monitoring and debugging.
    """

    received: int = 0
    accepted: int = 0
    rejected_stale_token: int = 0
    rejected_unknown_manager: int = 0
    rejected_other: int = 0


@dataclass(slots=True)
class PendingTransferState:
    """
    State for a pending job leadership transfer (Section 8.3).

    When a transfer arrives before a workflow is dispatched,
    we store it here to apply when the workflow arrives.
    """

    job_id: str
    workflow_ids: list[str]
    new_manager_id: str
    new_manager_addr: tuple[str, int]
    fence_token: int
    old_manager_id: str | None
    received_at: float
