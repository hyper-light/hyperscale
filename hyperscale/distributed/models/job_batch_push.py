"""Wire model ``JobBatchPush`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .dc_stats import DCStats
    from .step_stats import StepStats


@dataclass(slots=True)
class JobBatchPush(Message):
    """
    Batched statistics push notification.

    Sent periodically (Tier 2) with aggregated progress data.
    Contains step-level statistics and detailed progress.
    Includes per-DC breakdown for granular visibility.
    """

    job_id: str  # Job identifier
    status: str  # Current JobStatus
    step_stats: list["StepStats"] = field(default_factory=list)
    total_completed: int = 0  # Aggregated across all DCs
    total_failed: int = 0  # Aggregated across all DCs
    overall_rate: float = 0.0  # Aggregated across all DCs
    elapsed_seconds: float = 0.0
    # Per-datacenter breakdown (for clients that want granular visibility)
    per_dc_stats: list["DCStats"] = field(default_factory=list)
