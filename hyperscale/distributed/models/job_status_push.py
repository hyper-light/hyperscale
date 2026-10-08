"""Wire model ``JobStatusPush`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .dc_stats import DCStats


@dataclass(slots=True)
class JobStatusPush(Message):
    """
    Push notification for job status changes.

    Sent from Gate/Manager to Client when significant status changes occur.
    This is a Tier 1 (immediate) notification for:
    - Job started
    - Job completed
    - Job failed
    - Datacenter completion

    Includes both aggregated totals AND per-DC breakdown for visibility.
    """

    job_id: str  # Job identifier
    status: str  # JobStatus value
    message: str  # Human-readable status message
    total_completed: int = 0  # Completed count (aggregated across all DCs)
    total_failed: int = 0  # Failed count (aggregated across all DCs)
    overall_rate: float = 0.0  # Current rate (aggregated across all DCs)
    elapsed_seconds: float = 0.0  # Time since submission
    is_final: bool = False  # True if job is complete (no more updates)
    # Per-datacenter breakdown (for clients that want granular visibility)
    per_dc_stats: list["DCStats"] = field(default_factory=list)
    fence_token: int = 0  # Fencing token for at-most-once semantics
    # Client callback for gate-owned L3 delivery. This lets any surviving gate
    # deliver forwarded status updates without depending on local callback
    # replication from the original accepting gate.
    callback_addr: tuple[str, int] | None = None
