"""Wire model ``DCStats`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .job_progress import JobProgress
    from .job_status_push import JobStatusPush


@dataclass(slots=True)
class DCStats(Message):
    """
    Per-datacenter statistics for real-time status updates.

    Used in JobStatusPush to provide per-DC visibility without
    the full detail of JobProgress (which includes workflow-level stats).
    """

    datacenter: str  # Datacenter identifier
    status: str  # DC-specific status
    completed: int = 0  # Completed in this DC
    failed: int = 0  # Failed in this DC
    rate: float = 0.0  # Rate in this DC
