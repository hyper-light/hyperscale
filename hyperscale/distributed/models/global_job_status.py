"""Wire model ``GlobalJobStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .job_progress import JobProgress


@dataclass(slots=True)
class GlobalJobStatus(Message):
    """
    Global job status aggregated by gate across datacenters.

    This is what gets returned to the client.
    """

    job_id: str  # Job identifier
    status: str  # JobStatus value
    datacenters: list["JobProgress"] = field(default_factory=list)
    total_completed: int = 0  # Global total completed
    total_failed: int = 0  # Global total failed
    overall_rate: float = 0.0  # Global aggregate rate
    elapsed_seconds: float = 0.0  # Time since submission
    completed_datacenters: int = 0  # DCs finished
    failed_datacenters: int = 0  # DCs failed
    errors: list[str] = field(default_factory=list)
    resolution_details: str = ""
    timestamp: float = 0.0  # Monotonic time when job was submitted
    fence_token: int = 0
    progress_percentage: float = 0.0  # Progress as percentage (0.0-100.0)
    # AD-38 Part 8: when the job's leader (at ``fence_token``) held the view
    # answered, on its monotonic clock -- with ``fence_token``, the version
    # a SESSION read carries back (0.0: no versioned view, e.g. a terminal
    # record).
    view_time: float = 0.0
