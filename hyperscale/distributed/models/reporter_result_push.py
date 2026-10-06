"""Wire model ``ReporterResultPush`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class ReporterResultPush(Message):
    """
    Push notification for reporter submission result.

    Sent from Manager/Gate to Client after submitting results to a reporter.
    Each reporter config generates one notification (success or failure).

    This is sent as a background task completes, not batched.
    Clients can track which reporters succeeded or failed for a job.
    """

    job_id: str  # Job the results were for
    reporter_type: str  # ReporterTypes enum value (e.g., "json", "datadog")
    success: bool  # Whether submission succeeded
    error: str | None = None  # Error message if failed
    elapsed_seconds: float = 0.0  # Time taken for submission
    # Source information for multi-DC scenarios
    source: str = ""  # "manager" or "gate"
    datacenter: str = ""  # Datacenter that submitted (manager only)
