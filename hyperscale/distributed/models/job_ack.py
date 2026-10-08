"""Wire model ``JobAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobAck(Message):
    """
    Acknowledgment of job submission.

    Returned immediately after job is accepted for processing.
    If rejected due to not being leader, leader_addr provides redirect target.

    Protocol Version (AD-25):
    - protocol_version_major/minor: Server's protocol version
    - capabilities: Comma-separated negotiated features
    """

    job_id: str  # Job identifier
    accepted: bool  # Whether job was accepted
    error: str | None = None  # Error message if rejected
    queued_position: int = 0  # Position in queue (if queued)
    leader_addr: tuple[str, int] | None = None  # Leader address for redirect
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated negotiated features
    # Retry hint when ``accepted`` is False and the rejection is
    # retryable (e.g., gate-replication quorum temporarily unavailable).
    # 0.0 means "no retry hint provided" — caller may retry immediately
    # or apply its own backoff.
    retry_after_seconds: float = 0.0
    # AD-40: this answers a submission whose idempotency key was already
    # decided -- the decision is the original's, for the original job.
    was_duplicate: bool = False
    original_job_id: str | None = None
