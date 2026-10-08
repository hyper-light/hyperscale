"""Wire model ``RateLimitResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class RateLimitResponse(Message):
    """
    Response indicating rate limit exceeded.

    Returned when a client exceeds their request rate limit.
    Client should wait retry_after_seconds before retrying.

    Protocol:
    1. Client sends request via TCP
    2. Server checks rate limit for client_id (from addr) + operation
    3. If exceeded, returns RateLimitResponse with retry_after
    4. Client waits out retry_after and retries (ClientJobSubmitter)

    Integration:
    - Gate: Rate limits job_submit, job_status, cancel, workflow_query
    - Manager: Rate limits workflow_dispatch, provision requests
    - Both use ServerRateLimiter with per-client token buckets
    """

    operation: str  # Operation that was rate limited
    retry_after_seconds: float  # Seconds to wait before retry
    error: str = "Rate limit exceeded"  # Error message
    tokens_remaining: float = 0.0  # Remaining tokens (for debugging)
