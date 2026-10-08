"""Wire model ``JobGlobalTimeout`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobGlobalTimeout(Message):
    """
    Gate → Manager: Global timeout declared (AD-34 multi-DC coordination).

    Gate has determined the job is globally timed out (based on timeout reports
    from DCs, overall timeout exceeded, or all DCs stuck). Manager must cancel
    job locally and mark as timed out.

    Fence token validation prevents stale timeout decisions after leader transfers.
    """

    job_id: str
    reason: str  # Why gate timed out the job
    timed_out_at: float  # Gate's timestamp
    fence_token: int  # Gate's fence token for this decision
