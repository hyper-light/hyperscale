"""``PendingJobCancellation`` -- a gate cancel some of its job's target
datacenters have not confirmed (AD-20)."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class PendingJobCancellation:
    """
    What a cancel forwards to its job's datacenters, and those that
    confirmed it.

    Kept while a target datacenter has not confirmed the cancel, so the
    gate re-drives it there: a datacenter is unconfirmed exactly when it is
    one of the job's targets and not among ``confirmed_datacenters`` --
    a datacenter the job moved to (AD-36) is driven too.
    """

    use_ad20: bool
    requester_id: str
    fence_token: int
    reason: str
    timestamp: float
    confirmed_datacenters: set[str] = field(default_factory=set)
