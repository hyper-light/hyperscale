"""``JobLeadership`` -- pickled under the namespace
``hyperscale.distributed.jobs.job_leadership_tracker`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class JobLeadership:
    """
    Leadership information for a single job.

    Attributes:
        leader_id: Node ID of the current leader
        leader_addr: TCP address (host, port) of the leader
        fencing_token: Monotonic token for consistency (higher = newer epoch)
    """

    leader_id: str
    leader_addr: tuple[str, int]
    fencing_token: int
