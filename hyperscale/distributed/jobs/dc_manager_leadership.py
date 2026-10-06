"""``DCManagerLeadership`` -- pickled under the namespace
``hyperscale.distributed.jobs.job_leadership_tracker`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class DCManagerLeadership:
    """
    Leadership information for a manager within a datacenter for a specific job.

    Used by gates to track which manager leads each job in each DC.
    When a manager fails, another manager takes over and the gate must
    be notified to update routing.

    Attributes:
        manager_id: Node ID of the manager leading this job in this DC
        manager_addr: TCP address (host, port) of the manager
        fencing_token: Monotonic token for consistency (higher = newer epoch)
    """

    manager_id: str
    manager_addr: tuple[str, int]
    fencing_token: int
