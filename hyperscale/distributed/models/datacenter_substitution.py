from dataclasses import dataclass, field


@dataclass(slots=True)
class DatacenterSubstitution:
    """A datacenter a job lost while it ran there, and the datacenter its
    unfinished workflows re-ran in (AD-36 Part 13).

    The workflows whose results the lost datacenter delivered before it
    was lost keep their result slot with it; every other workflow's
    result comes from the replacement. Its work up to its last progress
    report counts in the job's totals. ``replacement_datacenter`` is ""
    when the lost datacenter had delivered every workflow's result --
    only its final result was missing: nothing re-runs, and the job no
    longer waits on it.
    """

    lost_datacenter: str
    replacement_datacenter: str
    completed_workflow_ids: list[str] = field(default_factory=list)
    total_completed: int = 0
    total_failed: int = 0
