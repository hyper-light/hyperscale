"""Wire model ``OrphanedJobInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class OrphanedJobInfo:
    """
    Information about a job whose leaders are unknown/failed (Section 9.5.1).

    Tracks jobs in orphan state pending either leader discovery or timeout.
    """

    job_id: str
    orphan_timestamp: float  # When job became orphaned
    last_known_gate: tuple[str, int] | None
    last_known_manager: tuple[str, int] | None
    datacenter_id: str = ""
