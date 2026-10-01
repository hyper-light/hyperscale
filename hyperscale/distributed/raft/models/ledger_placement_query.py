"""
Where is a job-ledger entry held? (AD-38 GLOBAL placement check.)
"""

from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class LedgerPlacementQuery(Message):
    """Asks the job group's Raft leader which members hold the entry whose
    encoded event is ``payload`` -- only the leader knows each member's
    acknowledged log position."""

    job_id: str = ""
    payload: bytes = b""
