"""
Answer to a LedgerPlacementQuery.
"""

from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class LedgerPlacementResult(Message):
    """``holders``: members known to hold the entry; empty when the
    receiver is not the group's leader or does not have the entry."""

    job_id: str = ""
    holders: list[str] = field(default_factory=list)
