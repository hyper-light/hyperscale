"""
Outcome of a forwarded AD-38 ledger proposal.
"""

from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class LedgerProposalResult(Message):
    """``appended``: the receiver was leader and put the entry in its log.
    ``committed``: the entry reached a majority and was applied."""

    job_id: str = ""
    appended: bool = False
    committed: bool = False
