"""
Forwarded AD-38 ledger proposal (job leader -> the job group's Raft leader).
"""

from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class LedgerProposal(Message):
    """One job-ledger WAL entry for the receiving member to propose.

    Only the Raft leader of the job's group can append; a member that
    holds a job's ledger but not its group's leadership forwards here.
    Carries the entry's event type and encoded event, never a pickled
    command: the receiver builds the Raft command itself.
    """

    job_id: str = ""
    event_type: int = 0
    payload: bytes = b""
