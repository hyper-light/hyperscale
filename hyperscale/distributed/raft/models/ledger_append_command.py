import msgspec

from hyperscale.distributed.ledger.events.event_type import JobEventType

# The command type of every entry a job's group commits: one job-ledger
# WAL entry (AD-38), mirrored by every member into its ledger replica.
LEDGER_APPEND_COMMAND = "ledger_append"


class LedgerAppendCommand(msgspec.Struct, frozen=True, array_like=True):
    """One job-ledger WAL entry to commit in the job's Raft group: the
    job, the event's type and its payload. Encoded with msgspec -- a log
    entry is read back from peers and from disk (D1), never unpickled."""

    job_id: str
    ledger_event_type: JobEventType
    ledger_payload: bytes
