"""
Raft log entry model.

Represents a single entry in the Raft log with term, index,
serialized command payload, and metadata.
"""

from dataclasses import dataclass

from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

# The command type of the blank entry a leader appends at the start of its
# term (Raft section 8). Raft consumes these entries itself; they never
# reach the state machine.
RAFT_NO_OP_COMMAND = "raft_no_op"

# AD-52 section 14: the log entry schema versions this build reads, oldest
# to newest. A leader writes the newest version every member it replicates
# to reads; a member never applies an entry outside its range -- it stops
# applying there instead, rather than diverge.
RAFT_LOG_SCHEMA_VERSIONS: tuple[int, int] = (1, 1)


@dataclass(slots=True)
class RaftLogEntry:
    """
    Single entry in a Raft log.

    Each entry carries a serialized command that will be applied
    to the state machine once committed by a majority.

    Attributes:
        term: The leader's term when this entry was created.
        index: 1-based position in the log.
        command: The command's encoded bytes (``LedgerAppendCommand``,
            msgspec), or empty for Raft's own entries.
        command_type: What the entry carries (``LEDGER_APPEND_COMMAND``,
            or Raft's own configuration and no-op entries).
        job_id: The job this entry belongs to.
        hlc: The leader's hybrid logical clock timestamp at proposal
            (AD-39): replicated with the entry, so every member applies
            the same time, and checked against the offset bound by
            followers before they append.
        schema_version: The encoding of ``command`` (AD-52 section 14).
    """

    term: int
    index: int
    command: bytes
    command_type: str
    job_id: str
    hlc: HLCTimestamp
    schema_version: int = RAFT_LOG_SCHEMA_VERSIONS[0]

    @property
    def timestamp(self) -> float:
        """The entry's replicated wall-clock time, in Unix seconds."""
        return self.hlc.seconds
