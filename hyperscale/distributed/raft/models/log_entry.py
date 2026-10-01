"""
Raft log entry model.

Represents a single entry in the Raft log with term, index,
serialized command payload, and metadata.
"""

from dataclasses import dataclass

from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp


@dataclass(slots=True)
class RaftLogEntry:
    """
    Single entry in a Raft log.

    Each entry carries a serialized command that will be applied
    to the state machine once committed by a majority.

    Attributes:
        term: The leader's term when this entry was created.
        index: 1-based position in the log.
        command: Serialized command bytes (cloudpickle).
        command_type: String identifier for dispatch (supports both
            RaftCommandType and GateRaftCommandType since both are str enums).
        job_id: The job this entry belongs to.
        hlc: The leader's hybrid logical clock timestamp at proposal
            (AD-39): replicated with the entry, so every member applies
            the same time, and checked against the offset bound by
            followers before they append.
    """

    term: int
    index: int
    command: bytes
    command_type: str
    job_id: str
    hlc: HLCTimestamp

    @property
    def timestamp(self) -> float:
        """The entry's replicated wall-clock time, in Unix seconds."""
        return self.hlc.seconds
