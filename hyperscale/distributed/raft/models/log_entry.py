"""
Raft log entry model.

Represents a single entry in the Raft log with term, index,
serialized command payload, and metadata.
"""

from dataclasses import dataclass


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
        timestamp: Monotonic timestamp when the entry was created.
    """

    term: int
    index: int
    command: bytes
    command_type: str
    job_id: str
    timestamp: float
