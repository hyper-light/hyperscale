"""``RaftSnapshot`` -- pickled under the namespace
``hyperscale.distributed.raft.snapshot`` (see that module)."""

from dataclasses import dataclass

from .models.raft_configuration import RaftConfiguration


@dataclass(slots=True)
class RaftSnapshot:
    """
    Immutable snapshot of applied Raft state at a log position.

    Attributes:
        last_included_index: Log index of the last entry in the snapshot.
        last_included_term: Term of the last entry in the snapshot.
        state_data: Serialized application state at this point.
        configuration: The group's configuration in force at this point.
    """

    last_included_index: int
    last_included_term: int
    state_data: bytes
    configuration: RaftConfiguration
