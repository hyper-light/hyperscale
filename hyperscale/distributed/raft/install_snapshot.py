"""``InstallSnapshot`` -- pickled under the namespace
``hyperscale.distributed.raft.snapshot`` (see that module)."""

from dataclasses import dataclass

from .models.messages import Message


@dataclass(slots=True)
class InstallSnapshot(Message):
    """
    Raft InstallSnapshot RPC message.

    Sent by a leader to a member whose next entry it no longer holds.
    Carries the state at the snapshot point and the configuration in
    force there (``RaftConfiguration.dump()``): the entries that set it
    are compacted away. Decoded with the restricted unpickler every
    ``Message`` uses -- it arrives from the network.
    """

    job_id: str
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    configuration: bytes
    data: bytes  # Serialized snapshot state
