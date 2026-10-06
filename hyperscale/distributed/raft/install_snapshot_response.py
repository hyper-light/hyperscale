"""``InstallSnapshotResponse`` -- pickled under the namespace
``hyperscale.distributed.raft.snapshot`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass

from .models.messages import Message

if TYPE_CHECKING:
    from .install_snapshot import InstallSnapshot


@dataclass(slots=True)
class InstallSnapshotResponse(Message):
    """Response to InstallSnapshot RPC: on success, the member holds
    everything through ``match_index``."""

    job_id: str
    term: int
    success: bool
    follower_id: str
    match_index: int
