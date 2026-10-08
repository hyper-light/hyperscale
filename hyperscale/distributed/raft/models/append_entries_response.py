"""``AppendEntriesResponse`` -- pickled under the namespace
``hyperscale.distributed.raft.models.messages`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from hyperscale.distributed.models.message import Message

if TYPE_CHECKING:
    from .append_entries import AppendEntries


@dataclass(slots=True)
class AppendEntriesResponse(Message):
    """
    Response to AppendEntries RPC.

    Includes conflict info for efficient log reconciliation
    (Section 5.3 optimization).
    """

    job_id: str = ""
    term: int = 0
    success: bool = False
    follower_id: str = ""
    match_index: int = 0
    conflict_term: int | None = None
    conflict_index: int | None = None
    # AD-39: the follower refused the entries because their HLC is
    # further ahead of its clock than the offset bound -- not a log
    # conflict, so the leader must not backtrack on it.
    clock_offset_rejected: bool = False
    # The request's ``read_round``, echoed.
    read_round: int = 0
    # The newest log entry schema the follower reads (AD-52 section 14).
    schema_version: int = 1
