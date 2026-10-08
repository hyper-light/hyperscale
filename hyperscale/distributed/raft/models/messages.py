"""
Raft RPC message models.

Defines the four core Raft RPC messages: RequestVote, RequestVoteResponse,
AppendEntries, and AppendEntriesResponse. All extend Message for
consistent serialization via cloudpickle.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from hyperscale.distributed.models.message import Message

from .log_entry import RaftLogEntry
from .append_entries import AppendEntries
from .append_entries_response import AppendEntriesResponse
from .request_vote import RequestVote
from .request_vote_response import RequestVoteResponse

_REHOMED = (
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
