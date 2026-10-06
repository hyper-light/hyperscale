"""
Raft state models per the Raft paper.

Separates persistent state (survives crashes), volatile state
(rebuilt on restart), and leader-only volatile state.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field

from .log_entry import RaftLogEntry
from .raft_leader_volatile_state import RaftLeaderVolatileState
from .raft_persistent_state import RaftPersistentState
from .raft_volatile_state import RaftVolatileState

_REHOMED = (
    RaftPersistentState,
    RaftVolatileState,
    RaftLeaderVolatileState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
