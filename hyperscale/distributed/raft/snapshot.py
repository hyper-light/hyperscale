"""
Raft snapshot support for log compaction and state transfer (Raft
section 7).

Provides:
- RaftSnapshot: the applied state at a log position, with the
  configuration in force there
- SnapshotManager: creates snapshots, applies them, compacts logs
- InstallSnapshot: leader-to-follower state transfer

A group that can snapshot its state machine compacts its log through
its applied entries (``RaftNode`` decides when); a member whose next
entry the leader no longer holds receives the snapshot via
InstallSnapshot instead of individual AppendEntries.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .logging_models import RaftDebug, RaftInfo, RaftWarning
from .models.messages import Message
from .models.raft_configuration import RaftConfiguration
from .install_snapshot import InstallSnapshot
from .install_snapshot_response import InstallSnapshotResponse
from .raft_snapshot import RaftSnapshot
from .snapshot_manager import SnapshotManager

_REHOMED = (
    InstallSnapshot,
    InstallSnapshotResponse,
    RaftSnapshot,
    SnapshotManager,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
