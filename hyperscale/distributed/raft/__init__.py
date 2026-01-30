"""
Raft consensus module.

Provides per-job Raft consensus for leader election, log replication,
and deterministic state machine application across cluster nodes.
"""

from .raft_log import RaftLog
from .raft_node import RaftNode

__all__ = [
    "RaftLog",
    "RaftNode",
]
