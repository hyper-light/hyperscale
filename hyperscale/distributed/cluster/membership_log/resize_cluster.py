"""
ResizeCluster — operator-driven cluster size change (AD-52 §6, §18).

Hyperscale never auto-scales the control plane. ResizeCluster is the
explicit log entry that records an operator's --cluster-size change.
Apply rules:
  - Cannot reduce cluster_size below the current voter count.
  - Cannot increase cluster_size beyond a hard sanity ceiling (default
    25 — well above any reasonable Raft group).
  - Forces a subsequent joint-consensus transition because the quorum
    target moves.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import EntryMetadata


@dataclass(frozen=True, slots=True)
class ResizeCluster:
    new_cluster_size: int = 0
    metadata: EntryMetadata = field(default_factory=EntryMetadata)
