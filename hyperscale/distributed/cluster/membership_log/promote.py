"""
Promote — promote a learner to voter (AD-52 §7).

The leader proposes Promote when the learner's commit index is within
learner_promote_threshold of the leader's commit_index. If the promotion
changes the voting set, the apply layer triggers an EnterJoint /
LeaveJoint pair to perform the change under joint consensus.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import EntryMetadata


@dataclass(frozen=True, slots=True)
class Promote:
    node_id: str = ""
    metadata: EntryMetadata = field(default_factory=EntryMetadata)
