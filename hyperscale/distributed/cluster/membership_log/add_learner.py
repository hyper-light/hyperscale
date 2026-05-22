"""
AddLearner — admit a non-voting catch-up member (AD-52 §6, §7).

Learners receive Raft log + snapshot but do not count toward quorum
until promoted via Promote. The promotion path is the only way a
learner becomes a voter.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import EntryMetadata


@dataclass(frozen=True, slots=True)
class AddLearner:
    node_id: str = ""
    role: str = ""
    advertised_address: tuple[str, int] = ("", 0)
    capabilities: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    learner_added_at_epoch: int = 0
    metadata: EntryMetadata = field(default_factory=EntryMetadata)
