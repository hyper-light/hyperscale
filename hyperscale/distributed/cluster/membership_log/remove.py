"""
Remove — evict a member from the cluster (AD-52 §6, §13).

Three reasons distinguish operator-initiated vs detector-initiated
removal. "leave" is a voluntary drain-then-leave path (AD-52 §13);
"evict" is a tombstone-confirmed SWIM dead transition (AD-52 §8);
"force" is the operator escape hatch that skips the tombstone window
(AD-52 §13).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .base import EntryMetadata


class RemoveReason(str, Enum):
    LEAVE = "leave"
    EVICT = "evict"
    FORCE = "force"


@dataclass(frozen=True, slots=True)
class Remove:
    node_id: str = ""
    reason: RemoveReason = RemoveReason.EVICT
    metadata: EntryMetadata = field(default_factory=EntryMetadata)
