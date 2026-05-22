"""
LeaveJoint — complete a joint-consensus transition (AD-52 §6).

Once LeaveJoint commits, quorum reverts to a single majority of the new
configuration. The apply layer enforces that LeaveJoint always follows
an EnterJoint within the same membership_epoch range.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import EntryMetadata


@dataclass(frozen=True, slots=True)
class LeaveJoint:
    members: frozenset[str] = field(default_factory=frozenset)
    metadata: EntryMetadata = field(default_factory=EntryMetadata)
