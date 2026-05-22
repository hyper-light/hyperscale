"""
EnterJoint — begin a joint-consensus transition (AD-52 §6).

Quorum during the joint window requires majorities of BOTH old_members
AND new_members. Eliminates the single-step membership-change race that
admits minority-partition split-brain.

Carries cluster_uuid_at_create on the bootstrap entry so the very first
EnterJoint records the cluster UUID minted at that moment. On all
subsequent EnterJoints this field equals the existing ClusterMetadata
cluster_uuid (the apply layer rejects mismatches).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import EntryMetadata


@dataclass(frozen=True, slots=True)
class EnterJoint:
    old_members: frozenset[str] = field(default_factory=frozenset)
    new_members: frozenset[str] = field(default_factory=frozenset)
    cluster_uuid_at_create: str = ""
    metadata: EntryMetadata = field(default_factory=EntryMetadata)
