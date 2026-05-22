"""
UpdateMetadata — update non-identity metadata on a member (AD-52 §6).

Used for drain status (AD-52 §13), AD-37 role-aware bracket settings,
AD-25 capability bumps, etc. metadata_delta is a sorted tuple of
(key, value) pairs that overlay onto the existing capabilities dict.
A None value in metadata_delta removes the key.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import EntryMetadata


@dataclass(frozen=True, slots=True)
class UpdateMetadata:
    node_id: str = ""
    # Sorted (key, value | None) pairs — None values delete keys. The
    # apply layer iterates this in order so the result is deterministic
    # across followers (AD-52 §15).
    metadata_delta: tuple[tuple[str, str | None], ...] = field(default_factory=tuple)
    metadata: EntryMetadata = field(default_factory=EntryMetadata)
