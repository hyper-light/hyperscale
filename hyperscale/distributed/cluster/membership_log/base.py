"""
EntryMetadata — the common (schema_version, committed_at_term,
committed_at_epoch) carried on every membership log entry per AD-52 §6.

The metadata is composed into each entry rather than inherited from a
shared base class (AD-1: composition over inheritance, and frozen
dataclass inheritance with __slots__ is brittle).
"""

from __future__ import annotations

from dataclasses import dataclass


CURRENT_SCHEMA_VERSION: int = 1


@dataclass(frozen=True, slots=True)
class EntryMetadata:
    """
    Fields:
        schema_version       AD-52 §14 schema version of this entry's
                             payload. Apply-layer dispatch uses
                             (entry_type, schema_version) to pick the
                             right decoder.
        committed_at_term    Raft term in which this entry was committed.
                             Filled in by the apply layer; 0 for not-yet-
                             committed proposals.
        committed_at_epoch   Membership epoch after applying this entry.
                             Filled in by the apply layer.
    """

    schema_version: int = CURRENT_SCHEMA_VERSION
    committed_at_term: int = 0
    committed_at_epoch: int = 0
