"""
Schema versioning (AD-52 §14).

Centralizes the schema_version registry for the cluster module. Composed
with membership_log.schema_dispatch.SchemaDispatcher — this module
defines the supported (entry_type, schema_version) matrix for the
current release; the dispatcher is the mutable registry.

Per AD-52 §14:
  "Old schemas remain supported until a configurable retention (default:
   two major releases). Forward compatibility (unknown fields ignored)
   is per AD-25. ... This is the standard SOTA approach (etcd v3 schema,
   CockroachDB versioned migrations). Hyperscale piggybacks on AD-25's
   protocol versioning for transport-level compat and adds explicit
   Raft-log schema versioning for state-machine compat."
"""

from __future__ import annotations

from dataclasses import dataclass

from .membership_log.base import CURRENT_SCHEMA_VERSION


# Two-major-release retention window. When CURRENT_SCHEMA_VERSION rolls
# forward to N, we keep decoders for N-1 and N-2; older decoders may be
# removed.
RETENTION_BACK_VERSIONS: int = 2


@dataclass(frozen=True, slots=True)
class SchemaSupportWindow:
    """The schema versions this binary speaks for a given entry type."""

    entry_type: str
    write_version: int
    read_min_version: int
    read_max_version: int


def supported_window_for(entry_type: str) -> SchemaSupportWindow:
    """The default support window: write at CURRENT_SCHEMA_VERSION, read
    from (CURRENT - RETENTION_BACK_VERSIONS) through CURRENT inclusive."""
    return SchemaSupportWindow(
        entry_type=entry_type,
        write_version=CURRENT_SCHEMA_VERSION,
        read_min_version=max(1, CURRENT_SCHEMA_VERSION - RETENTION_BACK_VERSIONS),
        read_max_version=CURRENT_SCHEMA_VERSION,
    )


def is_supported_for_apply(entry_type: str, schema_version: int) -> bool:
    """True if the apply layer should accept the given (type, version);
    False means refuse to apply ("future schema we don't speak")."""
    window = supported_window_for(entry_type)
    return window.read_min_version <= schema_version <= window.read_max_version
