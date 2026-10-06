"""
Conflict-free Replicated Data Types (CRDTs) for cross-datacenter synchronization.

CRDTs allow coordination-free updates with guaranteed eventual consistency.
They are particularly useful for cross-DC stat aggregation where network
latency makes tight coordination prohibitively expensive.

See AD-14 in docs/architecture.md for design rationale.

This module is the wire namespace of the models below. Each lives in a
file of its own and is re-homed here -- its ``__module__`` set to this
module -- so its pickled form names this module, exactly as before the
split: mixed-version clusters keep talking and data written earlier
keeps loading.
"""

from __future__ import annotations

from .async_safe_job_stats_crdt import AsyncSafeJobStatsCRDT
from .g_counter import GCounter
from .job_stats_crdt import JobStatsCRDT
from .lww_map import LWWMap
from .lww_register import LWWRegister

_WIRE_MODELS = (
    GCounter,
    LWWRegister,
    LWWMap,
    JobStatsCRDT,
    AsyncSafeJobStatsCRDT,
)

for _wire_model in _WIRE_MODELS:
    _wire_model.__module__ = __name__
