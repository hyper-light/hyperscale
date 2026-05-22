"""
RegisterDatacenter — gate-cluster entry recording a manager DC catalog
record (AD-52 §17).

Emitted exclusively into the gate cluster's Raft log when a per-DC
manager cluster's leader completes its own bootstrap and federates with
the gate tier. Older registrations for the same dc_id are not
overwritten — the catalog is generation-aware (different cluster_uuid_of_dc
= new generation).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import EntryMetadata


@dataclass(frozen=True, slots=True)
class RegisterDatacenter:
    dc_id: str = ""
    region_code: str = ""
    cluster_uuid_of_dc: str = ""
    manager_seeds: tuple[str, ...] = field(default_factory=tuple)
    advertised_endpoint: tuple[str, int] = ("", 0)
    registered_at_gate_epoch: int = 0
    metadata: EntryMetadata = field(default_factory=EntryMetadata)
