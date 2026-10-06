"""Serialized form of a ``JobStatsCRDT`` (``JobStatsCRDT.to_dict``)."""

from __future__ import annotations

from typing import TypedDict

from .lww_register_snapshot import LWWRegisterSnapshot


class JobStatsCRDTSnapshot(TypedDict):
    """Per-datacenter counters, rates and statuses of one job's stats CRDT."""

    job_id: str
    completed: dict[str, int]
    failed: dict[str, int]
    rates: dict[str, LWWRegisterSnapshot[float]]
    statuses: dict[str, LWWRegisterSnapshot[str]]
