"""``ManagerVersionSkewHandler.get_version_metrics`` -- the manager's protocol version against its gates'."""

from __future__ import annotations

from typing import TypedDict


class ManagerVersionMetrics(TypedDict):
    """The local protocol version and feature count, the gates negotiated with, and how many
    gates run each protocol version."""

    local_version: str
    local_feature_count: int
    gate_count: int
    gate_versions: dict[str, int]
