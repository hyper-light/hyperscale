"""Serialized form produced by ``AuditEvent.to_dict``."""

from typing import TypedDict


class AuditEventRecord(TypedDict):
    """Fixed-shape dictionary form of one audit event."""

    type: str
    timestamp: float
    node: str | None
    details: dict[str, object]
