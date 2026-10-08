"""Statistics shape produced by ``AuditLog.get_stats``."""

from typing import TypedDict


class AuditLogStats(TypedDict):
    """Occupancy and per-type counts of the bounded audit log."""

    current_events: int
    max_events: int
    total_recorded: int
    events_dropped: int
    event_counts: dict[str, int]
