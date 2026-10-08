"""Statistics shape shared by every SWIM piggyback gossip buffer's ``get_stats``."""

from typing import TypedDict


class GossipBufferStats(TypedDict):
    """Occupancy, eviction, and size-limit counters of a gossip buffer."""

    pending_updates: int
    total_evicted: int
    total_stale_removed: int
    size_limited_count: int
    oversized_updates: int
    overflow_events: int
    max_piggyback_size: int
    max_updates: int
