"""``HealthGossipBufferConfig`` -- pickled under the namespace
``hyperscale.distributed.swim.gossip.health_gossip_buffer`` (see that module)."""

from dataclasses import dataclass

# Maximum size for health piggyback section (leaves room for membership gossip)
MAX_HEALTH_PIGGYBACK_SIZE = 600  # bytes


@dataclass(slots=True)
class HealthGossipBufferConfig:
    """Configuration for HealthGossipBuffer."""

    # Maximum entries in the buffer
    max_entries: int = 500

    # Staleness threshold - entries older than this are removed
    stale_age_seconds: float = 30.0

    # Maximum bytes for health piggyback data
    max_piggyback_size: int = MAX_HEALTH_PIGGYBACK_SIZE

    # Broadcast multiplier (lower than membership since health is best-effort)
    broadcast_multiplier: int = 2

    # Minimum broadcasts for healthy nodes (they're less urgent)
    min_broadcasts_healthy: int = 3

    # Minimum broadcasts for overloaded nodes (propagate faster)
    min_broadcasts_overloaded: int = 8
