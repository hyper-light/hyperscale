"""
Gossip and message dissemination for SWIM protocol.

Includes:
- PiggybackUpdate: Membership updates (alive/suspect/dead/join/leave)
- GossipBuffer: Membership gossip buffer with broadcast counting
- HealthGossipBuffer: Health state gossip buffer (Phase 6.1)
"""

from .piggyback_update import PiggybackUpdate

from .gossip_buffer import (
    GossipBuffer,
    MAX_PIGGYBACK_SIZE,
    MAX_UDP_PAYLOAD,
)

from .health_gossip_buffer import (
    HealthGossipBuffer,
    HealthGossipBufferConfig,
    HealthGossipEntry,
    OverloadSeverity,
    MAX_HEALTH_PIGGYBACK_SIZE,
)


__all__ = [
    # Membership gossip
    'PiggybackUpdate',
    'GossipBuffer',
    'MAX_PIGGYBACK_SIZE',
    'MAX_UDP_PAYLOAD',
    # Health gossip (Phase 6.1)
    'HealthGossipBuffer',
    'HealthGossipBufferConfig',
    'HealthGossipEntry',
    'OverloadSeverity',
    'MAX_HEALTH_PIGGYBACK_SIZE',
]

