"""
Gossip and message dissemination for SWIM protocol.
"""

from .piggyback_update import PiggybackUpdate

from .gossip_buffer import (
    GossipBuffer,
    MAX_PIGGYBACK_SIZE,
    MAX_UDP_PAYLOAD,
)


__all__ = [
    'PiggybackUpdate',
    'GossipBuffer',
    'MAX_PIGGYBACK_SIZE',
    'MAX_UDP_PAYLOAD',
]

