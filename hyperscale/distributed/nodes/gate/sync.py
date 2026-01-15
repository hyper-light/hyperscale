"""
Gate state synchronization module.

Provides state sync infrastructure for peer gate coordination.

Classes:
- VersionedStateClock: Per-datacenter version tracking using Lamport timestamps

This is re-exported from the server.events package.
"""

from hyperscale.distributed.server.events import VersionedStateClock

__all__ = [
    "VersionedStateClock",
]
