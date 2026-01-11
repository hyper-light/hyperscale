"""
Gate statistics collection module.

Provides time-windowed stats collection for cross-DC aggregation.

Classes:
- WindowedStatsCollector: Cross-DC stats aggregation with drift tolerance
- WindowedStatsPush: Stats push message for client notification

These are re-exported from the jobs package.
"""

from hyperscale.distributed_rewrite.jobs import (
    WindowedStatsCollector,
    WindowedStatsPush,
)

__all__ = [
    "WindowedStatsCollector",
    "WindowedStatsPush",
]
