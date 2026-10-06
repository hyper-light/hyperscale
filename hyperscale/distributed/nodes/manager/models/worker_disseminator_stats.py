"""``WorkerDisseminator.get_stats`` -- the manager's worker-state dissemination counters."""

from __future__ import annotations

from typing import TypedDict

from hyperscale.distributed.swim.gossip.gossip_buffer_stats import GossipBufferStats


class WorkerDisseminatorStats(TypedDict):
    """Worker incarnations tracked and the gossip buffer's own counters."""

    tracked_worker_incarnations: int
    gossip_buffer_stats: GossipBufferStats
