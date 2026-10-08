"""``ProgressState`` -- pickled under the namespace
``hyperscale.distributed.nodes.manager.stats`` (see that module)."""

from enum import Enum


class ProgressState(Enum):
    """
    Progress state for AD-19 Three-Signal Health Model.

    Tracks dispatch throughput relative to expected capacity.
    """

    NORMAL = "normal"  # >= 80% of expected throughput
    SLOW = "slow"  # 50-80% of expected throughput
    DEGRADED = "degraded"  # 20-50% of expected throughput
    STUCK = "stuck"  # < 20% of expected throughput
