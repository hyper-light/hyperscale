"""Export shape produced by ``Metrics.to_dict``."""

from typing import TypedDict


class SwimMetricsSnapshot(TypedDict):
    """All SWIM protocol counters, grouped by subsystem."""

    uptime_seconds: float
    log_write_failures: int
    probes: dict[str, int]
    membership: dict[str, int]
    suspicions: dict[str, int]
    elections: dict[str, int]
    leadership: dict[str, int]
    errors: dict[str, int]
    gossip: dict[str, int]
    rate_limiting: dict[str, int]
