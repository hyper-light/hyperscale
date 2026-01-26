"""
Durability levels for AD-38 tiered commit pipeline.

Defines the three-tier durability model:
- LOCAL: Process crash recovery (<1ms)
- REGIONAL: Node failure within DC (2-10ms)
- GLOBAL: Region failure (50-300ms)
"""

from enum import Enum


class DurabilityLevel(Enum):
    """
    Durability level for job operations.

    Each level provides progressively stronger guarantees
    at the cost of higher latency.
    """

    LOCAL = "local"
    """
    Survives process crash only.
    Written to local WAL with fsync.
    Latency: <1ms
    Use case: High-throughput progress updates
    """

    REGIONAL = "regional"
    """
    Survives node failure within datacenter.
    Replicated to other nodes in DC.
    Latency: 2-10ms
    Use case: Workflow dispatch, workflow complete
    """

    GLOBAL = "global"
    """
    Survives region failure.
    Committed to global job ledger.
    Latency: 50-300ms
    Use case: Job create, cancel, complete
    """

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, DurabilityLevel):
            return NotImplemented
        order = [
            DurabilityLevel.LOCAL,
            DurabilityLevel.REGIONAL,
            DurabilityLevel.GLOBAL,
        ]
        return order.index(self) < order.index(other)

    def __le__(self, other: object) -> bool:
        if not isinstance(other, DurabilityLevel):
            return NotImplemented
        return self == other or self < other

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, DurabilityLevel):
            return NotImplemented
        return other < self

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, DurabilityLevel):
            return NotImplemented
        return self == other or self > other
