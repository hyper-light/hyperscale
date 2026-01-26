"""
Session consistency levels for AD-38 read operations.
"""

from enum import Enum


class ConsistencyLevel(Enum):
    """
    Read consistency level for job state queries.

    Trade-off between freshness and latency.
    """

    EVENTUAL = "eventual"
    SESSION = "session"
    BOUNDED_STALENESS = "bounded_staleness"
    STRONG = "strong"
