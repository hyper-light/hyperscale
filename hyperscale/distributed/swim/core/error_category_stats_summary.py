"""Per-category entry produced by ``ErrorHandler.get_stats_summary``."""

from typing import TypedDict


class ErrorCategoryStatsSummary(TypedDict):
    """Error counters and circuit state for one error category."""

    error_count: int
    error_rate: float
    circuit_state: str
