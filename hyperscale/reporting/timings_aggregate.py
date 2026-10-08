from collections import Counter, defaultdict
from typing import Dict, List


class TimingsAggregate:
    """
    A TEST step's results, reduced as each arrives to what the step's result
    set is built from: every timing's values and the success, status, and
    context counts. A run keeps these values instead of the results.
    """

    __slots__ = (
        "timing_types",
        "timing_values",
        "successes",
        "statuses",
        "result_contexts",
        "error_contexts",
        "errors",
    )

    def __init__(self) -> None:
        # Set by the step's first timed result (a custom result names its own).
        self.timing_types: List[str] | None = None
        self.timing_values: Dict[str, List[int | float]] = defaultdict(list)
        self.successes: Counter[bool] = Counter()
        self.statuses: Counter[int] = Counter()
        self.result_contexts: Counter[str] = Counter()
        self.error_contexts: Counter[str] = Counter()
        self.errors = 0
