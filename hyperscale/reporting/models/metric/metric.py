from __future__ import annotations

import time
from typing import Generic, List, Optional, TypeVarTuple

from .metric_types import (
    COUNT,
    DISTRIBUTION,
    RATE,
    SAMPLE,
    TIMING,
)

T = TypeVarTuple("T")


class Metric(Generic[*T]):
    __slots__ = (
        "value",
        "tags",
        "precision",
        "metric_type",
        "timestamp",
    )

    def __init__(
        self,
        value: int | float | Metric,
        metric_type: COUNT | DISTRIBUTION | RATE | SAMPLE| TIMING,
        precision: Optional[int] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        if tags is None:
            tags = []

        if isinstance(value, Metric):
            value = value.value

        self.value: int | float = value

        self.tags: List[str] = tags
        self.precision = precision
        self.metric_type = metric_type
        self.timestamp = time.monotonic()
