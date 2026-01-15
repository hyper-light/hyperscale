from __future__ import annotations

from enum import Enum, auto


class SLOComplianceLevel(Enum):
    """SLO compliance classification."""

    EXCEEDING = auto()
    MEETING = auto()
    WARNING = auto()
    VIOLATING = auto()
    CRITICAL = auto()
