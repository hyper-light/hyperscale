"""``RefreshRate`` lives here rather than in ``refresh_rate.py`` because
that module re-exports ``RefreshRateMap``, which needs this enum at class
definition time: defining both there would make the two modules import
each other."""

from enum import Enum


class RefreshRate(Enum):
    LOW = 15
    MEDIUM = 30
    HIGH = 60
    ULTRA = 120
