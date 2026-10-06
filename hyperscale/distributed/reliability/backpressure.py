"""
Backpressure for Stats Updates (AD-23).

Provides tiered retention for stats with automatic aggregation and
backpressure signaling based on buffer fill levels.

Retention Tiers:
- HOT: 0-60s, full resolution, ring buffer (max 1000 entries)
- WARM: 1-60min, 10s aggregates (max 360 entries)
- COLD: 1-24h, 1min aggregates (max 1440 entries)
- ARCHIVE: final summary only

Backpressure Levels:
- NONE: <70% fill, accept all
- THROTTLE: 70-85% fill, reduce frequency
- BATCH: 85-95% fill, batched updates only
- REJECT: >95% fill, reject non-critical

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Generic, TypeVar, Callable
from hyperscale.distributed.runtime import Clock, RealClock

from .stats_buffer import _DEFAULT_CLOCK
from .backpressure_level import BackpressureLevel
from .backpressure_signal import BackpressureSignal
from .stats_buffer import StatsBuffer
from .stats_buffer_config import StatsBufferConfig
from .stats_entry import StatsEntry

_REHOMED = (
    BackpressureLevel,
    StatsEntry,
    StatsBufferConfig,
    StatsBuffer,
    BackpressureSignal,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
