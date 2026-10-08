"""
Manager stats module.

Handles windowed stats aggregation, backpressure signaling, and
throughput tracking per AD-19 and AD-23 specifications.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TYPE_CHECKING, Any
from hyperscale.distributed.reliability import (
    BackpressureLevel as StatsBackpressureLevel,
    BackpressureSignal,
    StatsBuffer,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning
from hyperscale.distributed.runtime import Clock

from .manager_stats_coordinator import SendFunc
from .backpressure_level import BackpressureLevel
from .manager_stats_coordinator import ManagerStatsCoordinator
from .progress_state import ProgressState

_REHOMED = (
    ProgressState,
    BackpressureLevel,
    ManagerStatsCoordinator,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
