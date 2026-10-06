"""
Event Loop Health Monitor for proactive CPU saturation detection.

Detects event loop lag and system pressure before failures cascade.
Integrates with LHM to proactively adjust timeouts when the node
is under stress.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Awaitable
from collections import deque
from hyperscale.logging.hyperscale_logging_models import ServerDebug
from hyperscale.distributed.swim.core.protocols import LoggerProtocol, TaskRunnerProtocol
from hyperscale.distributed.runtime import Clock, RealClock

from .health_monitor_shared import _DEFAULT_CLOCK
from .event_loop_health_monitor import EventLoopHealthMonitor
from .health_sample import HealthSample


async def measure_event_loop_lag() -> float:
    """
    One-shot measurement of event loop lag.
    
    Returns:
        Lag ratio (0.0 = no lag, 1.0 = 100% lag)
    """
    expected = 0.01
    start = _DEFAULT_CLOCK.monotonic()
    await _DEFAULT_CLOCK.sleep(expected)
    actual = _DEFAULT_CLOCK.monotonic() - start
    return (actual - expected) / expected

_REHOMED = (
    HealthSample,
    EventLoopHealthMonitor,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
