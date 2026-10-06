"""
Worker Health State (AD-19).

Three-signal health model for workers, monitored by managers.

Signals:
1. Liveness: Is the worker process alive and responsive?
2. Readiness: Can the worker accept new work?
3. Progress: Is work completing at expected rate?

Routing decisions based on combined signals:
- route: All signals healthy, send work
- drain: Not ready but alive, stop new work
- investigate: Progress issues, check worker
- evict: Dead or stuck, remove from pool

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from enum import Enum
from hyperscale.distributed.runtime import Clock, RealClock

from .worker_health_state import _DEFAULT_CLOCK
from .progress_state import ProgressState
from .routing_decision import RoutingDecision
from .worker_health_config import WorkerHealthConfig
from .worker_health_state import WorkerHealthState

_REHOMED = (
    ProgressState,
    RoutingDecision,
    WorkerHealthConfig,
    WorkerHealthState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
