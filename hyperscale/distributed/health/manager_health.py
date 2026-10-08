"""
Manager Health State (AD-19).

Three-signal health model for managers, monitored by gates.

Signals:
1. Liveness: Is the manager process alive and responsive?
2. Readiness: Can the manager accept new jobs? (has quorum, accepting, has workers)
3. Progress: Is work being dispatched at expected rate?

Routing decisions and DC health integration:
- route: All signals healthy, send jobs
- drain: Not ready but alive, stop new jobs
- investigate: Progress issues, check manager
- evict: Dead or stuck, remove from pool

DC Health Classification:
- ALL managers NOT liveness → DC = UNHEALTHY
- MAJORITY managers NOT readiness → DC = DEGRADED
- ANY manager progress == "stuck" → DC = DEGRADED

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.health.worker_health import ProgressState, RoutingDecision

from .manager_health_state import _DEFAULT_CLOCK
from .manager_health_config import ManagerHealthConfig
from .manager_health_state import ManagerHealthState

_REHOMED = (
    ManagerHealthConfig,
    ManagerHealthState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
