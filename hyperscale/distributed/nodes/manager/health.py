"""
Manager health module for worker health monitoring.

Handles SWIM callbacks, worker health tracking, AD-18 hybrid overload detection,
AD-26 deadline extensions, and AD-30 hierarchical failure detection with job-level suspicion.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from typing import TYPE_CHECKING, Any
from hyperscale.distributed.models import WorkerHeartbeat
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning
from hyperscale.distributed.runtime import Clock, RealClock

from .health_shared import _DEFAULT_CLOCK
from .job_suspicion import JobSuspicion
from .manager_health_monitor import ManagerHealthMonitor

_REHOMED = (
    JobSuspicion,
    ManagerHealthMonitor,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
